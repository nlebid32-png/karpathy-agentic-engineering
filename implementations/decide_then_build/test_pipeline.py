"""Tests for the decide-then-build pipeline. Run: pytest test_pipeline.py"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import pipeline as pl


# ── verdict extraction ────────────────────────────────────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("GO. Strong opportunity, proceed.", "GO"),
    ("Verdict: NO-GO. Kill this.", "NO-GO"),
    ("CONDITIONAL — proceed with caution on the data rights.", "CONDITIONAL"),
    ("We should PROCEED with the plan.", "GO"),
    ("This is a hard pass; REJECT.", "NO-GO"),
    ("...ambiguous waffle...", "CONDITIONAL"),
])
def test_extract_verdict(text, expected):
    assert pl.extract_verdict(text) == expected


# ── gating ────────────────────────────────────────────────────────────────────

def test_nogo_stops_before_spawning():
    out = pl.decide_then_build(
        "build a thing", domain="optimization", n_agents=4,
        council_fn=lambda t: {"verdict": "NO-GO", "synthesis": "NO-GO. Bad idea."},
    )
    assert out["stopped_at"] == "decide"
    assert out["strategies"] is None
    assert out["spawn_plan"] is None
    assert out["winner"] is None


def test_go_without_results_awaits():
    out = pl.decide_then_build(
        "optimize search", domain="optimization", n_agents=3,
        council_fn=lambda t: {"verdict": "GO", "synthesis": "GO."},
    )
    assert out["stopped_at"] == "awaiting_results"
    assert len(out["strategies"]) == 3
    assert len(out["spawn_plan"]) == 3
    assert out["winner"] is None


def test_conditional_still_proceeds_to_plan():
    out = pl.decide_then_build(
        "optimize search", domain="optimization", n_agents=2,
        council_fn=lambda t: {"verdict": "CONDITIONAL", "synthesis": "CONDITIONAL."},
    )
    # conditional is not a veto — plan is produced, surfaced for human judgment
    assert out["decision"]["verdict"] == "CONDITIONAL"
    assert out["spawn_plan"] is not None


def test_full_flow_with_results_picks_winner():
    cands = [
        pl.Candidate("agent-1", "caching net_lines: 46 improved 8%"),
        pl.Candidate("agent-2", "hash map test edge case validate net_lines: 11 improved 21%"),
    ]
    out = pl.decide_then_build(
        "optimize search", domain="optimization", n_agents=2,
        council_fn=lambda t: {"verdict": "GO", "synthesis": "GO."},
        results_fn=lambda: cands,
    )
    assert out["stopped_at"] == "complete"
    assert out["winner"]["winner"] in {"agent-1", "agent-2"}


# ── seeding + plan integrity ──────────────────────────────────────────────────

def test_strategies_are_orthogonal_in_pipeline():
    out = pl.decide_then_build(
        "optimize search", domain="optimization", n_agents=3,
        council_fn=lambda t: {"verdict": "GO", "synthesis": "GO."},
    )
    assert out["orthogonality"]["max_pair_similarity"] <= 0.75
    names = [s["name"] for s in out["strategies"]]
    assert len(set(names)) == 3


def test_spawn_plan_prompts_are_complete():
    out = pl.decide_then_build(
        "cut latency", domain="optimization", n_agents=2,
        council_fn=lambda t: {"verdict": "GO", "synthesis": "GO. caching + algo."},
    )
    for p in out["spawn_plan"]:
        assert "cut latency" in p["dispatch_prompt"]          # task threaded through
        assert p["strategy"] in p["dispatch_prompt"]          # strategy threaded through
        assert "result.md" in p["dispatch_prompt"]            # result contract present


def test_demo_runs_end_to_end_offline():
    out = pl._demo()
    assert out["stopped_at"] == "complete"
    assert out["decision"]["verdict"] == "GO"
    assert out["winner"]["winner"] is not None
    assert len(out["spawn_plan"]) == 3
