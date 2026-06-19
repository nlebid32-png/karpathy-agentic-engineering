"""Tests for the council-panel judge. Run: pytest test_council_judge.py"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import council_judge as cj


def _cands():
    return [
        cj.Candidate("agent-1", "added caching net_lines: 46 improved 8%"),
        cj.Candidate("agent-2", "hash map test edge case validate net_lines: 11 improved 21%"),
        cj.Candidate("agent-3", "guards error handle net_lines: 30 3% improvement"),
    ]


def test_panel_has_three_disjoint_judges():
    keys = [a["key"] for a in cj.JUDGE_PANEL]
    assert keys == ["correctness", "simplicity", "effectiveness"]
    assert len(set(keys)) == 3


def test_anonymize_strips_identity_and_is_reversible():
    cands = _cands()
    anon, label_map = cj.anonymize(cands, shuffle=False)
    # judges see Candidate A/B/C labels, never agent-N
    for rec in anon:
        assert rec["label"] in ("A", "B", "C")
        assert "agent-" not in rec["label"]
    # the map reverses labels back to real agents
    assert set(label_map.values()) == {"agent-1", "agent-2", "agent-3"}


def test_anonymize_shuffle_decouples_label_from_agent_order():
    cands = _cands()
    _, stable = cj.anonymize(cands, shuffle=False)
    _, shuffled = cj.anonymize(cands, shuffle=True)
    # at least one label maps to a different agent once shuffled (content-derived offset)
    assert stable != shuffled or len(cands) == 1


def test_heuristic_axes_are_orthogonal_signals():
    # correctness rewards tests/guards; simplicity rewards small diffs; they disagree
    correct_heavy = "test edge case validate guard assert error handle net_lines: 200"
    simple_heavy = "net_lines: 2 one-line change"
    assert cj.heuristic_score(correct_heavy, "correctness") > cj.heuristic_score(simple_heavy, "correctness")
    assert cj.heuristic_score(simple_heavy, "simplicity") > cj.heuristic_score(correct_heavy, "simplicity")


def test_judge_ranks_and_picks_winner():
    v = cj.judge(_cands(), task="cut latency")
    assert v["winner"] in {"agent-1", "agent-2", "agent-3"}
    assert v["ranked"][0]["rank"] == 1
    assert v["ranked"][0]["agent"] == v["winner"]
    # every candidate scored on all three axes
    for r in v["ranked"]:
        assert set(r["axes"].keys()) == {"correctness", "simplicity", "effectiveness"}


def test_judge_winner_has_highest_panel_score():
    v = cj.judge(_cands(), task="cut latency")
    scores = [r["panel_score"] for r in v["ranked"]]
    assert scores == sorted(scores, reverse=True)
    assert v["winner_panel_score"] == max(scores)


def test_judge_accepts_pluggable_score_fn():
    # a stub judge that always favors agent-3 via its summary content
    def stub(summary, axis_key, task=""):
        return 9.0 if "guards" in summary else 1.0
    v = cj.judge(_cands(), score_fn=stub, task="x")
    assert v["winner"] == "agent-3"
    assert v["scorer"] == "llm"  # non-heuristic fn => reported as llm-path


def test_empty_candidates_returns_no_winner():
    v = cj.judge([], task="x")
    assert v["winner"] is None
    assert v["ranked"] == []


def test_scorer_label_is_heuristic_by_default():
    v = cj.judge(_cands())
    assert v["scorer"] == "heuristic"


def test_demo_set_is_judgeable():
    v = cj.judge(cj.DEMO, task="cut p50 latency")
    assert v["winner"] is not None
    assert len(v["ranked"]) == 3
