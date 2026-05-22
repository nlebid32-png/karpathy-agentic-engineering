"""
Tests for LLM Council harness (unit tests — no API calls required).
Run: pytest test_council.py

Integration test (requires ANTHROPIC_API_KEY):
    python council.py "your question here"
"""
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from council import (
    ADVISOR_PERSONAS,
    CHAIRMAN_SYSTEM,
    PEER_REVIEW_SYSTEM,
    CouncilVerdict,
    _anonymize_analyses,
)


def test_exactly_five_advisors():
    assert len(ADVISOR_PERSONAS) == 5


def test_advisor_names_are_unique():
    names = [p["name"] for p in ADVISOR_PERSONAS]
    assert len(names) == len(set(names)), "All advisor names must be unique"


def test_all_advisors_have_substantive_system_prompts():
    for persona in ADVISOR_PERSONAS:
        assert len(persona["system"]) >= 80, (
            f"Advisor '{persona['name']}' system prompt is too short to enforce a disjoint persona"
        )


def test_advisor_personas_have_disjoint_primary_objectives():
    # Each persona must forbid what the others permit — check for unique key constraints
    systems = [p["system"].lower() for p in ADVISOR_PERSONAS]
    # Contrarian must forbid balance; Expansionist must ignore risks
    contrarian = next(s for s in systems if "contrarian" in ADVISOR_PERSONAS[systems.index(s)]["name"].lower())
    expansionist = next(s for s in systems if "expansionist" in ADVISOR_PERSONAS[systems.index(s)]["name"].lower())
    assert "forbidden" in contrarian or "strictly" in contrarian
    assert "ignore" in expansionist or "not your concern" in expansionist


def test_chairman_system_prompt_requires_go_nogo():
    assert "GO" in CHAIRMAN_SYSTEM
    assert "NO-GO" in CHAIRMAN_SYSTEM
    assert "CONDITIONAL-GO" in CHAIRMAN_SYSTEM


def test_peer_review_system_is_adversarial():
    assert "flattery" in PEER_REVIEW_SYSTEM.lower() or "no flattery" in PEER_REVIEW_SYSTEM.lower()


def test_council_verdict_serializes_to_valid_json():
    verdict = CouncilVerdict(
        query="Should we rewrite in Rust?",
        advisor_analyses={"Contrarian": "fatal: memory unsafe by default", "Expansionist": "10x perf"},
        peer_reviews={"Contrarian_reviews_Expansionist": "missed compile times"},
        chairman_synthesis="1. CRITICAL RISKS: rewrite cost\n6. FINAL VERDICT: NO-GO",
    )
    parsed = json.loads(verdict.to_json())
    assert parsed["query"] == "Should we rewrite in Rust?"
    assert "chairman_synthesis" in parsed
    assert "advisor_analyses" in parsed
    assert "peer_reviews" in parsed


def test_run_council_raises_on_missing_api_key():
    from council import run_council
    original = os.environ.pop("ANTHROPIC_API_KEY", None)
    try:
        with pytest.raises(EnvironmentError, match="ANTHROPIC_API_KEY"):
            run_council("test query")
    finally:
        if original is not None:
            os.environ["ANTHROPIC_API_KEY"] = original


def test_verdict_dataclass_fields_complete():
    import dataclasses
    fields = {f.name for f in dataclasses.fields(CouncilVerdict)}
    assert fields == {"query", "advisor_analyses", "peer_reviews", "chairman_synthesis"}


def test_anonymize_analyses_removes_advisor_names():
    analyses = {
        "Contrarian": "fatal flaw here",
        "Expansionist": "scale opportunity here",
        "SecurityParanoid": "attack vector here",
    }
    anonymized, reveal_map = _anonymize_analyses(analyses)
    # Opaque IDs must not contain original advisor names
    for opaque_id in anonymized:
        assert opaque_id not in analyses, f"Opaque ID '{opaque_id}' leaks an advisor name"
    # All original content must be preserved (values unchanged)
    assert set(anonymized.values()) == set(analyses.values())


def test_anonymize_analyses_reveal_map_is_complete():
    analyses = {"Alpha": "text_a", "Beta": "text_b", "Gamma": "text_c"}
    anonymized, reveal_map = _anonymize_analyses(analyses)
    # reveal_map must reconstruct all original advisor names
    assert set(reveal_map.values()) == set(analyses.keys())
    assert set(reveal_map.keys()) == set(anonymized.keys())


def test_anonymize_analyses_uses_voice_prefix():
    analyses = {"Advisor1": "a", "Advisor2": "b"}
    anonymized, _ = _anonymize_analyses(analyses)
    for key in anonymized:
        assert key.startswith("Voice_"), f"Expected 'Voice_X' prefix, got '{key}'"


def test_anonymize_analyses_count_preserved():
    analyses = {f"Advisor_{i}": f"text_{i}" for i in range(5)}
    anonymized, reveal_map = _anonymize_analyses(analyses)
    assert len(anonymized) == 5
    assert len(reveal_map) == 5
