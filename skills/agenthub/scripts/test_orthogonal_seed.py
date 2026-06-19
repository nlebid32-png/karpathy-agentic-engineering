"""Tests for the orthogonal strategy seeder. Run: pytest test_orthogonal_seed.py"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import orthogonal_seed as ous


def test_seed_returns_n_strategies():
    s = ous.seed("optimization", 3)
    assert len(s) == 3
    assert [x["agent"] for x in s] == [1, 2, 3]


def test_seeded_strategies_are_distinct():
    s = ous.seed("optimization", 5)
    names = [x["name"] for x in s]
    assert len(set(names)) == 5  # all five lenses distinct


def test_seed_wraps_with_variant_when_n_exceeds_pool():
    pool_size = len(ous.STRATEGY_SETS["debugging"])
    s = ous.seed("debugging", pool_size + 1)
    assert len(s) == pool_size + 1
    # the wrapped agent reuses a base lens but is nudged to a variant
    assert "Variant" in s[pool_size]["constraint"]
    assert s[pool_size]["name"] == s[0]["name"]


def test_unknown_domain_raises():
    with pytest.raises(ValueError):
        ous.seed("nonsense-domain", 2)


def test_zero_agents_raises():
    with pytest.raises(ValueError):
        ous.seed("general", 0)


def test_all_domains_have_at_least_five_strategies():
    for domain, strategies in ous.STRATEGY_SETS.items():
        assert len(strategies) >= 5, f"{domain} has < 5 strategies"
        # every strategy has a name + a constraint
        for s in strategies:
            assert s["name"] and s["constraint"]


def test_orthogonality_report_passes_for_disjoint_set():
    s = ous.seed("optimization", 3)
    report = ous.orthogonality_report(s)
    # curated strategies are deliberately disjoint → should not warn
    assert report["max_pair_similarity"] <= 0.75
    assert report["verdict"].startswith("GO")
    assert report["agent_count"] == 3


def test_orthogonality_report_flags_correlated_set():
    # hand-build a deliberately correlated set to prove the guard fires
    correlated = [
        {"name": "A", "dispatch_strategy": "add caching to reduce latency with cache results"},
        {"name": "B", "dispatch_strategy": "add caching to reduce latency with cache results now"},
    ]
    report = ous.orthogonality_report(correlated)
    assert report["max_pair_similarity"] > 0.75
    assert report["verdict"].startswith("WARN")


def test_dispatch_strategy_is_template_ready():
    s = ous.seed("copywriting", 2)
    for x in s:
        assert x["name"] in x["dispatch_strategy"]
        assert len(x["dispatch_strategy"]) > 20
