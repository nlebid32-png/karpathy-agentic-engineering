"""Tests for diversity check / attention collapse detector. Run: pytest test_diversity_check.py"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from diversity_check import (
    COLLAPSE_THRESHOLD,
    DiversityResult,
    _line_jaccard,
    _word_jaccard,
    check_diversity,
    check_diversity_from_log,
)


# --- Unit tests for similarity metrics ---

def test_word_jaccard_identical_strings():
    assert _word_jaccard("hello world", "hello world") == 1.0


def test_word_jaccard_completely_different():
    score = _word_jaccard("apple banana cherry", "dog elephant fox")
    assert score == 0.0


def test_word_jaccard_partial_overlap():
    score = _word_jaccard("a b c d", "c d e f")
    # intersection = {c, d}, union = {a, b, c, d, e, f} → 2/6
    assert abs(score - 2 / 6) < 0.01


def test_line_jaccard_identical_code():
    code = "def foo():\n    return 1\n"
    assert _line_jaccard(code, code) == 1.0


def test_line_jaccard_empty_strings():
    assert _line_jaccard("", "") == 1.0


def test_line_jaccard_one_empty():
    score = _line_jaccard("def foo(): pass", "")
    assert score == 0.0


# --- check_diversity tests ---

def test_no_prior_attempts_returns_go():
    result = check_diversity("def foo(): pass", [])
    assert result.verdict == "GO"
    assert result.score == 0.0


def test_identical_candidate_returns_stop():
    code = "def sort(items):\n    return sorted(items)\n"
    result = check_diversity(code, [code])
    assert result.verdict == "STOP"
    assert result.score > COLLAPSE_THRESHOLD


def test_completely_different_candidate_returns_go():
    prior = "def bubble_sort(arr):\n    for i in range(len(arr)):\n        pass\n"
    candidate = "import heapq\ndef heap_sort(data):\n    heapq.heapify(data)\n    return [heapq.heappop(data) for _ in data]\n"
    result = check_diversity(candidate, [prior])
    assert result.verdict == "GO"


def test_result_contains_most_similar_attempt():
    prior_a = "def foo(): return 1"
    prior_b = "def foo(): return 2"  # closer to candidate
    candidate = "def foo(): return 2 + 0"
    result = check_diversity(candidate, [prior_a, prior_b])
    assert result.most_similar_attempt == prior_b


def test_result_is_dataclass_with_all_fields():
    result = check_diversity("x = 1", ["x = 1"])
    assert isinstance(result.score, float)
    assert isinstance(result.word_similarity, float)
    assert isinstance(result.line_similarity, float)
    assert result.verdict in ("GO", "STOP")
    assert len(result.explanation) > 0


def test_score_is_max_of_word_and_line():
    candidate = "def foo():\n    return sorted(items)\n"
    prior = "def bar():\n    return sorted(items)\n"
    result = check_diversity(candidate, [prior])
    assert result.score == max(result.word_similarity, result.line_similarity)


def test_collapse_threshold_is_above_half():
    # Threshold must be strict enough to avoid false positives on related code
    assert COLLAPSE_THRESHOLD >= 0.5
    assert COLLAPSE_THRESHOLD < 1.0


# --- check_diversity_from_log tests ---

def test_from_log_handles_missing_file(tmp_path):
    result = check_diversity_from_log("def foo(): pass", tmp_path / "missing.jsonl")
    assert result.verdict == "GO"
    assert result.score == 0.0


def test_from_log_reads_code_patches(tmp_path):
    log_file = tmp_path / "loop_log.jsonl"
    entry = {"iteration": 0, "metric": 0.5, "code_patch": "def foo(): return 1"}
    log_file.write_text(json.dumps(entry) + "\n", encoding="utf-8")

    result = check_diversity_from_log("def foo(): return 1", log_file, last_n=3)
    assert result.verdict == "STOP"


def test_from_log_skips_entries_without_code_patch(tmp_path):
    log_file = tmp_path / "loop_log.jsonl"
    # Entry with no code_patch field
    entry = {"iteration": 0, "metric": 0.5}
    log_file.write_text(json.dumps(entry) + "\n", encoding="utf-8")

    result = check_diversity_from_log("def foo(): pass", log_file, last_n=3)
    assert result.verdict == "GO"  # No patches found → no comparison
