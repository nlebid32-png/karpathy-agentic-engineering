"""
Diversity Score Utility — Attention Collapse Detector
Source: nanochat training heuristics; llm.c (loss spike = unstable geometric landscape)
Judge Finding #2 from session_log.md

THE PROBLEM (from Karpathy's Diagnostic Heuristic Prompting, Technique 6):
When an agent is stuck in attention collapse, it repeatedly generates structurally
similar patches. Asking it to "try something different" is insufficient because
the context window is already biased toward the failed pattern.

THIS TOOL:
Computes similarity between a candidate patch and a set of prior failed attempts.
Returns a float 0.0–1.0 and a GO/STOP signal.
- Score > COLLAPSE_THRESHOLD: STOP — likely attention collapse, escalate to council
- Score <= COLLAPSE_THRESHOLD: GO — candidate is sufficiently novel, proceed

Two similarity metrics are computed and the maximum is used (conservative):
1. Word-level Jaccard: set overlap on whitespace-tokenized words
2. Line-level Jaccard: set overlap on non-empty lines (better for code structure)

No external dependencies required.

Usage:
    from diversity_check import check_diversity, DiversityResult

    result = check_diversity(
        candidate="def sort(items): return sorted(items)",
        prior_attempts=["def sort(arr): return sorted(arr)", "def sort(x): return x.sort()"],
    )
    if result.verdict == "STOP":
        print(f"Attention collapse detected (score={result.score:.2f}). Discard approach.")
    else:
        print(f"Novel enough (score={result.score:.2f}). Proceed.")
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

COLLAPSE_THRESHOLD = 0.75  # score above this = STOP signal


@dataclass
class DiversityResult:
    score: float                   # 0.0 (completely different) to 1.0 (identical)
    word_similarity: float
    line_similarity: float
    most_similar_attempt: str      # which prior attempt is closest
    verdict: str                   # "GO" or "STOP"
    explanation: str


def _word_jaccard(text_a: str, text_b: str) -> float:
    """Jaccard similarity on whitespace-separated word tokens."""
    tokens_a = set(re.split(r"\s+", text_a.strip().lower()))
    tokens_b = set(re.split(r"\s+", text_b.strip().lower()))
    tokens_a.discard("")
    tokens_b.discard("")
    if not tokens_a and not tokens_b:
        return 1.0
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def _line_jaccard(text_a: str, text_b: str) -> float:
    """Jaccard similarity on non-empty lines (captures code structure well)."""
    lines_a = {line.strip() for line in text_a.splitlines() if line.strip()}
    lines_b = {line.strip() for line in text_b.splitlines() if line.strip()}
    if not lines_a and not lines_b:
        return 1.0
    if not lines_a or not lines_b:
        return 0.0
    return len(lines_a & lines_b) / len(lines_a | lines_b)


def _pairwise_similarity(candidate: str, prior: str) -> tuple[float, float]:
    """Returns (word_sim, line_sim) between candidate and one prior attempt."""
    return _word_jaccard(candidate, prior), _line_jaccard(candidate, prior)


def check_diversity(candidate: str, prior_attempts: list[str]) -> DiversityResult:
    """
    Check whether a candidate patch is sufficiently different from prior failed attempts.

    Args:
        candidate: The new patch/code to evaluate.
        prior_attempts: List of prior failed code strings.

    Returns:
        DiversityResult with score, verdict ("GO" or "STOP"), and explanation.

    Pass condition: verdict == "GO" means proceed; "STOP" means attention collapse likely.
    """
    if not prior_attempts:
        return DiversityResult(
            score=0.0,
            word_similarity=0.0,
            line_similarity=0.0,
            most_similar_attempt="",
            verdict="GO",
            explanation="No prior attempts to compare against. Proceeding.",
        )

    best_word = 0.0
    best_line = 0.0
    most_similar = ""

    for prior in prior_attempts:
        word_sim, line_sim = _pairwise_similarity(candidate, prior)
        combined = max(word_sim, line_sim)
        if combined > max(best_word, best_line):
            best_word = word_sim
            best_line = line_sim
            most_similar = prior

    score = max(best_word, best_line)

    if score > COLLAPSE_THRESHOLD:
        verdict = "STOP"
        explanation = (
            f"Similarity score {score:.2f} exceeds collapse threshold {COLLAPSE_THRESHOLD}. "
            f"Candidate is structurally similar to a prior failed attempt. "
            f"Likely attention collapse. Discard approach, return to first principles."
        )
    else:
        verdict = "GO"
        explanation = (
            f"Similarity score {score:.2f} is below threshold {COLLAPSE_THRESHOLD}. "
            f"Candidate is sufficiently novel. Proceeding."
        )

    return DiversityResult(
        score=score,
        word_similarity=best_word,
        line_similarity=best_line,
        most_similar_attempt=most_similar,
        verdict=verdict,
        explanation=explanation,
    )


def check_diversity_from_log(candidate: str, log_path: Path, last_n: int = 3) -> DiversityResult:
    """
    Convenience wrapper: reads prior code_patch entries from loop_log.jsonl.
    Falls back gracefully if log doesn't exist or has no patch entries.
    """
    if not log_path.exists():
        return check_diversity(candidate, [])

    patches: list[str] = []
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    for line in reversed(lines):
        if len(patches) >= last_n:
            break
        try:
            entry = json.loads(line)
            if "code_patch" in entry and entry["code_patch"]:
                patches.append(entry["code_patch"])
        except (json.JSONDecodeError, KeyError):
            continue

    return check_diversity(candidate, patches)


if __name__ == "__main__":
    # Quick demo
    failed_attempt_1 = """
def sort_items(items):
    return sorted(items, key=lambda x: x.value)
"""
    failed_attempt_2 = """
def sort_items(items):
    result = sorted(items, key=lambda x: x.value)
    return result
"""
    # Structurally identical candidate — should STOP
    candidate_collapse = """
def sort_items(items):
    sorted_items = sorted(items, key=lambda x: x.value)
    return sorted_items
"""
    # Genuinely different candidate — should GO
    candidate_novel = """
def sort_items(items):
    heap = []
    for item in items:
        heapq.heappush(heap, (item.value, item))
    return [heapq.heappop(heap)[1] for _ in range(len(heap))]
"""
    print("--- Collapse check ---")
    r1 = check_diversity(candidate_collapse, [failed_attempt_1, failed_attempt_2])
    print(f"Verdict: {r1.verdict}  Score: {r1.score:.3f}")
    print(r1.explanation)

    print("\n--- Novel check ---")
    r2 = check_diversity(candidate_novel, [failed_attempt_1, failed_attempt_2])
    print(f"Verdict: {r2.verdict}  Score: {r2.score:.3f}")
    print(r2.explanation)
