#!/usr/bin/env python3
"""
Council-Panel Judge for AgentHub
=================================
MERGE (session 5): ports the llm_council's anonymized orthogonal panel into
AgentHub's winner selection (/hub:eval).

THE PROBLEM
-----------
result_ranker.py ranks agents by a single numeric metric (or raw diff-stats). That
leaves three holes:
  1. Qualitative tasks (best refactor, best copy, best research) have no clean metric.
  2. Metric ties / near-ties need a principled tiebreaker, not "whichever sorted first".
  3. A single LLM judge is a monoculture — the same correlated-judgment bias the
     council was built to break. One judge has one blind spot; every candidate it
     mis-weighs loses for the same reason.

THE FIX (from implementations/llm_council/council.py)
-----------------------------------------------------
Two council mechanisms, ported:
  - ANONYMIZE first. Strip agent-N identity → Candidate A/B/C before judging, so
    position and "agent-1 is usually good" priors can't leak in. (council._anonymize)
  - ORTHOGONAL PANEL. Three judges with DISJOINT rubrics that cannot collapse into
    the same score: Correctness, Simplicity (the Karpathy lens), Effectiveness. A
    solution can be correct-but-complex or simple-but-incomplete — the axes disagree
    by design, which is the point. Then synthesize per-axis scores into a ranking.

This de-biases SELECTION the same way session-4's orthogonal_seed de-biased GENERATION.
Together: the council's anti-correlation principle now guards both ends of agenthub.

Scoring is pluggable so the tool runs (and tests) without network:
  - default: a transparent offline heuristic (clearly labeled).
  - with ANTHROPIC_API_KEY: real Claude judges, one call per (candidate, axis).

Usage:
    python council_judge.py --session 20260317-143022          # read agent results
    python council_judge.py --demo
    # or import: judge(candidates, score_fn=...) -> ranked verdict
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field

# ── The orthogonal judge panel (disjoint rubrics, à la council personas) ──────
JUDGE_PANEL: list[dict] = [
    {
        "key": "correctness",
        "name": "Correctness & Robustness",
        "rubric": (
            "Judge ONLY whether the solution actually works and handles failure: edge "
            "cases, error paths, invariants, tests. Ignore elegance, brevity, and speed. "
            "A clever solution that breaks on empty input scores low here."
        ),
    },
    {
        "key": "simplicity",
        "name": "Simplicity & Maintainability",
        "rubric": (
            "Judge ONLY simplicity — least complexity, smallest changed surface, clearest "
            "intent for the next human. The Karpathy lens. Ignore whether it is correct or "
            "fast. A sprawling diff that works still scores low here."
        ),
    },
    {
        "key": "effectiveness",
        "name": "Goal Effectiveness",
        "rubric": (
            "Judge ONLY how completely the solution achieves the stated task goal (and the "
            "metric's intent, if any). Ignore code aesthetics and defensive depth. Solving "
            "the wrong problem beautifully scores low here."
        ),
    },
]


@dataclass
class Candidate:
    agent: str                     # real identity, e.g. "agent-2"
    summary: str                   # the artifact under review (result.md / diff text)
    scores: dict = field(default_factory=dict)   # axis_key -> float 0..10
    panel_score: float = 0.0
    rank: int = 0


def anonymize(candidates: list[Candidate], shuffle: bool = True) -> tuple[list[dict], dict]:
    """
    Strip identity before judging. Returns (anon_records, label->agent map).
    Port of council._anonymize_analyses: judges see Candidate A/B/C, never agent-N.
    shuffle=False keeps order stable for deterministic tests.
    """
    order = list(range(len(candidates)))
    if shuffle:
        # position-independent: rotate by a content-derived offset (no RNG dependency,
        # deterministic, but decouples label from original agent index)
        offset = sum(len(c.summary) for c in candidates) % max(1, len(candidates))
        order = order[offset:] + order[:offset]
    labels = [chr(ord("A") + i) for i in range(len(candidates))]
    anon, label_map = [], {}
    for label, idx in zip(labels, order):
        anon.append({"label": label, "summary": candidates[idx].summary})
        label_map[label] = candidates[idx].agent
    return anon, label_map


def heuristic_score(summary: str, axis_key: str, task: str = "") -> float:
    """
    Transparent offline scorer (no network). Clearly a heuristic, not a real judge —
    it exists so the panel RUNS and is testable without an API key. Returns 0..10.
    Each axis keys off different, disjoint signals (mirrors the orthogonal rubrics).
    """
    text = summary.lower()
    if axis_key == "correctness":
        signals = ("test", "edge case", "error", "handle", "validate", "guard", "assert", "except")
        hits = sum(text.count(s) for s in signals)
        return min(10.0, 2.0 + hits * 1.5)
    if axis_key == "simplicity":
        # fewer net lines = simpler; parse "net_lines"/"+N/-N" if present
        m = re.search(r"net[ _]lines?[:=]\s*(-?\d+)", text) or re.search(r"([-+]?\d+)\s*net", text)
        net = abs(int(m.group(1))) if m else len(summary) // 40
        return max(0.0, 10.0 - net / 12.0)
    if axis_key == "effectiveness":
        signals = ("improv", "reduc", "faster", "increase", "achiev", "%", "delta", "metric", "goal")
        hits = sum(text.count(s) for s in signals)
        tied = sum(text.count(w) for w in task.lower().split() if len(w) > 4)
        return min(10.0, 2.0 + hits * 1.2 + tied * 0.5)
    return 5.0


def _llm_score(summary: str, axis: dict, task: str, client) -> float:
    """Real Claude judge for one (candidate, axis). Used when ANTHROPIC_API_KEY is set."""
    prompt = (
        f"Task under evaluation: {task or '(unspecified)'}\n\n"
        f"Candidate solution:\n{summary}\n\n"
        f"{axis['rubric']}\n\nReturn ONLY an integer 0-10."
    )
    resp = client.messages.create(
        model="claude-sonnet-4-6", max_tokens=8,
        system=f"You are the '{axis['name']}' judge. Score on your axis only.",
        messages=[{"role": "user", "content": prompt}],
    )
    m = re.search(r"\d+", resp.content[0].text)
    return float(max(0, min(10, int(m.group())))) if m else 5.0


def judge(candidates: list[Candidate], score_fn=None, task: str = "",
          shuffle: bool = True) -> dict:
    """
    Run the anonymized orthogonal panel and return a ranked verdict.
    score_fn(summary, axis_key, task) -> float; defaults to the offline heuristic.
    """
    if not candidates:
        return {"winner": None, "ranked": [], "panel": [a["key"] for a in JUDGE_PANEL]}
    score_fn = score_fn or heuristic_score

    anon, label_map = anonymize(candidates, shuffle=shuffle)
    by_agent = {c.agent: c for c in candidates}

    # each orthogonal judge scores every anonymized candidate on its axis only
    for rec in anon:
        agent = label_map[rec["label"]]
        cand = by_agent[agent]
        for axis in JUDGE_PANEL:
            cand.scores[axis["key"]] = round(float(score_fn(rec["summary"], axis["key"], task)), 2)
        cand.panel_score = round(sum(cand.scores.values()) / len(JUDGE_PANEL), 3)

    ranked = sorted(candidates, key=lambda c: c.panel_score, reverse=True)
    for i, c in enumerate(ranked):
        c.rank = i + 1

    return {
        "winner": ranked[0].agent,
        "winner_panel_score": ranked[0].panel_score,
        "ranked": [
            {"rank": c.rank, "agent": c.agent, "panel_score": c.panel_score, "axes": c.scores}
            for c in ranked
        ],
        "panel": [a["name"] for a in JUDGE_PANEL],
        "anonymized": True,
        "scorer": "heuristic" if getattr(score_fn, "__name__", "") == "heuristic_score" else "llm",
    }


def _read_session_candidates(session_id: str) -> list[Candidate]:
    """Load agent result.md files from a session's results board."""
    results_dir = os.path.join(".agenthub", "board", "results")
    cands = []
    if os.path.isdir(results_dir):
        for fn in sorted(os.listdir(results_dir)):
            m = re.match(r"(agent-\d+)-result\.md", fn)
            if m:
                text = open(os.path.join(results_dir, fn), encoding="utf-8").read()
                cands.append(Candidate(agent=m.group(1), summary=text))
    return cands


DEMO = [
    Candidate("agent-1", "Added caching layer. net_lines: 46. improved p50 by 8%. No new tests."),
    Candidate("agent-2", "Replaced O(n^2) with hash map. net_lines: 11. validate empty input, added test for edge case. improved p50 by 21%."),
    Candidate("agent-3", "Minor loop tweaks and error handling guards. net_lines: 30. handle timeout, assert preconditions. 3% improvement."),
]


def _print_verdict(v: dict, task: str):
    print(f"\nCouncil-Panel Judge  (anonymized, {len(v['panel'])} orthogonal judges)")
    if task:
        print(f"Task: {task}")
    print(f"Panel: {', '.join(v['panel'])}   scorer={v['scorer']}\n")
    print(f"{'RANK':<5}{'AGENT':<10}{'PANEL':<7}{'correct':<9}{'simple':<8}{'effect':<8}")
    print("-" * 47)
    for r in v["ranked"]:
        a = r["axes"]
        print(f"{r['rank']:<5}{r['agent']:<10}{r['panel_score']:<7}"
              f"{a['correctness']:<9}{a['simplicity']:<8}{a['effectiveness']:<8}")
    print(f"\nWinner: {v['winner']} (panel {v['winner_panel_score']}/10). "
          f"Run /hub:merge on its branch.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Council-panel judge for AgentHub winner selection.")
    ap.add_argument("--session")
    ap.add_argument("--task", default="")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--demo", action="store_true")
    args = ap.parse_args()

    if args.demo:
        cands = DEMO
        args.task = args.task or "cut p50 latency on /search"
    elif args.session:
        cands = _read_session_candidates(args.session)
        if not cands:
            print(f"No agent result files found for session {args.session}", file=sys.stderr)
            return 1
    else:
        print("Error: provide --session or --demo", file=sys.stderr)
        return 1

    score_fn = heuristic_score
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            import anthropic
            client = anthropic.Anthropic()
            score_fn = lambda s, k, t: _llm_score(  # noqa: E731
                s, next(a for a in JUDGE_PANEL if a["key"] == k), t, client)
        except Exception as e:
            print(f"[council_judge] LLM judge unavailable ({e}); using heuristic.", file=sys.stderr)

    verdict = judge(cands, score_fn=score_fn, task=args.task)
    if args.json:
        print(json.dumps(verdict, indent=2))
    else:
        _print_verdict(verdict, args.task)
    return 0


if __name__ == "__main__":
    sys.exit(main())
