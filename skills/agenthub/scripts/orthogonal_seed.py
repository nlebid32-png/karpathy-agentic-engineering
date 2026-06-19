#!/usr/bin/env python3
"""
Orthogonal Strategy Seeder for AgentHub
========================================
MERGE (session 4): ports the llm_council's Muon-style orthogonalization into
AgentHub's parallel-competition spawn.

THE PROBLEM
-----------
AgentHub spawns N agents on the same task, but agent-templates.md leaves strategy
assignment to the coordinator ("assign each agent a different strategy") with no
mechanism guaranteeing the strategies are actually disjoint. N agents seeded with
near-identical prompts produce *correlated* attempts — they explore the same narrow
slice of the solution space, so running 5 of them is barely better than running 1.

THE FIX (from implementations/llm_council/council.py)
-----------------------------------------------------
The council's insight, borrowed from the Muon optimizer (modded-nanogpt / llm.c):
orthogonalize the agents. Assign disjoint, non-overlapping constraint vectors so no
two agents *can* converge on the same output. Here each "strategy" is a disjoint lens
on how to attack the task, so the N worktrees cover N genuinely different regions.

VERIFICATION
------------
We don't just assert orthogonality — we measure it. The selected strategy set is run
through the toolkit's own diversity_check (implementations/diagnostic/diversity_check.py).
If any pair scores above the collapse threshold, the seeding is flagged as too
correlated. This is the same attention-collapse detector the autonomous loop uses,
now guarding the competition's diversity at spawn time.

Usage:
    python orthogonal_seed.py --domain optimization --agents 3 --task "cut p50 latency"
    python orthogonal_seed.py --domain refactoring --agents 4 --json
    python orthogonal_seed.py --list-domains
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# ── Curated orthogonal strategy sets ─────────────────────────────────────────
# Each strategy is a disjoint constraint vector (à la council personas): a single
# lens with a "sole purpose" so two agents on different strategies cannot produce
# the same solution. Ordered by typical leverage; the seeder takes the first N.
STRATEGY_SETS: dict[str, list[dict]] = {
    "optimization": [
        {"name": "Algorithmic", "constraint": "Reduce the complexity class. Better data structures, eliminate redundant work and repeated passes. Touch nothing about caching or I/O."},
        {"name": "Caching", "constraint": "Store and reuse computed results. Memoization, result caches, cache headers. Do not change the underlying algorithm."},
        {"name": "I/O & batching", "constraint": "Attack the I/O boundary. Batch calls, parallelize requests, connection pooling, lazy loading. Leave in-memory compute alone."},
        {"name": "Allocation", "constraint": "Minimize allocations and copies. Object pooling, streaming over buffering, in-place ops. Ignore algorithmic and I/O angles."},
        {"name": "Concurrency", "constraint": "Exploit parallelism. Async, worker pools, pipelining. Do not optimize the single-threaded path itself."},
    ],
    "refactoring": [
        {"name": "Decomposition", "constraint": "Split god functions/classes into single-responsibility units. Change structure only, not naming conventions or types."},
        {"name": "Naming & clarity", "constraint": "Make intent self-evident through names and shape. Delete comments that compensate for unclear code. Do not restructure modules."},
        {"name": "Decoupling", "constraint": "Break coupling. Invert dependencies, shrink interfaces, remove hidden globals. Leave internal logic untouched."},
        {"name": "Dedup & dead-code", "constraint": "DRY and delete. Consolidate duplication, remove unreachable/unused code. Do not add abstractions."},
        {"name": "Contracts & types", "constraint": "Strengthen types, add invariants and fail-fast guards. Change signatures only, not control flow."},
    ],
    "copywriting": [
        {"name": "Benefit-led", "constraint": "Open with the top user outcomes. Features are subordinate. No testimonials, no urgency framing."},
        {"name": "Social proof", "constraint": "Lead with testimonials, logos, and hard stats. Persuade by consensus, not by benefit claims."},
        {"name": "Urgency/scarcity", "constraint": "Frame around limited time/supply and cost of inaction. FOMO-driven CTA. Avoid slow narrative."},
        {"name": "Problem-agitation", "constraint": "Open on the reader's pain, agitate it, then resolve. Lead with the problem, never the product."},
        {"name": "Story-led", "constraint": "Carry a single narrative arc with a protagonist. Persuade through story, not bullet lists or stats."},
    ],
    "research": [
        {"name": "Breadth-first", "constraint": "Survey the whole landscape. Many sources, shallow each, map the territory. Do not deep-dive one source."},
        {"name": "Depth-first", "constraint": "Go deep on the few most authoritative primary sources. Ignore breadth and secondary commentary."},
        {"name": "Contrarian", "constraint": "Hunt disconfirming evidence and the strongest counter-case. Do not assemble supporting evidence."},
        {"name": "Quantitative", "constraint": "Anchor on numbers, benchmarks, and datasets only. No qualitative or anecdotal sourcing."},
        {"name": "Historical", "constraint": "Trace precedent and evolution over time. Explain via how it got here, not the current snapshot."},
    ],
    "debugging": [
        {"name": "Bisection", "constraint": "Narrow by halving — bisect commits/inputs/config until the fault localizes. Do not add instrumentation."},
        {"name": "State-inspection", "constraint": "Add logging/tracing/observability to watch state at the boundary. Do not change code paths to test theories."},
        {"name": "Hypothesis-first", "constraint": "Form an explicit theory, design one discriminating test, run it. No broad logging, no bisection."},
        {"name": "Minimal-repro", "constraint": "Shrink to the smallest input that still fails. Delete everything irrelevant before theorizing."},
        {"name": "Differential", "constraint": "Diff a working case against the broken one. Find the first divergence. Do not reason from the broken case alone."},
    ],
    "general": [
        {"name": "Minimalist", "constraint": "Fewest possible changes and lines. Smallest diff that solves it. Resist adding anything."},
        {"name": "Robustness", "constraint": "Handle every edge case and failure mode. Defensive and explicit over terse."},
        {"name": "Performance", "constraint": "Optimize for speed/efficiency above all else, accepting more code if it's faster."},
        {"name": "Readability", "constraint": "Optimize for the next human. Clarity and obvious structure over cleverness or brevity."},
        {"name": "Innovation", "constraint": "Take the unconventional approach the others won't. Reframe the problem itself."},
    ],
}


def _load_diversity_check():
    """
    Import the toolkit's diversity_check util to verify orthogonality.
    Graceful fallback to an inline word-Jaccard so this script stays standalone
    when run outside the karpathy toolkit (e.g. as a vendored skill elsewhere).
    """
    here = Path(__file__).resolve()
    # walk up to the repo root and into implementations/diagnostic/
    for parent in here.parents:
        cand = parent / "implementations" / "diagnostic"
        if (cand / "diversity_check.py").exists():
            sys.path.insert(0, str(cand))
            try:
                from diversity_check import check_diversity  # type: ignore
                return check_diversity
            except ImportError:
                break

    # Fallback: minimal word-Jaccard returning an object with .score
    class _R:
        def __init__(self, score):
            self.score = score
            self.verdict = "STOP" if score > 0.75 else "GO"

    def _fallback(candidate: str, prior_attempts: list[str]) -> "_R":
        def jac(a, b):
            ta, tb = set(a.lower().split()), set(b.lower().split())
            if not ta and not tb:
                return 1.0
            if not ta or not tb:
                return 0.0
            return len(ta & tb) / len(ta | tb)
        worst = max((jac(candidate, p) for p in prior_attempts), default=0.0)
        return _R(worst)

    return _fallback


def seed(domain: str, n: int, task: str = "") -> list[dict]:
    """
    Return N maximally-disjoint strategies for the domain.
    Each entry: {agent, name, constraint, dispatch_strategy}.
    """
    domain = domain.lower()
    if domain not in STRATEGY_SETS:
        raise ValueError(
            f"Unknown domain '{domain}'. Known: {', '.join(sorted(STRATEGY_SETS))}"
        )
    pool = STRATEGY_SETS[domain]
    if n < 1:
        raise ValueError("agents must be >= 1")

    chosen = []
    for i in range(n):
        base = pool[i % len(pool)]
        # If N exceeds the curated set, later agents get a "+variant" nudge so
        # they don't duplicate the wrapped-around strategy verbatim.
        variant = "" if i < len(pool) else f" Variant {i // len(pool) + 1}: push this lens to an extreme the first pass would avoid."
        chosen.append({
            "agent": i + 1,
            "name": base["name"],
            "constraint": base["constraint"] + variant,
            "dispatch_strategy": f"{base['name']} — {base['constraint']}{variant}",
        })
    return chosen


def orthogonality_report(strategies: list[dict]) -> dict:
    """
    Measure how disjoint the seeded strategies actually are, using the toolkit's
    diversity_check. Returns the worst pairwise similarity and a GO/WARN verdict.
    Lower max_pair_similarity = more orthogonal = better solution-space coverage.
    """
    check = _load_diversity_check()
    texts = [s["dispatch_strategy"] for s in strategies]
    worst = 0.0
    worst_pair = ("", "")
    for i, t in enumerate(texts):
        others = texts[:i] + texts[i + 1:]
        if not others:
            continue
        r = check(t, others)
        if r.score > worst:
            worst = r.score
            worst_pair = (strategies[i]["name"], "set")
    return {
        "max_pair_similarity": round(worst, 3),
        "verdict": "WARN: strategies too correlated" if worst > 0.75 else "GO: orthogonal",
        "worst_offender": worst_pair[0],
        "agent_count": len(strategies),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed AgentHub's N agents with orthogonal strategies.")
    ap.add_argument("--domain", default="general", help="optimization|refactoring|copywriting|research|debugging|general")
    ap.add_argument("--agents", type=int, default=3)
    ap.add_argument("--task", default="")
    ap.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    ap.add_argument("--list-domains", action="store_true")
    args = ap.parse_args()

    if args.list_domains:
        for d, s in STRATEGY_SETS.items():
            print(f"{d:14} {len(s)} strategies: {', '.join(x['name'] for x in s)}")
        return 0

    try:
        strategies = seed(args.domain, args.agents, args.task)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    report = orthogonality_report(strategies)

    if args.json:
        print(json.dumps({"task": args.task, "domain": args.domain,
                          "strategies": strategies, "orthogonality": report}, indent=2))
        return 0

    print(f"\nOrthogonal seeding - domain={args.domain}, agents={args.agents}")
    if args.task:
        print(f"Task: {args.task}")
    print(f"Orthogonality: {report['verdict']} (max pairwise similarity {report['max_pair_similarity']})\n")
    for s in strategies:
        print(f"  agent-{s['agent']}  [{s['name']}]")
        print(f"     {s['constraint']}\n")
    print("Slot each 'dispatch_strategy' into the {strategy} field of the "
          "optimizer/refactorer template in agent-templates.md.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
