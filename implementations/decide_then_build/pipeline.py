#!/usr/bin/env python3
"""
Decide-Then-Build Pipeline  (the capstone)
==========================================
MERGE (session 6): chains the toolkit's llm_council with agenthub's orthogonal_seed
and council_judge into one gated agentic motion.

Today these are three tools a human chains by hand. The capstone makes it one flow,
and — crucially — GATES it:

    ┌──────────┐   GO     ┌──────────────┐        ┌──────────────┐   ┌──────────────┐
    │ COUNCIL  │ ───────> │ ORTHOGONAL   │ ─────> │  AGENTHUB    │ > │  COUNCIL     │
    │ decide   │          │ SEED         │ spawn  │  builds (N)  │   │ JUDGE select │
    │ GO/NO-GO │          │ N strategies │ plan   │  (worktrees) │   │ (anonymized) │
    └────┬─────┘          └──────────────┘        └──────────────┘   └──────────────┘
         │ NO-GO
         └─► STOP — spawn nothing. The veto saves N worktrees of wasted parallel work.

Why each gate matters (and which Karpathy idea it carries):
  1. DECIDE  — the council's orthogonal panel says GO/NO-GO *before* spending compute.
               A NO-GO here is the cheapest possible failure.
  2. SEED    — orthogonal_seed gives the N builders disjoint strategies so the
               competition explores N regions, not one (Muon anti-correlation, gen side).
  3. BUILD   — agenthub's real runtime: N subagents in git worktrees. The pipeline
               emits the spawn PLAN; the actual building is agenthub's job. Honest
               boundary: with no results yet, the pipeline reports `awaiting_results`.
  4. JUDGE   — council_judge picks the winner via an anonymized orthogonal panel
               (Muon anti-correlation, selection side).

Everything except the live council call and the live agent build is deterministic and
testable offline — council_fn and results_fn are pluggable.

Usage:
    python pipeline.py --task "cut p50 latency on /search" --domain optimization --agents 3
    python pipeline.py --demo
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# ── locate sibling toolkit modules without hard package deps ──────────────────
_ROOT = Path(__file__).resolve().parents[2]  # repo root
for _p in (_ROOT / "skills" / "agenthub" / "scripts",):
    if _p.exists():
        sys.path.insert(0, str(_p))

# seed (no network) and judge (offline heuristic default) are safe to import eagerly.
from orthogonal_seed import seed as seed_strategies, orthogonality_report  # noqa: E402
from council_judge import judge as council_judge, Candidate  # noqa: E402


def extract_verdict(synthesis: str) -> str:
    """Parse GO / NO-GO / CONDITIONAL from the chairman's free-text synthesis."""
    t = (synthesis or "").upper()
    if re.search(r"\bNO[\s-]?GO\b|\bKILL\b|\bDO NOT PROCEED\b|\bREJECT\b", t):
        return "NO-GO"
    if re.search(r"\bCONDITIONAL\b|\bPROCEED WITH CAUTION\b|\bQUALIFIED\b", t):
        return "CONDITIONAL"
    if re.search(r"\bGO\b|\bPROCEED\b|\bAPPROVE\b", t):
        return "GO"
    return "CONDITIONAL"  # ambiguous → don't blindly spawn; surface for a human


def _default_council_fn(task: str) -> dict:
    """
    Real council decision. Lazy-imports council (which imports anthropic) so the
    pipeline stays importable/testable without the SDK or an API key.
    """
    try:
        sys.path.insert(0, str(_ROOT / "implementations" / "llm_council"))
        from council import run_council  # type: ignore
    except Exception as e:  # noqa: BLE001
        return {"verdict": "CONDITIONAL", "synthesis": f"(council unavailable: {e})",
                "available": False}
    query = (
        f"Should we build the following, and if so what are the 2-4 key strategic "
        f"directions to attempt in parallel? Give a clear GO / NO-GO / CONDITIONAL "
        f"at the top.\n\nTASK: {task}"
    )
    v = run_council(query)
    return {"verdict": extract_verdict(v.chairman_synthesis),
            "synthesis": v.chairman_synthesis, "available": True}


def plan_spawn(task: str, strategies: list[dict], synthesis: str) -> list[dict]:
    """Turn seeded strategies into agenthub dispatch prompts (ready for /hub:spawn)."""
    context = (synthesis or "").strip()
    context = (context[:280] + "…") if len(context) > 280 else context
    plan = []
    for s in strategies:
        plan.append({
            "agent": s["agent"],
            "strategy": s["name"],
            "dispatch_prompt": (
                f"You are agent-{s['agent']}. Task: {task}\n"
                f"Your assigned (orthogonal) strategy: {s['dispatch_strategy']}\n"
                f"Council context: {context}\n"
                f"Build the best solution under YOUR strategy only. Commit improvements; "
                f"post your result to .agenthub/board/results/agent-{s['agent']}-result.md."
            ),
        })
    return plan


def decide_then_build(
    task: str,
    domain: str = "general",
    n_agents: int = 3,
    *,
    council_fn=None,
    results_fn=None,
) -> dict:
    """
    Run the gated pipeline. Returns a structured result with each phase's output.

    council_fn(task)  -> {"verdict","synthesis"}    (default: real council, lazy)
    results_fn()      -> list[Candidate] | None     (agent build outputs, if available)
    """
    council_fn = council_fn or _default_council_fn
    out: dict = {"task": task, "domain": domain, "n_agents": n_agents}

    # ── Phase 1: DECIDE (gate) ────────────────────────────────────────────────
    decision = council_fn(task)
    verdict = decision.get("verdict", "CONDITIONAL")
    out["decision"] = {"verdict": verdict, "synthesis": decision.get("synthesis", "")}

    if verdict == "NO-GO":
        out["stopped_at"] = "decide"
        out["reason"] = "Council vetoed (NO-GO) - spawning nothing. Cheapest failure."
        out["strategies"] = out["spawn_plan"] = out["winner"] = None
        return out

    # ── Phase 2: SEED (orthogonal strategies) ─────────────────────────────────
    strategies = seed_strategies(domain, n_agents, task)
    out["strategies"] = strategies
    out["orthogonality"] = orthogonality_report(strategies)

    # ── Phase 3: SPAWN PLAN (agenthub executes the real builds) ───────────────
    out["spawn_plan"] = plan_spawn(task, strategies, decision.get("synthesis", ""))
    out["spawn_command"] = (
        f"# in your git repo:\n"
        f"python skills/agenthub/scripts/hub_init.py --task \"{task}\" --agents {n_agents}\n"
        f"# then /hub:spawn each agent with the dispatch_prompt above"
    )

    # ── Phase 4: JUDGE (only once agent results exist) ────────────────────────
    candidates = results_fn() if results_fn else None
    if not candidates:
        out["stopped_at"] = "awaiting_results"
        out["reason"] = (
            "Spawn plan ready. Run the agents in agenthub, then re-invoke with "
            "results_fn (or `council_judge.py --session <id>`) to pick the winner."
        )
        out["winner"] = None
        return out

    out["winner"] = council_judge(candidates, task=task)
    out["stopped_at"] = "complete"
    out["reason"] = f"Winner: {out['winner']['winner']} (council-panel selected)."
    return out


# ── demo: full flow offline (stub council GO + agenthub demo results) ─────────
def _demo() -> dict:
    from council_judge import DEMO as DEMO_RESULTS

    def stub_council(task):
        return {"verdict": "GO",
                "synthesis": "GO. Strongest directions: algorithmic and caching. "
                             "Watch correctness on edge cases.", "available": False}

    return decide_then_build(
        "cut p50 latency on /search", domain="optimization", n_agents=3,
        council_fn=stub_council, results_fn=lambda: DEMO_RESULTS,
    )


def _print(out: dict) -> None:
    d = out["decision"]
    print(f"\n[1] DECIDE  -> {d['verdict']}")
    if d["synthesis"]:
        print(f"            {d['synthesis'][:120]}")
    if out.get("stopped_at") == "decide":
        print(f"\n  [STOP] {out['reason']}")
        return
    print(f"\n[2] SEED    -> {out['n_agents']} orthogonal strategies "
          f"({out['orthogonality']['verdict']})")
    for s in out["strategies"]:
        print(f"            agent-{s['agent']}: {s['name']}")
    print(f"\n[3] BUILD   -> spawn plan ready ({len(out['spawn_plan'])} dispatch prompts)")
    if out.get("stopped_at") == "awaiting_results":
        print(f"\n  [PAUSE] {out['reason']}")
        return
    w = out["winner"]
    print(f"\n[4] JUDGE   -> winner {w['winner']} (panel {w['winner_panel_score']}/10, anonymized)")
    for r in w["ranked"]:
        a = r["axes"]
        print(f"            #{r['rank']} {r['agent']}: panel {r['panel_score']} "
              f"(correct {a['correctness']}, simple {a['simplicity']}, effect {a['effectiveness']})")
    print(f"\n  [OK] {out['reason']}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Council-gated, orthogonally-seeded, panel-judged build pipeline.")
    ap.add_argument("--task")
    ap.add_argument("--domain", default="general")
    ap.add_argument("--agents", type=int, default=3)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--demo", action="store_true")
    args = ap.parse_args()

    if args.demo:
        out = _demo()
    elif args.task:
        out = decide_then_build(args.task, args.domain, args.agents)
    else:
        print("Error: provide --task or --demo", file=sys.stderr)
        return 1

    print(json.dumps(out, indent=2)) if args.json else _print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
