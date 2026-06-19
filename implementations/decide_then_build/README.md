# Decide-Then-Build Pipeline

The capstone integration (session 6). Chains three toolkit/skill modules into one
**gated** agentic motion so a parallel build is never spawned on a bad idea, never
seeded with correlated strategies, and never selected by a single biased judge.

```
council (decide GO/NO-GO) ──GO──> orthogonal_seed (N disjoint strategies)
        │ NO-GO                         │
        └─► STOP, spawn nothing         ▼
                              agenthub spawn plan ──builds──> council_judge (anonymized
                                                              orthogonal panel) ─► winner
```

## Run

```bash
# full flow, offline demo (stub council GO + agenthub demo results)
python pipeline.py --demo

# real task (live council needs ANTHROPIC_API_KEY; falls back to CONDITIONAL if absent)
python pipeline.py --task "cut p50 latency on /search" --domain optimization --agents 3
python pipeline.py --task "..." --json     # machine-readable
```

Domains (from `orthogonal_seed`): optimization, refactoring, copywriting, research,
debugging, general.

## What each phase carries

| Phase | Module | Karpathy idea |
|---|---|---|
| **DECIDE** | `implementations/llm_council/council.py` | orthogonal panel → GO/NO-GO before spending compute |
| **SEED** | `skills/agenthub/scripts/orthogonal_seed.py` | Muon anti-correlation, generation side |
| **BUILD** | agenthub runtime (real worktrees) | parallel competition; pipeline emits the plan |
| **JUDGE** | `skills/agenthub/scripts/council_judge.py` | Muon anti-correlation, selection side |

## Boundaries (honest)

The pipeline orchestrates and gates; it does **not** spawn the real subagents — that's
agenthub's runtime (git worktrees + live agents). With no agent results yet it returns
`stopped_at: "awaiting_results"` and the spawn plan. Feed results back via `results_fn`
(or run `council_judge.py --session <id>`) to complete the JUDGE phase.

`council_fn` and `results_fn` are injectable, so every phase except the live council
call and the live build is deterministic and tested offline (`test_pipeline.py`, 13/13).
