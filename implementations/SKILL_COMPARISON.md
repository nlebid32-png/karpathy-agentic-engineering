# Skill ↔ Implementation Comparison & Merge Log

Head-to-head between this toolkit's hand-built modules and the vendored third-party
skills (`skills/`, by Alireza Rezvani, MIT) that implement the same Karpathy ideas.
Goal: keep what each does better, merge the best into our implementations.

Vendored 2026-06-16: `autoresearch-agent`, `agenthub`, `karpathy-coder`,
`self-improving-agent`, `adversarial-reviewer`.

---

## 1. autoresearch-agent  vs  implementations/autonomous_loop/

Both descend from karpathy/autoresearch: edit one file, run a fixed eval, keep
improvements, discard regressions, loop.

### Architecture difference (the core split)

| | **autonomous_loop (ours)** | **autoresearch-agent (skill)** |
|---|---|---|
| Who drives the loop | `runner.py` — a Python `while` loop | The AI agent itself; it calls `run_experiment.py` per iteration |
| Evaluation | **in-process** — `prepare.py` imports `train.py` fresh each iter | **subprocess** — runs an arbitrary `evaluate_cmd` |
| Editable surface | `train.py` only (Python) | any file (JS, images, prompts, configs…) |
| Rollback | in-memory snapshot → restore **only** `train.py` | `git reset --hard HEAD~1` (whole tree) |
| Provenance | `loop_log.jsonl` (metric only) | `results.tsv` (commit + metric + keep/discard/crash) |
| Failure modes | regression only (crash → `-inf`) | timeout / crash / metric-parse-fail, each distinct |
| Git required | no | yes |
| Steerable mid-run | yes — re-reads `program.md` every iteration | via `config.cfg` |

### What the skill does better
1. **Language-agnostic.** Subprocess eval optimizes *anything* with a measurable
   metric — bundle size, image bytes, test pass-rate, prompt CTR — not just a Python
   `train.run()`. This is closer to the real Karpathy use cases.
2. **Failure taxonomy.** Timeout (2.5× budget hard kill), non-zero exit, and
   metric-parse-failure are handled as **distinct discard reasons**. Ours only
   notices a metric regression; a `train.py` that hangs would block the loop forever.
3. **Commit-level provenance.** Every attempt is a git commit tagged keep/discard in
   `results.tsv` — a replayable audit trail. Ours logs the number but not the verdict.
4. **Direction-aware.** Supports `metric_direction: lower` — and Karpathy's actual
   metric (`val_bpb`) is **lower-is-better**. Ours hardcoded higher-is-better. ← real bug.

### What ours does better
1. **Targeted rollback.** We restore **only the agent-editable file** from an
   in-memory snapshot. The skill's `git reset --hard HEAD~1` nukes the *entire* working
   tree — destructive if the agent touched multiple files or there's unrelated
   uncommitted work. Our scoping is strictly safer in a mixed tree.
2. **No git dependency.** Runs in any directory; good for throwaway experiments.
3. **In-process speed.** No subprocess/process-spawn overhead per iteration — matters
   when the eval is cheap and you want thousands of iterations.
4. **Immutable-oracle separation is explicit.** `prepare.py` is the oracle the agent
   literally cannot edit (it's not on the editable surface), making metric-gaming harder.

### MERGED into autonomous_loop (this session)
- ✅ **Direction-aware optimization** (`metric_direction: lower|higher`, default higher
  for back-comat) — fixes the val_bpb correctness gap. `runner.py` + `program.md` + docstrings.
- ✅ **Status-aware logging** — `loop_log.jsonl` now records `status`
  (`keep` / `discard` / `rollback`) and `best_so_far`, adopting the skill's verdict ledger.
- ✅ **Per-iteration hard timeout** — a thread watchdog kills a hung iteration at
  `time_budget_seconds`, importing the skill's robustness into our in-process model.
- ⏸️ **Kept ours:** targeted file-only rollback, no-git operation, in-process eval —
  these are our advantages; not replaced.

### Recommendation
Use **autonomous_loop** for fast Python-only experiments in a mixed/unclean tree.
Use **autoresearch-agent** for language-agnostic optimization in a clean git repo where
each attempt should be a commit. They now share direction + status semantics.

---

## 2. agenthub  vs  implementations/llm_council/

Both run N model instances in parallel — but for **different jobs**.

| | **llm_council (ours)** | **agenthub (skill)** |
|---|---|---|
| Produces | a **judgment** (GO/NO-GO verdict) | an **artifact** (merged winning code) |
| Agents | 5 **orthogonal** personas (disjoint constraints, Muon-inspired) | N agents, **same** task prompt |
| Isolation | thread-parallel API calls | **git worktrees** (real file isolation) |
| Selection | anonymized circular peer review → Chairman synthesis | judge (metric or LLM) → merge winner |
| Anti-correlation | **designed in** (disjoint personas can't converge) | none — identical prompts → correlated attempts |
| Output side-effects | none (advisory) | mutates the repo (merges a branch) |

### Verdict: complementary, not redundant
- **Council** answers *"should we, and which direction?"* — diverse perspectives on a
  decision, no code produced.
- **agenthub** answers *"which implementation wins?"* — parallel builds, best merged.

They **compose**: council to decide the approach → agenthub to execute N competing
implementations of it → a council-style panel to judge the bake-off.

### What each does better
- **agenthub:** real worktree isolation (agents can't clobber each other), produces
  runnable artifacts, metric-or-judge ranking, one-shot `/hub:run` lifecycle.
- **council:** the **orthogonalization thesis** — agenthub's N identical prompts yield
  correlated attempts that explore a narrow slice of the solution space; the council's
  disjoint-constraint seeding (and anonymized peer review to break judge monoculture)
  is a strictly better diversity mechanism.

### MERGE opportunities
1. ✅ **Orthogonal seeding for agenthub** — BUILT (session 4).
   `skills/agenthub/scripts/orthogonal_seed.py` ports the council's Muon
   orthogonalization: given a domain + N, it emits N disjoint strategy constraint
   vectors (each with a "sole purpose" so two agents can't converge) instead of one
   shared prompt → wider solution-space coverage. The selected set is verified with
   the toolkit's own `implementations/diagnostic/diversity_check.py` — if lenses are
   too correlated (>0.75 similarity) it prints `WARN` so you reseed before spawning.
   6 domains (optimization, refactoring, copywriting, research, debugging, general).
   Wired into `agent-templates.md`. Tests 9/9. Smoke run: 3-agent optimization set
   scored 0.114 similarity (orthogonal). This is the merge with teeth — the project's
   own council insight + diversity util now improve a third-party agent framework.
2. ✅ **Council-panel judge for `/hub:eval`** — BUILT (session 5).
   `skills/agenthub/scripts/council_judge.py` replaces the single-metric/single-judge
   selection with an **anonymized** (Candidate A/B/C, no agent-N identity) **orthogonal
   3-judge panel**: Correctness & Robustness, Simplicity (Karpathy lens), Goal
   Effectiveness — disjoint rubrics that can't collapse to the same score. Ports
   council._anonymize + the disjoint-persona design. Pluggable scorer: offline
   heuristic (testable, no network) or real Claude judges with ANTHROPIC_API_KEY.
   Wired into `coordination-strategies.md`. Tests 10/10. Demo shows the payoff: the
   metric winner (agent-2, 21%) and agent-3 land in a near-tie (6.66 vs 6.633) because
   agent-3 wins the correctness axis — a single metric judge would miss that the
   "winner" is the more fragile solution. This de-biases SELECTION the way merge #1
   de-biased GENERATION — the council's anti-correlation now guards both ends of agenthub.
3. **Council→agenthub pipeline** — a `decide-then-build` command: council verdict gates
   whether agenthub spawns, and seeds the build directions from the Chairman synthesis.
   *Next — the capstone that chains all three: council decides → orthogonal_seed spawns →
   council_judge selects.*

### Recommendation
Keep both. Council stays the decision layer; agenthub becomes the execution layer.
Next iteration: port orthogonal seeding into agenthub (merge #1).

---

## Cross-cutting: karpathy-coder, self-improving-agent, adversarial-reviewer
- **karpathy-coder** overlaps `karpathy_playbook.md` but adds *executable* enforcement
  (`complexity_checker.py`, `diff_surgeon.py`, `assumption_linter.py`, `goal_verifier.py`).
  Action: wire these as the autonomous loop's pre-commit gate (a real karpathy-gate).
- **self-improving-agent** overlaps nothing here — it curates Claude auto-memory
  (MEMORY.md → CLAUDE.md/rules → skills). Complementary; adopt as-is.
- **adversarial-reviewer** complements the council: hostile single-reviewer personas
  vs. the council's orthogonal panel. Use for diff review, council for decisions.
