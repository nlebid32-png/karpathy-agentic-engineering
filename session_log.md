# Session Log — Karpathy Agentic Engineering Build Session

**Date:** 2026-05-21
**Model:** claude-sonnet-4-6
**Directive:** Implement Karpathy's agentic engineering principles as a working toolkit.
**Source document:** Karpathy's AI Research Synthesis (Gemini Deep Research, 20 pages)

---

## Step 1: Classification Results

### Section 2 Implementation Targets

| # | Technique | Classification | Rationale |
|---|-----------|---------------|-----------|
| 1 | Autonomous Experimentation Loop | `BUILD` | Pure Python. Three-file autoresearch architecture (program.md, runner.py, prepare.py) with JSON logging and git rollback hook. No weight access. |
| 2 | Agent-Native Legible Surfaces | `SCAFFOLD` | No existing codebase to refactor. Built AgentNativeBase + AgentResponse template demonstrating the full pattern. Every method returns typed JSON state + structured error codes. |
| 3 | Asynchronous LLM Council Harness | `BUILD` | Implementable via Anthropic API. 5 orthogonal advisors, anonymous peer review, Chairman synthesis. Verifiable without API key via unit tests. |
| 4 | Synthetic SFT Data Pipeline / BOS Masking | `DEFER` | Training loop gradient validation requires weight access. Data generation half is buildable — see DEFER Registry. |
| 5 | Mid-Training Diagnostic Eval Harness | `DEFER` | Source table explicitly marks "Requires Weight Access = True." GPU training environment required. |

---

## Step 2: Build Log

### BUILD 1: Autonomous Experimentation Loop

**Files created:**
- `implementations/autonomous_loop/program.md` — human-edited state file template
- `implementations/autonomous_loop/prepare.py` — immutable evaluation oracle
- `implementations/autonomous_loop/runner.py` — loop runner (reads program.md, calls prepare.evaluate, logs, rollbacks)
- `implementations/autonomous_loop/train.py` — agent-editable stub implementation
- `implementations/autonomous_loop/test_runner.py` — 6 unit tests

**Verifiability:**
- Pass condition: 6/6 tests pass
- Result: **6/6 PASS** ✓
- Run: `pytest implementations/autonomous_loop/test_runner.py`

**What it enforces:**
- program.md is re-read every iteration (human can update mid-run without stopping agent)
- prepare.py dynamically reloads train.py fresh each iteration (agent changes immediately evaluated)
- Metric regression > 5% triggers `git revert --no-commit HEAD`
- Time budget enforced via `time.monotonic()`, terminates gracefully
- All results appended to `loop_log.jsonl` as structured JSON

**Invocation:** `python implementations/autonomous_loop/runner.py`

---

### SCAFFOLD 2: Agent-Native Legible Surfaces

**Files created:**
- `implementations/agent_native_surfaces/agent_api.py` — AgentResponse, ErrorCode, AgentNativeBase, ExampleResourceService
- `implementations/agent_native_surfaces/test_api.py` — 10 unit tests

**Verifiability:**
- Pass condition: 10/10 tests pass
- Result: **10/10 PASS** ✓
- Run: `pytest implementations/agent_native_surfaces/test_api.py`

**What it enforces:**
- Every API call returns `AgentResponse` — no bare exceptions escape
- `error_code` is an `ErrorCode` enum (6 values), never a prose string
- Every failure includes a `recovery_hint` with a concrete next action
- Every response includes `state_snapshot` — complete current state as JSON
- `get_schema()` available so agents can validate inputs before calling

**Invocation:** `from agent_api import AgentNativeBase, AgentResponse, ErrorCode` — subclass for any new service.

---

### BUILD 3: Asynchronous LLM Council Harness

**Files created:**
- `implementations/llm_council/council.py` — full council orchestrator
- `implementations/llm_council/test_council.py` — 9 unit tests (no API key required)

**Verifiability:**
- Pass condition: 9/9 unit tests pass; integration test produces populated CouncilVerdict
- Result: **9/9 PASS** ✓ (unit); integration requires `ANTHROPIC_API_KEY`
- Run: `pytest implementations/llm_council/test_council.py`
- Integration: `ANTHROPIC_API_KEY=sk-... python implementations/llm_council/council.py "your question"`

**What it enforces:**
- 5 advisors with strictly disjoint constraints (orthogonalization principle from Muon optimizer)
- All 5 advisor calls dispatched in parallel via `ThreadPoolExecutor`
- Peer review phase: each advisor reviews one other, anonymously (circular assignment)
- Chairman receives all analyses + reviews, outputs structured 6-section verdict with GO/NO-GO
- Full verdict serializable to JSON for logging and audit

**Invocation:**
```python
from council import run_council
verdict = run_council("Should we use PostgreSQL or MongoDB?")
print(verdict.chairman_synthesis)
```

---

## Step 3: Full Test Summary

| Implementation | Tests | Result | API Key Required |
|----------------|-------|--------|-----------------|
| Autonomous Loop | 6 | 6/6 PASS ✓ | No |
| Agent-Native Surfaces | 10 | 10/10 PASS ✓ | No |
| LLM Council (unit) | 9 | 9/9 PASS ✓ | No |
| LLM Council (integration) | 1 | Requires API key | Yes |
| **Total (offline)** | **25** | **25/25 PASS** | — |

---

## Step 4 & 5: Deliverables

- `karpathy_playbook.md` — 5-phase agentic operating manual with Jagged Ghost protocol, context rules, verification checklist
- `karpathy_prompts.md` — 13 prompt templates covering all 6 Section 1 techniques + Chairman synthesis + overnight loop

---

## Step 6: Judge Report

**Judge prompt applied:**
> "You are a critical reviewer. Identify any gap between what was requested and what was delivered. Be specific. No flattery."

**Findings:**

**Finding 1 — SFT Data Generator under-classified.**
The research document's SFT Data Pipeline item has two separable halves: (a) generating Claude conversations with BOS/EOS formatting, and (b) the PyTorch data loader that masks prompt tokens during the backward pass. Half (a) requires no weight access and was not built. It was conflated with half (b) into a single DEFER. A future session should build `implementations/sft_data_gen/generator.py` that uses the Anthropic API to emit BOS/EOS delimited JSONL ready for nanochat-style training.

**Finding 2 — Diagnostic Heuristic (Technique 6) has no code artifact.**
Section 1 Technique 6 (Diagnostic Heuristic Prompting) appears only as a prompt template in `karpathy_prompts.md`. The research describes this as a self-checking loop the agent runs on its own outputs — specifically computing a semantic diversity score against prior failed attempts. A proper BUILD would be a `diversity_check.py` utility that computes token overlap or cosine similarity between a candidate patch and a log of recent failures, returning a similarity score before any patch is applied.

**Finding 3 — Git rollback in runner.py is imprecise.**
`git revert --no-commit HEAD` reverts the entire last commit, not just the specific file changed in the last iteration. In the autoresearch architecture, rollback should restore only `train.py` to its prior state (e.g., `git checkout HEAD -- train.py`). The current implementation could silently revert unrelated work if the agent is operating in a broader repository.

**Finding 4 — Council peer review is not fully anonymous.**
The peer review key is stored as `{reviewer}_reviews_{reviewee}`, which allows the Chairman to infer authorship of each analysis. A stronger implementation would strip attribution labels before the peer review phase and use opaque IDs, preventing the Chairman from weighting one advisor's synthesis over another based on known persona.

---

## DEFER Registry

### DEFER 1: Synthetic SFT Data Generator

**What to build:** `implementations/sft_data_gen/generator.py`
A Python script using the Anthropic API that generates multi-turn technical dialogues
and emits JSONL with explicit BOS/EOS token delimiters separating prompt from response.
Each record must set `loss_mask=0` for all prompt token positions.

**Infrastructure needed:**
- Anthropic API key (for generation)
- PyTorch data loader (for validation that masking is correct during training)
- nanochat-compatible JSONL format

**Estimated effort:** 1–2 hour session. Data generation half requires no weight access.
Gradient masking validation requires a nanochat or llm.c training environment.

**Source:** karpathy/nanochat Issue #570 (no masking for response loss in SFT training)

---

### DEFER 2: Mid-Training Diagnostic Eval Harness

**What to build:** CI pipeline that samples model checkpoints during training and evaluates
against HumanEval (code) and ChatCORE (conversational alignment) benchmarks.
Purpose: monitor capability acquisition dynamics during the mid-training phase,
before RLHF ramps down the learning rate.

**Infrastructure needed:**
- GPU training environment with checkpoint export
- HumanEval + ChatCORE evaluation frameworks
- Model weight access (explicitly marked True in source table)

**Estimated effort:** Substantial. Not buildable without a training pipeline.
Not actionable in a Claude Code session alone.

**Source:** karpathy/nanochat Issue #68 (midtraining learning rate dynamics bug)

---

### DEFER 3: Diversity Score Utility (from Judge Finding 2)

**What to build:** `implementations/diagnostic/diversity_check.py`
A utility that reads `loop_log.jsonl`, extracts the last N code patches,
and computes pairwise token overlap (or sentence embedding cosine similarity)
between the candidate patch and prior failures.
Returns a float 0.0–1.0 and a GO/STOP signal.

**Infrastructure needed:**
- `loop_log.jsonl` from the autonomous loop (available)
- Optional: `sentence-transformers` for semantic similarity (or simple token overlap as fallback)

**Estimated effort:** 30–60 minutes. Buildable now with no external dependencies.

---

*Session complete: 2026-05-21. 3 implementations delivered (25/25 tests pass), 3 deferred with actionable specs, 4 judge findings logged.*

---

## Session 2 — 2026-05-23

**Directive:** Apply toolkit to a real project. Confirm target, audit, wire modules, run council.

**Target confirmed:** Canvas AI Pipeline (`G:\My Drive\Claude work folder\canvas-ai-pipeline\`)

**Rationale:** 4 Python files across a live multi-step generation pipeline (scraper → AI processor → formatter → Flask web API). Active development (Module 3 shipped as app.py between sessions). Real async/sync architectural decisions pending. Vault agent is already operational — less surface area for new integration. Canvas pipeline hypothesis from program.md confirmed correct.

---

### Agent-Native Surfaces Audit

**canvas_scraper.py**
- ❌ Raises bare `ValueError` / `requests.HTTPError` — no `ErrorCode`, no `recovery_hint`
- ❌ No `health_check()` — no way to verify Canvas API reachability before a run
- ❌ No `state_snapshot` on success — agent cannot confirm what was fetched
- ✅ `paginate()` hides pagination details cleanly

**ai_processor.py**
- ❌ `generate_response()` raises bare exceptions — not AgentResponse-wrapped
- ❌ Single-shot generation — no retry, no quality gate, no diversity detection
- ❌ `main()` uses `input()` — completely non-agentic; blocks scheduled agent callers
- ✅ Prompt templates clearly separated from generation logic

**output_formatter.py**
- ❌ No AgentResponse wrapping — bare `os.path.exists()` guard + print()
- ❌ No state surfacing — caller cannot distinguish "exported 0 files" from "file not found"

**app.py (Flask web API)**
- ❌ `/api/generate` returns `{"error": str(e)}` on failure — no ErrorCode, no recovery_hint
- ❌ Synchronous Anthropic API call blocks HTTP response (2-5s median, unknown p99)
- ❌ No timeout configuration on any of the three boundaries: agent HTTP client, Flask worker, Anthropic SDK
- ✅ `/api/stats` provides aggregate pipeline state
- ✅ `/api/last_refresh` enables staleness detection
- ✅ `/api/refresh` allows on-demand Canvas resync

---

### Top 3 Places Council Review Would Have Caught a Design Mistake

1. **`input()` in `ai_processor.py main()`** — Goldratt would identify this as the constraint that prevents full pipeline automation. Musk would ask "who required this to be interactive?" — no owner → illegitimate requirement. The app.py Flask API was added as a workaround, but the CLI path remains broken for agent use.

2. **Synchronous generation with undefined timeout boundaries** — Agent HTTP client timeout, Flask worker timeout, and Anthropic SDK timeout are three distinct configuration points being treated as one. Kahneman: planning fallacy — 2-5s estimate assumed as worst case, tail risk (~45s+ on overloaded API) not modeled. No retry/circuit-breaker policy exists.

3. **No quality gate between generation and file write** — `generate_response()` output goes directly to `generated_responses.json` with no validation. No length check, no placeholder count check, no diversity check across assignments. Ohno: defect waste — downstream `output_formatter.py` exports docx files that may contain bad content with no detection mechanism.

---

### Task 4: diversity_check Wired into Canvas Pipeline

**Concrete gap exposed:** `ai_processor.py::generate_response()` had no retry logic and no attention collapse detection. On a poor first attempt (formulaic, too short, excessive placeholders), there was no mechanism to detect or recover.

**Implementation:**
- Copied `implementations/diagnostic/diversity_check.py` → `canvas-ai-pipeline/diversity_check.py`
- Split `generate_response()` into `_generate_once()` (raw API call) and `generate_response()` (retry harness)
- Retry harness: first attempt always accepted; on subsequent attempts, `check_diversity()` computes Jaccard similarity against prior attempts; score > 0.75 → STOP signal → retry with explicit variation instruction appended to prompt
- Default `max_retries=3`; caller (app.py) unchanged — signature-compatible

**Verdict:** toolkit module activated against a real gap. No toolkit source code modified — the gap was real in the target project, not in the module.

---

### Task 5: Council Query — Canvas Generate Architecture

**Query:** Should `/api/generate` keep synchronous HTTP response, convert to async job polling, or use a threading.Thread fire-and-forget queue?

**Council verdict (5 new personas — Ohno, Musk, Kahneman, Dalio, Goldratt):**

**CONDITIONAL-GO — Option A (synchronous), with three required conditions:**
1. Measure and document actual Anthropic API p95/p99 latency under real workload
2. Set explicit timeouts on all three boundaries: agent HTTP client, Flask worker, Anthropic SDK call — with consistent values derived from measurement
3. Add observable failure signaling so the agent receives a deterministic error on timeout rather than a hang

**Key Council findings:**
- Options B and C introduce distributed-systems patterns (job queues, polling contracts) with no measured justification — Goldratt's constraint is the absence of p99 data, not the architecture
- The "must not block HTTP" requirement has no owner → phantom constraint → Musk's deletion test fails it
- Option C (threading.Thread) was disqualified: silent thread death produces a job ID that never resolves with no error signal, which is worse than a 30s timeout
- Kahneman flagged availability heuristic driving complexity: the vivid >30s timeout scenario has no measured probability
- The pre-mortem finding: "agent not updated to new polling contract" is a high-probability failure path for Options B and C

**Full verdict saved to:** `council_canvas_generate_arch.json`

---

*Session 2 complete: 2026-05-23. Target confirmed (Canvas AI Pipeline). Audit complete (3 files, 11 gaps catalogued). diversity_check wired into ai_processor.py generation loop. Council ran 5 new domain-grounded personas on a live architectural decision. CONDITIONAL-GO verdict logged.*

## Session 3 — 2026-06-16: Skill vendor + compare & merge

Vendored 5 third-party skills (Rezvani, MIT) into `skills/`: autoresearch-agent,
agenthub, karpathy-coder, self-improving-agent, adversarial-reviewer — all
security-audited before install.

**Compare & merge** (`implementations/SKILL_COMPARISON.md`): head-to-head of
autoresearch-agent vs our autonomous_loop, and agenthub vs our llm_council.

Merged into `autonomous_loop/runner.py` (the autoresearch-agent's best ideas):
- **Direction-aware optimization** (`metric_direction: lower|higher`). Fixes a real
  correctness bug — our loop hardcoded higher-is-better, but Karpathy's own metric
  (val_bpb) is LOWER-is-better. Default stays "higher" for back-compat.
- **Status-aware ledger** — loop_log.jsonl now records keep/discard/rollback + best_so_far.
- **Hard per-iteration timeout** (thread watchdog) — a hung train.py can't block the loop.
Kept OUR advantages: file-only targeted rollback, no-git operation, in-process eval.

Tests: 19/19 pass (11 original + 8 new). End-to-end verified on a lower-is-better
sequence (regression rollback + target stop both correct).

**Next iteration (spec'd, not built):** port the council's Muon-style orthogonal
seeding into agenthub — its N agents currently share one prompt (correlated attempts);
disjoint strategy constraints would widen solution coverage. Highest-value on-theme merge.
