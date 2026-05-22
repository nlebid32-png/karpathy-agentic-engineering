# Karpathy Agentic Engineering Playbook

Operating manual for every Claude Code session.
Source: Sequoia AI Ascent 2026 | karpathy/autoresearch | "How I Use LLMs" | llm-council | nanochat

---

## Core Axioms

1. **The context window is RAM.** Manage it with OS-level precision. Bloat = hallucination.
2. **Verification is architecture, not a prompt clause.** "Check your work" does nothing. External test harnesses do everything.
3. **Taste scales through constraints, not instructions.** Encode architectural rules into schemas, CI, and linting — not prose.
4. **LLM intelligence is jagged.** Route RL-heavy tasks to autonomous loops. Route aesthetic/spatial tasks to human checkpoints.
5. **The spec is the new source code.** Treat program.md with the same rigor as production Python.
6. **You can outsource execution. You cannot outsource understanding.** The human defines what correctness means.

---

## The Human-as-Orchestrator / Agent-as-Executor Model

```
HUMAN                               AGENT
------                              ------
Writes program.md                   Reads program.md every iteration
Defines success metric              Runs against prepare.py (immutable)
Designs test harnesses              Edits train.py / implementation files
Builds verification infrastructure  Executes within constraints
Approves council verdicts           Generates candidates in parallel
Updates architectural constraints   Logs all attempts and metrics
Calls rollback when needed          Proposes next hypothesis
```

**Critical separation:** The human never touches implementation files during a run.
The agent never touches program.md or prepare.py.

---

## Phase 1: Map the Jagged Intelligence Coverage

Before starting any project, create a domain coverage map. Every module classified:

| Module Type | Verifiability | Autonomy Level | Human Checkpoint |
|-------------|--------------|----------------|-----------------|
| Algorithm logic | High — unit tests | Full autonomy | On regression only |
| Data pipelines | High — schema + count assertions | Full autonomy | On schema change |
| API integrations | Medium — contract tests | Supervised | On new endpoints |
| Database schemas | High — FK constraints enforce rules | Full autonomy | On migrations |
| Auth/security logic | Medium — static analysis | Supervised | Always |
| UI/UX layout | Low — no automated signal | Prototype only | Always |
| Spatial/visual reasoning | Very low | Never delegate | Always |

**Rule:** If you cannot write a pass/fail condition in code, the task requires a human checkpoint.
Do not delegate fully without a verification mechanism.

---

## Phase 2: Construct the Verification Infrastructure

Build this **before** writing any implementation code.

```
tests/
  unit/         # function-level, < 1s each
  integration/  # service-level, < 30s total
  contract/     # API shape validation, < 5s

ci/
  pre_commit    # lint + unit suite
  on_merge      # full suite + integration
```

**The generation-verification loop:**

```
Agent writes code → test suite runs → PASS/FAIL returned to agent → agent iterates
```

Agent must never report completion to the human until all tests return PASS.
The test suite is the oracle. The human reading the code is not the oracle — that bottleneck
operates at human reading speed.

**Contradiction to avoid:** Adding "carefully check your work" to a prompt is an architectural
failure. LLMs are probabilistic token predictors. They cannot reliably execute deterministic
internal logic checks on their own output. The check must be external and programmatic.

---

## Phase 3: Encode Taste into Systemic Constraints

Move every architectural rule from human memory into code.

| Rule | Wrong (prose instruction) | Right (hard constraint) |
|------|--------------------------|------------------------|
| Use UUIDs, not string IDs | "please use UUIDs" | DB column type = UUID + FK constraint |
| No N+1 queries | "be careful with loops" | Query count assertion in integration test |
| Auth required on all endpoints | "remember to check auth" | Middleware that throws 401, no exceptions |
| snake_case everywhere | "name things consistently" | Linter rule enforced in CI |
| No magic numbers | "avoid magic numbers" | mypy + named constants module |
| Validated input at boundaries | "validate user input" | Pydantic model at every entry point |

**The wall principle:** Claude learns the "taste" of a system by slamming into hard-coded
systemic walls that force it to route around bad architectural decisions.
Polite instructions do not survive context resets.

---

## Phase 4: Orchestrate via Context State Documents

Code is increasingly a byproduct of the system specification.

**program.md schema (use this exact structure):**

```markdown
## Current Task
[One clear, bounded task. If it takes more than one sentence, split it.]

## Success Metric
[Exact scalar threshold or pass/fail condition expressible in code]

## Constraints
- [hard constraint 1]
- [hard constraint 2]

## Off-Limits
- [files the agent must not modify]

## Current Hypothesis
[What this iteration is testing]

## Previous Attempts
[What failed and why — one line per attempt]
```

**Session initialization command (use verbatim):**

```
Read program.md and execute the next experimental loop.
Do not ask for clarification.
Rely entirely on the state parameters defined in the markdown file.
Report only: metric achieved, files changed, next hypothesis.
```

---

## Phase 5: Execute the Asynchronous Review Cycle

For any significant architectural decision, run the LLM Council before merging.

```
1. Write decision query (one focused question)
2. council.py dispatches to 5 orthogonal advisors (parallel, ~30s)
3. Anonymous peer review phase (circular, each reviews one other)
4. Chairman synthesizes into GO / NO-GO / CONDITIONAL-GO verdict
5. Human reviews Chairman output and makes the final call
```

**When to invoke the council:**
- Before merging a PR with schema changes
- Before adopting a new dependency
- Before selecting between architecture patterns
- Before any irreversible infrastructure decision (DB choice, auth design, API contracts)

**When NOT to invoke:** Routine feature implementation, bug fixes, refactors within
an already-decided architecture.

---

## The Jagged Ghost Handling Protocol

**"Jagged ghost" = the agent confidently does the wrong thing**

The most dangerous failure mode is not an obvious crash — it is an agent that passes all tests
but is subtly wrong in ways that only become visible in production.

### Detection signals

- Same error appears 3+ times with minor variation (attention collapse)
- Output diverges from spec without the agent acknowledging it
- Agent modifies off-limits files
- Tests pass but manual inspection shows the behavior is wrong
- Agent produces verbose explanations instead of code (hedging signal)

### Response protocol

1. **STOP.** Do not continue the current loop.
2. **Wipe context.** Open a completely fresh session.
3. **Isolate the failing unit.** Extract the minimum reproduction: one file + one error trace.
4. **Fresh session prompt:**
   ```
   Here is a fresh execution environment. No prior context.
   FILE: [filename]
   [exact file content]
   ERROR:
   [exact error trace]
   Diagnose the logic failure. Rewrite the function. No prose.
   ```
5. **Do NOT reference prior failed attempts** in the new context window.
6. **Diversity check:** Before applying the new patch, compare its structure to prior failed attempts. If similarity is high, escalate to council review.

### Attention collapse check prompt

```
Before writing any code: read the last [N] failed attempts in the error log.
Describe the common failure pattern in one sentence.
Is your current proposed solution structurally similar?
- If YES: assume attention collapse. Discard the approach.
  Return to first principles. Propose a fundamentally different architecture.
- If NO: proceed.
```

---

## Context Window Management Rules

| Situation | Action |
|-----------|--------|
| Debugging session with 3+ failed attempts | Wipe context, fresh session |
| More than 8 files loaded in context | Load only the target file + error |
| Conversation exceeds 40 turns | Summarize state to program.md, fresh session |
| Error stack trace is in the context | Remove before next prompt |
| Multiple unrelated tasks in one session | Split into separate sessions |
| Agent produces hedging language | Likely context pollution — reset |

**Load from disk (selective Read), never inject entire codebase.**
The context window is RAM. Treat it as a limited resource.

---

## Verification Checklist (per implementation)

Before marking any build complete:

- [ ] Pass condition is defined in code, not prose
- [ ] A test exists that would fail if the implementation is wrong
- [ ] Tests pass deterministically across multiple runs (no flakiness)
- [ ] Agent did not modify off-limits files
- [ ] Output behavior matches spec, not just test assertions
- [ ] Variable names use full English dictionary words (BPE efficiency — no abbreviations)
- [ ] Judge prompt applied and gaps logged

---

## Session Opening Protocol

Every new Claude Code session begins with this sequence:

```
1. Read program.md — current task, constraints, off-limits files
2. Read relevant test files — what does "done" look like in code?
3. State the current metric and its target value
4. State the last failed attempt and why it failed
5. State the current hypothesis for this iteration
6. Execute
```

---

## BPE Naming Convention Rule

Add this to every project's CLAUDE.md:

```
NAMING RULE: All variable, function, and class names must use standard English
dictionary words. Never use: cfg, mgr, util, proc, calc, usr, btn, idx, arr,
parseHTTPReqURL, UsrAuthMod. Always use: configuration, manager, utility,
calculate, user, button, index, array, parseHttpRequestUrl, UserAuthModule.

Reason: rare abbreviations are shattered into fragmented BPE subword tokens,
reducing the attention mechanism's ability to track semantic meaning across
long sequences.
```

---

## The Understanding Moat

> "You can outsource your thinking, but you can't outsource your understanding."

**Human cognitive bandwidth is reserved for:**
- Defining what "correct" means in this specific domain
- Building the verification infrastructure (tests, CI, contracts)
- Setting architectural constraints and encoding them as hard walls
- Interpreting council verdicts and making final GO/NO-GO calls
- Identifying when the agent has hit a jagged ghost failure mode

**The agent handles:**
- Implementation within defined constraints
- Test suite execution and iteration
- Multiple candidate generation
- Metric logging and hypothesis tracking
- Rollback on regression

The developer's role has not been eliminated.
It has shifted from typing syntax to operating as the systems architect
of an OS where the LLM is the CPU and the context window is the RAM.

---

*Last updated: 2026-05-21. Update this file when session learnings contradict or extend any section.*
