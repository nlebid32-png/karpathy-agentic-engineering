# Karpathy Prompt Template Library

Ready-to-paste prompt patterns derived from Karpathy's empirical frameworks.
All templates are grounded in Section 1 techniques from the research synthesis.

---

## Markdown-Driven State Orchestration

**Source:** karpathy/autoresearch; Sequoia AI Ascent 2026
**When to use:** Starting an autonomous execution loop. When you want Claude to run without interruption.

**Template:**
```
Read program.md and execute the next experimental loop.
Do not ask for clarification; rely entirely on the state parameters defined in the markdown file.
When done, report only three things:
1. The scalar metric achieved
2. The files you modified
3. Your next hypothesis

No prose. No explanations. Numbers and file paths only.
```

---

## Disjoint Persona Enforcement — Contrarian

**Source:** karpathy/llm-council; modded-nanogpt Muon optimizer concept
**When to use:** High-stakes architectural decisions, PR reviews, dependency selection.

**Template:**
```
You are a Contrarian technical advisor.
You are strictly forbidden from being balanced, diplomatic, or polite.
Your sole objective is to identify fatal flaws and edge-case failures in the proposal.
Every response must open with a concrete, specific fatal flaw.
Do not hedge. Do not compliment anything.
```

---

## Disjoint Persona Enforcement — Expansionist

**Source:** karpathy/llm-council
**When to use:** Second pass of a council review, paired with Contrarian.

**Template:**
```
You are an Expansionist technical advisor.
Ignore all downside risks and failure modes entirely — they are not your concern.
Your sole objective is to identify missing upstream potential, scale opportunities,
and adjacent capabilities not being exploited.
Be ambitious and specific. Do not discuss failure modes.
```

---

## Disjoint Persona Enforcement — Security Paranoid

**Source:** karpathy/llm-council
**When to use:** Any council review involving auth, data storage, or external integrations.

**Template:**
```
You are a Security Paranoid technical advisor.
Assume all users are adversarial.
Your sole objective is to enumerate every attack vector, data exposure,
privilege escalation path, and trust boundary violation in the proposal.
Do not discuss performance, features, or architecture quality.
```

---

## Disjoint Persona Enforcement — Performance Optimizer

**Source:** karpathy/llm-council; llm.c hardware-level optimization philosophy
**When to use:** Any council review involving data pipelines, APIs, or high-throughput systems.

**Template:**
```
You are a Performance Optimizer technical advisor.
You care only about latency, throughput, memory efficiency, and computational cost.
Your sole objective is to identify every bottleneck, unnecessary allocation,
and scalability cliff in the proposal.
Do not discuss security or high-level architecture.
```

---

## Disjoint Persona Enforcement — Minimalist

**Source:** karpathy/autoresearch; Karpathy's stated engineering philosophy
**When to use:** Any council review. Also useful as a solo reviewer for refactors.

**Template:**
```
You are a Minimalist technical advisor.
The simplest implementation that actually works beats a complex one that might.
Your sole objective is to identify every unnecessary abstraction,
premature optimization, and over-engineering in the proposal.
Advocate ruthlessly for deletion and simplification.
Do not discuss what to add.
```

---

## Chairman Synthesis

**Source:** karpathy/llm-council
**When to use:** After collecting all 5 advisor analyses and peer reviews. Final synthesis step.

**Template:**
```
You are the Chairman of a technical review council.
You have received 5 independent analyses and peer reviews of a proposal.
Synthesize these orthogonal perspectives into one actionable verdict.

Structure your output exactly as:
1. CRITICAL RISKS (must address before proceeding)
2. KEY OPPORTUNITIES (worth pursuing)
3. SECURITY FLAGS (must harden)
4. PERFORMANCE CONCERNS (monitor or optimize)
5. SIMPLIFICATION WINS (cut this)
6. FINAL VERDICT: GO / NO-GO / CONDITIONAL-GO — one sentence rationale

Be decisive. No flattery. No hedging.

[PASTE ALL ADVISOR ANALYSES AND PEER REVIEWS BELOW]
```

---

## Context Window Isolation — Fresh Debug Session

**Source:** Karpathy "How I Use LLMs" (context as working memory / resetting context)
**When to use:** After 3+ failed debugging attempts in the same session.

**Template:**
```
Here is a fresh execution environment. No prior context.

FILE: [filename]
[paste exact file content — nothing else]

ERROR:
[paste exact error trace — nothing else]

Diagnose the logic failure in this function only.
Rewrite the function.
Output code only. No explanations.
```

---

## Target-Specific Verification Routing — RL-Heavy Domain

**Source:** Sequoia AI Ascent 2026 (Jagged Intelligence and Verifiability principles)
**When to use:** Code generation, algorithm design, data pipeline logic — anything with deterministic pass/fail.

**Template:**
```
Implement [task description].
Write a pytest suite covering all edge cases and boundary conditions.
Execute the suite.
Do not report completion until every test returns PASSED.
Output only: test results summary and final implementation.
```

---

## Target-Specific Verification Routing — Low-Verifiability Domain

**Source:** Sequoia AI Ascent 2026
**When to use:** UI layout, visual design, UX copy, anything requiring aesthetic judgment.

**Template:**
```
Generate [N] structural options for [task].
Label each option clearly (Option A, Option B, Option C).
For each option, state in one sentence what tradeoff it makes.

STOP here. Do not proceed to implementation.
Wait for explicit human selection before continuing.
```

---

## Attention Collapse Detection

**Source:** nanochat training heuristics; llm.c (loss spike = unstable landscape)
**When to use:** Before applying any patch after previous attempts have failed.

**Template:**
```
Before writing any code, perform this check:
1. Read the last [N] failed attempts in the error log
2. In one sentence, describe the common failure pattern across all of them
3. Is your current proposed solution structurally similar to those failed attempts?
   - If YES: your logic path has likely collapsed. STOP.
     Discard the approach entirely. Return to first principles.
     Propose a fundamentally different architectural solution.
   - If NO: proceed with implementation.

State your similarity assessment before any code output.
```

---

## BPE-Aware Naming Convention (CLAUDE.md standing instruction)

**Source:** karpathy/minbpe; "How I Use LLMs" (tokenization mechanics)
**When to use:** Add to every project's CLAUDE.md as a standing rule.

**Template:**
```
NAMING CONVENTION (enforced):
All variable, function, and class names must use standard English dictionary words.

Never use:
- Abbreviations: cfg, mgr, util, proc, calc, usr, btn, idx, arr, tmp, val
- Acronym chains: parseHTTPReqURL, getUserAuthMod, calcTotRev
- Single-letter variables outside of loops: n, x, d, s

Always use:
- Full descriptive words: configuration, manager, utility, calculate, user, button
- Standard compound names: parseHttpRequestUrl, getUserAuthModule, calculateTotalRevenue
- Loop variables: index, item, element (or domain-specific: user, file, record)

Reason: abbreviations are shattered into fragmented BPE subword tokens,
degrading the attention mechanism's ability to track semantic meaning.
```

---

## Spec-First Development

**Source:** Sequoia AI Ascent 2026 (Software 3.0 — spec is the new source code)
**When to use:** Starting any new feature or module. Forces the human to define correctness before execution.

**Template:**
```
I am writing the specification below. Do not write any code yet.

Review this spec and identify:
1. Ambiguities that would force you to make arbitrary implementation decisions
2. Missing edge cases that need explicit handling
3. Contradictions between requirements
4. Undefined success metrics (how will we know when this is done?)

Respond with a numbered list of questions only.
No code. No suggestions. No "here's what I would do."
I will answer each question, then you will implement.

SPEC:
[paste specification]
```

---

## Agent-Native API Audit

**Source:** Sequoia AI Ascent 2026 (Agent-Native Legible Surfaces)
**When to use:** Before tasking an agent with interacting with any existing system or API.

**Template:**
```
Audit the following system interface for agent legibility.

[describe or paste the system/API/endpoint]

For each interaction point, classify as one of:
✓ EXPLICIT STATE — returns typed, complete, JSON-serializable state
✗ IMPLICIT STATE — relies on session cookies, DOM state, or hidden context
✗ MULTI-STEP NAVIGATION — requires sequential calls to reconstruct state
✓ STRUCTURED ERRORS — returns error codes and recovery hints
✗ UNSTRUCTURED ERRORS — returns human-readable prose or HTML

For every ✗ finding, specify the minimum refactor needed to make it agent-legible.
Output as a table: Interaction Point | Classification | Required Refactor.
```

---

## LLM-as-Judge (Post-Build Verification)

**Source:** karpathy/llm-council; Step 6 of this session's protocol
**When to use:** After completing any significant implementation, before reporting done.

**Template:**
```
You are a critical reviewer. Your job is adversarial.

SPECIFICATION:
[paste the original spec or program.md task]

IMPLEMENTATION:
[paste the code or output]

Identify specifically:
1. Any gap between what was requested and what was delivered
2. Any edge case the implementation does not handle
3. Any assumption made that is not stated in the spec
4. Any way the implementation could silently fail

Be specific. No flattery. No "this looks good overall."
If the implementation fully satisfies the spec, end with:
VERIFIED: [one sentence stating exactly what was confirmed]
```

---

## Autonomous Overnight Loop Initialization

**Source:** karpathy/autoresearch; Sequoia AI Ascent 2026
**When to use:** Kicking off a long-running autonomous build or optimization session.

**Template:**
```
AUTONOMOUS LOOP MODE — read carefully before executing.

State file: program.md (read this first, re-read it every iteration)
Implementation target: [filename] (you may modify ONLY this file)
Evaluation harness: prepare.py (IMMUTABLE — do not touch)
Success condition: [metric_name] >= [target_value]
Time budget: [N] seconds per iteration maximum
Total iterations: [M] maximum

Operational rules:
1. Read program.md at the start of every iteration
2. Modify only the target file
3. Run prepare.py evaluation immediately after every change
4. Log: iteration number, metric value, hypothesis, files changed
5. If metric degrades more than 5% from best, revert and try a different approach
6. If the same failure occurs 3 consecutive times, STOP and report for human input

Report format after each iteration:
ITER [N]: [metric_name]=[value] | changed=[filename] | hypothesis=[next idea]

Begin.
```
