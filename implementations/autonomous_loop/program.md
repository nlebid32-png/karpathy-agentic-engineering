# Program State

## Task
Optimize the target function defined in train.py.
The agent reads this file at the start of every iteration.
The human edits this file to change the task, variables, or hypothesis.
The agent NEVER modifies this file.

## Constraints
- metric: validation_score
- target: 0.95
- max_iterations: 20
- time_budget_seconds: 600

## Variables to Sweep
- learning_rate: [0.001, 0.01, 0.1]
- batch_size: [16, 32, 64]
- hidden_dim: [128, 256, 512]

## Current Hypothesis
Baseline run. Establish floor metric before sweeping variables.

## Previous Attempts
None yet.

## Success Definition
validation_score >= 0.95 on the held-out evaluation set defined in prepare.py.

## Off-Limits
- prepare.py (immutable oracle — do not touch)
- program.md (human-owned state — do not touch)
- loop_log.jsonl (append-only log — do not modify existing entries)

## Notes
Source: karpathy/autoresearch architecture.
Human updates this file. Agent reads it. Agent only modifies train.py.
