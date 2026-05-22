# Program State — Karpathy Toolkit Active Loop

## Current Directive
Apply the toolkit to a real project. Identify where each module adds concrete value,
implement the integrations, and refine based on what actually breaks.

## Target Project
TBD — awaiting confirmation: Canvas pipeline or Vault agent.

## Active Task
[ ] Confirm target project
[ ] Audit target project against agent-native surfaces checklist
[ ] Identify top 3 places council review would have caught a design mistake
[ ] Wire diversity_check into the project's existing loop/test cycle
[ ] Run one real council query on a live architectural decision in the project

## Success Metric
At least 2 toolkit modules actively integrated into the target project,
with one real council verdict logged and one real diversity check triggered.

## Constraints
- Only modify toolkit implementations if a real use case exposes a gap
- Log every session's work to session_log.md under a new dated entry
- If a module integration requires > 2 hours, break it into a sub-task

## Current Hypothesis
The Canvas API wrapper is the highest-leverage first target for agent_native_surfaces —
Canvas uses session cookies + multi-step navigation, which is exactly the implicit-state
problem the template was designed for.

## Previous Attempts
Session 1 (2026-05-21): Built all 5 modules from scratch. 60/60 tests pass.
                        All 4 judge findings resolved. Toolkit is complete as a standalone.

## Off-Limits
- program.md (human-owned — do not modify during a run)
- session_log.md (append only — do not modify existing entries)

## Next Action
Confirm target project → run audit → log findings → schedule next iteration.
