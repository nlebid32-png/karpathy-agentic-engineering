"""
Autonomous Experimentation Loop
Source: karpathy/autoresearch; Sequoia AI Ascent 2026

Reads program.md at the start of every iteration, executes the evaluation harness,
logs metrics to loop_log.jsonl, and restores train.py from snapshot on metric regression.

Usage:
    python runner.py

The human edits program.md to change task/constraints between runs.
The agent edits train.py only.

Rollback design: before each iteration the runner snapshots train.py content in memory.
If the metric degrades past the threshold, train.py is restored from that snapshot —
targeting only the agent-editable file rather than the entire git HEAD.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROGRAM_MD = Path(__file__).parent / "program.md"
TRAIN_FILE = Path(__file__).parent / "train.py"
LOG_FILE = Path(__file__).parent / "loop_log.jsonl"
ROLLBACK_THRESHOLD = 0.05  # 5% degradation triggers restore


def read_program_state() -> dict:
    """Parse program.md for loop parameters."""
    content = PROGRAM_MD.read_text(encoding="utf-8")
    state: dict = {
        "metric_name": "score",
        "target_metric": 1.0,
        "max_iterations": 10,
        "time_budget_seconds": 300,
    }
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("- metric:"):
            state["metric_name"] = line.split(":", 1)[1].strip()
        elif line.startswith("- target:"):
            state["target_metric"] = float(line.split(":", 1)[1].strip())
        elif line.startswith("- max_iterations:"):
            state["max_iterations"] = int(line.split(":", 1)[1].strip())
        elif line.startswith("- time_budget_seconds:"):
            state["time_budget_seconds"] = int(line.split(":", 1)[1].strip())
    return state


def log_result(iteration: int, metric: float, state: dict, notes: str = "") -> None:
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "iteration": iteration,
        "metric": metric,
        "metric_name": state.get("metric_name", "score"),
        "notes": notes,
    }
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def snapshot_train() -> str:
    """Capture current train.py content before each iteration."""
    if TRAIN_FILE.exists():
        return TRAIN_FILE.read_text(encoding="utf-8")
    return ""


def restore_snapshot(snapshot: str, reason: str) -> None:
    """
    Restore train.py from snapshot captured before the regressing iteration.
    Targets only the agent-editable file — no git operations on unrelated files.
    """
    print(f"[ROLLBACK] {reason}. Restoring train.py to last best state.")
    TRAIN_FILE.write_text(snapshot, encoding="utf-8")
    print("[ROLLBACK] train.py restored.")


def run_loop() -> None:
    state = read_program_state()
    print(
        f"[LOOP START] metric={state['metric_name']} "
        f"target={state['target_metric']} "
        f"max_iter={state['max_iterations']} "
        f"budget={state['time_budget_seconds']}s"
    )

    import prepare

    best_metric = float("-inf")
    best_snapshot = snapshot_train()
    start_time = time.monotonic()

    for i in range(state["max_iterations"]):
        elapsed = time.monotonic() - start_time
        if elapsed > state["time_budget_seconds"]:
            print(f"[LOOP] Time budget exhausted at iteration {i}. Stopping.")
            break

        # Re-read program.md every iteration so human can update mid-run
        state = read_program_state()

        # Snapshot train.py before running so we can restore on regression
        pre_iter_snapshot = snapshot_train()

        iter_start = time.monotonic()
        metric = prepare.evaluate(i, state)
        iter_elapsed = time.monotonic() - iter_start

        print(f"[ITER {i:03d}] {state['metric_name']}={metric:.4f}  ({iter_elapsed:.1f}s)")
        log_result(i, metric, state)

        if best_metric != float("-inf") and metric < best_metric * (1 - ROLLBACK_THRESHOLD):
            restore_snapshot(
                best_snapshot,
                f"metric {metric:.4f} regressed from best {best_metric:.4f}",
            )
        else:
            if metric > best_metric:
                best_metric = metric
                best_snapshot = pre_iter_snapshot  # lock in the snapshot that achieved best

        if metric >= state["target_metric"]:
            print(f"[LOOP] Target reached: {metric:.4f} >= {state['target_metric']}")
            break

    print(f"[LOOP END] Best {state['metric_name']}: {best_metric:.4f}")


if __name__ == "__main__":
    run_loop()
