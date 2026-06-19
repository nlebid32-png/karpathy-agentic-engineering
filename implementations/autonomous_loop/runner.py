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
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
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
        # MERGE (autoresearch-agent): direction-aware optimization. Karpathy's own
        # metric (val_bpb) is lower-is-better; defaults to "higher" for back-compat.
        "metric_direction": "higher",
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
        elif line.startswith("- metric_direction:"):
            direction = line.split(":", 1)[1].strip().lower()
            if direction in ("higher", "lower"):
                state["metric_direction"] = direction
    return state


def is_better(metric: float, best: float, direction: str) -> bool:
    """Direction-aware improvement check. MERGE: autoresearch-agent is_improvement()."""
    if best in (float("-inf"), float("inf")):
        return True
    return metric < best if direction == "lower" else metric > best


def is_regression(metric: float, best: float, direction: str, threshold: float) -> bool:
    """Direction-aware regression past the rollback threshold."""
    if best in (float("-inf"), float("inf")):
        return False
    if direction == "lower":
        return metric > best * (1 + threshold)  # worse = larger
    return metric < best * (1 - threshold)


def target_reached(metric: float, target: float, direction: str) -> bool:
    """Direction-aware stop condition."""
    return metric <= target if direction == "lower" else metric >= target


def log_result(
    iteration: int, metric: float, state: dict, notes: str = "",
    status: str = "", best_so_far: "float | None" = None,
) -> None:
    # MERGE (autoresearch-agent results.tsv): record the verdict + running best,
    # not just the raw metric. status in {keep, discard, rollback}.
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "iteration": iteration,
        "metric": metric,
        "metric_name": state.get("metric_name", "score"),
        "status": status,
        "best_so_far": best_so_far,
        "notes": notes,
    }
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def evaluate_with_timeout(eval_fn, iteration: int, state: dict, budget_seconds: float) -> float:
    """
    Run the in-process evaluation under a hard wall-clock timeout.
    MERGE (autoresearch-agent): a hung train.py no longer blocks the loop forever.
    Returns the metric, or float("-inf") on timeout (a failed attempt — the
    direction-aware regression logic in run_loop then rolls back).
    Note: a worker thread can't be force-killed in CPython; we abandon it and move
    on. Keep train.py cooperative (no uninterruptible C calls) for clean kills.
    """
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(eval_fn, iteration, state)
        try:
            return float(future.result(timeout=budget_seconds))
        except FutureTimeout:
            print(f"[TIMEOUT] iteration {iteration} exceeded {budget_seconds:.0f}s — discarding.")
            return float("-inf")


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


def worst_value(direction: str) -> float:
    """Direction-aware 'no result yet' sentinel: +inf for lower-better, -inf for higher."""
    return float("inf") if direction == "lower" else float("-inf")


def run_loop() -> None:
    state = read_program_state()
    direction = state["metric_direction"]
    print(
        f"[LOOP START] metric={state['metric_name']} ({direction}-is-better) "
        f"target={state['target_metric']} "
        f"max_iter={state['max_iterations']} "
        f"budget={state['time_budget_seconds']}s"
    )

    import prepare

    best_metric = worst_value(direction)
    best_snapshot = snapshot_train()
    start_time = time.monotonic()

    for i in range(state["max_iterations"]):
        elapsed = time.monotonic() - start_time
        if elapsed > state["time_budget_seconds"]:
            print(f"[LOOP] Time budget exhausted at iteration {i}. Stopping.")
            break

        # Re-read program.md every iteration so human can update mid-run
        state = read_program_state()
        direction = state["metric_direction"]

        # Snapshot train.py before running so we can restore on regression
        pre_iter_snapshot = snapshot_train()

        # MERGE: hard per-iteration timeout — a hung eval no longer blocks the loop.
        iter_start = time.monotonic()
        metric = evaluate_with_timeout(
            prepare.evaluate, i, state, float(state["time_budget_seconds"])
        )
        iter_elapsed = time.monotonic() - iter_start

        # MERGE: any ±inf is a failed attempt (crash/timeout/parse-fail), regardless
        # of direction — a real metric is never infinite.
        failed = metric in (float("inf"), float("-inf"))

        if failed:
            print(f"[ITER {i:03d}] FAILED (crash/timeout)  ({iter_elapsed:.1f}s)")
            log_result(i, metric, state, notes="eval_failed",
                       status="discard", best_so_far=best_metric)
            restore_snapshot(best_snapshot, f"iteration {i} failed to produce a metric")
            continue

        print(f"[ITER {i:03d}] {state['metric_name']}={metric:.4f}  ({iter_elapsed:.1f}s)")

        if is_regression(metric, best_metric, direction, ROLLBACK_THRESHOLD):
            log_result(i, metric, state, notes=f"regressed_from_{best_metric:.4f}",
                       status="rollback", best_so_far=best_metric)
            restore_snapshot(
                best_snapshot,
                f"metric {metric:.4f} regressed from best {best_metric:.4f}",
            )
        elif is_better(metric, best_metric, direction):
            best_metric = metric
            best_snapshot = pre_iter_snapshot  # lock in the snapshot that achieved best
            log_result(i, metric, state, notes="new_best",
                       status="keep", best_so_far=best_metric)
        else:
            # within threshold, not a new best — kept but not recorded as best
            log_result(i, metric, state, notes="no_improvement",
                       status="discard", best_so_far=best_metric)

        if target_reached(metric, state["target_metric"], direction):
            print(f"[LOOP] Target reached: {metric:.4f} ({direction}) vs {state['target_metric']}")
            break

    print(f"[LOOP END] Best {state['metric_name']}: {best_metric:.4f}")


if __name__ == "__main__":
    run_loop()
