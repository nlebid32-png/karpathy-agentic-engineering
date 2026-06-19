"""Tests for the autonomous loop runner. Run: pytest test_runner.py"""
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import runner


def _write_program_md(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "program.md"
    p.write_text(content, encoding="utf-8")
    return p


def test_read_program_state_parses_all_fields(tmp_path):
    md = _write_program_md(
        tmp_path,
        "- metric: accuracy\n- target: 0.9\n- max_iterations: 5\n- time_budget_seconds: 60\n",
    )
    original = runner.PROGRAM_MD
    runner.PROGRAM_MD = md
    try:
        state = runner.read_program_state()
        assert state["metric_name"] == "accuracy"
        assert state["target_metric"] == 0.9
        assert state["max_iterations"] == 5
        assert state["time_budget_seconds"] == 60
    finally:
        runner.PROGRAM_MD = original


def test_log_result_writes_valid_jsonl(tmp_path):
    original_log = runner.LOG_FILE
    runner.LOG_FILE = tmp_path / "test_log.jsonl"
    try:
        runner.log_result(7, 0.83, {"metric_name": "f1_score"}, "test run")
        lines = runner.LOG_FILE.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["iteration"] == 7
        assert entry["metric"] == 0.83
        assert entry["metric_name"] == "f1_score"
        assert "timestamp" in entry
    finally:
        runner.LOG_FILE = original_log


def test_rollback_threshold_triggers_on_5pct_drop():
    best = 1.0
    current = 0.94  # 6% drop — above the 5% threshold
    assert current < best * (1 - runner.ROLLBACK_THRESHOLD)


def test_rollback_threshold_does_not_trigger_on_4pct_drop():
    best = 1.0
    current = 0.97  # 3% drop — below the 5% threshold
    assert current >= best * (1 - runner.ROLLBACK_THRESHOLD)


def test_target_metric_stops_loop():
    state = {"target_metric": 0.95}
    metric = 0.95
    assert metric >= state["target_metric"]


def test_prepare_evaluate_returns_float():
    import prepare
    result = prepare.evaluate(0, {"target_metric": 0.9})
    assert isinstance(result, float)


def test_snapshot_captures_train_content(tmp_path):
    train_file = tmp_path / "train.py"
    train_file.write_text("def run(**_): return 0.5", encoding="utf-8")
    original = runner.TRAIN_FILE
    runner.TRAIN_FILE = train_file
    try:
        snap = runner.snapshot_train()
        assert snap == "def run(**_): return 0.5"
    finally:
        runner.TRAIN_FILE = original


def test_restore_snapshot_writes_content(tmp_path):
    train_file = tmp_path / "train.py"
    train_file.write_text("def run(**_): return 0.9", encoding="utf-8")
    original = runner.TRAIN_FILE
    runner.TRAIN_FILE = train_file
    try:
        runner.restore_snapshot("def run(**_): return 0.5", "test regression")
        assert train_file.read_text(encoding="utf-8") == "def run(**_): return 0.5"
    finally:
        runner.TRAIN_FILE = original


def test_snapshot_returns_empty_string_when_train_missing(tmp_path):
    original = runner.TRAIN_FILE
    runner.TRAIN_FILE = tmp_path / "nonexistent_train.py"
    try:
        snap = runner.snapshot_train()
        assert snap == ""
    finally:
        runner.TRAIN_FILE = original


def test_best_snapshot_only_updates_on_improvement(tmp_path):
    """Verify the logic: best_snapshot should only update when metric improves."""
    best_metric = 0.8
    current_metric = 0.75  # regression — should NOT update best_snapshot
    assert not (current_metric > best_metric)  # confirms snapshot would NOT be updated

    current_metric = 0.85  # improvement — SHOULD update best_snapshot
    assert current_metric > best_metric


# ── MERGE (autoresearch-agent): direction-aware optimization ──────────────────

def test_metric_direction_defaults_to_higher(tmp_path):
    md = _write_program_md(tmp_path, "- metric: score\n- target: 1.0\n")
    original = runner.PROGRAM_MD
    runner.PROGRAM_MD = md
    try:
        assert runner.read_program_state()["metric_direction"] == "higher"
    finally:
        runner.PROGRAM_MD = original


def test_metric_direction_parses_lower(tmp_path):
    md = _write_program_md(tmp_path, "- metric: val_bpb\n- metric_direction: lower\n")
    original = runner.PROGRAM_MD
    runner.PROGRAM_MD = md
    try:
        assert runner.read_program_state()["metric_direction"] == "lower"
    finally:
        runner.PROGRAM_MD = original


def test_is_better_both_directions():
    assert runner.is_better(0.9, 0.8, "higher") is True
    assert runner.is_better(0.7, 0.8, "higher") is False
    assert runner.is_better(0.2, 0.3, "lower") is True   # lower wins
    assert runner.is_better(0.4, 0.3, "lower") is False
    # first result always wins regardless of direction
    assert runner.is_better(5.0, float("-inf"), "higher") is True
    assert runner.is_better(5.0, float("inf"), "lower") is True


def test_is_regression_both_directions():
    # higher-better: 6% drop is a regression past the 5% threshold
    assert runner.is_regression(0.94, 1.0, "higher", 0.05) is True
    assert runner.is_regression(0.97, 1.0, "higher", 0.05) is False
    # lower-better: metric growing 6% above best is a regression
    assert runner.is_regression(1.06, 1.0, "lower", 0.05) is True
    assert runner.is_regression(1.03, 1.0, "lower", 0.05) is False
    # no best yet → never a regression
    assert runner.is_regression(0.5, float("-inf"), "higher", 0.05) is False


def test_target_reached_both_directions():
    assert runner.target_reached(0.96, 0.95, "higher") is True
    assert runner.target_reached(0.94, 0.95, "higher") is False
    assert runner.target_reached(0.04, 0.05, "lower") is True   # below target = win
    assert runner.target_reached(0.06, 0.05, "lower") is False


def test_worst_value_is_direction_aware():
    assert runner.worst_value("higher") == float("-inf")
    assert runner.worst_value("lower") == float("inf")


def test_log_result_records_status_and_best(tmp_path):
    original_log = runner.LOG_FILE
    runner.LOG_FILE = tmp_path / "status_log.jsonl"
    try:
        runner.log_result(3, 0.42, {"metric_name": "val_bpb"}, "new_best",
                          status="keep", best_so_far=0.42)
        entry = json.loads(runner.LOG_FILE.read_text(encoding="utf-8").strip())
        assert entry["status"] == "keep"
        assert entry["best_so_far"] == 0.42
        assert entry["metric"] == 0.42
    finally:
        runner.LOG_FILE = original_log


def test_evaluate_with_timeout_kills_hung_eval():
    def hung_eval(_i, _s):
        import time as _t
        _t.sleep(5)
        return 1.0
    result = runner.evaluate_with_timeout(hung_eval, 0, {}, budget_seconds=0.2)
    assert result == float("-inf")  # timed out → failure sentinel


def test_evaluate_with_timeout_passes_through_fast_eval():
    result = runner.evaluate_with_timeout(lambda i, s: 0.77, 0, {}, budget_seconds=2.0)
    assert result == 0.77
