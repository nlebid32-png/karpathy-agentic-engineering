"""
Immutable evaluation harness.
Source: karpathy/autoresearch (prepare.py / val_bpb evaluation script)

This file is the oracle. The agent edits train.py; this file measures the result.
The separation is critical: the agent cannot game the metric by modifying this file.
"""
import sys
from pathlib import Path


def evaluate(iteration: int, config: dict) -> float:
    """
    Load train.py fresh each iteration and return a scalar metric.
    Direction (higher- vs lower-is-better) is set by `metric_direction` in
    program.md and interpreted by runner.py; this oracle just returns the number.
    Returns -inf on any error — runner.py treats any ±inf as a failed attempt.

    Pass condition: runner.py's target_reached() vs config['target_metric'].
    """
    try:
        if "train" in sys.modules:
            del sys.modules["train"]

        sys.path.insert(0, str(Path(__file__).parent))
        import train

        result = train.run(iteration=iteration, config=config)
        assert isinstance(result, (int, float)), "train.run() must return a scalar metric"
        return float(result)

    except Exception as exc:
        print(f"[EVAL ERROR] iteration={iteration}: {exc}")
        return float("-inf")
