"""
Tests for SFT data generator. Run: pytest test_generator.py

All tests run without an API key — they test structure, validation,
and format correctness rather than generation quality.
"""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from generator import (
    BOS,
    EOS,
    SFTRecord,
    _build_chatml,
    _validate_record,
    generate_dataset,
)


# --- ChatML format tests ---

def test_build_chatml_contains_bos_and_eos():
    messages = [
        {"role": "user", "content": "What is gradient descent?"},
        {"role": "assistant", "content": "It minimizes loss by following the gradient."},
    ]
    chatml = _build_chatml(messages)
    assert chatml.count(BOS) == 2
    assert chatml.count(EOS) == 2


def test_build_chatml_role_appears_after_bos():
    messages = [{"role": "user", "content": "Hello"}]
    chatml = _build_chatml(messages)
    assert f"{BOS}user\n" in chatml


def test_build_chatml_content_between_bos_and_eos():
    messages = [{"role": "assistant", "content": "The answer is 42."}]
    chatml = _build_chatml(messages)
    assert "The answer is 42." in chatml
    bos_pos = chatml.index(BOS)
    eos_pos = chatml.index(EOS)
    assert bos_pos < eos_pos


def test_build_chatml_system_user_assistant_ordering():
    messages = [
        {"role": "system", "content": "You are helpful."},
        {"role": "user", "content": "question"},
        {"role": "assistant", "content": "answer"},
    ]
    chatml = _build_chatml(messages)
    sys_pos = chatml.index("system")
    user_pos = chatml.index("user")
    asst_pos = chatml.index("assistant")
    assert sys_pos < user_pos < asst_pos


# --- Validation tests ---

def test_validate_empty_messages_fails():
    record = SFTRecord(messages=[], loss_mask_roles=["assistant"], chatml="")
    errors = _validate_record(record)
    assert len(errors) > 0


def test_validate_no_assistant_turn_fails():
    messages = [{"role": "user", "content": "question"}]
    chatml = _build_chatml(messages)
    record = SFTRecord(messages=messages, loss_mask_roles=["assistant"], chatml=chatml)
    errors = _validate_record(record)
    assert any("assistant" in e for e in errors)


def test_validate_valid_record_passes():
    messages = [
        {"role": "system", "content": "Be helpful."},
        {"role": "user", "content": "Explain backprop."},
        {"role": "assistant", "content": "Backprop computes gradients via chain rule."},
    ]
    chatml = _build_chatml(messages)
    record = SFTRecord(messages=messages, loss_mask_roles=["assistant"], chatml=chatml)
    errors = _validate_record(record)
    assert errors == []


# --- SFTRecord serialization tests ---

def test_sft_record_serializes_to_valid_json():
    messages = [
        {"role": "user", "content": "Hello"},
        {"role": "assistant", "content": "Hi"},
    ]
    chatml = _build_chatml(messages)
    record = SFTRecord(messages=messages, loss_mask_roles=["assistant"], chatml=chatml)
    line = record.to_jsonl_line()
    parsed = json.loads(line)
    assert parsed["loss_mask_roles"] == ["assistant"]
    assert parsed["token_format"] == "chatml"
    assert "chatml" in parsed


def test_loss_mask_roles_contains_only_assistant():
    record = SFTRecord(
        messages=[{"role": "assistant", "content": "hi"}],
        loss_mask_roles=["assistant"],
        chatml="",
    )
    assert record.loss_mask_roles == ["assistant"]
    # Prompt/user/system roles must NOT be in the loss mask
    assert "user" not in record.loss_mask_roles
    assert "system" not in record.loss_mask_roles


def test_bos_eos_tokens_are_distinct():
    assert BOS != EOS
    assert len(BOS) > 0
    assert len(EOS) > 0


# --- Dataset generation API test (no API key required) ---

def test_generate_dataset_raises_on_missing_api_key(tmp_path):
    original = os.environ.pop("ANTHROPIC_API_KEY", None)
    try:
        with pytest.raises(EnvironmentError, match="ANTHROPIC_API_KEY"):
            generate_dataset(["test topic"], tmp_path / "out.jsonl")
    finally:
        if original is not None:
            os.environ["ANTHROPIC_API_KEY"] = original
