"""Tests for agent-native surfaces template. Run: pytest test_api.py"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from agent_api import AgentNativeBase, AgentResponse, ErrorCode, ExampleResourceService


def test_health_check_returns_ok():
    svc = ExampleResourceService()
    r = svc.health_check()
    assert r.ok is True
    assert r.data["status"] == "healthy"


def test_get_state_contains_required_fields():
    svc = ExampleResourceService()
    r = svc.get_state()
    assert r.ok
    assert "resource_count" in r.state_snapshot
    assert "resource_ids" in r.state_snapshot


def test_create_and_get_roundtrip():
    svc = ExampleResourceService()
    create_r = svc.create("widget", {"color": "blue"})
    assert create_r.ok
    rid = create_r.data["resource_id"]

    get_r = svc.get(rid)
    assert get_r.ok
    assert get_r.data["name"] == "widget"
    assert get_r.data["value"] == {"color": "blue"}


def test_get_missing_resource_returns_error_with_recovery_hint():
    svc = ExampleResourceService()
    r = svc.get("nonexistent_id")
    assert r.ok is False
    assert r.error_code == ErrorCode.NOT_FOUND.value
    assert len(r.recovery_hint) > 0
    assert "list_all" in r.recovery_hint


def test_invalid_name_returns_structured_error():
    svc = ExampleResourceService()
    r = svc.create("", "value")
    assert r.ok is False
    assert r.error_code == ErrorCode.INVALID_INPUT.value
    assert len(r.recovery_hint) > 0


def test_all_responses_serialize_to_json():
    svc = ExampleResourceService()
    r = svc.create("item", 42)
    parsed = json.loads(r.to_json())
    assert parsed["ok"] is True
    assert "state_snapshot" in parsed
    assert "data" in parsed


def test_state_snapshot_updates_after_create():
    svc = ExampleResourceService()
    before = svc.get_state().state_snapshot["resource_count"]
    svc.create("item_a", 1)
    svc.create("item_b", 2)
    after = svc.get_state().state_snapshot["resource_count"]
    assert after == before + 2


def test_delete_removes_from_state():
    svc = ExampleResourceService()
    rid = svc.create("temp", "x").data["resource_id"]
    svc.delete(rid)
    r = svc.get(rid)
    assert r.ok is False
    assert r.error_code == ErrorCode.NOT_FOUND.value


def test_every_failure_includes_state_snapshot():
    svc = ExampleResourceService()
    r = svc.delete("bad_id")
    assert r.ok is False
    assert isinstance(r.state_snapshot, dict)
    assert "resource_count" in r.state_snapshot


def test_get_schema_returns_all_methods():
    svc = ExampleResourceService()
    r = svc.get_schema()
    assert r.ok
    schema = r.data["schema"]
    assert "create" in schema
    assert "get" in schema
    assert "delete" in schema
