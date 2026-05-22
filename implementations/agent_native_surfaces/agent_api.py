"""
Agent-Native Legible Surfaces Template
Source: Karpathy Sequoia AI Ascent 2026 (Agent-Native Infrastructure)

The core problem: current web infrastructure relies on implicit state (session cookies,
hidden DOM elements, multi-step navigation). Agents hallucinate their position in
workflows because they cannot reliably infer that implicit state.

This template enforces the contract: every API call surfaces complete, explicit state.
No implicit state. No multi-step navigation. Every error includes a recovery hint.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class ErrorCode(Enum):
    OK = "ok"
    NOT_FOUND = "not_found"
    INVALID_INPUT = "invalid_input"
    STATE_CONFLICT = "state_conflict"
    PERMISSION_DENIED = "permission_denied"
    EXTERNAL_FAILURE = "external_failure"


RECOVERY_HINTS: dict[ErrorCode, str] = {
    ErrorCode.NOT_FOUND: "Check that the resource ID exists via list_all() before accessing.",
    ErrorCode.INVALID_INPUT: "Validate your input against the schema returned by get_schema().",
    ErrorCode.STATE_CONFLICT: "Fetch current state via get_state() and resolve conflicts before retrying.",
    ErrorCode.PERMISSION_DENIED: "Check required permissions via get_permissions() before this operation.",
    ErrorCode.EXTERNAL_FAILURE: "Retry after confirming external service health via health_check().",
}


@dataclass
class AgentResponse:
    """
    Universal return type for all agent-legible API calls.

    Agents must never receive bare exceptions, HTML error pages, or
    human-readable prose as errors. Every failure must include a
    machine-parseable error_code and an actionable recovery_hint.
    """
    ok: bool
    error_code: str = ErrorCode.OK.value
    recovery_hint: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    state_snapshot: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def success(cls, data: dict, state: dict) -> AgentResponse:
        return cls(ok=True, data=data, state_snapshot=state)

    @classmethod
    def failure(cls, code: ErrorCode, state: dict, detail: str = "") -> AgentResponse:
        hint = RECOVERY_HINTS.get(code, "")
        full_hint = f"{hint} Detail: {detail}".strip() if detail else hint
        return cls(
            ok=False,
            error_code=code.value,
            recovery_hint=full_hint,
            state_snapshot=state,
        )


class AgentNativeBase:
    """
    Abstract base class for agent-legible services.

    Contract every subclass must uphold:
    1. Every public method returns AgentResponse — no exceptions escape to the caller
    2. get_state() returns the complete current state as a JSON-serializable dict
    3. Single-call access to all state — no multi-step navigation required
    4. Errors are ErrorCode enum values, never prose strings
    5. Every error includes a recovery_hint the agent can act on immediately
    """

    def get_state(self) -> AgentResponse:
        raise NotImplementedError

    def health_check(self) -> AgentResponse:
        raise NotImplementedError

    def get_schema(self) -> AgentResponse:
        """Return the input schema so agents can validate before calling."""
        raise NotImplementedError


class ExampleResourceService(AgentNativeBase):
    """
    Concrete reference implementation of the agent-native pattern.
    Demonstrates create/get/delete with full state surfacing on every call.
    """

    def __init__(self) -> None:
        self._resources: dict[str, dict] = {}
        self._next_id: int = 0

    def _snapshot(self) -> dict:
        return {
            "resource_count": len(self._resources),
            "resource_ids": list(self._resources.keys()),
        }

    def get_state(self) -> AgentResponse:
        return AgentResponse.success(data={}, state=self._snapshot())

    def health_check(self) -> AgentResponse:
        return AgentResponse.success(data={"status": "healthy"}, state=self._snapshot())

    def get_schema(self) -> AgentResponse:
        schema = {
            "create": {"name": "str (required, non-empty)", "value": "any (required)"},
            "get": {"resource_id": "str (required, format: res_XXXX)"},
            "delete": {"resource_id": "str (required, format: res_XXXX)"},
            "list_all": {},
        }
        return AgentResponse.success(data={"schema": schema}, state=self._snapshot())

    def list_all(self) -> AgentResponse:
        return AgentResponse.success(
            data={"resources": dict(self._resources)},
            state=self._snapshot(),
        )

    def create(self, name: str, value: Any) -> AgentResponse:
        if not name or not isinstance(name, str):
            return AgentResponse.failure(
                ErrorCode.INVALID_INPUT,
                self._snapshot(),
                "name must be a non-empty string",
            )
        resource_id = f"res_{self._next_id:04d}"
        self._next_id += 1
        self._resources[resource_id] = {"name": name, "value": value}
        return AgentResponse.success(
            data={"resource_id": resource_id},
            state=self._snapshot(),
        )

    def get(self, resource_id: str) -> AgentResponse:
        if resource_id not in self._resources:
            return AgentResponse.failure(
                ErrorCode.NOT_FOUND,
                self._snapshot(),
                f"resource_id='{resource_id}' does not exist",
            )
        return AgentResponse.success(
            data=dict(self._resources[resource_id]),
            state=self._snapshot(),
        )

    def delete(self, resource_id: str) -> AgentResponse:
        if resource_id not in self._resources:
            return AgentResponse.failure(
                ErrorCode.NOT_FOUND,
                self._snapshot(),
                f"resource_id='{resource_id}' does not exist",
            )
        del self._resources[resource_id]
        return AgentResponse.success(
            data={"deleted": resource_id},
            state=self._snapshot(),
        )
