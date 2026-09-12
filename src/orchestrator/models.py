from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    BLOCKED = "blocked"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


TERMINAL = {Status.SUCCEEDED, Status.FAILED, Status.TIMED_OUT, Status.CANCELLED}


@dataclass(frozen=True)
class TaskSnapshot:
    task_id: str
    label: str
    status: Status
    agent_id: str
    requester_session_key: str
    child_session_key: str | None
    created_at: int
    started_at: int | None
    ended_at: int | None
    last_event_at: int
    progress_summary: str | None
    terminal_summary: str | None
    error: str | None
    raw: dict[str, Any]

    @property
    def parent_id(self) -> str:
        return f"oc:{self.task_id}"

    @property
    def child_id(self) -> str:
        return f"oc:{self.task_id}:agent:{self.agent_id or 'unknown'}"


@dataclass(frozen=True)
class Event:
    event_id: str
    task_id: str
    event_type: str
    status: Status
    timestamp: int
    summary: str
    snapshot: TaskSnapshot

