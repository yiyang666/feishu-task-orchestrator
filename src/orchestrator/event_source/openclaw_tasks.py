from __future__ import annotations

import hashlib
import json
import subprocess
from typing import Iterable

from orchestrator.models import Event, Status, TaskSnapshot
from orchestrator.presentation import latest_progress


STATUS_MAP = {
    "queued": Status.QUEUED,
    "running": Status.RUNNING,
    "blocked": Status.BLOCKED,
    "succeeded": Status.SUCCEEDED,
    "failed": Status.FAILED,
    "timed_out": Status.TIMED_OUT,
    "cancelled": Status.CANCELLED,
    "canceled": Status.CANCELLED,
}


class OpenClawTasksSource:
    """Polling adapter over the durable OpenClaw task ledger."""

    def __init__(
        self,
        binary: str = "openclaw",
        session_prefixes: tuple[str, ...] = (),
        automation_source_ids: tuple[str, ...] = (),
    ):
        self.binary = binary
        self.session_prefixes = session_prefixes
        self.automation_source_ids = automation_source_ids

    def fetch(self) -> list[TaskSnapshot]:
        proc = subprocess.run(
            [self.binary, "tasks", "list", "--json"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        payload = json.loads(proc.stdout)
        return [self._snapshot(item) for item in payload.get("tasks", []) if self._accept(item)]

    def _accept(self, item: dict) -> bool:
        if item.get("runtime") == "cron" and item.get("sourceId") in self.automation_source_ids:
            return True
        if not self.session_prefixes:
            return True
        keys = (
            item.get("requesterSessionKey", ""),
            item.get("ownerKey", ""),
            item.get("childSessionKey", ""),
        )
        return any(any(key.startswith(prefix) for prefix in self.session_prefixes) for key in keys)

    @staticmethod
    def _snapshot(item: dict) -> TaskSnapshot:
        status = STATUS_MAP.get(item.get("status", "queued"), Status.QUEUED)
        return TaskSnapshot(
            task_id=item["taskId"],
            label=item.get("label") or item.get("task") or item["taskId"],
            status=status,
            agent_id=item.get("agentId") or item.get("requesterAgentId") or "unknown",
            requester_session_key=item.get("requesterSessionKey") or "",
            child_session_key=item.get("childSessionKey"),
            created_at=int(item.get("createdAt") or 0),
            started_at=item.get("startedAt"),
            ended_at=item.get("endedAt"),
            last_event_at=int(item.get("lastEventAt") or item.get("createdAt") or 0),
            progress_summary=item.get("progressSummary"),
            terminal_summary=item.get("terminalSummary"),
            error=item.get("error"),
            raw=item,
        )

    @staticmethod
    def events(snapshot: TaskSnapshot) -> Iterable[Event]:
        summary = latest_progress(snapshot.raw, snapshot.status) or snapshot.label
        canonical = "|".join(
            [snapshot.task_id, snapshot.status, str(snapshot.last_event_at), summary]
        )
        event_id = hashlib.sha256(canonical.encode()).hexdigest()
        yield Event(
            event_id=event_id,
            task_id=snapshot.task_id,
            event_type="state_changed",
            status=snapshot.status,
            timestamp=snapshot.last_event_at,
            summary=summary,
            snapshot=snapshot,
        )
