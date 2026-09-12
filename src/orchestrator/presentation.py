from __future__ import annotations

from orchestrator.models import Status


def latest_progress(snapshot: dict, status: Status | str | None = None) -> str:
    """Keep delivery metadata from hiding a successful task result."""
    normalized = Status(status or snapshot.get("status", "queued"))
    progress = snapshot.get("progressSummary")
    terminal = snapshot.get("terminalSummary")
    error = snapshot.get("error")
    if normalized == Status.SUCCEEDED:
        return progress or terminal or error or ""
    if normalized in {Status.BLOCKED, Status.FAILED, Status.TIMED_OUT, Status.CANCELLED}:
        return error or terminal or progress or ""
    return progress or error or terminal or ""
