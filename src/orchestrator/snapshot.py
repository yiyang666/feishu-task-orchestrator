"""Local normalized agent-status snapshot.

The worker no longer writes to Feishu. Instead it polls the durable OpenClaw
task ledger, dedupes events into SQLite, and then materializes a single local
snapshot that any observer (CLI, dashboard, another agent) can read.

The snapshot is a plain JSON document with a stable shape. Nothing here talks to
Lark; this module is the whole presentation-neutral observation surface.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path

from orchestrator.models import Status


SCHEMA_VERSION = 1
DEFAULT_SNAPSHOT_PATH = Path("var/agent_status.json")

# runtimes that represent scheduled automation rather than an interactive run
CRON_RUNTIME = "cron"

# a terminal run is terminal if its status is one of these
_TERMINAL_STATUSES = {Status.SUCCEEDED, Status.FAILED, Status.TIMED_OUT, Status.CANCELLED}
_ACTIVE_STATUSES = {Status.QUEUED, Status.RUNNING, Status.BLOCKED}
_FAILURE_STATUSES = {Status.FAILED, Status.TIMED_OUT, Status.BLOCKED}

# local timezone offset applied to generated_at (Asia/Shanghai)
_LOCAL_OFFSET = "+08:00"


def _iso_local(epoch_ms: int | float | None) -> str | None:
    """Render epoch milliseconds as an ISO 8601 string with a local offset."""
    if not epoch_ms:
        return None
    seconds = float(epoch_ms) / 1000.0
    text = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(seconds))
    return f"{text}{_LOCAL_OFFSET}"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()) + _LOCAL_OFFSET


def default_roster() -> list[str]:
    """Fallback roster used when ``openclaw agents list`` is unavailable."""
    return ["main", "automation", "research", "code", "creative", "codex", "fast"]


def fetch_agent_roster(binary: str = "openclaw", timeout: int = 15) -> list[str]:
    """Read the configured agent roster via the OpenClaw CLI.

    Returns the ordered list of agent ids. Raises on failure so the caller can
    fall back to :func:`default_roster`.
    """
    import subprocess

    proc = subprocess.run(
        [binary, "agents", "list", "--json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    payload = json.loads(proc.stdout)
    items = payload if isinstance(payload, list) else payload.get("agents", [])
    ids: list[str] = []
    for item in items:
        agent_id = item.get("id") or item.get("agentId")
        if agent_id and agent_id not in ids:
            ids.append(str(agent_id))
    return ids


def _status_value(status) -> str:
    return status.value if isinstance(status, Status) else str(status)


def _task_entry(snapshot) -> dict:
    return {
        "task_id": snapshot.task_id,
        "label": snapshot.label,
        "status": _status_value(snapshot.status),
        "started_at": _iso_local(snapshot.started_at),
        "ended_at": _iso_local(snapshot.ended_at),
        "last_event_at": _iso_local(snapshot.last_event_at),
        "progress_summary": snapshot.progress_summary,
        "child_session_key": snapshot.child_session_key,
    }


def _terminal_entry(snapshot) -> dict:
    from orchestrator.presentation import latest_progress

    summary = latest_progress(snapshot.raw, snapshot.status) or snapshot.terminal_summary
    return {
        "label": snapshot.label,
        "status": _status_value(snapshot.status),
        "ended_at": _iso_local(snapshot.ended_at or snapshot.last_event_at),
        "terminal_summary": summary,
        "error": snapshot.error,
    }


def _empty_totals() -> dict:
    return {
        "running": 0,
        "blocked": 0,
        "succeeded": 0,
        "failed": 0,
        "timed_out": 0,
        "cancelled": 0,
        "queued": 0,
    }


def _build_agents(
    roster: list[str],
    snapshots: list,
    recent_terminal_limit: int = 5,
) -> dict[str, dict]:
    """Group task snapshots per configured agent.

    Agents with no observed tasks are explicitly marked ``idle`` so an observer
    can distinguish "quiet" from "not reporting".
    """
    ordered = list(roster)
    extra = sorted({
        snap.agent_id for snap in snapshots
        if snap.agent_id and snap.agent_id not in ordered
    })
    ordered.extend(extra)

    by_agent: dict[str, list] = {agent_id: [] for agent_id in ordered}
    for snap in snapshots:
        if snap.raw.get("runtime") == CRON_RUNTIME:
            continue
        by_agent.setdefault(snap.agent_id or "unknown", []).append(snap)

    result: dict[str, dict] = {}
    for agent_id in ordered:
        snaps = by_agent.get(agent_id, [])
        active: list[dict] = []
        terminal: list = []
        totals = _empty_totals()
        for snap in snaps:
            status = _status_value(snap.status)
            if status in totals:
                totals[status] += 1
            if snap.status in _ACTIVE_STATUSES:
                active.append(_task_entry(snap))
            elif snap.status in _TERMINAL_STATUSES:
                terminal.append(snap)
        terminal.sort(key=lambda item: item.ended_at or item.last_event_at or 0, reverse=True)
        entry = {
            "agent_id": agent_id,
            "active": active,
            "recent_terminal": [
                _terminal_entry(snap) for snap in terminal[:recent_terminal_limit]
            ],
            "totals": totals,
        }
        if not snaps:
            entry["state"] = "idle"
        result[agent_id] = entry
    return result


def _build_automations(store) -> list[dict]:
    rows = []
    for row in store.automation_rows():
        rows.append({
            "source_id": row["source_id"],
            "label": row["label"],
            "health": row["health"],
            "last_run_at": _iso_local(row["last_ended_at"] or row["last_started_at"]),
            "last_status": row["last_run_status"],
            "today_success": row["success_count"],
            "today_failed": row["failure_count"],
        })
    return rows


def _build_workboard(boards: list[dict] | None, cards: list[dict] | None) -> dict:
    cards = cards or []
    active_cards = []
    for card in cards:
        if str(card.get("status") or "") in {"done", "cancelled", "canceled"}:
            continue
        active_cards.append({
            "title": str(card.get("title") or card.get("summary") or card.get("id") or ""),
            "status": str(card.get("status") or "queued"),
            "agent_id": str(card.get("agentId") or "unassigned"),
            "updated_at": _iso_local(card.get("updatedAt") or card.get("createdAt")),
        })
    return {"boards": len(boards or []), "active_cards": active_cards}


def build_snapshot(
    *,
    store,
    snapshots: list,
    roster: list[str] | None = None,
    poll_latency_ms: float | None = None,
    fetched_at: str | None = None,
    boards: list[dict] | None = None,
    cards: list[dict] | None = None,
    recent_terminal_limit: int = 5,
) -> dict:
    """Assemble the normalized local status document.

    ``snapshots`` are :class:`~orchestrator.models.TaskSnapshot` objects from the
    task ledger. ``store`` provides automation rows. The returned dict is JSON
    serializable and carries no Feishu identifiers.
    """
    resolved_roster = list(roster) if roster else default_roster()
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "poll_latency_ms": round(float(poll_latency_ms), 3) if poll_latency_ms is not None else None,
        "agents": _build_agents(resolved_roster, snapshots, recent_terminal_limit),
        "automations": _build_automations(store),
        "workboard": _build_workboard(boards, cards),
        "source": {
            "tasks_total": len(snapshots),
            "fetched_at": fetched_at or _now_iso(),
            "fetch_ms": round(float(poll_latency_ms), 3) if poll_latency_ms is not None else None,
        },
    }


def write_snapshot(path: str | Path | None, snapshot: dict) -> Path:
    """Atomically write the snapshot: write a sibling ``.tmp`` then ``os.replace``."""
    target = Path(path) if path else DEFAULT_SNAPSHOT_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=False)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return target


def read_snapshot(path: str | Path | None = None) -> dict | None:
    """Read a previously written snapshot, or ``None`` when it is absent."""
    target = Path(path) if path else DEFAULT_SNAPSHOT_PATH
    if not target.exists():
        return None
    return json.loads(target.read_text(encoding="utf-8"))
