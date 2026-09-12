from __future__ import annotations

import json
import subprocess

from orchestrator.event_source.openclaw_tasks import OpenClawTasksSource, STATUS_MAP
from orchestrator.models import TaskSnapshot


class WorkboardSource:
    """Optional adapter; usable once the bundled Workboard plugin is enabled.

    Notification cursors are intentionally kept outside this adapter. The worker
    uses the durable task ledger today and can switch to replay-safe Workboard
    notifications without changing projections or state storage.
    """

    def __init__(self, binary: str = "openclaw"):
        self.binary = binary

    def available(self) -> bool:
        proc = subprocess.run(
            [self.binary, "workboard", "list", "--json"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return proc.returncode == 0

    def list_cards(self) -> dict:
        proc = subprocess.run(
            [self.binary, "workboard", "list", "--json"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return json.loads(proc.stdout)

    def catalog(self) -> tuple[list[dict], list[dict]]:
        payload = self.list_cards()
        boards = payload.get("boards", []) if isinstance(payload, dict) else []
        cards = payload.get("cards", payload if isinstance(payload, list) else [])
        return boards, cards

    def fetch(self) -> list[TaskSnapshot]:
        payload = self.list_cards()
        cards = payload.get("cards", payload if isinstance(payload, list) else [])
        snapshots: list[TaskSnapshot] = []
        for card in cards:
            linked_task_id = _linked_task_id(card)
            # A linked Background Task is canonical in the task ledger. Avoid
            # projecting it twice; standalone Workboard cards use their card id.
            if linked_task_id:
                continue
            card_id = str(card.get("id") or card.get("cardId") or "")
            if not card_id:
                continue
            status = _status(card.get("status"))
            updated = int(card.get("updatedAt") or card.get("createdAt") or 0)
            raw = {
                "taskId": f"wb:{card_id}",
                "status": status,
                "agentId": card.get("agentId") or "unassigned",
                "requesterSessionKey": card.get("sessionKey") or "workboard",
                "label": card.get("title") or card.get("summary") or card_id,
                "createdAt": int(card.get("createdAt") or updated),
                "lastEventAt": updated,
                "progressSummary": _recent_summary(card),
                "terminalSummary": card.get("result") or card.get("terminalSummary"),
                "detail": {"source": "workboard", "card": card},
            }
            snapshots.append(OpenClawTasksSource._snapshot(raw))
        return snapshots


def _linked_task_id(card: dict) -> str | None:
    for source in (card, card.get("linked", {}), card.get("execution", {}), card.get("metadata", {})):
        if isinstance(source, dict):
            value = source.get("taskId") or source.get("task_id")
            if value:
                return str(value)
    return None


def _status(value: str | None) -> str:
    mapping = {
        "triage": "queued", "backlog": "queued", "todo": "queued",
        "scheduled": "queued", "ready": "queued", "running": "running",
        "review": "running", "blocked": "blocked", "done": "succeeded",
    }
    normalized = mapping.get(value or "", value or "queued")
    return normalized if normalized in STATUS_MAP else "queued"


def _recent_summary(card: dict) -> str:
    events = card.get("recentEvents") or card.get("recent_events") or []
    if events and isinstance(events[-1], dict):
        event = events[-1]
        return str(event.get("summary") or event.get("message") or event.get("type") or "")
    return str(card.get("summary") or card.get("title") or "")
