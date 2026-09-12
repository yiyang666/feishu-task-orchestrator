from __future__ import annotations

import fcntl
import hashlib
import json
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

from orchestrator.state.store import Store


Runner = Callable[..., subprocess.CompletedProcess]


class WorkboardClient:
    def __init__(self, binary: str = "openclaw", runner: Runner = subprocess.run):
        self.binary = binary
        self.runner = runner

    def cards(self) -> list[dict]:
        payload = self._run([self.binary, "workboard", "list", "--json"], read_retries=3)
        return payload.get("cards", payload if isinstance(payload, list) else [])

    def create(
        self, title: str, *, board_id: str, labels: str, status: str,
        notes: str, agent_id: str | None = None,
    ) -> dict:
        argv = [
            self.binary, "workboard", "create", title, "--board", board_id,
            "--labels", labels, "--status", status, "--notes", notes, "--json",
        ]
        if agent_id:
            argv.extend(["--agent", agent_id])
        payload = self._run(argv)
        card = payload.get("card", payload)
        if not isinstance(card, dict) or not _card_id(card):
            raise RuntimeError("workboard create returned no card id")
        return card

    def _run(self, argv: list[str], *, read_retries: int = 1) -> dict:
        for attempt in range(read_retries):
            try:
                proc = self.runner(argv, check=True, capture_output=True, text=True, timeout=30)
                break
            except subprocess.CalledProcessError:
                if attempt + 1 >= read_retries:
                    raise
                time.sleep(0.15 * (attempt + 1))
        value = json.loads(proc.stdout)
        if not isinstance(value, (dict, list)):
            raise RuntimeError("unexpected Workboard JSON response")
        return value


class DispatchPrepareService:
    """Prepare the task model only; OpenClaw spawning remains owned by main."""

    def __init__(self, store: Store, workboard: WorkboardClient, lock_path: Path):
        self.store = store
        self.workboard = workboard
        self.lock_path = lock_path

    def prepare(
        self, task_id: str, title: str, *, channel_key: str, target_type: str,
        target_id: str, source_message_id: str | None, steps: list[tuple[str, str]],
    ) -> dict:
        if not steps:
            raise ValueError("at least one --step agent:title is required")
        with _locked(self.lock_path):
            cards = self.workboard.cards()
            root = self._ensure_root(cards, task_id, title)
            root_id = _card_id(root)
            context_steps = []
            for index, (agent_id, step_title) in enumerate(steps, 1):
                card = self._ensure_step(
                    cards, task_id, root_id, index, agent_id, step_title
                )
                context_steps.append({
                    "work_item_id": f"wb:{_card_id(card)}",
                    "card_id": _card_id(card),
                    "agent_id": agent_id,
                    "title": step_title,
                    "spawn": {
                        "agentId": agent_id,
                        "label": step_title,
                        "taskName": _task_name(task_id, index, agent_id),
                    },
                })
                cards.append(card)
            self.store.register_root(
                task_id, title, channel_key=channel_key, target_type=target_type,
                target_id=target_id, source_message_id=source_message_id,
            )
            self.store.sync_workboard([], _unique_cards([*cards, root]))
        return {
            "ok": True,
            "root_task_id": task_id,
            "board_id": task_id,
            "project_root_card_id": root_id,
            "steps": context_steps,
            "next_action": "main must spawn each step using the returned agentId and exact label",
        }

    def prepare_step(
        self, task_id: str, title: str, *, agent_id: str, kind: str,
        task_ref: str | None = None, status: str = "running",
    ) -> dict:
        project = self.store.project_by_id(task_id)
        if not project:
            raise ValueError(f"project not found: {task_id}; register root first")
        provisional_id = self.store.register_provisional_step(
            task_id, title, agent_id=agent_id, kind=kind,
            task_ref=task_ref, status=status,
        )
        with _locked(self.lock_path):
            cards = self.workboard.cards()
            root = self._ensure_root(cards, task_id, project["name"])
            marker_hash = hashlib.sha256(f"{kind}|{agent_id}|{title}".encode()).hexdigest()[:16]
            marker = f"fto-step-key:{task_id}:{marker_hash}"
            card = _find(
                cards, task_id, "project-step", marker, title=title, agent_id=agent_id
            ) or self.workboard.create(
                title, board_id=task_id, labels="project-step", status="ready",
                notes=f"{marker}; parent-root:{_card_id(root)}; kind:{kind}",
                agent_id=agent_id,
            )
            self.store.sync_workboard([], _unique_cards([*cards, root, card]))
            work_item_id = f"wb:{_card_id(card)}"
            self.store.promote_provisional_step(provisional_id, work_item_id)
            tracking = self.store.register_step_ref(
                work_item_id, kind=kind, task_ref=task_ref, status=status
            )
        return {
            "ok": True, "root_task_id": task_id, "work_item_id": work_item_id,
            "card_id": _card_id(card), "title": title, "agent_id": agent_id,
            "kind": kind, "task_ref": tracking["task_ref"], "status": status,
            "spawn": {
                "agentId": agent_id, "label": title,
                "taskName": _task_name(task_id, int(marker_hash[:4], 16), agent_id),
            } if kind == "spawn" else None,
        }

    def prepare_step_async(
        self, task_id: str, title: str, *, agent_id: str, kind: str,
        task_ref: str | None = None, status: str = "running",
    ) -> dict:
        """Register immediately and durably queue the slow Workboard synchronization."""
        project = self.store.project_by_id(task_id)
        if not project:
            raise ValueError(f"project not found: {task_id}; register root first")
        provisional_id = self.store.register_provisional_step(
            task_id, title, agent_id=agent_id, kind=kind,
            task_ref=task_ref, status=status,
        )
        marker_hash = hashlib.sha256(f"{kind}|{agent_id}|{title}".encode()).hexdigest()[:16]
        payload = {
            "provisional_id": provisional_id, "task_id": task_id, "title": title,
            "agent_id": agent_id, "kind": kind, "task_ref": task_ref,
            "status": status,
        }
        needs_sync = provisional_id.startswith("pending:")
        if needs_sync:
            self.store.enqueue_dispatch(
                key=f"workboard-step:{task_id}:{marker_hash}", project_id=task_id,
                operation="sync_step", payload=payload,
            )
        card_id = provisional_id.removeprefix("wb:") if not needs_sync else None
        return {
            "ok": True, "root_task_id": task_id, "work_item_id": provisional_id,
            "card_id": card_id, "workboard_sync": "pending" if needs_sync else "synced",
            "title": title,
            "agent_id": agent_id, "kind": kind, "task_ref": task_ref,
            "status": status,
            "spawn": {
                "agentId": agent_id, "label": title,
                "taskName": _task_name(task_id, int(marker_hash[:4], 16), agent_id),
            } if kind == "spawn" else None,
        }

    def sync_queued_step(self, payload: dict) -> dict:
        """Create/reuse Workboard cards and promote an already visible provisional step."""
        task_id = str(payload["task_id"])
        project = self.store.project_by_id(task_id)
        if not project:
            raise ValueError(f"project not found: {task_id}")
        title = str(payload["title"])
        agent_id = str(payload["agent_id"])
        kind = str(payload["kind"])
        with _locked(self.lock_path):
            cards = self.workboard.cards()
            root = self._ensure_root(cards, task_id, project["name"])
            marker_hash = hashlib.sha256(f"{kind}|{agent_id}|{title}".encode()).hexdigest()[:16]
            marker = f"fto-step-key:{task_id}:{marker_hash}"
            card = _find(
                cards, task_id, "project-step", marker, title=title, agent_id=agent_id
            ) or self.workboard.create(
                title, board_id=task_id, labels="project-step", status="ready",
                notes=f"{marker}; parent-root:{_card_id(root)}; kind:{kind}",
                agent_id=agent_id,
            )
            self.store.sync_workboard([], _unique_cards([*cards, root, card]))
            work_item_id = f"wb:{_card_id(card)}"
            provisional_id = str(payload["provisional_id"])
            if self.store.work_item_by_id(provisional_id):
                self.store.promote_provisional_step(provisional_id, work_item_id)
            item = self.store.work_item_by_id(work_item_id)
            if not item:
                raise RuntimeError("Workboard step missing after promotion")
        return {
            "work_item_id": work_item_id, "card_id": _card_id(card),
            "project_id": task_id, "kind": kind,
            "task_ref": payload.get("task_ref"), "status": item["status"],
        }

    def _ensure_root(self, cards: list[dict], task_id: str, title: str) -> dict:
        marker = f"fto-root:{task_id}"
        existing = _find(cards, task_id, "project-root", marker, title=title)
        return existing or self.workboard.create(
            title, board_id=task_id, labels="project-root", status="running",
            notes=marker,
        )

    def _ensure_step(
        self, cards: list[dict], task_id: str, root_id: str, index: int,
        agent_id: str, title: str,
    ) -> dict:
        marker = f"fto-step:{task_id}:{index}"
        existing = _find(
            cards, task_id, "project-step", marker, title=title, agent_id=agent_id
        )
        return existing or self.workboard.create(
            title, board_id=task_id, labels="project-step", status="ready",
            notes=f"{marker}; parent-root:{root_id}", agent_id=agent_id,
        )


def parse_step(value: str) -> tuple[str, str]:
    agent_id, separator, title = value.partition(":")
    if not separator or not agent_id.strip() or not title.strip():
        raise ValueError(f"invalid step {value!r}; expected agent:title")
    return agent_id.strip(), title.strip()


def _unique_cards(cards: list[dict]) -> list[dict]:
    unique: dict[str, dict] = {}
    for card in cards:
        card_id = _card_id(card)
        if card_id:
            unique[card_id] = card
    return list(unique.values())


def _find(
    cards: list[dict], board_id: str, label: str, marker: str, *,
    title: str, agent_id: str | None = None,
) -> dict | None:
    candidates = [
        card for card in cards
        if _board_id(card) == board_id and label in (card.get("labels") or [])
        and (marker in str(card.get("notes") or "") or card.get("title") == title)
    ]
    if agent_id is not None:
        candidates = [card for card in candidates if card.get("agentId") == agent_id]
    if len(candidates) > 1:
        raise RuntimeError(f"ambiguous Workboard cards for {marker}")
    return candidates[0] if candidates else None


def _board_id(card: dict) -> str:
    return str(
        card.get("boardId")
        or (card.get("metadata") or {}).get("automation", {}).get("boardId")
        or "default"
    )


def _card_id(card: dict) -> str:
    return str(card.get("id") or card.get("cardId") or "")


def _task_name(task_id: str, index: int, agent_id: str) -> str:
    slug = re.sub(r"[^a-z0-9_-]+", "-", task_id.lower()).strip("-_") or "task"
    agent = re.sub(r"[^a-z0-9_-]+", "-", agent_id.lower()).strip("-_") or "agent"
    return f"{slug[:36]}-{index}-{agent[:16]}"[:63]


@contextmanager
def _locked(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
