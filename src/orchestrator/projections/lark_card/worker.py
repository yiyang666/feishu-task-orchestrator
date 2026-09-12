from __future__ import annotations

import hashlib
import json
import logging
import signal
import threading
import time

from orchestrator.lark import LarkCli
from orchestrator.projections.lark_card.renderer import render_task

LOG = logging.getLogger(__name__)
ACTIVE = {"queued", "running", "blocked"}
TERMINAL = {"succeeded", "failed", "timed_out", "cancelled"}
CARD_TTL_MS = 14 * 24 * 60 * 60 * 1000
ROTATE_AHEAD_MS = 24 * 60 * 60 * 1000


class LarkProjectionWorker:
    """Independent Lark projector backed by a durable SQLite outbox."""

    def __init__(self, config, store):
        self.config, self.store = config, store
        self.lark = LarkCli(config.lark_cli_bin, config.lark_identity, config.lark_profile)
        self.stop_event = threading.Event()

    def run_once(self) -> int:
        self._plan()
        return self._drain()

    def _plan(self) -> int:
        views = self.store.list_task_views()
        focus: dict[str, dict] = {}
        for view in views:
            route = view.get("origin_detail") or {}
            key = route.get("channel_key")
            if route.get("target_type") == "chat" and key and view["status"] in ACTIVE:
                if key not in focus or int(view["created_at"]) > int(focus[key]["created_at"]):
                    focus[key] = view
        count, now = 0, int(time.time() * 1000)
        for view in views:
            route = view.get("origin_detail") or {}
            if route.get("target_type") not in {"chat", "user"} or not route.get("target_id"):
                continue
            current = self.store.latest_card(view["project_id"])
            if current and current["status"] in TERMINAL:
                continue
            rotate = bool(current and current.get("expires_at") and view["status"] in ACTIVE
                          and now >= int(current["expires_at"]) - ROTATE_AHEAD_MS)
            generation = int(current["generation"]) + 1 if rotate else int(current["generation"]) if current else 1
            content = json.dumps(render_task(view), ensure_ascii=False, separators=(",", ":"))
            if len(content.encode()) > 16_384:
                raise ValueError(f"card payload exceeds 16KB target: {view['project_id']}")
            digest = hashlib.sha256(content.encode()).hexdigest()
            if not current or rotate:
                should_pin = route["target_type"] == "chat" and focus.get(route["channel_key"], {}).get("project_id") == view["project_id"]
                count += self.store.enqueue(key=f"create:{view['project_id']}:{generation}",
                    task_id=view["project_id"], generation=generation, operation="create",
                    payload={"route": route, "status": view["status"], "digest": digest,
                             "content": content, "pin": should_pin})
                continue
            if current.get("payload_hash") != digest:
                count += self.store.enqueue(key=f"patch:{view['project_id']}:{generation}:{digest}",
                    task_id=view["project_id"], generation=generation, operation="patch",
                    payload={"route": route, "status": view["status"], "digest": digest, "content": content})
            should_pin = route["target_type"] == "chat" and focus.get(route["channel_key"], {}).get("project_id") == view["project_id"]
            if should_pin and not current.get("pinned"):
                count += self.store.enqueue(key=f"pin:{view['project_id']}:{generation}", task_id=view["project_id"], generation=generation, operation="pin", payload={})
            if (not should_pin or view["status"] in TERMINAL) and current.get("pinned"):
                count += self.store.enqueue(key=f"unpin:{view['project_id']}:{generation}", task_id=view["project_id"], generation=generation, operation="unpin", payload={})
        return count

    def _drain(self) -> int:
        completed = 0
        for row in self.store.pending_outbox():
            try:
                self._execute(row)
                self.store.finish_outbox(row["outbox_id"])
                completed += 1
            except Exception as exc:
                self.store.fail_outbox(row["outbox_id"], str(exc))
                LOG.warning("outbox operation failed id=%s op=%s", row["outbox_id"], row["operation"])
        return completed

    def _execute(self, row: dict) -> None:
        payload = json.loads(row["payload_json"] or "{}")
        current = self.store.card_row(row["task_id"], row["generation"])
        if row["operation"] == "create":
            route = payload["route"]
            uuid = hashlib.sha256(row["idempotency_key"].encode()).hexdigest()[:40]
            if route.get("source_message_id") and _is_thread(route.get("channel_key")):
                response = self.lark.api(
                    "POST", f"/open-apis/im/v1/messages/{route['source_message_id']}/reply",
                    data={"msg_type": "interactive", "content": payload["content"],
                          "uuid": uuid, "reply_in_thread": True},
                )
            else:
                response = self.lark.api("POST", "/open-apis/im/v1/messages",
                    params={"receive_id_type": "chat_id" if route["target_type"] == "chat" else "open_id"},
                    data={"receive_id": route["target_id"], "msg_type": "interactive", "content": payload["content"],
                          "uuid": uuid})
            while isinstance(response.get("data"), dict): response = response["data"]
            message_id = response.get("message_id") or response.get("message", {}).get("message_id")
            if not message_id: raise RuntimeError(f"message response missing id: {response}")
            self.store.save_card(row["task_id"], route, message_id=message_id, status=payload["status"],
                payload_hash=payload["digest"], generation=row["generation"],
                expires_at=int(time.time() * 1000) + CARD_TTL_MS)
            if payload.get("pin") and payload["status"] in ACTIVE:
                for old in self.store.pinned_cards(route["channel_key"]):
                    if old["message_id"] != message_id:
                        self.store.enqueue(key=f"unpin:{old['task_id']}:{old['generation']}", task_id=old["task_id"], generation=old["generation"], operation="unpin", payload={})
                self.store.enqueue(key=f"pin:{row['task_id']}:{row['generation']}", task_id=row["task_id"], generation=row["generation"], operation="pin", payload={})
            return
        if not current or not current.get("message_id"):
            raise RuntimeError("card generation is not created yet")
        if row["operation"] == "patch":
            self.lark.api("PATCH", f"/open-apis/im/v1/messages/{current['message_id']}", data={"content": payload["content"]})
            route = {key: current[key] for key in ("channel_key", "target_type", "target_id", "source_message_id")}
            self.store.save_card(row["task_id"], route, message_id=current["message_id"], status=payload["status"],
                payload_hash=payload["digest"], generation=row["generation"], expires_at=current.get("expires_at"))
            if payload["status"] in TERMINAL and current.get("pinned"):
                self.store.enqueue(key=f"unpin:{row['task_id']}:{row['generation']}", task_id=row["task_id"], generation=row["generation"], operation="unpin", payload={})
        elif row["operation"] == "pin":
            self.lark.api("POST", "/open-apis/im/v1/pins", data={"message_id": current["message_id"]})
            self.store.set_card_pinned(row["task_id"], row["generation"], True)
        elif row["operation"] == "unpin":
            self.lark.api("DELETE", f"/open-apis/im/v1/pins/{current['message_id']}")
            self.store.set_card_pinned(row["task_id"], row["generation"], False)
        else:
            raise ValueError(f"unknown outbox operation: {row['operation']}")

    def serve(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM): signal.signal(sig, lambda *_: self.stop_event.set())
        while not self.stop_event.is_set():
            try: self.run_once()
            except Exception: LOG.exception("Lark projection failed")
            self.stop_event.wait(self.config.poll_interval_seconds)


def _is_thread(channel_key: str | None) -> bool:
    value = (channel_key or "").lower()
    return ":topic:" in value or ":thread:" in value
