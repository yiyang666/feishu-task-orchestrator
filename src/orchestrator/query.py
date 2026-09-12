from __future__ import annotations

import json
from typing import Any

from orchestrator.state.store import Store


class QueryService:
    def __init__(self, store: Store):
        self.store = store

    def status(self, task_id: str) -> dict[str, Any] | None:
        return self.store.task_view(task_id)

    def list(self, *, active_only: bool = False, channel_key: str | None = None) -> list[dict]:
        return self.store.list_task_views(active_only=active_only, channel_key=channel_key)

    def timeline(self, task_id: str, limit: int = 50) -> list[dict]:
        view = self.status(task_id)
        return (view or {}).get("timeline", [])[-max(limit, 0):]


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)
