from __future__ import annotations

import json
import subprocess


class OpenClawSessionsSource:
    """Read session lifecycle for explicitly registered sessions_send steps."""

    def __init__(self, binary: str = "openclaw"):
        self.binary = binary

    def fetch(self) -> list[dict]:
        proc = subprocess.run(
            [self.binary, "sessions", "--all-agents", "--json", "--limit", "all"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        payload = json.loads(proc.stdout)
        return payload.get("sessions", []) if isinstance(payload, dict) else []
