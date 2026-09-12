from __future__ import annotations

import json
import os
import subprocess


class LarkError(RuntimeError):
    pass


class LarkCli:
    """Use lark-cli's protected profile; credentials never enter this process."""
    def __init__(self, binary="lark-cli", identity="bot", profile=None):
        self.binary, self.identity, self.profile = binary, identity, profile

    def api(self, method: str, path: str, *, data=None, params=None) -> dict:
        command = [self.binary]
        if self.profile:
            command += ["--profile", self.profile]
        command += ["api", method, path, "--as", self.identity, "--json"]
        if params is not None:
            command += ["--params", json.dumps(params, ensure_ascii=False)]
        if data is not None:
            command += ["--data", json.dumps(data, ensure_ascii=False)]
        child_env = {
            "HOME": os.getenv("HOME", "/Users/ethan"),
            "OPENCLAW_HOME": os.getenv("OPENCLAW_HOME", "/Users/ethan/.openclaw"),
            "PATH": os.getenv("PATH", "/opt/homebrew/bin:/usr/bin:/bin:/Users/ethan/.local/bin"),
            "LARKSUITE_CLI_NO_UPDATE_NOTIFIER": "1",
            "LARKSUITE_CLI_NO_SKILLS_NOTIFIER": "1",
        }
        proc = subprocess.run(command, capture_output=True, text=True, timeout=30,
                              start_new_session=True, env=child_env)
        stream = proc.stdout if proc.stdout.strip() else proc.stderr
        try:
            payload = json.loads(stream)
        except json.JSONDecodeError as exc:
            raise LarkError(f"lark-cli returned non-JSON (exit={proc.returncode})") from exc
        if proc.returncode or payload.get("ok") is not True:
            raise LarkError(json.dumps(payload.get("error", payload), ensure_ascii=False))
        return payload.get("data", {})
