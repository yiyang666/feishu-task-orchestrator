from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_dotenv(path: Path | None = None) -> None:
    """载入项目根目录的 .env（真实环境变量优先，不覆盖已有值）。"""
    target = path or (PROJECT_ROOT / ".env")
    if not target.is_file():
        return
    for raw in target.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


@dataclass(frozen=True)
class Config:
    db_path: Path
    snapshot_path: Path
    poll_interval_seconds: float
    soft_timeout_seconds: int
    hard_timeout_seconds: int
    session_prefixes: tuple[str, ...]
    automation_source_ids: tuple[str, ...]
    automation_labels: dict[str, str]
    openclaw_bin: str = "openclaw"
    lark_cli_bin: str = "lark-cli"
    lark_identity: str = "bot"
    lark_profile: str | None = None

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        prefixes = os.getenv("FTO_SESSION_PREFIXES", "")
        automation_sources = os.getenv("FTO_AUTOMATION_SOURCE_IDS", "")
        automation_labels = json.loads(os.getenv("FTO_AUTOMATION_LABELS_JSON", "{}"))
        return cls(
            db_path=Path(os.getenv("FTO_DB_PATH", "var/orchestrator.db")),
            snapshot_path=Path(
                os.getenv("FTO_SNAPSHOT_PATH", "var/agent_status.json")
            ),
            poll_interval_seconds=float(os.getenv("FTO_POLL_INTERVAL_SECONDS", "1")),
            soft_timeout_seconds=int(os.getenv("FTO_SOFT_TIMEOUT_SECONDS", "600")),
            hard_timeout_seconds=int(os.getenv("FTO_HARD_TIMEOUT_SECONDS", "7200")),
            session_prefixes=tuple(p.strip() for p in prefixes.split(",") if p.strip()),
            automation_source_ids=tuple(
                source.strip() for source in automation_sources.split(",") if source.strip()
            ),
            automation_labels=automation_labels,
            openclaw_bin=os.getenv("FTO_OPENCLAW_BIN", "openclaw"),
            lark_cli_bin=os.getenv("FTO_LARK_CLI_BIN", "lark-cli"),
            lark_identity=os.getenv("FTO_LARK_IDENTITY", "bot"),
            lark_profile=os.getenv("FTO_LARK_PROFILE") or None,
        )
