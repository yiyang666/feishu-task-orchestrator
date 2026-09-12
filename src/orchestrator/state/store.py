from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

from orchestrator.models import Event, TaskSnapshot


LOG = logging.getLogger(__name__)


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  parent_id TEXT NOT NULL,
  child_id TEXT NOT NULL,
  status TEXT NOT NULL,
  last_event_at INTEGER NOT NULL,
  snapshot_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  status TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  summary TEXT NOT NULL,
  projected INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(task_id)
);
CREATE TABLE IF NOT EXISTS projections (
  projection_key TEXT PRIMARY KEY,
  external_id TEXT NOT NULL,
  payload_json TEXT,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints (
  checkpoint_key TEXT PRIMARY KEY,
  checkpoint_value TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  origin TEXT NOT NULL,
  status TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS work_items (
  work_item_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  parent_work_item_id TEXT,
  task_id TEXT UNIQUE,
  title TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  status TEXT NOT NULL,
  latest_progress TEXT NOT NULL,
  started_at INTEGER,
  ended_at INTEGER,
  last_event_at INTEGER NOT NULL,
  raw_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(project_id)
);
CREATE TABLE IF NOT EXISTS automation_monitors (
  source_id TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  health TEXT NOT NULL,
  active_run_id TEXT,
  last_run_status TEXT NOT NULL,
  last_started_at INTEGER,
  last_ended_at INTEGER,
  last_success_at INTEGER,
  last_result TEXT NOT NULL,
  last_error TEXT NOT NULL,
  consecutive_failures INTEGER NOT NULL,
  window_date TEXT NOT NULL,
  success_count INTEGER NOT NULL,
  failure_count INTEGER NOT NULL,
  dirty INTEGER NOT NULL DEFAULT 1,
  last_projected_at INTEGER,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS automation_runs (
  task_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  status TEXT NOT NULL,
  ended_at INTEGER,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS task_cards (
  task_id TEXT NOT NULL,
  generation INTEGER NOT NULL DEFAULT 1,
  channel_key TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT NOT NULL,
  source_message_id TEXT,
  message_id TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  payload_hash TEXT,
  expires_at INTEGER,
  pinned INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(task_id,generation)
);
CREATE TABLE IF NOT EXISTS projection_outbox (
  outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
  idempotency_key TEXT NOT NULL UNIQUE,
  task_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  operation TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at INTEGER NOT NULL,
  last_error TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_outbox (
  outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
  idempotency_key TEXT NOT NULL UNIQUE,
  project_id TEXT NOT NULL,
  operation TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at INTEGER NOT NULL,
  last_error TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS binding_diagnostics (
  diagnostic_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  project_id TEXT,
  severity TEXT NOT NULL,
  kind TEXT NOT NULL,
  message TEXT NOT NULL,
  details_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  resolved_at INTEGER
);
CREATE TABLE IF NOT EXISTS work_item_refs (
  work_item_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  task_ref TEXT,
  registered_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY(work_item_id) REFERENCES work_items(work_item_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS planner_notes (
  note_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  text TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS ignored_runs (
  task_id TEXT PRIMARY KEY,
  reason TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ignored_projects (
  project_id TEXT PRIMARY KEY,
  reason TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
"""


class Store:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript(SCHEMA)
            self._migrate(db)

    @staticmethod
    def _migrate(db: sqlite3.Connection) -> None:
        columns = {row[1] for row in db.execute("PRAGMA table_info(task_cards)")}
        if "expires_at" not in columns:
            db.execute("ALTER TABLE task_cards ADD COLUMN expires_at INTEGER")
        if "pinned" not in columns:
            db.execute("ALTER TABLE task_cards ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0")

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        try:
            yield db
            db.commit()
        finally:
            db.close()

    def ingest(self, event: Event) -> bool:
        snap = event.snapshot
        now = int(time.time() * 1000)
        with self.connect() as db:
            if db.execute(
                "SELECT 1 FROM ignored_runs WHERE task_id=?", (snap.task_id,)
            ).fetchone():
                return False
            db.execute(
                """INSERT INTO tasks(task_id,parent_id,child_id,status,last_event_at,snapshot_json,updated_at)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(task_id) DO UPDATE SET status=excluded.status,
                     last_event_at=excluded.last_event_at,snapshot_json=excluded.snapshot_json,
                     updated_at=excluded.updated_at""",
                (snap.task_id, snap.parent_id, snap.child_id, snap.status, snap.last_event_at,
                 json.dumps(snap.raw, ensure_ascii=False), now),
            )
            cur = db.execute(
                """INSERT OR IGNORE INTO events(event_id,task_id,event_type,status,timestamp,summary,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (event.event_id, event.task_id, event.event_type, event.status,
                 event.timestamp, event.summary, now),
            )
            self._upsert_work_item(db, event)
            return cur.rowcount == 1

    def task_versions(self) -> dict[str, tuple[str, int]]:
        """Return the minimal durable cursor used to skip unchanged history."""
        with self.connect() as db:
            return {
                str(row["task_id"]): (str(row["status"]), int(row["last_event_at"]))
                for row in db.execute("SELECT task_id,status,last_event_at FROM tasks")
            }

    @staticmethod
    def _upsert_work_item(db: sqlite3.Connection, event: Event) -> None:
        snap = event.snapshot
        raw = snap.raw
        now = int(time.time() * 1000)
        linked = db.execute(
            "SELECT work_item_id,project_id FROM work_items WHERE task_id=?", (snap.task_id,)
        ).fetchone()
        if linked:
            work_item_id, project_id = linked
            if project_id == "unfiled":
                candidate, candidates, confidence = _diagnostic_match(db, snap)
                diagnostic = db.execute(
                    """SELECT diagnostic_id,details_json FROM binding_diagnostics
                       WHERE task_id=? AND resolved_at IS NULL""",
                    (snap.task_id,),
                ).fetchone()
                if diagnostic:
                    try:
                        details = json.loads(diagnostic["details_json"] or "{}")
                    except json.JSONDecodeError:
                        details = {}
                    details.update({"candidate_roots": candidates, "confidence": confidence})
                    db.execute(
                        """UPDATE binding_diagnostics SET project_id=COALESCE(project_id,?),
                           details_json=?,updated_at=? WHERE diagnostic_id=?""",
                        (candidate, json.dumps(details, ensure_ascii=False), now,
                         diagnostic["diagnostic_id"]),
                    )
            else:
                db.execute(
                    "UPDATE binding_diagnostics SET resolved_at=?,updated_at=? WHERE task_id=? AND resolved_at IS NULL",
                    (now, now, snap.task_id),
                )
        else:
            planned = db.execute(
                """SELECT work_item_id,project_id FROM work_items
                   WHERE task_id IS NULL AND title=? AND agent_id=?
                   ORDER BY updated_at DESC LIMIT 1""",
                (snap.label, "jarvis" if snap.agent_id == "main" else snap.agent_id),
            ).fetchone()
            if planned:
                work_item_id, project_id = planned
                bind_event_id = f"automatic-bind:{snap.task_id}:{work_item_id}"
                db.execute(
                    """INSERT OR IGNORE INTO events
                       (event_id,task_id,event_type,status,timestamp,summary,projected,created_at)
                       VALUES(?,?, 'binding_matched',?,?,?,0,?)""",
                    (
                        bind_event_id, snap.task_id, snap.status, snap.last_event_at,
                        f"自动关联 Run {snap.task_id} → Work Item {work_item_id}", now,
                    ),
                )
                db.execute(
                    "UPDATE binding_diagnostics SET resolved_at=?,updated_at=? WHERE task_id=? AND resolved_at IS NULL",
                    (now, now, snap.task_id),
                )
            else:
                project_id = "unfiled"
                work_item_id = f"run:{snap.task_id}"
                db.execute(
                    """INSERT OR IGNORE INTO projects
                       (project_id,name,origin,status,raw_json,created_at,updated_at)
                       VALUES('unfiled','待归档','unfiled','running','{}',?,?)""",
                    (now, now),
                )
                candidate, candidates, confidence = _diagnostic_match(db, snap)
                message = (
                    f"未匹配到预建步骤：{snap.agent_id or 'unknown'} / {snap.label}；"
                    "Run 已进入待归档，请补建步骤或人工绑定"
                )
                diagnostic_id = f"unmatched-run:{snap.task_id}"
                prior = db.execute(
                    "SELECT 1 FROM binding_diagnostics WHERE diagnostic_id=?", (diagnostic_id,)
                ).fetchone()
                if not _task_was_bound(db, snap.task_id):
                    db.execute(
                        """INSERT INTO binding_diagnostics
                           (diagnostic_id,task_id,project_id,severity,kind,message,details_json,
                            created_at,updated_at,resolved_at)
                           VALUES(?,?,?,'warning','unmatched_run',?,?,?, ?,NULL)
                           ON CONFLICT(diagnostic_id) DO UPDATE SET
                             project_id=COALESCE(binding_diagnostics.project_id,excluded.project_id),
                             message=excluded.message,details_json=excluded.details_json,
                             updated_at=excluded.updated_at""",
                        (
                            diagnostic_id, snap.task_id, candidate, message,
                            json.dumps({
                                "agent_id": snap.agent_id,
                                "label": snap.label,
                                "requester_session_key": snap.requester_session_key,
                                "parent_flow_id": raw.get("parentFlowId"),
                                "candidate_roots": candidates,
                                "confidence": confidence,
                            }, ensure_ascii=False),
                            now, now,
                        ),
                    )
                if not prior and not _task_was_bound(db, snap.task_id):
                    LOG.warning(
                        "unmatched_run task_id=%s agent=%s label=%r candidate_project=%s",
                        snap.task_id, snap.agent_id, snap.label, candidate or "none",
                    )
        db.execute(
            """INSERT INTO work_items
               (work_item_id,project_id,parent_work_item_id,task_id,title,agent_id,status,
                latest_progress,started_at,ended_at,last_event_at,raw_json,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(work_item_id) DO UPDATE SET
                 task_id=excluded.task_id,title=excluded.title,agent_id=excluded.agent_id,
                 status=excluded.status,latest_progress=excluded.latest_progress,
                 started_at=excluded.started_at,ended_at=excluded.ended_at,
                 last_event_at=excluded.last_event_at,raw_json=excluded.raw_json,
                 updated_at=excluded.updated_at""",
            (
                work_item_id, project_id, None, snap.task_id, snap.label,
                "jarvis" if snap.agent_id == "main" else snap.agent_id,
                snap.status, event.summary, snap.started_at, snap.ended_at,
                snap.last_event_at, json.dumps(raw, ensure_ascii=False), now,
            ),
        )

    def sync_workboard(self, boards: list[dict], cards: list[dict]) -> None:
        now = int(time.time() * 1000)
        names = {
            str(board.get("id") or board.get("boardId")):
            str(board.get("name") or board.get("title") or board.get("id") or board.get("boardId"))
            for board in boards if board.get("id") or board.get("boardId")
        }
        root_names = {
            _board_id(card): str(card.get("title"))
            for card in cards
            if "project-root" in (card.get("labels") or []) and card.get("title")
        }
        with self.connect() as db:
            ignored_board_ids = {
                str(row[0]) for row in db.execute("SELECT project_id FROM ignored_projects")
            }
            user_board_ids = {
                _board_id(card) for card in cards
                if "project-root" in (card.get("labels") or [])
                and _board_id(card) not in ignored_board_ids
            }
            for card in cards:
                card_id = str(card.get("id") or card.get("cardId") or "")
                if not card_id:
                    continue
                board_id = _board_id(card)
                if board_id not in user_board_ids:
                    continue
                project_id = board_id
                labels = card.get("labels") or []
                is_root = "project-root" in labels
                project_name = root_names.get(board_id) or names.get(board_id, board_id)
                old_project = db.execute(
                    "SELECT raw_json FROM projects WHERE project_id=?", (project_id,)
                ).fetchone()
                project_raw = {"boardId": board_id}
                if old_project:
                    try:
                        project_raw.update(json.loads(old_project["raw_json"] or "{}"))
                    except json.JSONDecodeError:
                        pass
                db.execute(
                    """INSERT INTO projects(project_id,name,origin,status,raw_json,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?) ON CONFLICT(project_id) DO UPDATE SET
                       name=excluded.name,raw_json=excluded.raw_json,updated_at=excluded.updated_at""",
                    (project_id, project_name, "user", "running", json.dumps(project_raw, ensure_ascii=False), now, now),
                )
                if is_root:
                    continue
                linked_task_id = _linked_task_id(card)
                parent_ids = card.get("parentIds") or card.get("parent_ids") or []
                parent_id = str(parent_ids[0]) if parent_ids else None
                db.execute(
                    """INSERT INTO work_items
                       (work_item_id,project_id,parent_work_item_id,task_id,title,agent_id,status,
                        latest_progress,started_at,ended_at,last_event_at,raw_json,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(work_item_id) DO UPDATE SET
                        project_id=excluded.project_id,parent_work_item_id=excluded.parent_work_item_id,
                        task_id=COALESCE(excluded.task_id,work_items.task_id),
                        title=excluded.title,agent_id=excluded.agent_id,
                        status=CASE WHEN work_items.task_id IS NULL AND NOT EXISTS(
                          SELECT 1 FROM work_item_refs r WHERE r.work_item_id=work_items.work_item_id
                        ) THEN excluded.status ELSE work_items.status END,
                        latest_progress=CASE WHEN work_items.task_id IS NULL AND NOT EXISTS(
                          SELECT 1 FROM work_item_refs r WHERE r.work_item_id=work_items.work_item_id
                        ) THEN excluded.latest_progress ELSE work_items.latest_progress END,
                        last_event_at=CASE WHEN work_items.task_id IS NULL AND NOT EXISTS(
                          SELECT 1 FROM work_item_refs r WHERE r.work_item_id=work_items.work_item_id
                        ) THEN excluded.last_event_at ELSE work_items.last_event_at END,
                        raw_json=CASE WHEN work_items.task_id IS NULL AND NOT EXISTS(
                          SELECT 1 FROM work_item_refs r WHERE r.work_item_id=work_items.work_item_id
                        ) THEN excluded.raw_json ELSE work_items.raw_json END,
                        updated_at=excluded.updated_at""",
                    (
                        f"wb:{card_id}", project_id, f"wb:{parent_id}" if parent_id else None,
                        linked_task_id, str(card.get("title") or card.get("summary") or card_id),
                        str(card.get("agentId") or "unassigned"), _workboard_status(card.get("status")),
                        _workboard_progress(card), None, None,
                        int(card.get("updatedAt") or card.get("createdAt") or now),
                        json.dumps(card, ensure_ascii=False), now,
                    ),
                )

    def ingest_automation(self, snapshot: TaskSnapshot, local_date: str) -> bool:
        raw = snapshot.raw
        source_id = str(raw.get("sourceId") or snapshot.task_id)
        now = int(time.time() * 1000)
        result = snapshot.terminal_summary or snapshot.progress_summary or ""
        error = snapshot.error or ""
        success = snapshot.status.value == "succeeded"
        failed = snapshot.status.value in {"failed", "timed_out", "blocked"}
        run_timestamp = snapshot.ended_at or snapshot.started_at or snapshot.created_at
        run_date = datetime.fromtimestamp(run_timestamp / 1000).astimezone().date().isoformat()
        current_day_run = run_date == local_date
        with self.connect() as db:
            old = db.execute(
                "SELECT * FROM automation_monitors WHERE source_id=?", (source_id,)
            ).fetchone()
            run = db.execute(
                "SELECT status FROM automation_runs WHERE task_id=?", (snapshot.task_id,)
            ).fetchone()
            counted_success = current_day_run and success and (not run or run["status"] != "succeeded")
            counted_failure = current_day_run and failed and (not run or run["status"] not in {"failed", "timed_out", "blocked"})
            same_day = bool(old and old["window_date"] == local_date)
            success_count = (old["success_count"] if same_day else 0) + int(counted_success)
            failure_count = (old["failure_count"] if same_day else 0) + int(counted_failure)
            last_success = snapshot.ended_at if success else (old["last_success_at"] if old else None)
            if current_day_run and snapshot.status.value in {"queued", "running"}:
                health = "running"
            elif success_count > 0 and not (current_day_run and failed):
                health = "healthy"
            else:
                health = "unhealthy"
            previous_health = old["health"] if old else None
            previous_bucket = (old["last_projected_at"] or 0) // 3_600_000 if old else -1
            current_bucket = now // 3_600_000
            dirty = int(
                old is None or health != previous_health
                or (current_day_run and failed)
                or current_bucket != previous_bucket
            )
            consecutive = 0 if success else ((old["consecutive_failures"] if old else 0) + int(counted_failure))
            db.execute(
                """INSERT INTO automation_monitors
                   (source_id,label,health,active_run_id,last_run_status,last_started_at,last_ended_at,
                    last_success_at,last_result,last_error,consecutive_failures,window_date,
                    success_count,failure_count,dirty,last_projected_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(source_id) DO UPDATE SET
                    label=excluded.label,health=excluded.health,active_run_id=excluded.active_run_id,
                    last_run_status=excluded.last_run_status,last_started_at=excluded.last_started_at,
                    last_ended_at=excluded.last_ended_at,last_success_at=excluded.last_success_at,
                    last_result=excluded.last_result,last_error=excluded.last_error,
                    consecutive_failures=excluded.consecutive_failures,window_date=excluded.window_date,
                    success_count=excluded.success_count,failure_count=excluded.failure_count,
                    dirty=MAX(automation_monitors.dirty,excluded.dirty),updated_at=excluded.updated_at""",
                (
                    source_id, snapshot.label, health,
                    snapshot.task_id if snapshot.status.value in {"queued", "running"} else None,
                    snapshot.status, snapshot.started_at, snapshot.ended_at, last_success,
                    result, error, consecutive, local_date, success_count, failure_count,
                    dirty, old["last_projected_at"] if old else None, now,
                ),
            )
            db.execute(
                """INSERT INTO automation_runs(task_id,source_id,status,ended_at,updated_at)
                   VALUES(?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET
                   status=excluded.status,ended_at=excluded.ended_at,updated_at=excluded.updated_at""",
                (snapshot.task_id, source_id, snapshot.status, snapshot.ended_at, now),
            )
            return bool(dirty)

    def ensure_automation(self, source_id: str, label: str, local_date: str) -> None:
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute(
                """INSERT OR IGNORE INTO automation_monitors
                   (source_id,label,health,active_run_id,last_run_status,last_started_at,last_ended_at,
                    last_success_at,last_result,last_error,consecutive_failures,window_date,
                    success_count,failure_count,dirty,last_projected_at,updated_at)
                   VALUES(?,?,'unhealthy',NULL,'missing',NULL,NULL,NULL,'','今天尚无成功推送',
                          0,?,0,0,1,NULL,?)""",
                (source_id, label, local_date, now),
            )

    def refresh_automation_health(self, local_date: str) -> int:
        now = int(time.time() * 1000)
        start = datetime.fromisoformat(local_date).astimezone()
        start_ms = int(start.timestamp() * 1000)
        end_ms = int((start + timedelta(days=1)).timestamp() * 1000)
        changed = 0
        with self.connect() as db:
            monitors = list(db.execute("SELECT * FROM automation_monitors"))
            for monitor in monitors:
                runs = list(db.execute(
                    """SELECT status,ended_at FROM automation_runs
                       WHERE source_id=? AND COALESCE(ended_at,updated_at)>=?
                       AND COALESCE(ended_at,updated_at)<? ORDER BY COALESCE(ended_at,updated_at)""",
                    (monitor["source_id"], start_ms, end_ms),
                ))
                success_count = sum(row["status"] == "succeeded" for row in runs)
                failure_count = sum(row["status"] in {"failed", "timed_out", "blocked"} for row in runs)
                latest_status = runs[-1]["status"] if runs else "missing"
                if latest_status in {"queued", "running"}:
                    health = "running"
                elif latest_status == "succeeded":
                    health = "healthy"
                else:
                    health = "unhealthy"
                transition = health != monitor["health"] or monitor["window_date"] != local_date
                dirty = int(bool(monitor["dirty"]) or transition)
                if transition:
                    changed += 1
                db.execute(
                    """UPDATE automation_monitors SET health=?,last_run_status=?,window_date=?,
                       success_count=?,failure_count=?,dirty=?,updated_at=? WHERE source_id=?""",
                    (
                        health, latest_status, local_date, success_count, failure_count,
                        dirty, now, monitor["source_id"],
                    ),
                )
        return changed

    def automation_rows(self, dirty_only: bool = False) -> list[sqlite3.Row]:
        query = "SELECT * FROM automation_monitors"
        if dirty_only:
            query += " WHERE dirty=1"
        query += " ORDER BY updated_at DESC"
        with self.connect() as db:
            return list(db.execute(query))

    def mark_automation_projected(self, source_id: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE automation_monitors SET dirty=0,last_projected_at=? WHERE source_id=?",
                (int(time.time() * 1000), source_id),
            )

    def project_rows(self) -> list[dict]:
        with self.connect() as db:
            projects = [dict(row) for row in db.execute(
                "SELECT * FROM projects ORDER BY updated_at DESC"
            )]
            items = [dict(row) for row in db.execute(
                """SELECT w.*,r.kind AS dispatch_kind,r.task_ref
                   FROM work_items w LEFT JOIN work_item_refs r USING(work_item_id)
                   ORDER BY w.last_event_at DESC"""
            )]
        by_project: dict[str, list[dict]] = {}
        for item in items:
            by_project.setdefault(item["project_id"], []).append(item)
        for project in projects:
            project["items"] = by_project.get(project["project_id"], [])
        return projects

    def register_root(
        self, project_id: str, title: str, *, channel_key: str,
        target_type: str, target_id: str, source_message_id: str | None = None,
    ) -> dict:
        """Register a user-origin root task before subagents are spawned."""
        now = int(time.time() * 1000)
        raw = {
            "channel_key": channel_key, "target_type": target_type,
            "target_id": target_id, "source_message_id": source_message_id,
        }
        with self.connect() as db:
            # An explicit new registration is the only operation allowed to revive
            # a previously cleaned project id.
            db.execute("DELETE FROM ignored_projects WHERE project_id=?", (project_id,))
            db.execute(
                """INSERT INTO projects(project_id,name,origin,status,raw_json,created_at,updated_at)
                   VALUES(?,?, 'user','running',?,?,?) ON CONFLICT(project_id) DO UPDATE SET
                   name=excluded.name,origin='user',status='running',raw_json=excluded.raw_json,
                   updated_at=excluded.updated_at""",
                (project_id, title, json.dumps(raw, ensure_ascii=False), now, now),
            )
            row = db.execute(
                "SELECT project_id,name,origin,status,created_at,updated_at FROM projects WHERE project_id=?",
                (project_id,),
            ).fetchone()
            if not row:
                raise RuntimeError(f"root registration verification failed: {project_id}")
            return dict(row)

    def task_view(self, task_id: str) -> dict | None:
        project = self.project_by_id(task_id)
        if project is None:
            item = self.work_item_for_task(task_id)
            if not item:
                return None
            project = self.project_by_id(item["project_id"])
        if not project:
            return None
        items = self.work_items_for_project(project["project_id"])
        events: list[dict] = []
        with self.connect() as db:
            for item in items:
                if not item.get("task_id"):
                    continue
                rows = db.execute(
                    "SELECT event_id,task_id,event_type,status,timestamp,summary FROM events WHERE task_id=? ORDER BY timestamp,event_id",
                    (item["task_id"],),
                )
                events.extend(dict(row) for row in rows)
            notes = db.execute(
                "SELECT note_id,kind,text,timestamp FROM planner_notes WHERE project_id=? ORDER BY timestamp,note_id",
                (project["project_id"],),
            )
            for note in notes:
                events.append({
                    "event_id": note["note_id"], "task_id": project["project_id"],
                    "event_type": "planner_action",
                    "status": "blocked" if note["kind"] == "blocked" else (
                        "succeeded" if note["kind"] == "deliver" else "running"
                    ),
                    "timestamp": note["timestamp"], "summary": note["text"],
                    "kind": note["kind"], "actor": "jarvis",
                })
        events.sort(key=lambda row: (row["timestamp"], row["event_id"]))
        statuses = [item["status"] for item in items]
        if any(status in {"failed", "timed_out"} for status in statuses):
            status = "failed"
        elif any(status == "blocked" for status in statuses):
            status = "blocked"
        elif items and all(status in {"succeeded", "cancelled"} for status in statuses):
            status = "succeeded"
        elif any(status == "running" for status in statuses):
            status = "running"
        else:
            status = project["status"]
        result = dict(project)
        result["status"] = status
        result["items"] = items
        result["timeline"] = events
        try:
            result["origin_detail"] = json.loads(result.get("raw_json") or "{}")
        except json.JSONDecodeError:
            result["origin_detail"] = {}
        result["card"] = self.latest_card(result["project_id"])
        result["warnings"] = self.binding_diagnostics(result["project_id"])
        return result

    def binding_diagnostics(self, project_id: str | None = None) -> list[dict]:
        query = "SELECT * FROM binding_diagnostics WHERE resolved_at IS NULL"
        params: tuple = ()
        if project_id is not None:
            query += " AND project_id=?"
            params = (project_id,)
        query += " ORDER BY created_at DESC"
        with self.connect() as db:
            rows = [dict(row) for row in db.execute(query, params)]
        for row in rows:
            try:
                row["details"] = json.loads(row.pop("details_json"))
            except json.JSONDecodeError:
                row["details"] = {}
            details = row["details"]
            row["agent"] = details.get("agent_id") or "unknown"
            row["label"] = details.get("label") or ""
            row["discovered_at"] = row["created_at"]
            row["candidate_root"] = row.get("project_id")
            row["candidate_roots"] = details.get("candidate_roots") or (
                [row["project_id"]] if row.get("project_id") else []
            )
            row["confidence"] = details.get("confidence") or (
                "exact" if row.get("project_id") else "unmatched"
            )
            row["suggested_fix"] = details.get("suggested_fix") or f"fto bind {row['task_id']} <work_item_id>"
        return rows

    def register_step_ref(
        self, work_item_id: str, *, kind: str, task_ref: str | None = None,
        status: str = "running",
    ) -> dict:
        if kind not in {"send", "spawn", "inline"}:
            raise ValueError(f"unsupported step kind: {kind}")
        if status not in {"queued", "running", "blocked", "succeeded", "failed", "timed_out", "cancelled"}:
            raise ValueError(f"unsupported step status: {status}")
        now = int(time.time() * 1000)
        with self.connect() as db:
            item = db.execute("SELECT * FROM work_items WHERE work_item_id=?", (work_item_id,)).fetchone()
            if not item:
                raise ValueError(f"work item not found: {work_item_id}")
            old = db.execute("SELECT * FROM work_item_refs WHERE work_item_id=?", (work_item_id,)).fetchone()
            registered_at = int(old["registered_at"]) if old else now
            effective_ref = task_ref or (old["task_ref"] if old else None)
            db.execute(
                """INSERT INTO work_item_refs(work_item_id,kind,task_ref,registered_at,updated_at)
                   VALUES(?,?,?,?,?) ON CONFLICT(work_item_id) DO UPDATE SET
                   kind=excluded.kind,task_ref=COALESCE(excluded.task_ref,work_item_refs.task_ref),
                   updated_at=excluded.updated_at""",
                (work_item_id, kind, task_ref, registered_at, now),
            )
            synthetic = item["task_id"]
            if kind in {"send", "inline"} and not synthetic:
                synthetic = f"{kind}:{hashlib.sha256(work_item_id.encode()).hexdigest()[:24]}"
                raw = {"kind": kind, "task_ref": effective_ref, "synthetic": True}
                db.execute(
                    """INSERT OR IGNORE INTO tasks
                       (task_id,parent_id,child_id,status,last_event_at,snapshot_json,updated_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (synthetic, f"oc:{synthetic}", f"oc:{synthetic}:agent:{item['agent_id']}",
                     status, now, json.dumps(raw, ensure_ascii=False), now),
                )
                db.execute("UPDATE work_items SET task_id=? WHERE work_item_id=?", (synthetic, work_item_id))
            ref_changed = not old or old["kind"] != kind or (
                task_ref is not None and old["task_ref"] != task_ref
            )
            if ref_changed or item["status"] != status:
                self._set_step_status(db, work_item_id, synthetic, status, now,
                                      _step_status_summary(kind, status))
            return {
                "work_item_id": work_item_id, "project_id": item["project_id"],
                "kind": kind, "task_ref": effective_ref, "status": status,
            }

    def register_provisional_step(
        self, project_id: str, title: str, *, agent_id: str, kind: str,
        task_ref: str | None = None, status: str = "running",
    ) -> str:
        """Persist a dispatch step before the Workboard round trip completes."""
        marker = hashlib.sha256(
            f"{project_id}|{kind}|{agent_id}|{title}".encode()
        ).hexdigest()[:24]
        work_item_id = f"pending:{marker}"
        now = int(time.time() * 1000)
        with self.connect() as db:
            existing = db.execute(
                """SELECT w.work_item_id FROM work_items w
                   JOIN work_item_refs r USING(work_item_id)
                   WHERE w.project_id=? AND w.title=? AND w.agent_id=? AND r.kind=?
                     AND w.work_item_id NOT LIKE 'pending:%'
                   ORDER BY w.updated_at DESC LIMIT 1""",
                (project_id, title, agent_id, kind),
            ).fetchone()
            if existing:
                work_item_id = str(existing["work_item_id"])
        if not work_item_id.startswith("pending:"):
            self.register_step_ref(
                work_item_id, kind=kind, task_ref=task_ref, status=status
            )
            self.reconcile_project_statuses()
            return work_item_id
        with self.connect() as db:
            db.execute(
                """INSERT OR IGNORE INTO work_items
                   (work_item_id,project_id,parent_work_item_id,task_id,title,agent_id,status,
                    latest_progress,started_at,ended_at,last_event_at,raw_json,updated_at)
                   VALUES(?,?,NULL,NULL,?,?,?,?,?,?,?,'{}',?)""",
                (work_item_id, project_id, title, agent_id, status,
                 _step_status_summary(kind, status), now,
                 now if status in {"succeeded", "failed", "timed_out", "cancelled"} else None,
                 now, now),
            )
        self.register_step_ref(
            work_item_id, kind=kind, task_ref=task_ref, status=status
        )
        self.reconcile_project_statuses()
        return work_item_id

    def promote_provisional_step(self, provisional_id: str, work_item_id: str) -> None:
        """Move local dispatch state onto the durable Workboard Work Item id."""
        if provisional_id == work_item_id:
            return
        now = int(time.time() * 1000)
        with self.connect() as db:
            source = db.execute(
                "SELECT * FROM work_items WHERE work_item_id=?", (provisional_id,)
            ).fetchone()
            target = db.execute(
                "SELECT * FROM work_items WHERE work_item_id=?", (work_item_id,)
            ).fetchone()
            if not source or not target:
                raise ValueError("provisional or Workboard step missing during promotion")
            # Release the UNIQUE work_items.task_id slot before moving the
            # synthetic/session task reference onto the durable wb: row.
            db.execute(
                "UPDATE work_items SET task_id=NULL WHERE work_item_id=?",
                (provisional_id,),
            )
            db.execute(
                """UPDATE work_items SET task_id=?,status=?,latest_progress=?,
                   started_at=?,ended_at=?,last_event_at=?,updated_at=? WHERE work_item_id=?""",
                (source["task_id"], source["status"], source["latest_progress"],
                 source["started_at"], source["ended_at"], source["last_event_at"],
                 now, work_item_id),
            )
            db.execute("DELETE FROM work_item_refs WHERE work_item_id=?", (work_item_id,))
            db.execute(
                "UPDATE work_item_refs SET work_item_id=?,updated_at=? WHERE work_item_id=?",
                (work_item_id, now, provisional_id),
            )
            db.execute("DELETE FROM work_items WHERE work_item_id=?", (provisional_id,))

    def enqueue_dispatch(self, *, key: str, project_id: str, operation: str, payload: dict) -> bool:
        """Persist slow Workboard synchronization outside the interactive CLI path."""
        now = int(time.time() * 1000)
        with self.connect() as db:
            cur = db.execute(
                """INSERT OR IGNORE INTO dispatch_outbox
                   (idempotency_key,project_id,operation,payload_json,status,attempts,
                    available_at,last_error,created_at,updated_at)
                   VALUES(?,?,?,?,?,0,?,'',?,?)""",
                (key, project_id, operation, json.dumps(payload, ensure_ascii=False),
                 "pending", now, now, now),
            )
            return cur.rowcount == 1

    def pending_dispatch(self, limit: int = 10) -> list[dict]:
        now = int(time.time() * 1000)
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                """SELECT * FROM dispatch_outbox
                   WHERE status='pending' AND available_at<=?
                   ORDER BY outbox_id LIMIT ?""",
                (now, limit),
            )]

    def finish_dispatch(self, outbox_id: int) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE dispatch_outbox SET status='done',updated_at=? WHERE outbox_id=?",
                (int(time.time() * 1000), outbox_id),
            )

    def fail_dispatch(self, outbox_id: int, error: str, max_attempts: int = 8) -> None:
        now = int(time.time() * 1000)
        with self.connect() as db:
            row = db.execute(
                "SELECT attempts FROM dispatch_outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
            attempts = int(row[0]) + 1
            status = "dead" if attempts >= max_attempts else "pending"
            delay = min(300_000, 2 ** min(attempts, 8) * 1000)
            db.execute(
                """UPDATE dispatch_outbox SET status=?,attempts=?,available_at=?,last_error=?,updated_at=?
                   WHERE outbox_id=?""",
                (status, attempts, now + delay, error[:1000], now, outbox_id),
            )

    @staticmethod
    def _set_step_status(
        db: sqlite3.Connection, work_item_id: str, task_id: str | None,
        status: str, timestamp: int, summary: str,
    ) -> None:
        terminal = status in {"succeeded", "failed", "timed_out", "cancelled"}
        db.execute(
            """UPDATE work_items SET status=?,latest_progress=?,
               started_at=COALESCE(started_at,?),ended_at=CASE WHEN ? THEN ? ELSE ended_at END,
               last_event_at=?,updated_at=? WHERE work_item_id=?""",
            (status, summary, timestamp, int(terminal), timestamp, timestamp, timestamp, work_item_id),
        )
        if task_id:
            db.execute(
                "UPDATE tasks SET status=?,last_event_at=?,updated_at=? WHERE task_id=?",
                (status, timestamp, timestamp, task_id),
            )
            event_id = f"step:{task_id}:{status}:{timestamp}"
            db.execute(
                """INSERT OR IGNORE INTO events
                   (event_id,task_id,event_type,status,timestamp,summary,projected,created_at)
                   VALUES(?,?, 'step_state_changed',?,?,?,0,?)""",
                (event_id, task_id, status, timestamp, summary, timestamp),
            )

    def add_planner_note(self, project_id: str, kind: str, text: str) -> dict:
        if kind not in {"planning", "dispatch", "waiting", "summarize", "deliver", "blocked"}:
            raise ValueError(f"unsupported planner note kind: {kind}")
        if not self.project_by_id(project_id):
            raise ValueError(f"project not found: {project_id}")
        now = int(time.time() * 1000)
        note_id = f"note:{hashlib.sha256(f'{project_id}|{kind}|{text}|{now}'.encode()).hexdigest()[:32]}"
        with self.connect() as db:
            db.execute(
                """INSERT INTO planner_notes(note_id,project_id,kind,text,timestamp,created_at)
                   VALUES(?,?,?,?,?,?)""",
                (note_id, project_id, kind, text, now, now),
            )
        return {"note_id": note_id, "project_id": project_id, "kind": kind, "text": text, "timestamp": now}

    def sync_session_steps(self, sessions: list[dict]) -> int:
        """Advance sessions_send Work Items from a uniquely matched target session."""
        changed = 0
        with self.connect() as db:
            rows = list(db.execute(
                """SELECT w.*,r.task_ref,r.registered_at FROM work_items w
                   JOIN work_item_refs r USING(work_item_id)
                   WHERE r.kind='send' AND w.status NOT IN ('succeeded','failed','timed_out','cancelled')"""
            ))
            for item in rows:
                reference = item["task_ref"]
                if reference:
                    matches = [s for s in sessions if reference in {s.get("key"), s.get("sessionId"), s.get("runId")}]
                else:
                    matches = [
                        s for s in sessions
                        if s.get("agentId") == item["agent_id"]
                        and int(s.get("updatedAt") or 0) >= int(item["registered_at"])
                        and _session_has_main(s)
                    ]
                if len(matches) != 1:
                    continue
                session = matches[0]
                session_ref = str(session.get("key") or session.get("sessionId"))
                timestamp = int(session.get("updatedAt") or int(time.time() * 1000))
                ref_changed = reference != session_ref
                if ref_changed:
                    db.execute(
                        "UPDATE work_item_refs SET task_ref=?,updated_at=? WHERE work_item_id=?",
                        (session_ref, timestamp, item["work_item_id"]),
                    )
                changed += int(ref_changed)
        if changed:
            self.reconcile_project_statuses()
        return changed

    def sync_spawn_session_steps(self, sessions: list[dict]) -> int:
        """Advance spawn Work Items when the Background Task row is missing.

        OpenClaw can retain the child session while omitting its runtime=subagent
        task row. Match only an explicitly registered step using agent, exact
        label, main parentage and registration time; never infer across roots.
        """
        changed = 0
        with self.connect() as db:
            rows = list(db.execute(
                """SELECT w.*,r.task_ref,r.registered_at FROM work_items w
                   JOIN work_item_refs r USING(work_item_id)
                   WHERE r.kind='spawn'
                     AND w.status NOT IN ('succeeded','failed','timed_out','cancelled')"""
            ))
            claimed = {
                str(row[0]) for row in db.execute(
                    "SELECT task_ref FROM work_item_refs WHERE task_ref IS NOT NULL"
                ) if row[0]
            }
            for item in rows:
                reference = item["task_ref"]
                if reference:
                    matches = [
                        session for session in sessions
                        if reference in _session_refs(session)
                    ]
                else:
                    matches = [
                        session for session in sessions
                        if session.get("kind") == "spawn-child"
                        and session.get("agentId") == item["agent_id"]
                        and session.get("label") == item["title"]
                        and int(session.get("sessionStartedAt") or session.get("updatedAt") or 0)
                            >= int(item["registered_at"]) - 5_000
                        and _session_has_main(session)
                        and not (_session_refs(session) & claimed)
                    ]
                if len(matches) != 1:
                    continue
                session = matches[0]
                session_ref = str(session.get("key") or session.get("sessionId") or "")
                if not session_ref:
                    continue
                status = _session_status(session.get("status"))
                timestamp = int(session.get("lastInteractionAt") or session.get("updatedAt") or time.time() * 1000)
                started_at = int(session.get("sessionStartedAt") or timestamp)
                task_id = item["task_id"] or f"session:{hashlib.sha256(session_ref.encode()).hexdigest()[:24]}"
                old_status = str(item["status"])
                ref_changed = reference != session_ref
                if not item["task_id"]:
                    raw = {"source": "session_fallback", "session": session}
                    db.execute(
                        """INSERT OR IGNORE INTO tasks
                           (task_id,parent_id,child_id,status,last_event_at,snapshot_json,updated_at)
                           VALUES(?,?,?,?,?,?,?)""",
                        (task_id, f"oc:{task_id}", f"oc:{task_id}:agent:{item['agent_id']}",
                         status, timestamp, json.dumps(raw, ensure_ascii=False), timestamp),
                    )
                    db.execute(
                        "UPDATE work_items SET task_id=?,started_at=COALESCE(started_at,?) WHERE work_item_id=?",
                        (task_id, started_at, item["work_item_id"]),
                    )
                if ref_changed:
                    db.execute(
                        "UPDATE work_item_refs SET task_ref=?,updated_at=? WHERE work_item_id=?",
                        (session_ref, timestamp, item["work_item_id"]),
                    )
                if old_status != status or ref_changed:
                    self._set_step_status(
                        db, item["work_item_id"], task_id, status, timestamp,
                        f"spawn 会话兜底：{item['title']} {_step_status_summary('spawn', status)}",
                    )
                    changed += 1
                db.execute(
                    """UPDATE binding_diagnostics SET resolved_at=?,updated_at=?
                       WHERE kind='missing_spawn_run' AND task_id=? AND resolved_at IS NULL""",
                    (timestamp, timestamp, item["work_item_id"]),
                )
        if changed:
            self.reconcile_project_statuses()
        return changed

    def flag_stale_spawn_steps(self, soft_timeout_seconds: int, now_ms: int | None = None) -> int:
        """Surface registered spawn steps that have no observable run/session."""
        now = int(now_ms or time.time() * 1000)
        cutoff = now - max(1, soft_timeout_seconds) * 1000
        changed = 0
        with self.connect() as db:
            rows = list(db.execute(
                """SELECT w.work_item_id,w.project_id,w.title,w.agent_id,r.registered_at
                   FROM work_items w JOIN work_item_refs r USING(work_item_id)
                   JOIN projects p USING(project_id)
                   WHERE r.kind='spawn' AND w.task_id IS NULL AND w.status='running'
                     AND r.registered_at<=? AND p.status IN ('queued','running','blocked')""",
                (cutoff,),
            ))
            for item in rows:
                diagnostic_id = f"missing-spawn-run:{item['work_item_id']}"
                message = (
                    f"子任务已登记但未发现可观察 Run/Session：{item['agent_id']} / {item['title']}；"
                    "请检查派发是否成功或手动更新步骤状态"
                )
                details = json.dumps({
                    "work_item_id": item["work_item_id"], "agent_id": item["agent_id"],
                    "label": item["title"], "candidate_roots": [item["project_id"]],
                    "confidence": "exact",
                    "suggested_fix": f"fto step {item['project_id']} --title {json.dumps(item['title'], ensure_ascii=False)} "
                                     f"--agent {item['agent_id']} --kind spawn --status <状态>",
                }, ensure_ascii=False)
                cur = db.execute(
                    """INSERT OR IGNORE INTO binding_diagnostics
                       (diagnostic_id,task_id,project_id,severity,kind,message,details_json,
                        created_at,updated_at,resolved_at)
                       VALUES(?,?,?,'warning','missing_spawn_run',?,?,?, ?,NULL)""",
                    (diagnostic_id, item["work_item_id"], item["project_id"], message,
                     details, now, now),
                )
                changed += int(cur.rowcount == 1)
                if cur.rowcount == 1:
                    LOG.warning("missing_spawn_run work_item_id=%s project_id=%s",
                                item["work_item_id"], item["project_id"])
        return changed

    def sync_task_refs(self) -> int:
        """Bind explicit Background Task taskId/runId refs to prepared Work Items."""
        pairs: list[tuple[str, str]] = []
        with self.connect() as db:
            refs = list(db.execute(
                """SELECT w.work_item_id,w.task_id,r.task_ref FROM work_items w
                   JOIN work_item_refs r USING(work_item_id)
                   WHERE r.kind='spawn' AND r.task_ref IS NOT NULL"""
            ))
            tasks = list(db.execute("SELECT task_id,snapshot_json FROM tasks"))
            by_ref: dict[str, str] = {}
            for task in tasks:
                by_ref[str(task["task_id"])] = str(task["task_id"])
                try:
                    raw = json.loads(task["snapshot_json"] or "{}")
                except json.JSONDecodeError:
                    raw = {}
                for key in ("runId", "taskId"):
                    if raw.get(key):
                        by_ref[str(raw[key])] = str(task["task_id"])
            for ref in refs:
                task_id = by_ref.get(str(ref["task_ref"]))
                if task_id and ref["task_id"] != task_id:
                    pairs.append((task_id, str(ref["work_item_id"])))
        changed = 0
        for task_id, work_item_id in pairs:
            result = self.bind_run(task_id, work_item_id)
            changed += int(result["changed"])
        return changed

    def reconcile_binding_diagnostics(self) -> int:
        """Resolve dangling/bound warnings and detach invalid historical candidates."""
        now = int(time.time() * 1000)
        changed = 0
        with self.connect() as db:
            rows = list(db.execute(
                """SELECT d.diagnostic_id,d.task_id,d.project_id,d.kind,d.details_json,
                          d.created_at,t.snapshot_json,
                          w.project_id AS work_project,p.created_at AS project_created_at,
                          p.status AS project_status
                   FROM binding_diagnostics d
                   LEFT JOIN tasks t ON t.task_id=d.task_id
                   LEFT JOIN work_items w ON w.task_id=d.task_id
                   LEFT JOIN projects p ON p.project_id=d.project_id
                   WHERE d.resolved_at IS NULL"""
            ))
            for row in rows:
                if row["kind"] == "missing_spawn_run":
                    try:
                        details = json.loads(row["details_json"] or "{}")
                    except json.JSONDecodeError:
                        details = {}
                    item = db.execute(
                        "SELECT task_id,status FROM work_items WHERE work_item_id=?",
                        (details.get("work_item_id"),),
                    ).fetchone()
                    if item and item["task_id"] is None and item["status"] == "running" \
                            and row["project_status"] not in {"succeeded", "failed", "cancelled"}:
                        continue
                    db.execute(
                        "UPDATE binding_diagnostics SET resolved_at=?,updated_at=? WHERE diagnostic_id=?",
                        (now, now, row["diagnostic_id"]),
                    )
                    changed += 1
                    continue
                if (row["work_project"] is None or row["work_project"] != "unfiled"
                        or row["project_status"] in {"succeeded", "failed", "cancelled"}):
                    db.execute(
                        "UPDATE binding_diagnostics SET resolved_at=?,updated_at=? WHERE diagnostic_id=?",
                        (now, now, row["diagnostic_id"]),
                    )
                    changed += 1
                    continue
                project_created = row["project_created_at"]
                try:
                    task_raw = json.loads(row["snapshot_json"] or "{}")
                except json.JSONDecodeError:
                    task_raw = {}
                run_created = int(task_raw.get("createdAt") or row["created_at"])
                invalid_candidate = (
                    row["project_id"] is not None and (
                        project_created is None
                        or run_created < int(project_created)
                        or run_created - int(project_created) > 1_800_000
                    )
                )
                if invalid_candidate:
                    db.execute(
                        "UPDATE binding_diagnostics SET project_id=NULL,updated_at=? WHERE diagnostic_id=?",
                        (now, row["diagnostic_id"]),
                    )
                    changed += 1
        return changed

    def bind_run(self, task_id: str, work_item_id: str) -> dict:
        """Bind an orphan Run to a planned Work Item and append one audit event."""
        now = int(time.time() * 1000)
        event_id = f"manual-bind:{task_id}:{work_item_id}"
        with self.connect() as db:
            source = db.execute(
                "SELECT * FROM work_items WHERE task_id=?", (task_id,)
            ).fetchone()
            if not source:
                raise ValueError(f"run task not found: {task_id}")
            target = db.execute(
                "SELECT * FROM work_items WHERE work_item_id=?", (work_item_id,)
            ).fetchone()
            if not target:
                raise ValueError(f"work item not found: {work_item_id}")
            if source["work_item_id"] == work_item_id:
                return {
                    "ok": True, "changed": False, "task_id": task_id,
                    "work_item_id": work_item_id, "project_id": source["project_id"],
                    "audit_event_id": event_id,
                }
            if source["project_id"] != "unfiled":
                raise ValueError(
                    f"run already bound to {source['work_item_id']} in {source['project_id']}"
                )
            if target["task_id"] and target["task_id"] != task_id:
                raise ValueError(f"work item already bound to task: {target['task_id']}")
            db.execute("DELETE FROM work_items WHERE work_item_id=?", (source["work_item_id"],))
            db.execute(
                """UPDATE work_items SET task_id=?,status=?,latest_progress=?,started_at=?,
                   ended_at=?,last_event_at=?,raw_json=?,updated_at=? WHERE work_item_id=?""",
                (
                    task_id, source["status"], source["latest_progress"], source["started_at"],
                    source["ended_at"], source["last_event_at"], source["raw_json"], now,
                    work_item_id,
                ),
            )
            summary = f"人工补绑 Run {task_id} → Work Item {work_item_id}"
            db.execute(
                """INSERT OR IGNORE INTO events
                   (event_id,task_id,event_type,status,timestamp,summary,projected,created_at)
                   VALUES(?,?, 'binding_corrected',?,?,?,0,?)""",
                (event_id, task_id, source["status"], now, summary, now),
            )
            db.execute(
                """UPDATE binding_diagnostics SET resolved_at=?,updated_at=?
                   WHERE task_id=? AND resolved_at IS NULL""",
                (now, now, task_id),
            )
            project_id = str(target["project_id"])
        self.reconcile_project_statuses()
        return {
            "ok": True, "changed": True, "task_id": task_id,
            "work_item_id": work_item_id, "project_id": project_id,
            "audit_event_id": event_id,
        }

    def list_task_views(self, active_only: bool = False, channel_key: str | None = None) -> list[dict]:
        result = []
        for project in self.project_rows():
            if project.get("origin") != "user" or project["project_id"] == "unfiled":
                continue
            view = self.task_view(project["project_id"])
            if not view:
                continue
            if active_only and view["status"] not in {"queued", "running", "blocked"}:
                continue
            if channel_key and view["origin_detail"].get("channel_key") != channel_key:
                continue
            result.append(view)
        return result

    def reconcile_project_statuses(self) -> int:
        """Materialize aggregate Work Item state onto user project rows."""
        now, changed = int(time.time() * 1000), 0
        with self.connect() as db:
            projects = list(db.execute("SELECT project_id,status FROM projects WHERE project_id!='unfiled'"))
            for project in projects:
                statuses = [row[0] for row in db.execute(
                    "SELECT status FROM work_items WHERE project_id=?", (project["project_id"],)
                )]
                if not statuses:
                    continue
                if any(status in {"failed", "timed_out"} for status in statuses):
                    aggregate = "failed"
                elif any(status == "blocked" for status in statuses):
                    aggregate = "blocked"
                elif all(status in {"succeeded", "cancelled"} for status in statuses):
                    aggregate = "succeeded"
                elif any(status == "running" for status in statuses):
                    aggregate = "running"
                else:
                    aggregate = "queued"
                if aggregate != project["status"]:
                    db.execute("UPDATE projects SET status=?,updated_at=? WHERE project_id=?",
                               (aggregate, now, project["project_id"]))
                    changed += 1
        return changed

    def reconcile_stale_empty_projects(
        self, *, max_idle_ms: int = 3_600_000, now_ms: int | None = None,
    ) -> int:
        """Cancel inactive user roots that never acquired a Work Item."""
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        cutoff = now - max_idle_ms
        with self.connect() as db:
            cur = db.execute(
                """UPDATE projects SET status='cancelled',updated_at=?
                   WHERE origin='user' AND project_id!='unfiled'
                     AND status IN ('queued','running','blocked') AND updated_at<?
                     AND NOT EXISTS(
                       SELECT 1 FROM work_items w WHERE w.project_id=projects.project_id
                     )""",
                (now, cutoff),
            )
            return cur.rowcount

    def cleanup_test_roots(
        self, project_ids: list[str], *, task_ids: list[str] | None = None,
    ) -> dict:
        """Remove test roots and tombstone historical Runs so polling cannot recreate them."""
        roots = sorted(set(project_ids))
        explicit_tasks = set(task_ids or [])
        if not roots:
            raise ValueError("at least one project id is required")
        now = int(time.time() * 1000)
        with self.connect() as db:
            placeholders = ",".join("?" for _ in roots)
            existing = {
                row[0] for row in db.execute(
                    f"SELECT project_id FROM projects WHERE project_id IN ({placeholders})", roots
                )
            }
            cards = [dict(row) for row in db.execute(
                f"SELECT task_id,generation,message_id FROM task_cards WHERE task_id IN ({placeholders})",
                roots,
            )]
            linked_tasks = {
                row[0] for row in db.execute(
                    f"SELECT task_id FROM work_items WHERE project_id IN ({placeholders}) AND task_id IS NOT NULL",
                    roots,
                )
            }
            diagnostic_tasks = {
                row[0] for row in db.execute(
                    f"SELECT task_id FROM binding_diagnostics WHERE project_id IN ({placeholders})",
                    roots,
                )
            }
            removed_tasks = sorted(explicit_tasks | linked_tasks | diagnostic_tasks)
            for project_id in roots:
                db.execute(
                    """INSERT INTO ignored_projects(project_id,reason,created_at)
                       VALUES(?,?,?) ON CONFLICT(project_id) DO UPDATE SET
                       reason=excluded.reason,created_at=excluded.created_at""",
                    (project_id, "cleanup", now),
                )
            for task_id in removed_tasks:
                db.execute(
                    "INSERT OR IGNORE INTO ignored_runs(task_id,reason,created_at) VALUES(?,?,?)",
                    (task_id, f"cleanup:{','.join(roots)}", now),
                )
            if removed_tasks:
                task_placeholders = ",".join("?" for _ in removed_tasks)
                db.execute(
                    f"DELETE FROM binding_diagnostics WHERE task_id IN ({task_placeholders})",
                    removed_tasks,
                )
                db.execute(
                    f"DELETE FROM work_items WHERE task_id IN ({task_placeholders})",
                    removed_tasks,
                )
                db.execute(f"DELETE FROM events WHERE task_id IN ({task_placeholders})", removed_tasks)
                db.execute(f"DELETE FROM tasks WHERE task_id IN ({task_placeholders})", removed_tasks)
            db.execute(
                f"DELETE FROM binding_diagnostics WHERE project_id IN ({placeholders})", roots
            )
            db.execute(f"DELETE FROM work_items WHERE project_id IN ({placeholders})", roots)
            db.execute(f"DELETE FROM planner_notes WHERE project_id IN ({placeholders})", roots)
            db.execute(f"DELETE FROM projection_outbox WHERE task_id IN ({placeholders})", roots)
            db.execute(f"DELETE FROM dispatch_outbox WHERE project_id IN ({placeholders})", roots)
            db.execute(f"DELETE FROM task_cards WHERE task_id IN ({placeholders})", roots)
            db.execute(f"DELETE FROM projects WHERE project_id IN ({placeholders})", roots)
        return {
            "ok": True, "requested_roots": roots, "deleted_roots": sorted(existing),
            "tombstoned_project_ids": roots,
            "tombstoned_task_ids": removed_tasks, "cards": cards,
        }

    def card_row(self, task_id: str, generation: int = 1) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM task_cards WHERE task_id=? AND generation=?", (task_id, generation)).fetchone()
            return dict(row) if row else None

    def latest_card(self, task_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM task_cards WHERE task_id=? ORDER BY generation DESC LIMIT 1", (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def save_card(self, task_id: str, route: dict, *, message_id: str | None,
                  status: str, payload_hash: str, generation: int = 1,
                  expires_at: int | None = None) -> None:
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute(
                """INSERT INTO task_cards(task_id,generation,channel_key,target_type,target_id,
                   source_message_id,message_id,status,payload_hash,expires_at,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(task_id,generation) DO UPDATE SET
                   message_id=COALESCE(excluded.message_id,task_cards.message_id),status=excluded.status,
                   payload_hash=excluded.payload_hash,expires_at=COALESCE(excluded.expires_at,task_cards.expires_at),
                   updated_at=excluded.updated_at""",
                (task_id, generation, route["channel_key"], route["target_type"], route["target_id"],
                 route.get("source_message_id"), message_id, status, payload_hash, expires_at, now, now),
            )

    def set_card_pinned(self, task_id: str, generation: int, pinned: bool) -> None:
        with self.connect() as db:
            db.execute("UPDATE task_cards SET pinned=?,updated_at=? WHERE task_id=? AND generation=?",
                       (int(pinned), int(time.time() * 1000), task_id, generation))

    def pinned_cards(self, channel_key: str) -> list[dict]:
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                "SELECT * FROM task_cards WHERE channel_key=? AND pinned=1 ORDER BY updated_at", (channel_key,)
            )]

    def enqueue(self, *, key: str, task_id: str, generation: int, operation: str, payload: dict) -> bool:
        now = int(time.time() * 1000)
        with self.connect() as db:
            cur = db.execute(
                """INSERT OR IGNORE INTO projection_outbox
                   (idempotency_key,task_id,generation,operation,payload_json,status,attempts,
                    available_at,last_error,created_at,updated_at)
                   VALUES(?,?,?,?,?,'pending',0,?,'',?,?)""",
                (key, task_id, generation, operation, json.dumps(payload, ensure_ascii=False), now, now, now),
            )
            return cur.rowcount == 1

    def pending_outbox(self, limit: int = 20) -> list[dict]:
        now = int(time.time() * 1000)
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                """SELECT * FROM projection_outbox WHERE status='pending' AND available_at<=?
                   ORDER BY outbox_id LIMIT ?""", (now, limit)
            )]

    def finish_outbox(self, outbox_id: int) -> None:
        with self.connect() as db:
            db.execute("UPDATE projection_outbox SET status='done',updated_at=? WHERE outbox_id=?",
                       (int(time.time() * 1000), outbox_id))

    def fail_outbox(self, outbox_id: int, error: str, max_attempts: int = 8) -> None:
        now = int(time.time() * 1000)
        with self.connect() as db:
            row = db.execute("SELECT attempts FROM projection_outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
            attempts = int(row[0]) + 1
            status = "dead" if attempts >= max_attempts else "pending"
            delay = min(300_000, 2 ** min(attempts, 8) * 1000)
            db.execute(
                """UPDATE projection_outbox SET status=?,attempts=?,available_at=?,last_error=?,updated_at=?
                   WHERE outbox_id=?""",
                (status, attempts, now + delay, error[:1000], now, outbox_id),
            )

    def work_item_for_task(self, task_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM work_items WHERE task_id=?", (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def work_item_by_id(self, work_item_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM work_items WHERE work_item_id=?", (work_item_id,)
            ).fetchone()
            return dict(row) if row else None

    def project_by_id(self, project_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM projects WHERE project_id=?", (project_id,)
            ).fetchone()
            return dict(row) if row else None

    def work_items_for_project(self, project_id: str) -> list[dict]:
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                """SELECT w.*,r.kind AS dispatch_kind,r.task_ref
                   FROM work_items w LEFT JOIN work_item_refs r USING(work_item_id)
                   WHERE w.project_id=? ORDER BY w.last_event_at""",
                (project_id,),
            )]

    def pending_events(self) -> list[sqlite3.Row]:
        with self.connect() as db:
            return list(db.execute(
                """SELECT e.*, t.parent_id, t.child_id, t.snapshot_json
                   FROM events e JOIN tasks t USING(task_id)
                   WHERE e.projected=0 ORDER BY e.timestamp, e.event_id"""
            ))

    def mark_projected(self, event_id: str) -> None:
        with self.connect() as db:
            db.execute("UPDATE events SET projected=1 WHERE event_id=?", (event_id,))

    def get_projection(self, key: str) -> str | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT external_id FROM projections WHERE projection_key=?", (key,)
            ).fetchone()
            return row[0] if row else None

    def set_projection(self, key: str, external_id: str, payload: dict | None = None) -> None:
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute(
                """INSERT INTO projections(projection_key,external_id,payload_json,updated_at)
                   VALUES(?,?,?,?) ON CONFLICT(projection_key) DO UPDATE SET
                   external_id=excluded.external_id,payload_json=excluded.payload_json,
                   updated_at=excluded.updated_at""",
                (key, external_id, json.dumps(payload or {}, ensure_ascii=False), now),
            )

    def set_checkpoint(self, key: str, value: str) -> None:
        with self.connect() as db:
            db.execute(
                """INSERT INTO checkpoints(checkpoint_key,checkpoint_value,updated_at)
                   VALUES(?,?,?) ON CONFLICT(checkpoint_key) DO UPDATE SET
                   checkpoint_value=excluded.checkpoint_value,updated_at=excluded.updated_at""",
                (key, value, int(time.time() * 1000)),
            )

    def get_checkpoint(self, key: str) -> str | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT checkpoint_value FROM checkpoints WHERE checkpoint_key=?", (key,)
            ).fetchone()
            return row[0] if row else None

    def dashboard(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT snapshot_json FROM tasks ORDER BY updated_at DESC").fetchall()
        return [json.loads(row[0]) for row in rows]

    def task_rows(self) -> list[sqlite3.Row]:
        with self.connect() as db:
            return list(db.execute(
                """SELECT task_id,parent_id,child_id,status,last_event_at,snapshot_json
                   FROM tasks ORDER BY updated_at"""
            ))


def _linked_task_id(card: dict) -> str | None:
    for source in (card, card.get("linked", {}), card.get("execution", {}), card.get("metadata", {})):
        if isinstance(source, dict):
            value = source.get("taskId") or source.get("task_id")
            if value:
                return str(value)
    return None


def _board_id(card: dict) -> str:
    automation = card.get("metadata", {}).get("automation", {})
    return str(
        card.get("boardId") or card.get("board_id")
        or automation.get("boardId") or automation.get("board_id")
        or "default"
    )


def _workboard_status(value: str | None) -> str:
    return {
        "triage": "queued", "backlog": "queued", "todo": "queued",
        "scheduled": "queued", "ready": "queued", "running": "running",
        "review": "running", "blocked": "blocked", "done": "succeeded",
    }.get(value or "", value or "queued")


def _workboard_progress(card: dict) -> str:
    events = card.get("recentEvents") or card.get("recent_events") or []
    if events and isinstance(events[-1], dict):
        return str(events[-1].get("summary") or events[-1].get("message") or events[-1].get("type") or "")
    return str(card.get("summary") or card.get("title") or "")


def _diagnostic_match(
    db: sqlite3.Connection, snapshot: TaskSnapshot,
) -> tuple[str | None, list[str], str]:
    """Choose newest eligible root and expose ambiguity instead of going silent."""
    if _task_was_bound(db, snapshot.task_id):
        return None, [], "bound"
    requester = snapshot.requester_session_key or ""
    rows = db.execute(
        """SELECT project_id,raw_json,created_at FROM projects
           WHERE origin='user' AND project_id!='unfiled'
             AND status IN ('queued','running','blocked')
           ORDER BY updated_at DESC"""
    )
    candidates: list[tuple[str, int, int]] = []
    for row in rows:
        run_created = int(snapshot.created_at or 0)
        root_created = int(row["created_at"])
        if run_created < root_created or run_created - root_created > 1_800_000:
            continue
        try:
            route = json.loads(row["raw_json"] or "{}")
        except json.JSONDecodeError:
            continue
        target_id = str(route.get("target_id") or "")
        if target_id and target_id in requester:
            count = db.execute(
                "SELECT count(*) FROM work_items WHERE project_id=?", (row["project_id"],)
            ).fetchone()[0]
            candidates.append((str(row["project_id"]), int(count), root_created))
    empty = [candidate for candidate in candidates if candidate[1] == 0]
    pool = empty or candidates
    pool.sort(key=lambda candidate: candidate[2], reverse=True)
    roots = [candidate[0] for candidate in pool]
    if not roots:
        return None, [], "unmatched"
    return roots[0], roots, "exact" if len(roots) == 1 else "ambiguous"


def _task_was_bound(db: sqlite3.Connection, task_id: str) -> bool:
    return db.execute(
        """SELECT 1 FROM events WHERE task_id=?
           AND event_type IN ('binding_corrected','binding_matched') LIMIT 1""",
        (task_id,),
    ).fetchone() is not None


def _session_has_main(session: dict) -> bool:
    if session.get("spawnedBy", "").startswith("agent:main:"):
        return True
    for participant in session.get("participants") or []:
        identity = participant.get("identity") or {}
        if identity.get("type") == "agent" and identity.get("id") == "main":
            return True
    return False


def _session_refs(session: dict) -> set[str]:
    return {
        str(value) for value in (
            session.get("key"), session.get("sessionId"), session.get("runId")
        ) if value
    }


def _session_status(value: str | None) -> str:
    return {
        "done": "succeeded", "completed": "succeeded", "succeeded": "succeeded",
        "failed": "failed", "error": "failed", "timed_out": "timed_out",
        "cancelled": "cancelled", "canceled": "cancelled", "aborted": "cancelled",
        "blocked": "blocked", "queued": "queued", "running": "running",
    }.get(str(value or "running"), "running")


def _step_status_summary(kind: str, status: str) -> str:
    verb = {
        "queued": "已排队", "running": "已登记", "blocked": "已阻塞",
        "succeeded": "已完成", "failed": "失败", "timed_out": "已超时",
        "cancelled": "已取消",
    }[status]
    return f"{kind} 步骤{verb}"
