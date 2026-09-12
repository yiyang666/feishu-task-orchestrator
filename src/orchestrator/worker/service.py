from __future__ import annotations

import json
import logging
import signal
import threading
import time
from datetime import datetime

from orchestrator.config import Config
from orchestrator.dispatch import DispatchPrepareService, WorkboardClient
from orchestrator.event_source.openclaw_tasks import OpenClawTasksSource
from orchestrator.event_source.sessions import OpenClawSessionsSource
from orchestrator.event_source.workboard import WorkboardSource
from orchestrator.snapshot import (
    build_snapshot,
    default_roster,
    fetch_agent_roster,
    write_snapshot,
)
from orchestrator.state.store import Store


LOG = logging.getLogger(__name__)


class Worker:
    """Local-only observer over the durable OpenClaw task ledger.

    The worker polls the task ledger, dedupes events into SQLite, refreshes
    automation health, and writes a normalized status snapshot. It performs no
    Feishu writes at all.
    """

    def __init__(self, config: Config):
        self.config = config
        self.store = Store(config.db_path)
        self.source = OpenClawTasksSource(
            config.openclaw_bin, config.session_prefixes, config.automation_source_ids
        )
        self.workboard = WorkboardSource(config.openclaw_bin)
        self.sessions = OpenClawSessionsSource(config.openclaw_bin)
        self.stop_event = threading.Event()
        self.roster: list[str] | None = None
        self.last_boards: list[dict] = []
        self.last_cards: list[dict] = []
        self.last_workboard_fetch = 0.0
        self.dispatch = DispatchPrepareService(
            self.store, WorkboardClient(config.openclaw_bin),
            config.db_path.parent / "dispatch.lock",
        )

    def _drain_dispatch(self) -> int:
        completed = 0
        for row in self.store.pending_dispatch():
            try:
                if row["operation"] != "sync_step":
                    raise ValueError(f"unknown dispatch operation: {row['operation']}")
                self.dispatch.sync_queued_step(json.loads(row["payload_json"]))
                self.store.finish_dispatch(row["outbox_id"])
                completed += 1
            except Exception as exc:
                self.store.fail_dispatch(row["outbox_id"], str(exc))
                LOG.warning("dispatch outbox failed id=%s", row["outbox_id"], exc_info=True)
        return completed

    def _resolve_roster(self) -> list[str]:
        if self.roster is None:
            try:
                ids = fetch_agent_roster(self.config.openclaw_bin)
                self.roster = ids or default_roster()
            except Exception:
                LOG.warning("agent roster unavailable; using default roster", exc_info=True)
                self.roster = default_roster()
        return self.roster

    def run_once(self) -> int:
        ingested = 0
        ingested += self._drain_dispatch()
        local_date = datetime.now().astimezone().date().isoformat()
        ingested += self.store.reconcile_stale_empty_projects()
        for source_id in self.config.automation_source_ids:
            self.store.ensure_automation(
                source_id, self.config.automation_labels.get(source_id, source_id), local_date
            )
        started = time.monotonic()
        snapshots = sorted(self.source.fetch(), key=lambda item: item.last_event_at)
        poll_latency_ms = (time.monotonic() - started) * 1000.0
        now_monotonic = time.monotonic()
        if now_monotonic - self.last_workboard_fetch >= 10:
            try:
                boards, cards = self.workboard.catalog()
                self.last_boards, self.last_cards = boards, cards
                self.store.sync_workboard(boards, cards)
            except Exception:
                LOG.warning("workboard catalog unavailable; keeping last snapshot", exc_info=True)
            finally:
                self.last_workboard_fetch = now_monotonic
        known_versions = self.store.task_versions()
        for snapshot in snapshots:
            if snapshot.raw.get("runtime") == "cron":
                ingested += int(self.store.ingest_automation(snapshot, local_date))
                continue
            if known_versions.get(snapshot.task_id) == (snapshot.status.value, snapshot.last_event_at):
                continue
            for event in self.source.events(snapshot):
                ingested += int(self.store.ingest(event))
        sessions = self.sessions.fetch()
        ingested += self.store.sync_task_refs()
        ingested += self.store.sync_spawn_session_steps(sessions)
        ingested += self.store.sync_session_steps(sessions)
        ingested += self.store.flag_stale_spawn_steps(self.config.soft_timeout_seconds)
        ingested += self.store.reconcile_binding_diagnostics()
        ingested += self.store.reconcile_project_statuses()
        ingested += self.store.refresh_automation_health(local_date)
        snapshot_doc = build_snapshot(
            store=self.store,
            snapshots=snapshots,
            roster=self._resolve_roster(),
            poll_latency_ms=poll_latency_ms,
            fetched_at=datetime.now().astimezone().isoformat(timespec="seconds"),
            boards=self.last_boards,
            cards=self.last_cards,
        )
        write_snapshot(self.config.snapshot_path, snapshot_doc)
        self.store.set_checkpoint("last_successful_poll_ms", str(int(time.time() * 1000)))
        return ingested

    def serve(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: self.stop_event.set())
        LOG.info("worker started; db=%s", self.config.db_path)
        delay = self.config.poll_interval_seconds
        while not self.stop_event.is_set():
            try:
                changed = self.run_once()
                delay = self.config.poll_interval_seconds
                if changed:
                    LOG.info("observed task changes=%d", changed)
            except Exception:
                LOG.exception("poll failed")
                delay = min(max(delay * 2, 5), 60)
            self.stop_event.wait(delay)
