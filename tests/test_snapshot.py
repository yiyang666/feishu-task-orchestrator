import json
import tempfile
import unittest
from pathlib import Path

from orchestrator.event_source.openclaw_tasks import OpenClawTasksSource
from orchestrator.snapshot import build_snapshot, read_snapshot, write_snapshot
from orchestrator.state.store import Store


def _snapshot(**overrides):
    item = {
        "taskId": "task-1", "status": "running", "agentId": "code",
        "requesterSessionKey": "group", "label": "写代码",
        "createdAt": 100, "lastEventAt": 200, "progressSummary": "编译中",
    }
    item.update(overrides)
    return OpenClawTasksSource._snapshot(item)


class FakeStore:
    def __init__(self, rows=None):
        self._rows = rows or []

    def automation_rows(self, dirty_only=False):
        return self._rows


class SnapshotTests(unittest.TestCase):
    def test_structure_is_complete(self):
        store = FakeStore([
            {
                "source_id": "cron-news", "label": "新闻推送", "health": "healthy",
                "last_run_status": "succeeded", "last_started_at": 100,
                "last_ended_at": 200, "last_success_at": 200,
                "success_count": 3, "failure_count": 1,
            },
        ])
        snapshots = [
            _snapshot(taskId="run-1", status="running", agentId="code", label="写代码"),
            _snapshot(taskId="run-2", status="failed", agentId="code", label="跑测试",
                      error="assertion failed", terminalSummary="assertion failed"),
            _snapshot(taskId="cron-1", status="succeeded", agentId="automation",
                      runtime="cron", sourceId="cron-news"),
        ]
        document = build_snapshot(
            store=store, snapshots=snapshots, roster=["code", "research"],
            poll_latency_ms=612.5, fetched_at="2026-09-11T19:00:00+08:00",
            boards=[{"id": "b1"}], cards=[{"id": "c1", "title": "步骤", "status": "running",
                                           "agentId": "code", "updatedAt": 300}],
        )
        for key in ("generated_at", "poll_latency_ms", "agents", "automations",
                    "workboard", "source"):
            self.assertIn(key, document)
        self.assertTrue(document["generated_at"].endswith("+08:00"))
        self.assertEqual(document["poll_latency_ms"], 612.5)
        self.assertEqual(document["source"]["tasks_total"], 3)
        self.assertEqual(document["source"]["fetched_at"], "2026-09-11T19:00:00+08:00")

        code = document["agents"]["code"]
        self.assertEqual(len(code["active"]), 1)
        self.assertEqual(code["active"][0]["task_id"], "run-1")
        self.assertEqual(code["active"][0]["progress_summary"], "编译中")
        self.assertTrue(code["active"][0]["started_at"] is None or
                        code["active"][0]["started_at"].endswith("+08:00"))
        self.assertEqual(code["totals"]["running"], 1)
        self.assertEqual(code["totals"]["failed"], 1)
        self.assertEqual(len(code["recent_terminal"]), 1)
        self.assertEqual(code["recent_terminal"][0]["error"], "assertion failed")

        # cron runs are excluded from agent grouping
        self.assertNotIn("cron-1", json.dumps(document["agents"]))

        self.assertEqual(document["automations"][0]["source_id"], "cron-news")
        self.assertEqual(document["automations"][0]["today_success"], 3)
        self.assertEqual(document["workboard"]["boards"], 1)
        self.assertEqual(len(document["workboard"]["active_cards"]), 1)

    def test_idle_agent_is_marked(self):
        document = build_snapshot(store=FakeStore(), snapshots=[], roster=["code", "research"])
        self.assertEqual(document["agents"]["research"]["state"], "idle")
        self.assertEqual(document["agents"]["research"]["active"], [])
        self.assertEqual(document["agents"]["research"]["recent_terminal"], [])
        self.assertEqual(
            document["agents"]["research"]["totals"],
            {"running": 0, "blocked": 0, "succeeded": 0, "failed": 0,
             "timed_out": 0, "cancelled": 0, "queued": 0},
        )

    def test_recent_terminal_is_truncated_and_ordered(self):
        snapshots = [
            _snapshot(taskId=f"t{i}", status="succeeded", agentId="code",
                      label=f"任务{i}", endedAt=100 + i, lastEventAt=100 + i)
            for i in range(8)
        ]
        document = build_snapshot(store=FakeStore(), snapshots=snapshots, roster=["code"])
        terminal = document["agents"]["code"]["recent_terminal"]
        self.assertEqual(len(terminal), 5)
        labels = [entry["label"] for entry in terminal]
        self.assertEqual(labels, ["任务7", "任务6", "任务5", "任务4", "任务3"])
        self.assertEqual(document["agents"]["code"]["totals"]["succeeded"], 8)

    def test_write_snapshot_is_atomic_and_roundtrips(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "nested" / "agent_status.json"
            document = {"generated_at": "2026-09-11T19:00:00+08:00", "agents": {}}
            written = write_snapshot(target, document)
            self.assertEqual(written, target)
            self.assertTrue(target.exists())
            self.assertEqual(read_snapshot(target), document)
            # no leftover temp files
            leftovers = [p.name for p in target.parent.iterdir() if p.name != target.name]
            self.assertEqual(leftovers, [])
            self.assertEqual(read_snapshot(Path(directory) / "missing.json"), None)

    def test_build_snapshot_from_real_store_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.ensure_automation("cron-news", "新闻推送", "2026-09-11")
            document = build_snapshot(
                store=store, snapshots=[_snapshot(agentId="code")], roster=["code", "main"]
            )
            self.assertEqual(document["automations"][0]["source_id"], "cron-news")
            self.assertEqual(document["agents"]["main"]["state"], "idle")


if __name__ == "__main__":
    unittest.main()
