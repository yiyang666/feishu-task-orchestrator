import tempfile
import unittest
from pathlib import Path

from orchestrator.event_source.openclaw_tasks import OpenClawTasksSource
from orchestrator.query import QueryService
from orchestrator.state.store import Store


class QueryTests(unittest.TestCase):
    def test_registered_root_exposes_task_tree_and_timeline(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("root-1", "真实任务", channel_key="dm:ou_1",
                                target_type="user", target_id="ou_1")
            store.sync_workboard([], [
                {"id": "root", "title": "真实任务", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "root-1"}}},
                {"id": "step", "title": "调研", "agentId": "research", "status": "running",
                 "metadata": {"automation": {"boardId": "root-1"}}},
            ])
            snap = OpenClawTasksSource._snapshot({
                "taskId": "run-1", "status": "running", "agentId": "research",
                "label": "调研", "createdAt": 100, "startedAt": 120,
                "lastEventAt": 200, "progressSummary": "检索中",
            })
            store.ingest(next(iter(OpenClawTasksSource.events(snap))))
            view = QueryService(store).status("root-1")
            self.assertEqual(view["status"], "running")
            self.assertEqual(view["origin_detail"]["target_id"], "ou_1")
            self.assertEqual(view["items"][0]["agent_id"], "research")
            self.assertTrue(any(event["summary"] == "检索中" for event in view["timeline"]))
            self.assertIsNone(view["card"])

    def test_list_filters_channel(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("a", "A", channel_key="dm:a", target_type="user", target_id="a")
            store.register_root("b", "B", channel_key="dm:b", target_type="user", target_id="b")
            self.assertEqual([v["project_id"] for v in QueryService(store).list(channel_key="dm:a")], ["a"])
