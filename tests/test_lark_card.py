import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from orchestrator.projections.lark_card.renderer import render_task
from orchestrator.projections.lark_card.renderer import _compact_timeline
from orchestrator.projections.lark_card.worker import LarkProjectionWorker
from orchestrator.state.store import Store


class LarkCardTests(unittest.TestCase):
    def test_timeline_compacts_repeated_progress(self):
        events = [
            {"task_id": "t1", "timestamp": 1000, "status": "running", "summary": "检索中"},
            {"task_id": "t1", "timestamp": 2000, "status": "running", "summary": "检索中"},
            {"task_id": "t1", "timestamp": 3000, "status": "running", "summary": "长中间结论"},
            {"task_id": "t1", "timestamp": 4000, "status": "running", "summary": "另一段结论"},
            {"task_id": "t1", "timestamp": 5000, "status": "succeeded", "summary": "最终结论"},
        ]
        rows = _compact_timeline(events, [{"task_id": "t1", "title": "检索资料", "agent_id": "research"}])
        self.assertEqual(len(rows), 2)
        self.assertIn("08:00", rows[0])
        self.assertEqual(sum("检索资料" in row for row in rows), 2)
        self.assertNotIn("中间结论", "".join(rows))
        self.assertIn("已完成", rows[-1])

    def test_renderer_has_four_sections_and_stays_small(self):
        view = {"project_id": "p", "name": "任务", "status": "failed", "created_at": 1000,
                "items": [{"task_id": "t1", "title": "步骤", "agent_id": "code", "status": "failed",
                           "latest_progress": "SECRET_LONG_RESULT 测试失败", "started_at": 1000, "ended_at": 2000}],
                "timeline": [{"task_id": "t1", "timestamp": 2000, "status": "failed",
                              "summary": "SECRET_LONG_RESULT 测试失败"}]}
        card = render_task(view)
        text = json.dumps(card, ensure_ascii=False)
        self.assertIn("📌任务状态", text)
        self.assertIn("📝时间线", text)
        self.assertIn("需要关注", text)
        self.assertIn("code", text)
        self.assertIn("步骤", text)
        self.assertNotIn("SECRET_LONG_RESULT", text)
        self.assertLess(len(text.encode()), 16_384)

    def test_planner_timeline_is_normalized_and_header_status_unchanged(self):
        view = {
            "name": "总任务", "status": "succeeded", "created_at": 1000, "items": [],
            "timeline": [
                {"task_id": "root", "event_type": "planner_action", "kind": "planning",
                 "actor": "jarvis", "timestamp": 1000, "status": "running",
                 "summary": "正在规划，附带不应展示的长结论 SECRET_PLAN"},
                {"task_id": "root", "event_type": "planner_action", "kind": "planning",
                 "actor": "jarvis", "timestamp": 2000, "status": "running",
                 "summary": "重复规划，仍不展示 SECRET_PLAN_2"},
                {"task_id": "root", "event_type": "planner_action", "kind": "waiting",
                 "actor": "jarvis", "timestamp": 3000, "status": "running",
                 "summary": "等待中 SECRET_WAIT"},
                {"task_id": "root", "event_type": "planner_action", "kind": "deliver",
                 "actor": "jarvis", "timestamp": 4000, "status": "succeeded",
                 "summary": "最终结论 SECRET_DELIVER"},
            ],
        }
        card = render_task(view)
        text = json.dumps(card, ensure_ascii=False)
        self.assertEqual(card["header"]["template"], "green")
        self.assertEqual(card["header"]["title"]["content"], "总任务 · 已完成")
        self.assertEqual(card["header"]["subtitle"]["content"], "实时任务状态卡")
        self.assertIn("任务：总任务 · 已完成", card["config"]["summary"]["content"])
        metrics = card["body"]["elements"][0]
        self.assertEqual(metrics["tag"], "column_set")
        self.assertEqual(len(metrics["columns"]), 3)
        metric_text = json.dumps(metrics, ensure_ascii=False)
        self.assertIn("开始时间", metric_text)
        self.assertIn("子任务数", metric_text)
        self.assertIn("总耗时", metric_text)
        self.assertIn("Jarvis", text)
        self.assertIn("等待中", text)
        self.assertIn("汇总并交付", text)
        self.assertIn("08:00:02", text)
        self.assertEqual(text.count("规划任务"), 1)
        self.assertNotIn("SECRET_", text)

    def test_renderer_surfaces_binding_warning(self):
        view = {
            "name": "漏建步骤", "status": "queued", "created_at": 1000,
            "items": [], "timeline": [],
            "warnings": [{"message": "未匹配到预建步骤：code / 检查"}],
        }
        text = json.dumps(render_task(view), ensure_ascii=False)
        self.assertIn("派发关联异常", text)
        self.assertIn("未匹配到预建步骤", text)

    def test_projector_creates_once_then_patches_same_message(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("p", "任务", channel_key="dm:ou_1", target_type="user", target_id="ou_1")
            config = SimpleNamespace(lark_cli_bin="lark-cli", lark_identity="bot", lark_profile=None,
                                     poll_interval_seconds=1)
            worker = LarkProjectionWorker(config, store)
            worker.lark = MagicMock()
            worker.lark.api.return_value = {"message_id": "om_1"}
            self.assertEqual(worker.run_once(), 1)
            self.assertEqual(store.card_row("p")["message_id"], "om_1")
            self.assertEqual(worker.run_once(), 0)
            store.register_root("p", "任务改名", channel_key="dm:ou_1", target_type="user", target_id="ou_1")
            self.assertEqual(worker.run_once(), 1)
            method, path = worker.lark.api.call_args.args[:2]
            self.assertEqual((method, path), ("PATCH", "/open-apis/im/v1/messages/om_1"))

    def test_outbox_survives_failure_and_retries_without_duplicate_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("p", "任务", channel_key="dm:ou_1", target_type="user", target_id="ou_1")
            config = SimpleNamespace(lark_cli_bin="lark-cli", lark_identity="bot", lark_profile=None,
                                     poll_interval_seconds=1)
            worker = LarkProjectionWorker(config, store)
            worker.lark = MagicMock()
            worker.lark.api.side_effect = RuntimeError("temporary network failure")
            self.assertEqual(worker.run_once(), 0)
            with store.connect() as db:
                row = db.execute("SELECT attempts,status FROM projection_outbox").fetchone()
                self.assertEqual((row["attempts"], row["status"]), (1, "pending"))
                db.execute("UPDATE projection_outbox SET available_at=0")
            restarted = LarkProjectionWorker(config, Store(store.path))
            restarted.lark = MagicMock()
            restarted.lark.api.return_value = {"message_id": "om_retry"}
            self.assertEqual(restarted.run_once(), 1)
            self.assertEqual(Store(store.path).latest_card("p")["message_id"], "om_retry")
            with store.connect() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM projection_outbox").fetchone()[0], 1)

    def test_active_card_rotates_to_next_generation_before_expiry(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("p", "任务", channel_key="dm:ou_1", target_type="user", target_id="ou_1")
            config = SimpleNamespace(lark_cli_bin="lark-cli", lark_identity="bot", lark_profile=None,
                                     poll_interval_seconds=1)
            worker = LarkProjectionWorker(config, store)
            worker.lark = MagicMock()
            worker.lark.api.side_effect = [{"message_id": "om_1"}, {"message_id": "om_2"}]
            worker.run_once()
            with store.connect() as db:
                db.execute("UPDATE task_cards SET expires_at=1 WHERE task_id='p'")
            worker.run_once()
            cards = []
            with store.connect() as db:
                cards = list(db.execute("SELECT generation,message_id FROM task_cards ORDER BY generation"))
            self.assertEqual([(r["generation"], r["message_id"]) for r in cards], [(1, "om_1"), (2, "om_2")])

    def test_topic_card_replies_in_thread(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("p", "话题任务", channel_key="feishu:group:oc_1:topic:omt_1",
                                target_type="chat", target_id="oc_1", source_message_id="om_source")
            config = SimpleNamespace(lark_cli_bin="lark-cli", lark_identity="bot", lark_profile=None,
                                     poll_interval_seconds=1)
            worker = LarkProjectionWorker(config, store)
            worker.lark = MagicMock()
            worker.lark.api.return_value = {"message_id": "om_topic"}
            worker.run_once()
            method, path = worker.lark.api.call_args_list[0].args[:2]
            data = worker.lark.api.call_args_list[0].kwargs["data"]
            self.assertEqual((method, path), ("POST", "/open-apis/im/v1/messages/om_source/reply"))
            self.assertTrue(data["reply_in_thread"])

    def test_same_channel_only_newest_root_is_pinned(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("old", "旧任务", channel_key="group:1", target_type="chat", target_id="oc_1")
            store.register_root("new", "新任务", channel_key="group:1", target_type="chat", target_id="oc_1")
            with store.connect() as db:
                db.execute("UPDATE projects SET created_at=1 WHERE project_id='old'")
                db.execute("UPDATE projects SET created_at=2 WHERE project_id='new'")
            config = SimpleNamespace(lark_cli_bin="lark-cli", lark_identity="bot", lark_profile=None,
                                     poll_interval_seconds=1)
            worker = LarkProjectionWorker(config, store)
            worker.lark = MagicMock()
            def api(method, path, **kwargs):
                if path.endswith("/messages"):
                    return {"message_id": "om_new" if "新任务" in kwargs["data"]["content"] else "om_old"}
                return {}
            worker.lark.api.side_effect = api
            worker.run_once()
            worker.run_once()
            self.assertEqual(store.latest_card("old")["pinned"], 0)
            self.assertEqual(store.latest_card("new")["pinned"], 1)


if __name__ == "__main__":
    unittest.main()
