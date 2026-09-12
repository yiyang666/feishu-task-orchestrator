import tempfile
import unittest
import subprocess
from pathlib import Path

from orchestrator.dispatch import DispatchPrepareService, WorkboardClient, parse_step
from orchestrator.state.store import Store


class FakeWorkboard:
    def __init__(self):
        self.values = []
        self.created = 0

    def cards(self):
        return list(self.values)

    def create(self, title, *, board_id, labels, status, notes, agent_id=None):
        self.created += 1
        card = {
            "id": f"card-{self.created}", "title": title, "labels": [labels],
            "status": status, "notes": notes, "agentId": agent_id,
            "metadata": {"automation": {"boardId": board_id}},
        }
        self.values.append(card)
        return card


class DispatchTests(unittest.TestCase):
    def test_workboard_list_retries_transient_read_failure(self):
        calls = []
        def runner(argv, **kwargs):
            calls.append(argv)
            if len(calls) < 3:
                raise subprocess.CalledProcessError(1, argv)
            return subprocess.CompletedProcess(argv, 0, stdout='{"cards": []}', stderr="")
        self.assertEqual(WorkboardClient(runner=runner).cards(), [])
        self.assertEqual(len(calls), 3)

    def test_parse_step(self):
        self.assertEqual(parse_step("research:调研:细节"), ("research", "调研:细节"))
        with self.assertRaises(ValueError):
            parse_step("research")

    def test_prepare_is_idempotent_and_returns_spawn_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = Store(root / "state.db")
            workboard = FakeWorkboard()
            service = DispatchPrepareService(store, workboard, root / "dispatch.lock")
            kwargs = dict(
                channel_key="feishu:dm:ou_1", target_type="user", target_id="ou_1",
                source_message_id="om_1", steps=[("research", "调研"), ("codex", "实现")],
            )
            first = service.prepare("root-1", "真实任务", **kwargs)
            second = service.prepare("root-1", "真实任务", **kwargs)
            self.assertEqual(workboard.created, 3)
            self.assertEqual(first["project_root_card_id"], second["project_root_card_id"])
            self.assertEqual(first["steps"][0]["spawn"]["label"], "调研")
            self.assertEqual(first["steps"][1]["agent_id"], "codex")
            view = store.task_view("root-1")
            self.assertEqual(len(view["items"]), 2)
            self.assertEqual(view["origin_detail"]["source_message_id"], "om_1")

    def test_prepare_step_is_idempotent_and_tracks_kind(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = Store(root / "state.db")
            store.register_root("root-1", "真实任务", channel_key="dm:1", target_type="user", target_id="1")
            workboard = FakeWorkboard()
            service = DispatchPrepareService(store, workboard, root / "dispatch.lock")
            first = service.prepare_step(
                "root-1", "发送实现单", agent_id="codex", kind="send"
            )
            second = service.prepare_step(
                "root-1", "发送实现单", agent_id="codex", kind="send",
                task_ref="agent:codex:direct:1",
            )
            self.assertEqual(workboard.created, 2)
            self.assertEqual(first["work_item_id"], second["work_item_id"])
            view = store.task_view("root-1")
            self.assertEqual(view["items"][0]["dispatch_kind"], "send")
            self.assertEqual(view["items"][0]["task_ref"], "agent:codex:direct:1")

    def test_prepare_step_async_is_visible_before_workboard_sync(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = Store(root / "state.db")
            store.register_root("root-1", "真实任务", channel_key="dm:1",
                                target_type="user", target_id="1")
            workboard = FakeWorkboard()
            service = DispatchPrepareService(store, workboard, root / "dispatch.lock")
            value = service.prepare_step_async(
                "root-1", "快速派发", agent_id="code", kind="spawn"
            )
            self.assertTrue(value["work_item_id"].startswith("pending:"))
            self.assertEqual(value["workboard_sync"], "pending")
            self.assertEqual(workboard.created, 0)
            self.assertEqual(store.task_view("root-1")["items"][0]["title"], "快速派发")
            row = store.pending_dispatch()[0]
            service.sync_queued_step(__import__("json").loads(row["payload_json"]))
            store.finish_dispatch(row["outbox_id"])
            self.assertEqual(workboard.created, 2)
            item = store.task_view("root-1")["items"][0]
            self.assertTrue(item["work_item_id"].startswith("wb:"))
            self.assertEqual(store.pending_dispatch(), [])
            terminal = service.prepare_step_async(
                "root-1", "快速派发", agent_id="code", kind="spawn",
                status="succeeded",
            )
            self.assertEqual(terminal["workboard_sync"], "synced")
            self.assertEqual(terminal["card_id"], item["work_item_id"].removeprefix("wb:"))

    def test_async_workboard_sync_does_not_overwrite_newer_terminal_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = Store(root / "state.db")
            store.register_root("root-1", "真实任务", channel_key="dm:1",
                                target_type="user", target_id="1")
            workboard = FakeWorkboard()
            service = DispatchPrepareService(store, workboard, root / "dispatch.lock")
            service.prepare_step_async(
                "root-1", "快速派发", agent_id="code", kind="spawn"
            )
            # The execution may finish before the asynchronous Workboard create returns.
            service.prepare_step_async(
                "root-1", "快速派发", agent_id="code", kind="spawn",
                status="succeeded",
            )
            row = store.pending_dispatch()[0]
            result = service.sync_queued_step(__import__("json").loads(row["payload_json"]))
            self.assertEqual(result["status"], "succeeded")
            self.assertEqual(store.task_view("root-1")["status"], "succeeded")
            self.assertEqual(store.task_view("root-1")["items"][0]["status"], "succeeded")
