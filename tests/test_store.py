import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path

from orchestrator.event_source.openclaw_tasks import OpenClawTasksSource
from orchestrator.state.store import Store


class StoreTests(unittest.TestCase):
    def test_event_ingest_is_idempotent_and_survives_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.db"
            store = Store(path)
            snap = OpenClawTasksSource._snapshot({
                "taskId": "task-1", "status": "succeeded", "agentId": "research",
                "requesterSessionKey": "group", "label": "done",
                "createdAt": 100, "lastEventAt": 200, "terminalSummary": "完成",
            })
            event = list(OpenClawTasksSource.events(snap))[0]
            self.assertTrue(store.ingest(event))
            self.assertFalse(store.ingest(event))
            self.assertEqual(len(Store(path).pending_events()), 1)
            store.mark_projected(event.event_id)
            self.assertEqual(Store(path).pending_events(), [])

    def test_automation_monitor_counts_each_run_once_and_rolls_day(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            snap = OpenClawTasksSource._snapshot({
                "taskId": "cron-run-1", "status": "succeeded", "agentId": "automation",
                "runtime": "cron", "sourceId": "cron-news", "label": "新闻推送",
                "createdAt": 100, "startedAt": 100, "endedAt": 200, "lastEventAt": 200,
                "terminalSummary": "已推送",
            })
            run_date = datetime.fromtimestamp(0.2).astimezone().date().isoformat()
            self.assertTrue(store.ingest_automation(snap, run_date))
            store.mark_automation_projected("cron-news")
            store.ingest_automation(snap, run_date)
            row = store.automation_rows()[0]
            self.assertEqual(row["success_count"], 1)
            self.assertEqual(row["health"], "healthy")
            self.assertEqual(row["last_result"], "已推送")
            self.assertEqual(store.refresh_automation_health("2099-01-01"), 1)
            row = store.automation_rows()[0]
            self.assertEqual(row["health"], "unhealthy")
            self.assertEqual(row["success_count"], 0)

    def test_planned_workboard_step_binds_matching_run(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.sync_workboard([], [
                {"id": "root", "metadata": {"automation": {"boardId": "project-a"}}, "title": "项目A", "labels": ["project-root"]},
                {"id": "step", "metadata": {"automation": {"boardId": "project-a"}}, "title": "调研方案", "agentId": "research", "status": "running"},
            ])
            snap = OpenClawTasksSource._snapshot({
                "taskId": "run-1", "status": "running", "agentId": "research",
                "requesterSessionKey": "group", "label": "调研方案",
                "createdAt": 100, "lastEventAt": 200, "progressSummary": "检索中",
            })
            store.ingest(list(OpenClawTasksSource.events(snap))[0])
            item = store.work_item_for_task("run-1")
            self.assertEqual(item["work_item_id"], "wb:step")
            self.assertEqual(item["project_id"], "project-a")
            self.assertEqual(store.project_by_id("project-a")["name"], "项目A")
            store.ingest(list(OpenClawTasksSource.events(snap))[0])
            self.assertFalse(any(
                warning["task_id"] == "run-1" for warning in store.binding_diagnostics()
            ))

    def test_project_status_converges_and_unfiled_is_not_projected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.sync_workboard([], [
                {"id": "root", "metadata": {"automation": {"boardId": "project-a"}},
                 "title": "项目A", "labels": ["project-root"]},
                {"id": "step", "metadata": {"automation": {"boardId": "project-a"}},
                 "title": "步骤", "agentId": "research", "status": "done"},
            ])
            self.assertEqual(store.reconcile_project_statuses(), 1)
            self.assertEqual(store.project_by_id("project-a")["status"], "succeeded")
            snap = OpenClawTasksSource._snapshot({
                "taskId": "orphan", "status": "running", "agentId": "research",
                "label": "未归档", "createdAt": 100, "lastEventAt": 100,
            })
            store.ingest(next(iter(OpenClawTasksSource.events(snap))))
            self.assertNotIn("unfiled", [view["project_id"] for view in store.list_task_views()])

    def test_unmatched_run_is_unfiled_and_warns_unique_recent_root(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root(
                "root-missing-step", "漏建步骤", channel_key="feishu:dm:ou_1",
                target_type="user", target_id="ou_1",
            )
            store.register_root(
                "root-with-step", "正常任务", channel_key="feishu:dm:ou_1",
                target_type="user", target_id="ou_1",
            )
            store.sync_workboard([], [
                {"id": "normal-root", "title": "正常任务", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "root-with-step"}}},
                {"id": "normal-step", "title": "正常步骤", "labels": ["project-step"],
                 "agentId": "code", "metadata": {"automation": {"boardId": "root-with-step"}}},
            ])
            now = int(time.time() * 1000)
            snap = OpenClawTasksSource._snapshot({
                "taskId": "orphan-run", "status": "running", "agentId": "code",
                "requesterSessionKey": "agent:main:feishu:direct:ou_1",
                "label": "执行检查", "createdAt": now, "lastEventAt": now,
            })
            event = next(iter(OpenClawTasksSource.events(snap)))
            store.ingest(event)
            self.assertFalse(store.ingest(event))
            self.assertEqual(store.work_item_for_task("orphan-run")["project_id"], "unfiled")
            warnings = store.task_view("root-missing-step")["warnings"]
            self.assertEqual(len(warnings), 1)
            self.assertEqual(warnings[0]["kind"], "unmatched_run")
            self.assertEqual(warnings[0]["task_id"], "orphan-run")
            self.assertEqual(warnings[0]["agent"], "code")
            self.assertEqual(warnings[0]["label"], "执行检查")
            self.assertEqual(warnings[0]["candidate_root"], "root-missing-step")
            self.assertEqual(warnings[0]["candidate_roots"], ["root-missing-step"])
            self.assertEqual(warnings[0]["confidence"], "exact")
            self.assertIn("fto bind orphan-run", warnings[0]["suggested_fix"])

            store.sync_workboard([], [
                {"id": "missing-root", "title": "漏建步骤", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "root-missing-step"}}},
                {"id": "repair-step", "title": "执行检查", "labels": ["project-step"],
                 "agentId": "code", "metadata": {"automation": {"boardId": "root-missing-step"}}},
            ])
            first = store.bind_run("orphan-run", "wb:repair-step")
            second = store.bind_run("orphan-run", "wb:repair-step")
            self.assertTrue(first["changed"])
            self.assertFalse(second["changed"])
            self.assertEqual(store.work_item_for_task("orphan-run")["project_id"], "root-missing-step")
            self.assertEqual(store.task_view("root-missing-step")["warnings"], [])
            with store.connect() as db:
                count = db.execute(
                    "SELECT count(*) FROM events WHERE event_id=?", (first["audit_event_id"],)
                ).fetchone()[0]
            self.assertEqual(count, 1)
            store.ingest(event)
            self.assertEqual(store.task_view("root-missing-step")["warnings"], [])

    def test_historical_or_dangling_warning_does_not_pollute_new_root(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            before = int(time.time() * 1000) - 60_000
            snap = OpenClawTasksSource._snapshot({
                "taskId": "old-orphan", "status": "running", "agentId": "code",
                "requesterSessionKey": "agent:main:feishu:direct:ou_1",
                "label": "旧任务", "createdAt": before, "lastEventAt": before,
            })
            store.ingest(next(iter(OpenClawTasksSource.events(snap))))
            registered = store.register_root(
                "new-root", "新任务", channel_key="feishu:dm:ou_1",
                target_type="user", target_id="ou_1",
            )
            self.assertEqual(registered["project_id"], "new-root")
            store.ingest(next(iter(OpenClawTasksSource.events(snap))))
            self.assertEqual(store.task_view("new-root")["warnings"], [])
            with store.connect() as db:
                db.execute("DELETE FROM work_items WHERE task_id='old-orphan'")
            self.assertEqual(store.reconcile_binding_diagnostics(), 1)
            self.assertEqual(store.binding_diagnostics(), [])

    def test_ambiguous_orphan_warns_newest_root_and_lists_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root(
                "older-root", "旧根", channel_key="feishu:dm:ou_1",
                target_type="user", target_id="ou_1",
            )
            time.sleep(0.002)
            store.register_root(
                "newer-root", "新根", channel_key="feishu:dm:ou_1",
                target_type="user", target_id="ou_1",
            )
            now = int(time.time() * 1000)
            snap = OpenClawTasksSource._snapshot({
                "taskId": "ambiguous-run", "status": "running", "agentId": "code",
                "requesterSessionKey": "agent:main:feishu:direct:ou_1",
                "label": "歧义任务", "createdAt": now, "lastEventAt": now,
            })
            store.ingest(next(iter(OpenClawTasksSource.events(snap))))
            self.assertEqual(store.task_view("older-root")["warnings"], [])
            warning = store.task_view("newer-root")["warnings"][0]
            self.assertEqual(warning["confidence"], "ambiguous")
            self.assertEqual(warning["candidate_roots"], ["newer-root", "older-root"])
            self.assertEqual(store.binding_diagnostics()[0]["task_id"], "ambiguous-run")

    def test_empty_root_keeps_registered_status_then_expires(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root(
                "empty-root", "空根", channel_key="feishu:dm:ou_1",
                target_type="user", target_id="ou_1",
            )
            self.assertEqual(store.task_view("empty-root")["status"], "running")
            created_at = store.project_by_id("empty-root")["created_at"]
            self.assertEqual(store.reconcile_stale_empty_projects(now_ms=created_at + 3_600_001), 1)
            self.assertEqual(store.task_view("empty-root")["status"], "cancelled")

    def test_cleanup_tombstones_run_and_prevents_reingest(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root(
                "cleanup-root", "清理用例", channel_key="feishu:dm:ou_1",
                target_type="user", target_id="ou_1",
            )
            now = int(time.time() * 1000)
            snap = OpenClawTasksSource._snapshot({
                "taskId": "cleanup-run", "status": "succeeded", "agentId": "code",
                "requesterSessionKey": "agent:main:feishu:direct:ou_1",
                "label": "待清理", "createdAt": now, "lastEventAt": now,
            })
            event = next(iter(OpenClawTasksSource.events(snap)))
            store.ingest(event)
            result = store.cleanup_test_roots(
                ["cleanup-root"], task_ids=["cleanup-run"]
            )
            self.assertEqual(result["deleted_roots"], ["cleanup-root"])
            self.assertEqual(result["tombstoned_project_ids"], ["cleanup-root"])
            self.assertEqual(result["tombstoned_task_ids"], ["cleanup-run"])
            self.assertIsNone(store.project_by_id("cleanup-root"))
            self.assertEqual(store.binding_diagnostics(), [])
            self.assertFalse(store.ingest(event))
            self.assertIsNone(store.work_item_for_task("cleanup-run"))
            stale_cards = [
                {"id": "root", "title": "清理用例", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "cleanup-root"}}},
                {"id": "step", "title": "待清理", "labels": ["project-step"],
                 "agentId": "code", "metadata": {"automation": {"boardId": "cleanup-root"}}},
            ]
            store.sync_workboard([], stale_cards)
            self.assertIsNone(store.project_by_id("cleanup-root"))
            store.register_root(
                "cleanup-root", "显式重用", channel_key="feishu:dm:ou_1",
                target_type="user", target_id="ou_1",
            )
            self.assertIsNotNone(store.project_by_id("cleanup-root"))

    def test_send_step_and_planner_notes_complete_root(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("root-send", "派单任务", channel_key="dm:1", target_type="user", target_id="1")
            store.sync_workboard([], [
                {"id": "root", "title": "派单任务", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "root-send"}}},
                {"id": "send-step", "title": "实现", "labels": ["project-step"],
                 "agentId": "codex", "metadata": {"automation": {"boardId": "root-send"}}},
                {"id": "inline-step", "title": "汇总", "labels": ["project-step"],
                 "agentId": "jarvis", "metadata": {"automation": {"boardId": "root-send"}}},
            ])
            store.register_step_ref("wb:send-step", kind="send")
            store.register_step_ref("wb:inline-step", kind="inline", status="succeeded")
            store.add_planner_note("root-send", "planning", "正在规划任务")
            self.assertEqual(store.task_view("root-send")["status"], "running")
            with store.connect() as db:
                registered_at = db.execute(
                    "SELECT registered_at FROM work_item_refs WHERE work_item_id='wb:send-step'"
                ).fetchone()[0]
            changed = store.sync_session_steps([{
                "key": "agent:codex:feishu:direct:1", "sessionId": "session-1",
                "agentId": "codex", "status": "done", "updatedAt": registered_at + 100,
                "participants": [{"identity": {"type": "agent", "id": "main"}}],
            }])
            self.assertEqual(changed, 1)
            self.assertEqual(store.task_view("root-send")["status"], "running")
            store.register_step_ref("wb:send-step", kind="send", status="succeeded")
            view = store.task_view("root-send")
            self.assertEqual(view["status"], "succeeded")
            self.assertEqual(next(i for i in view["items"] if i["agent_id"] == "codex")["task_ref"],
                             "agent:codex:feishu:direct:1")
            self.assertTrue(any(e["event_type"] == "planner_action" for e in view["timeline"]))
            summaries = [event["summary"] for event in view["timeline"]]
            self.assertIn("send 步骤已登记", summaries)
            self.assertIn("send 步骤已完成", summaries)

    def test_spawn_task_ref_binds_by_run_id_idempotently(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("root-spawn", "显式派发", channel_key="dm:1", target_type="user", target_id="1")
            store.sync_workboard([], [
                {"id": "root", "title": "显式派发", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "root-spawn"}}},
                {"id": "spawn-step", "title": "实现功能", "labels": ["project-step"],
                 "agentId": "code", "metadata": {"automation": {"boardId": "root-spawn"}}},
            ])
            store.register_step_ref("wb:spawn-step", kind="spawn", task_ref="run-42")
            snap = OpenClawTasksSource._snapshot({
                "taskId": "task-42", "runId": "run-42", "status": "running",
                "agentId": "code", "requesterSessionKey": "dm", "label": "标题故意不同",
                "createdAt": 100, "lastEventAt": 200, "progressSummary": "执行中",
            })
            store.ingest(list(OpenClawTasksSource.events(snap))[0])
            self.assertEqual(store.sync_task_refs(), 1)
            self.assertEqual(store.sync_task_refs(), 0)
            self.assertEqual(store.work_item_for_task("task-42")["work_item_id"], "wb:spawn-step")

    def test_register_step_ref_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("root-idem", "幂等派发", channel_key="dm:1", target_type="user", target_id="1")
            store.sync_workboard([], [
                {"id": "root", "title": "幂等派发", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "root-idem"}}},
                {"id": "send-step", "title": "实现", "labels": ["project-step"],
                 "agentId": "codex", "metadata": {"automation": {"boardId": "root-idem"}}},
            ])
            store.register_step_ref("wb:send-step", kind="send", task_ref="session-1")
            store.register_step_ref("wb:send-step", kind="send", task_ref="session-1")
            with store.connect() as db:
                count = db.execute(
                    "SELECT count(*) FROM events WHERE task_id LIKE 'send:%'"
                ).fetchone()[0]
            self.assertEqual(count, 1)

    def test_spawn_step_falls_back_to_terminal_child_session(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("root-session", "短任务", channel_key="dm:1",
                                target_type="user", target_id="1")
            store.sync_workboard([], [
                {"id": "root", "title": "短任务", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "root-session"}}},
                {"id": "spawn-step", "title": "快速检查", "labels": ["project-step"],
                 "agentId": "code", "metadata": {"automation": {"boardId": "root-session"}}},
            ])
            store.register_step_ref("wb:spawn-step", kind="spawn")
            with store.connect() as db:
                registered_at = db.execute(
                    "SELECT registered_at FROM work_item_refs WHERE work_item_id='wb:spawn-step'"
                ).fetchone()[0]
            session = {
                "key": "agent:code:subagent:short", "sessionId": "session-short",
                "kind": "spawn-child", "label": "快速检查", "agentId": "code",
                "status": "done", "sessionStartedAt": registered_at + 10,
                "lastInteractionAt": registered_at + 1_000,
                "spawnedBy": "agent:main:feishu:direct:ou_1",
            }
            self.assertEqual(store.sync_spawn_session_steps([session]), 1)
            self.assertEqual(store.sync_spawn_session_steps([session]), 0)
            view = store.task_view("root-session")
            self.assertEqual(view["status"], "succeeded")
            self.assertEqual(view["items"][0]["status"], "succeeded")
            self.assertEqual(view["items"][0]["task_ref"], session["key"])
            self.assertTrue(any(e["event_type"] == "step_state_changed" for e in view["timeline"]))

    def test_missing_spawn_run_warns_after_soft_timeout_and_resolves_on_session(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "state.db")
            store.register_root("root-missing", "缺失运行", channel_key="dm:1",
                                target_type="user", target_id="1")
            store.sync_workboard([], [
                {"id": "root", "title": "缺失运行", "labels": ["project-root"],
                 "metadata": {"automation": {"boardId": "root-missing"}}},
                {"id": "spawn-step", "title": "可能丢失", "labels": ["project-step"],
                 "agentId": "code", "metadata": {"automation": {"boardId": "root-missing"}}},
            ])
            store.register_step_ref("wb:spawn-step", kind="spawn")
            with store.connect() as db:
                registered_at = db.execute(
                    "SELECT registered_at FROM work_item_refs WHERE work_item_id='wb:spawn-step'"
                ).fetchone()[0]
            self.assertEqual(store.flag_stale_spawn_steps(600, registered_at + 600_001), 1)
            warning = store.task_view("root-missing")["warnings"][0]
            self.assertEqual(warning["kind"], "missing_spawn_run")
            self.assertIn("fto step root-missing", warning["suggested_fix"])
            session = {
                "key": "agent:code:subagent:late", "kind": "spawn-child",
                "label": "可能丢失", "agentId": "code", "status": "done",
                "sessionStartedAt": registered_at + 1, "updatedAt": registered_at + 700_000,
                "spawnedBy": "agent:main:feishu:direct:ou_1",
            }
            self.assertEqual(store.sync_spawn_session_steps([session]), 1)
            self.assertEqual(store.task_view("root-missing")["warnings"], [])


if __name__ == "__main__":
    unittest.main()
