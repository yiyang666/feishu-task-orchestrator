import unittest

from orchestrator.event_source.openclaw_tasks import OpenClawTasksSource
from orchestrator.models import Status


class EventSourceTests(unittest.TestCase):
    def test_snapshot_and_event_are_deterministic(self):
        item = {
            "taskId": "task-1", "status": "running", "agentId": "research",
            "requesterSessionKey": "agent:main:feishu:group:oc_test",
            "label": "查天气", "createdAt": 100, "lastEventAt": 200,
            "progressSummary": "检索中",
        }
        snap = OpenClawTasksSource._snapshot(item)
        first = list(OpenClawTasksSource.events(snap))[0]
        second = list(OpenClawTasksSource.events(snap))[0]
        self.assertEqual(snap.status, Status.RUNNING)
        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(first.summary, "检索中")

    def test_group_filter_checks_requester_and_child_sessions(self):
        source = OpenClawTasksSource(session_prefixes=("agent:main:feishu:group:oc_target",))
        self.assertTrue(source._accept({"requesterSessionKey": "agent:main:feishu:group:oc_target"}))
        self.assertFalse(source._accept({"requesterSessionKey": "agent:main:feishu:direct:ou_x"}))

    def test_selected_automation_source_is_accepted_without_session(self):
        source = OpenClawTasksSource(
            session_prefixes=("agent:main:feishu:group:oc_target",),
            automation_source_ids=("cron-weather",),
        )
        self.assertTrue(source._accept({"runtime": "cron", "sourceId": "cron-weather"}))
        self.assertFalse(source._accept({"runtime": "cron", "sourceId": "other-cron"}))

    def test_empty_prefixes_accept_all_agents(self):
        source = OpenClawTasksSource(session_prefixes=())
        self.assertTrue(source._accept({"requesterSessionKey": "agent:codex:main"}))
        self.assertTrue(source._accept({"childSessionKey": "agent:research:subagent:x"}))
        self.assertTrue(source._accept({}))

    def test_succeeded_event_prefers_work_result_over_delivery_failure(self):
        item = {
            "taskId": "task-2", "status": "succeeded", "agentId": "codex",
            "label": "确定性任务", "createdAt": 100, "lastEventAt": 200,
            "progressSummary": "验收成功",
            "terminalSummary": "Required completion delivery failed",
        }
        event = list(OpenClawTasksSource.events(OpenClawTasksSource._snapshot(item)))[0]
        self.assertEqual(event.summary, "验收成功")


if __name__ == "__main__":
    unittest.main()
