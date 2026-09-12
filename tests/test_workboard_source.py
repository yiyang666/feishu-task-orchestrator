import unittest
from unittest.mock import patch

from orchestrator.event_source.workboard import WorkboardSource


class WorkboardSourceTests(unittest.TestCase):
    def test_maps_standalone_card_and_skips_linked_task(self):
        source = WorkboardSource()
        payload = {"cards": [
            {"id": "c1", "title": "独立任务", "status": "blocked", "updatedAt": 20},
            {"id": "c2", "title": "已关联", "status": "running", "taskId": "t1"},
        ]}
        with patch.object(source, "list_cards", return_value=payload):
            snapshots = source.fetch()
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].task_id, "wb:c1")
        self.assertEqual(str(snapshots[0].status), "blocked")


if __name__ == "__main__":
    unittest.main()

