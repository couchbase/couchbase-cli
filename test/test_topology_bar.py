import unittest
from typing import List

from cluster_manager import ClusterManager
from pbar import TopologyProgressBar


class TaskGetter:
    def __init__(self, tasks: List[any]):
        self.calls = 0
        self.tasks = tasks

    def get_task(self):
        self.calls += 1
        if len(self.tasks) == 0:
            return [], None
        return [self.tasks.pop(0)], None


class TopologyBarTest(unittest.TestCase):
    def test_topology_progress(self):
        client = ClusterManager('http://localhost:8091', 'u', 'p')
        tasks = TaskGetter([
            {'type': 'rebalance', 'status': 'running', 'progress': 0.5, 'recommendedRefreshPeriod': 0.1,
             'detailedProgress': {}},
            {'type': 'rebalance', 'status': 'running', 'progress': 0.6, 'recommendedRefreshPeriod': 0.1,
             'detailedProgress': {}},
            {'type': 'rebalance', 'status': 'running', 'progress': 0.7, 'recommendedRefreshPeriod': 0.1,
             'detailedProgress': {}},
            {'type': 'rebalance', 'status': 'notRunning', 'statusIsStale': True},
            {'type': 'rebalance', 'status': 'notRunning', 'masterRequestTimedOut': True},
            {'type': 'rebalance', 'status': 'running', 'progress': 0.9, 'recommendedRefreshPeriod': 0.1,
             'detailedProgress': {}},
            {'type': 'rebalance', 'status': 'notRunning'},
        ])

        client.get_tasks = tasks.get_task

        bar = TopologyProgressBar(client, 'rebalance', True)
        err = bar.show()

        self.assertIsNone(err)
        self.assertEqual(tasks.calls, 7)

    def test_topology_progress_with_error(self):
        client = ClusterManager('http://localhost:8091', 'u', 'p')
        tasks = TaskGetter([
            {'type': 'rebalance', 'status': 'running', 'progress': 0.5, 'recommendedRefreshPeriod': 0.1,
             'detailedProgress': {}},
            {'type': 'rebalance', 'status': 'running', 'progress': 0.6, 'recommendedRefreshPeriod': 0.1,
             'detailedProgress': {}},
            {'type': 'rebalance', 'status': 'running', 'progress': 0.7, 'recommendedRefreshPeriod': 0.1,
             'detailedProgress': {}},
            {'type': 'rebalance', 'status': 'notRunning', 'statusIsStale': True},
            {'type': 'rebalance', 'status': 'notRunning', 'masterRequestTimedOut': True},
            {'type': 'rebalance', 'status': 'notRunning', 'errorMessage': 'it broke'},
            {'type': 'rebalance', 'status': 'notRunning', 'masterRequestTimedOut': True},
        ])

        client.get_tasks = tasks.get_task

        bar = TopologyProgressBar(client, 'rebalance', True)
        err = bar.show()

        self.assertEqual(['it broke'], err)
        self.assertEqual(tasks.calls, 6)
