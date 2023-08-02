import unittest
from cbmgr import process_services


class TestProcessServices(unittest.TestCase):
    def test_process_services(self):
        tests = {
            "DataAndQueryService": {
                "service": "data",
                "expected": "kv"
            },
            "BlankService": {
                "service": "manager-only",
                "expected": ""
            },
            "ManagerOnlyAndData": {
                "service": "data,manager-only",
                "expected": None,
                "error": ["Invalid service configuration. A manager only node cannot run any other services."]
            },
            "UnsupportedClusterVersion": {
                "service": "manager-only",
                "expected": None,
                "error": ["The manager only service can only be used with >= 7.6.0 clusters"],
                "cluster_version": "7.2.0"
            }
        }

        for name, test in tests.items():
            with self.subTest(name):
                cluster_version = test.setdefault("cluster_version", "7.6.0")
                services, err = process_services(test["service"], True, cluster_version)
                self.assertEqual(err, test.setdefault("error", None))
                self.assertEqual(services, test["expected"])

    def test_process_services_version_none(self):
        services, err = process_services("manager-only", True)
        self.assertEqual(err, None)
        self.assertEqual(services, "")
