import unittest

from cbmgr import compare_versions, process_services


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
                services, err = process_services(test["service"], True, False, cluster_version)
                self.assertEqual(err, test.setdefault("error", None))
                self.assertEqual(services, test["expected"])

    def test_process_services_version_none(self):
        services, err = process_services("manager-only", True, False)
        self.assertEqual(err, None)
        self.assertEqual(services, "")


class TestCompareVersions(unittest.TestCase):
    def test_compare_versions(self):
        tests = {
            "V1GreaterThanV2": {
                "version1": "10.0.0",
                "version2": "6.5.2",
                "expected": 1
            },
            "V1EqualV2": {
                "version1": "7.0.0",
                "version2": "7.0.0",
                "expected": 0
            },
            "V1LessThanV2": {
                "version1": "6.5.2",
                "version2": "10.0.0",
                "expected": -1
            },
            "V1IsUnknown": {
                "version1": "",
                "version2": "6.4.3",
                "expected": 1
            },
            "V1IsZero": {
                "version1": "0.0.0",
                "version2": "6.4.3",
                "expected": 1
            },
            "V2IsUnknown": {
                "version1": "6.5.4",
                "version2": "",
                "expected": -1
            },
            "V2IsZero": {
                "version1": "7.2.3",
                "version2": "0.0.0",
                "expected": -1
            },
        }

        for name, test in tests.items():
            with self.subTest(name):
                result = compare_versions(test["version1"], test["version2"])
                self.assertEqual(result, test["expected"])
