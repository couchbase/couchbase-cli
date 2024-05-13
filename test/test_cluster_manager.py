#!/usr/bin/env python3

# Copyright 2022 Couchbase Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from cluster_manager import ClusterManager


class ClusterManagerTest(unittest.TestCase):
    def test_get_hostname_and_port(self):
        tests = {
            "IPWithNoSchemeWithNoPort": {
                "node": '10.200.300.400',
                "return": ['10.200.300.400', None],
            },
            "IPWithHTTPSchemeWithNoPort": {
                "node": 'http://10.200.300.400',
                "return": ['10.200.300.400', None],
            },
            "IPWithNoSchemeWithPort": {
                "node": '10.200.300.400:8091',
                "return": ['10.200.300.400', 8091],
            },
            "IPWithSchemeWithPort": {
                "node": 'http://10.200.300.400:8091',
                "return": ['10.200.300.400', 8091],
            },
            "DomainWithNoSchemeWithNoPort": {
                "node": 'test.com',
                "return": ['test.com', None],
            },
            "DomainWithHTTPSchemeWithNoPort": {
                "node": 'http://test.com',
                "return": ['test.com', None],
            },
            "DomainWithNoSchemeWithPort": {
                "node": 'test.com:8091',
                "return": ['test.com', 8091],
            },
            "DomainWithSchemeWithPort": {
                "node": 'http://test.com:8091',
                "return": ['test.com', 8091],
            },
            "InvalidUrl": {
                "node": '////not_valid_node_url',
                "return": [None, None],
            },
            "PortOutOfBoundsValueError": {
                "node": 'test.com:80910000000000',
                "return": [None, None],
            },
        }

        for name, test in tests.items():
            with self.subTest(name=name):
                hostname, port = ClusterManager._get_hostname_and_port(test['node'])
                self.assertEqual(hostname, test["return"][0])
                self.assertEqual(port, test["return"][1])

    def test_get_otp_names_of_matched_nodes(self):
        tests = {
            "NoKey_otpNode": {
                "nodes": [
                    {},
                ],
                "nodes_to_match": [],
                "errors": ["Unable to get the Otp name or the hostname of a cluster node"],
                "matched_nodes": None
            },
            "NoKey_otpNode_with_hostname": {
                "nodes": [
                    {
                        'hostname': '10.200.300.400:8091'
                    },
                ],
                "nodes_to_match": [],
                "errors": ["Unable to get the Otp name of the 10.200.300.400:8091 node"],
                "matched_nodes": None
            },
            "NoKey_hostname": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@10.200.300.400'
                    },
                ],
                "nodes_to_match": [],
                "errors": ["Unable to get the hostname of the ns_1@10.200.300.400 node"],
                "matched_nodes": None
            },
            "NoKey_ports": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@10.200.300.400',
                        'hostname': '10.200.300.400:8091'
                    },
                ],
                "nodes_to_match": ['test.com'],
                "errors": ["Unable to get the ports of the 10.200.300.400:8091 node"],
                "matched_nodes": None
            },
            "NoPortsKey_httpsMgmt": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@10.200.300.400',
                        'hostname': '10.200.300.400:8091',
                        'ports': {}
                    },
                ],
                "nodes_to_match": ['test.com'],
                "errors": ["Unable to get the HTTPS port of the 10.200.300.400:8091 node"],
                "matched_nodes": None
            },
            "NodeNotFound": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@10.200.300.400',
                        'hostname': 'test.com:8091',
                        'ports':
                            {
                                'httpsMgmt': 18091
                            }
                    },
                ],
                "nodes_to_match": ['test.com'],
                "errors": None,
                "matched_nodes": []
            },
            "NodeInHostname": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@10.200.300.400',
                        'hostname': 'test.com:8091',
                        'ports':
                            {
                                'httpsMgmt': 18091
                            }
                    },
                ],
                "nodes_to_match": ['test.com:8091'],
                "errors": None,
                "matched_nodes": ['ns_1@10.200.300.400']
            },
            "NodeInHostnameIPv6": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@::1',
                        'hostname': '[::1]:8091',
                        'ports':
                            {
                                'httpsMgmt': 18091
                            }
                    },
                ],
                "nodes_to_match": ['[::1]:8091'],
                "errors": None,
                "matched_nodes": ['ns_1@::1']
            },
            "NodeInHostnameSSL": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@10.200.300.400',
                        'hostname': 'test.com:8091',
                        'ports':
                            {
                                'httpsMgmt': 18091
                            }
                    },
                ],
                "nodes_to_match": ['test.com:18091'],
                "errors": None,
                "matched_nodes": ['ns_1@10.200.300.400']
            },
            "NodeInOtp": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@10.200.300.400',
                        'hostname': 'test.com:8091',
                        'ports':
                            {
                                'httpsMgmt': 18091
                            }
                    },
                ],
                "nodes_to_match": ['10.200.300.400:8091'],
                "errors": None,
                "matched_nodes": ['ns_1@10.200.300.400']
            },
            "NodeInOtpSSL": {
                "nodes": [
                    {
                        'otpNode': 'ns_1@10.200.300.400',
                        'hostname': 'test.com:8091',
                        'ports':
                            {
                                'httpsMgmt': 18091
                            }
                    },
                ],
                "nodes_to_match": ['10.200.300.400:18091'],
                "errors": None,
                "matched_nodes": ['ns_1@10.200.300.400']
            }
        }

        for name, test in tests.items():
            with self.subTest(name=name):
                matched_nodes, errors = ClusterManager._get_otp_names_of_matched_nodes(test['nodes'],
                                                                                       test['nodes_to_match'])
                self.assertEqual(errors, test['errors'])
                self.assertEqual(matched_nodes, test['matched_nodes'])
