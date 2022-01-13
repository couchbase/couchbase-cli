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

import imp
import re
import unittest

cbworkloadgen = imp.load_source('cbworkloadgen', './cbworkloadgen')


class WorkloadGenTest(unittest.TestCase):
    def test_construct_sink(self):
        tests = {
            "WithoutSchemeNoSSL": {
                "kwargs": {
                    "sink_str": "localhost:8091",
                    "ssl": False,
                },
                "valid": True,
                "result_str": "http://localhost:8091",
            },
            "WithSchemeHTTPNoSSL": {
                "kwargs": {
                    "sink_str": "http://localhost:8091",
                    "ssl": False,
                },
                "valid": True,
                "result_str": "http://localhost:8091",
            },
            "WithoutSchemeWithSSL": {
                "kwargs": {
                    "sink_str": "localhost:18091",
                    "ssl": True,
                },
                "valid": True,
                "result_str": "https://localhost:18091",
            },
            "WithSchemeHTTPSWithSSL": {
                "kwargs": {
                    "sink_str": "https://localhost:18091",
                    "ssl": True,
                },
                "valid": True,
                "result_str": "https://localhost:18091",
            },
            "WithSchemeHTTPSNoSSL": {
                "kwargs": {
                    "sink_str": "https://localhost:18091",
                    "ssl": False,
                },
                "valid": True,
                "result_str": "https://localhost:18091",
            },
            "WithSchemeCOUCHBASENoSSL": {
                "kwargs": {
                    "sink_str": "couchbase://localhost:8091",
                    "ssl": False,
                },
                "valid": True,
                "result_str": "http://localhost:8091",
            },
            "WithSchemeCOUCHBASESNoSSL": {
                "kwargs": {
                    "sink_str": "couchbases://localhost:18091",
                    "ssl": False,
                },
                "valid": True,
                "result_str": "https://localhost:18091",
            },
            "WithSchemeHTTPWithSSL": {
                "kwargs": {
                    "sink_str": "http://localhost:8091",
                    "ssl": True,
                },
                "message": "the ip:port of a node is supplied with 'http' scheme, which is not supported with the "
                "--ssl flag, please use 'https' or remove the scheme entirely",
            },
            "WithUnsupportedScheme": {
                "kwargs": {
                    "sink_str": "notsupported://localhost:8091",
                    "ssl": False,
                },
                "message": "the ip:port of a node is supplied with an unsupported scheme 'notsupported' "
                "(only 'http', 'https', 'couchbase' and 'couchbases' are supported)",
            },
            "EmptySinkString": {
                "kwargs": {
                    "sink_str": "",
                    "ssl": False,
                },
                "message": "the ip:port of a node seems to not be supplied, please check your CLI command",
            },
            "MoreThanOneScheme": {
                "kwargs": {
                    "sink_str": "http://http://localhost:8091",
                    "ssl": False,
                },
                "message": "the supplied ip:port of a node has incorrect format, check if a scheme is "
                "supplied more than once",
            },
            "PortOutOfRange": {
                "kwargs": {
                    "sink_str": "http://localhost:999999",
                    "ssl": False,
                },
                "message": "failed to get the port from the supplied ip:port of a node: Port out of range 0-65535",
            },
            "NoPort": {
                "kwargs": {
                    "sink_str": "http://localhost",
                    "ssl": False,
                },
                "message": "the ip:port of a node is supplied without a port",
            },
            "WithoutSchemeNoPort": {
                "kwargs": {
                    "sink_str": "localhost",
                    "ssl": False,
                },
                "message": "the ip:port of a node is supplied without a port",
            },
            "BadPortWithSSL": {
                "kwargs": {
                    "sink_str": "https://localhost:8091",
                    "ssl": True,
                },
                "message": "the ip:port of a node is supplied with a bad port '8091', you must use '18091' if you are "
                "using SSL encryption"
            },
        }

        construct_sink = cbworkloadgen.WorkloadGen().construct_sink

        for name, test in tests.items():
            with self.subTest(name=name):
                if "valid" in test and test["valid"]:
                    sink_str = construct_sink(**test["kwargs"])
                    self.assertEqual(sink_str, test["result_str"])
                    continue

                with self.assertRaisesRegex(cbworkloadgen.OptConstructError,
                                            re.escape(test["message"]) if "message" in test else ""):
                    construct_sink(**test["kwargs"])
