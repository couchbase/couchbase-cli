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
import json
import os
import unittest
from optparse import Values

from mock_server import MockRESTServer, generate_self_signed_cert

cbrecovery = imp.load_source('cbrecovery', './cbrecovery')

host = '127.0.0.1'
port = 6789

# setUpModule will generate new certificates for the mock HTTPS server used during unit testing.
#
# NOTE: We don't remove the key/cert once generated since they're included in the '.gitignore' file.


def setUpModule():
    generate_self_signed_cert(os.path.dirname(os.path.abspath(__file__)))


class RecoveryTest(unittest.TestCase):
    def test_pre_transfer(self):
        tests = {
            "MissingRecoveryMap": {
                "opts": Values({
                    "username": "",
                    "password": "",
                    # Assert that we use these credentials, and not the empty source credentials
                    "username_dest": "Administrator",
                    "password_dest": "asdasd",
                    "ssl": False,
                }),
                "response": {},
                "error": "Missing recovery map from response",
            },
            "MissingRecoveryMap1": {
                "opts": Values({
                    "username": "",
                    "password": "",
                    # Assert that we use these credentials, and not the empty source credentials
                    "username_dest": "Administrator",
                    "password_dest": "asdasd",
                    "ssl": False,
                }),
                "status": 400,
                "response": {"code": "not_needed"},
                "error": "error: unable to access REST API: " \
                "127.0.0.1:6789/pools/default/buckets/bucket/controller/startRecovery; please check source URL, " \
                "server status, username (-u) and password (-p); response: 400; reason: start_recovery",
                "cmd": "startRecovery",
                "bad_request_code": "not_needed",
            }
        }

        recovery = cbrecovery.Recovery()
        recovery.sink_bucket = "bucket"

        opts = Values({
            "username": "",
            "password": "",
            # Assert that we use these credentials, and not the empty source credentials
            "username_dest": "Administrator",
            "password_dest": "asdasd",
            "ssl": False,
        })

        def value(test, key):
            return test[key] if key in test else None

        for name, test in tests.items():
            with self.subTest(name):
                server = MockRESTServer(host, port)
                server.set_args({"/pools/default/buckets/bucket/controller/startRecovery": value(test, "response")})
                status = value(test, "status")
                if status:
                    server.args['override-status'] = status
                server.run()

                err, cmd, bad_request_code = recovery.pre_transfer(opts, "", f"http://{host}:{port}")
                self.assertEqual(value(test, "error"), err)
                self.assertEqual(value(test, "cmd"), cmd)
                self.assertEqual(value(test, "bad_request_code"), bad_request_code)
                self.assertIn('POST:/pools/default/buckets/bucket/controller/startRecovery', server.trace)

                server.shutdown()

    def test_post_transfer(self):
        server = MockRESTServer(host, port)
        server.run()

        recovery = cbrecovery.Recovery()
        recovery.sink_bucket = "bucket"

        opts = Values({
            "username": "",
            "password": "",
            # Assert that we use these credentials, and not the empty source credentials
            "username_dest": "Administrator",
            "password_dest": "asdasd",
            "dry_run": False,
            "ssl": False,
        })

        err, _ = recovery.post_transfer(opts, "", f"http://{host}:{port}", 0)
        self.assertIn('POST:/pools/default/buckets/bucket/controller/commitVBucket', server.trace)
        self.assertIsNone(err)

        server.shutdown()
