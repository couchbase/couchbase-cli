#!/usr/bin/env python3

# Copyright 2021 Couchbase Inc.
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

import re
import unittest
from pathlib import Path

from x509_adapter import X509Adapter, X509AdapterError, X509AdapterFactory


class X509AdapterFactoryTest(unittest.TestCase):
    def test_validation(self):
        tests = {
            "UnencryptedHTTP": {
                "kwargs": {
                    "host": "http://localhost:18091",
                    "client_ca": "/path/to/certs",
                    "client_pk": "/path/to/key",
                },
                "message": "certificate authentication is only supported for TLS connections",
            },
            "UnencryptedCouchbase": {
                "kwargs": {
                    "host": "couchbase://localhost:18091",
                    "client_ca": "/path/to/certs",
                    "client_pk": "/path/to/key",
                },
                "message": "certificate authentication is only supported for TLS connections",
            },
            "UnencryptedKey": {
                "kwargs": {
                    "host": "https://localhost:18091",
                    "client_ca": "/path/to/certs",
                    "client_pk": "/path/to/key",
                },
                "valid": True,
            },
            "PKCS#8": {
                "kwargs": {
                    "host": "https://localhost:18091",
                    "client_ca": "/path/to/certs",
                    "client_pk": "/path/to/key",
                    "password": "asdasd",
                },
                "valid": True,
            },
            "PKCS#12": {
                "kwargs": {
                    "host": "https://localhost:18091",
                    "client_ca": "/path/to/certs",
                    "password": "asdasd",
                },
                "valid": True,
            },
            "ExpectAKeyOrPassword": {
                "kwargs": {
                    "host": "https://localhost:18091",
                    "client_ca": "/path/to/certs",
                },
                "message": "client cert/key file provided without a password; expect an encrypted PKCS#12 file",
            },
        }

        for name, test in tests.items():
            with self.subTest(name=name):
                if "valid" in test and test["valid"]:
                    X509AdapterFactory(**test["kwargs"])
                    return

                with self.assertRaisesRegex(X509AdapterError, re.escape(test["message"]) if "message" in test else ""):
                    X509AdapterFactory(**test["kwargs"])

    def test_valid_cert_key_pair(self):
        adapter = X509AdapterFactory(host="https://localhost:19000",
                                     client_ca=self.path_to("valid_cert.pem"),
                                     client_pk=self.path_to("valid_key.pem")).generate()

        self.assertIsNotNone(adapter)

    def test_invalid_cert(self):
        msg = "invalid certificate, perhaps it's encrypted or an unsupported format"

        with self.assertRaisesRegex(X509AdapterError, msg):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("invalid_cert.pem"),
                               client_pk=self.path_to("valid_key.pem")).generate()

    def test_invalid_key(self):
        with self.assertRaisesRegex(X509AdapterError, re.escape("invalid key, perhaps it's an unsupported format")):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("valid_cert.pem"),
                               client_pk=self.path_to("invalid_key.pem")).generate()

    def test_empty_cert(self):
        with self.assertRaisesRegex(X509AdapterError, r"certificate file '.*' is empty"):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("empty_cert.pem"),
                               client_pk=self.path_to("valid_key.pem")).generate()

    def test_empty_key(self):
        with self.assertRaisesRegex(X509AdapterError, r"key file '.*' is empty"):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("valid_cert.pem"),
                               client_pk=self.path_to("empty_key.pem")).generate()

    def test_unencrypted_with_password(self):
        with self.assertRaisesRegex(X509AdapterError, re.escape("invalid password or PKCS#8 data")):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("valid_cert.pem"),
                               client_pk=self.path_to("valid_key.pem"),
                               password="asdasd").generate()

    def test_valid_encrypted_pkcs12(self):
        adapter = X509AdapterFactory(host="https://localhost:19000",
                                     client_ca=self.path_to("valid_cert_and_key.p12"),
                                     password="asdasd").generate()

        self.assertIsNotNone(adapter)

    def test_valid_encrypted_pkcs12_wrong_password(self):
        with self.assertRaisesRegex(X509AdapterError, re.escape("invalid password or PKCS#12 data")):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("valid_cert_and_key.p12"),
                               password="not-the-password").generate()

    def test_valid_encrypted_pkcs12_without_password(self):
        msg = "client cert/key file provided without a password; expect an encrypted PKCS#12 file"

        with self.assertRaisesRegex(X509AdapterError, re.escape(msg)):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("valid_cert_and_key.p12")).generate()

    def test_invalid_encrypted_pkcs12(self):
        with self.assertRaisesRegex(X509AdapterError, re.escape("invalid password or PKCS#12 data")):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("invalid_cert_and_key.p12"),
                               password="asdasd").generate()

    def test_valid_encrypted_pkcs8(self):
        for key in ["valid_key_p8.pem", "valid_key_p8.der"]:
            adapter = X509AdapterFactory(host="https://localhost:19000",
                                         client_ca=self.path_to("valid_cert.pem"),
                                         client_pk=self.path_to(key),
                                         password="asdasd").generate()

            self.assertIsNotNone(adapter)

    def test_valid_encrypted_pkcs8_wrong_password(self):
        for key in ["valid_key_p8.pem", "valid_key_p8.der"]:
            with self.assertRaisesRegex(X509AdapterError, re.escape("invalid password or PKCS#8 data")):
                X509AdapterFactory(host="https://localhost:19000",
                                   client_ca=self.path_to("valid_cert.pem"),
                                   client_pk=self.path_to(key),
                                   password="not-the-password").generate()

    def test_valid_encrypted_pkcs8_without_password(self):
        msg = "invalid key, perhaps it's an unsupported format or encrypted"

        for key in ["valid_key_p8.pem", "valid_key_p8.der"]:
            with self.assertRaisesRegex(X509AdapterError, re.escape(msg)):
                X509AdapterFactory(host="https://localhost:19000",
                                   client_ca=self.path_to("valid_cert.pem"),
                                   client_pk=self.path_to(key)).generate()

    def test_invalid_encrypted_pkcs8(self):
        for key in ["invalid_key_p8.pem", "invalid_key_p8.der"]:
            with self.assertRaisesRegex(X509AdapterError, re.escape("invalid password or PKCS#8 data")):
                X509AdapterFactory(host="https://localhost:19000",
                                   client_ca=self.path_to("valid_cert.pem"),
                                   client_pk=self.path_to(key),
                                   password="asdasd").generate()

    def test_multiple_certs_pem(self):
        adapter = X509AdapterFactory(host="https://localhost:19000",
                                     client_ca=self.path_to("valid_certs.pem"),
                                     client_pk=self.path_to("valid_key.pem")).generate()

        self.assertIsNotNone(adapter)

    def test_multiple_certs_pkcs12(self):
        adapter = X509AdapterFactory(host="https://localhost:19000",
                                     client_ca=self.path_to("valid_certs_and_key.p12"),
                                     password="asdasd").generate()

        self.assertIsNotNone(adapter)

    def test_mismatched_keys(self):
        msg = "[('x509 certificate routines', 'X509_check_private_key', 'key values mismatch')]"

        with self.assertRaisesRegex(X509AdapterError, re.escape(msg)):
            X509AdapterFactory(host="https://localhost:19000",
                               client_ca=self.path_to("valid_cert.pem"),
                               client_pk=self.path_to("valid_rsa_key.pem")).generate()

    def test_unsupported_key_types(self):
        tests = [("valid_ecdsa_key.pem", "EllipticCurvePrivateKey"), ("valid_ed25519_key.pem", "Ed25519PrivateKey")]

        for (filename, key_type) in tests:
            msg = f"unsupported key type, expected RSAPrivateKey/DSAPrivateKey got {key_type}"

            with self.assertRaisesRegex(X509AdapterError, re.escape(msg)):
                X509AdapterFactory(host="https://localhost:19000",
                                   client_ca=self.path_to("valid_cert.pem"),
                                   client_pk=self.path_to(filename)).generate()

    @classmethod
    def path_to(cls, filename: str) -> Path:
        return Path(__file__).parent / Path("testdata") / Path(filename)
