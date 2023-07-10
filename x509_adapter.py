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

import base64
import os  # noqa
import ssl
import tempfile
from pathlib import Path
from typing import Any, Callable, List, Optional

"""This environment variable is needed to prevent the "import pem" step is failing on MacOS.
pem imports OpenSSL, which imports cryptography, which seems to fail as the MacOS version of OpenSSL doesn't seem to
have the legacy algorithms required. Since we don't use any of these algorithms in this file, setting this environment
variable solves the issue without problem.
"""
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "true"  # noqa
import pem
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives.asymmetric import dsa, rsa
from cryptography.hazmat.primitives.serialization import (BestAvailableEncryption, Encoding, NoEncryption,
                                                          PrivateFormat, load_der_private_key, load_pem_private_key,
                                                          pkcs12)
from requests.adapters import HTTPAdapter


class X509AdapterError(Exception):
    """Exception raised for any known errors when creating a new 'X509Adapter'.

    Args:
        message: The message the exception will contain for the user; this will be printed (without a backtrace).
    """

    def __init__(self, message="an unknown client certificate authentication error occurred"):
        self.message = message

        super().__init__(self.message)


class X509Adapter(HTTPAdapter):
    """A 'HTTPAdapter' subclass which creates and uses an 'ssl' context which allows users authenticate using mTLS.

    Attributes:
        _ctx: The created 'ssl' context which will have been loaded with the clients cert/chain and key.
    """

    def __init__(self, cert: str, password: str = '', **kwargs):
        """Instantiates a new 'X509Adapter' using the given cert/chain.


        Args:
            cert: A PEM encoded x509 certificate or certificate chain with a possibly encrypted ptivate key.
            password: A password for the private key if it is encrypted.

        Raises:
            X509AdapterError: An error occurred constructing the 'ssl' context.
        """
        self._ctx = ssl.SSLContext()

        try:
            self._ctx.load_cert_chain(cert, password=password)
        except ssl.SSLError as e:
            raise X509AdapterError(e.reason if e.reason is not None else str(e)) from e

        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        if self._ctx:
            kwargs['ssl_context'] = self._ctx

        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        if self._ctx:
            kwargs['ssl_context'] = self._ctx

        return super().proxy_manager_for(*args, **kwargs)

# pylint: disable=too-few-public-methods


class X509AdapterFactory():
    """Factory/generator which creates an 'X509Adapter' used to allow users to perform mTLS authentication.

    Attributes:
        client_ca: Path to the client cert file (for PKCS#12, must contain the private key).
        client_pk: Path to the client private key file.
        password: The password for the client private key (or the client ca when using PKCS#12).
    """

    def __init__(self, host: str, client_ca: Path, client_pk: Optional[Path] = None, password: str = ""):
        """Create a new factory with the given options, the 'generate' function may be used to actually create the
        'X509Adapter'.

        Args:
            host: Host that we're going to perform mTLS authentication against.
            client_ca: Path to the client cert file (for PKCS#12, must contain the private key)
            client_pk: Path to the client private key.
            password: The password for the client private key (or the client ca when using PKCS#12).

        Raises:
            X509AdapterError: If the given host will not be a secure connection.
                              If a PKCS#12 style client cert/key file is provided without a password.
        """
        if not (host.startswith("https://") or host.startswith("couchbases://")):
            raise X509AdapterError("certificate authentication is only supported for TLS connections")

        if client_pk is None and password == "":
            raise X509AdapterError("client cert/key file provided without a password; expect an encrypted PKCS#12 file")

        self.client_ca = client_ca
        self.client_pk = client_pk
        self.password = password

    def generate(self) -> X509Adapter:
        """Generate an 'X509Adapter' which may be used to perform mTLS authentication, the users cert/key will be
        decrypted and then written to disk in a common format, keeping the private key encrypted if it was so.

        NOTE: We write to disk due to a limitation in the ssl library: it does not allow reading certificates/keys from
        memory. We used to use urllib3.contrib.pyopenssl for this but it has been deprecated, so we have been forced
        into using ssl. See MB-57219 and https://github.com/urllib3/urllib3/issues/2680.
        """

        certs = self._parse_certs()
        key = self._parse_key()

        with tempfile.NamedTemporaryFile() as f:
            f.write(key)
            f.write(certs)
            f.flush()
            return X509Adapter(f.name, password=self.password)

    def _parse_certs(self) -> bytes:
        data = self._read_file_bytes(self.client_ca)
        if not data:
            raise X509AdapterError(f"certificate file '{self.client_ca}' is empty")

        if self.password != "" and self.client_pk is None:
            return self._parse_certs_pkcs12(data)

        return self._parse_certs_unencrypted(data)

    @classmethod
    def _parse_certs_unencrypted(cls, data: bytes) -> bytes:
        parsed = pem.parse(data)
        if not parsed:
            raise X509AdapterError("invalid certificate, perhaps it's encrypted or an unsupported format")

        chain = bytearray()
        for cert in parsed:
            chain.extend(cert.as_bytes())

        return chain

    def _parse_certs_pkcs12(self, data: bytes) -> bytes:
        try:
            (_, cert, chain) = pkcs12.load_key_and_certificates(data, self.password.encode("utf-8"))
        except ValueError as error:
            raise X509AdapterError("invalid password or PKCS#12 data") from error

        if cert is None:
            raise X509AdapterError("invalid password or PKCS#12 data")

        encoded = bytearray()
        encoded.extend(cert.public_bytes(Encoding.PEM))

        for cert in chain:
            encoded.extend(cert.public_bytes(Encoding.PEM))

        return encoded

    def _parse_key(self) -> bytes:
        data = self._read_file_bytes(self.client_ca if self.client_pk is None else self.client_pk)
        if not data:
            raise X509AdapterError(f"key file '{self.client_pk}' is empty")

        if self.password != "" and self.client_pk is not None:
            return self._parse_key_pkcs8(data)

        if self.password != "":
            return self._parse_key_pkcs12(data)

        return self._parse_key_unencrypted(data)

    @classmethod
    def _parse_key_unencrypted(cls, data: bytes) -> bytes:
        try:
            key = load_pem_private_key(data, password=None)
        except (TypeError, ValueError, UnsupportedAlgorithm) as error:
            raise X509AdapterError("invalid key, perhaps it's an unsupported format or encrypted") from error

        cls._check_key_type(key)

        return key.private_bytes(Encoding.PEM, PrivateFormat.TraditionalOpenSSL, NoEncryption())

    def _parse_key_pkcs8(self, data: bytes) -> bytes:
        last_exception = None
        fns: List[Callable[..., Any]] = [load_pem_private_key, load_der_private_key]
        for fn in fns:
            try:
                key = fn(data, password=self.password.encode('utf-8'))
                break
            except (TypeError, ValueError, UnsupportedAlgorithm) as error:
                last_exception = error
        else:
            raise X509AdapterError("invalid password or PKCS#8 data") from last_exception

        self._check_key_type(key)

        return key.private_bytes(
            Encoding.PEM, PrivateFormat.TraditionalOpenSSL,
            BestAvailableEncryption(password=self.password.encode('utf-8')))

    def _parse_key_pkcs12(self, data: bytes) -> bytes:
        try:
            (key, _, _) = pkcs12.load_key_and_certificates(data, self.password.encode("utf-8"))
        except ValueError as error:
            raise X509AdapterError("invalid password or PKCS#12 data") from error

        if key is None:
            raise X509AdapterError("PKCS#12 file must contain at least one private key")

        self._check_key_type(key)

        return key.private_bytes(
            Encoding.PEM, PrivateFormat.TraditionalOpenSSL,
            BestAvailableEncryption(password=self.password.encode('utf-8')))

    @classmethod
    def _read_file_bytes(cls, path: Path) -> bytes:
        try:
            with open(path, "rb") as file:
                return file.read()
        except IOError as error:
            raise X509AdapterError(f"{error.strerror.lower()} '{path}'") from error

    @classmethod
    def _check_key_type(cls, key):
        if isinstance(key, rsa.RSAPrivateKey) or isinstance(key, dsa.DSAPrivateKey):
            return

        raise X509AdapterError(f"unsupported key type, expected RSAPrivateKey/DSAPrivateKey got"
                               f" {type(key).__name__.removeprefix('_')}")
