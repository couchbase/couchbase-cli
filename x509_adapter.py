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
from pathlib import Path
from typing import Optional, Tuple

import Crypto.IO.PKCS8 as pkcs8

"""This environment variable is needed to prevent the "import pem" step is failing on MacOS.
pem imports OpenSSL, which imports cryptography, which seems to fail as the MacOS version of OpenSSL doesn't seem to
have the legacy algorithms required. Since we don't use any of these algorithms in this file, setting this environment
variable solves the issue without problem.
"""
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "true"  # noqa
import pem
from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives.serialization import (Encoding, NoEncryption, PrivateFormat, load_der_private_key,
                                                          load_pem_private_key, pkcs12)
from OpenSSL.crypto import X509, PKey
from OpenSSL.SSL import Error as OpenSSLError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.contrib.pyopenssl import PyOpenSSLContext


class X509AdapterError(Exception):
    """Exception raised for any known errors when creating a new 'X509Adapter'.

    Args:
        message: The message the exception will contain for the user; this will be printed (without a backtrace).
    """

    def __init__(self, message="an unknown client certificate authentication error occurred"):
        self.message = message

        super().__init__(self.message)


class X509Adapter(HTTPAdapter):
    """A 'HTTPAdapter' subclass which creates and uses a 'pyopenssl' context which allows users authenticate using mTLS.

    Attributes:
        _ctx: The created 'pyopenssl' context which will have been loaded with the clients cert/chain and key.
    """

    def __init__(self, cert: bytes, chain: bytes, key: bytes, **kwargs):
        """Instantiates a new 'X509Adapter' using the given cert/chain and key.


        Args:
            cert: A PEM encoded x509 certificate.
            chain: PEM encoded certificate chain containing any certificates needed to verify 'cert'.
            key: An unencrypted PEM encoded private key or a PEM/DER encoded private key in the PKCS#8 format.

        Raises:
            X509AdapterError: An error occurred constructing the 'pyopenssl' context.
        """
        self._ctx = self._new_ssl_context(
            x509.load_pem_x509_certificate(cert),
            [x509.load_pem_x509_certificate(ca_cert.as_bytes()) for ca_cert in pem.parse(chain)],
            load_der_private_key(key, password=None))

        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        if self._ctx:
            kwargs['ssl_context'] = self._ctx

        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        if self._ctx:
            kwargs['ssl_context'] = self._ctx

        return super().proxy_manager_for(*args, **kwargs)

    # pylint: disable=protected-access
    @classmethod
    def _new_ssl_context(cls, cert, chain, key):
        ctx = PyOpenSSLContext(ssl.PROTOCOL_TLS)

        ctx._ctx.use_certificate(X509.from_cryptography(cert))

        for ca_cert in chain:
            ctx._ctx.add_extra_chain_cert(X509.from_cryptography(ca_cert))

        def remove_underscore(val: str) -> str:
            return val[len('_'):] if val.startswith('_') else val

        try:
            ctx._ctx.use_privatekey(PKey.from_cryptography_key(key))
        except OpenSSLError as error:
            raise X509AdapterError(str(error)) from error
        except TypeError as error:
            raise X509AdapterError(f"unsupported key type, expected RSAPrivateKey/DSAPrivateKey got"
                                   f" {remove_underscore(type(key).__name__)}") from error

        return ctx

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
        decrypted and remain in memory (it will not be written to disk, although it will be serialized/deserialized
        multiple times).
        """
        cert, chain = self._parse_certs()
        key = self._parse_key()

        return X509Adapter(cert=cert, chain=chain, key=key)

    def _parse_certs(self) -> Tuple[bytes, bytes]:
        data = self._read_file_bytes(self.client_ca)
        if not data:
            raise X509AdapterError(f"certificate file '{self.client_ca}' is empty")

        if self.password != "" and self.client_pk is None:
            return self._parse_certs_pkcs12(data)

        return self._parse_certs_unencrypted(data)

    @classmethod
    def _parse_certs_unencrypted(cls, data: bytes) -> Tuple[bytes, bytes]:
        parsed = pem.parse(data)
        if not parsed:
            raise X509AdapterError("invalid certificate, perhaps it's encrypted or an unsupported format")

        chain = bytearray()
        for cert in parsed[1:]:
            chain.extend(cert.as_bytes())

        return parsed[0].as_bytes(), chain

    def _parse_certs_pkcs12(self, data: bytes) -> Tuple[bytes, bytes]:
        try:
            (_, cert, chain) = pkcs12.load_key_and_certificates(data, self.password.encode("utf-8"))
        except ValueError as error:
            raise X509AdapterError("invalid password or PKCS#12 data") from error

        encoded = bytearray()
        for cert in chain:
            encoded.extend(cert.public_bytes(Encoding.PEM))

        return cert.public_bytes(Encoding.PEM), encoded

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

        return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())

    def _parse_key_pkcs8(self, data: bytes) -> bytes:
        parsed = pem.parse(data)
        if parsed and len(parsed) == 1:
            data = base64.b64decode(''.join(parsed[0].as_text().strip().split("\n")[1:-1]))

        try:
            (_, key, _) = pkcs8.unwrap(data, self.password.encode("utf-8"))
        except ValueError as error:
            raise X509AdapterError("invalid password or PKCS#8 data") from error

        return key

    def _parse_key_pkcs12(self, data: bytes) -> bytes:
        try:
            (key, _, _) = pkcs12.load_key_and_certificates(data, self.password.encode("utf-8"))
        except ValueError as error:
            raise X509AdapterError("invalid password or PKCS#12 data") from error

        if key is None:
            raise X509AdapterError("PKCS#12 file must contain at least one private key")

        return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())

    @classmethod
    def _read_file_bytes(cls, path: Path) -> bytes:
        try:
            with open(path, "rb") as file:
                return file.read()
        except IOError as error:
            raise X509AdapterError(f"{error.strerror.lower()} '{path}'") from error
