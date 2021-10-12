"""Mock server only emulates CB rest endpoints but has no functionality"""
import datetime
import json
import os
import re
import socket
import ssl
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

import requests
from cryptography import x509
from cryptography.hazmat.backends.openssl.backend import backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


# generate_self_signed_cert generates a key/self signed certificate pair which will be written to key.pem/cert.pem in
# the given directory.
#
# For more information, see https://cryptography.io/en/latest/x509/tutorial.
def generate_self_signed_cert(path: str, key_name: str = "key.pem", cert_name: str = "cert.pem"):
    if not os.path.isdir(path):
        raise ValueError("'path' should be an existing directory")

    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=backend,
    )

    with open(os.path.join(path, key_name), "wb") as file:
        file.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"California"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"Santa Clara"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Couchbase"),
        x509.NameAttribute(NameOID.COMMON_NAME, u"couchbase.com"),
    ])

    cert = x509.CertificateBuilder() \
        .subject_name(subject) \
        .issuer_name(issuer) \
        .public_key(key.public_key()) \
        .serial_number(x509.random_serial_number()) \
        .not_valid_before(datetime.datetime.utcnow()) \
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365)) \
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(u"localhost")]), critical=False) \
        .sign(key, hashes.SHA256(), backend=backend)

    with open(os.path.join(path, cert_name), "wb") as file:
        file.write(cert.public_bytes(serialization.Encoding.PEM))


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append(f'GET:{parsed.path}')
        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'GET' in fns:
                return self.handle_fn(fns['GET'], parsed.path, parsed.query)

        self.not_found()

    def do_POST(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append(f'POST:{parsed.path}')
        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'POST' in fns:
                return self.handle_fn(fns['POST'], parsed.path)

        self.not_found()

    def do_PATCH(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append(f'PATCH:{parsed.path}')
        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'PATCH' in fns:
                return self.handle_fn(fns['PATCH'], parsed.path)

        self.not_found()

    def do_PUT(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append(f'PUT:{parsed.path}')
        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'PUT' in fns:
                return self.handle_fn(fns['PUT'], parsed.path)

        self.not_found()

    def do_DELETE(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append(f'DELETE:{parsed.path}')
        if parsed.query:
            self.server.rest_server.queries.append(parsed.query)

        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'DELETE' in fns:
                return self.handle_fn(fns['DELETE'], parsed.path)
        self.not_found()

    def not_found(self):
        self.send_response(404)
        self.finish()

    def handle_fn(self, fn, path, params=None):
        content_len = int(self.headers.get('content-length', 0))
        post_body = self.rfile.read(content_len).decode('utf-8')

        if self.headers.get('Content-Type', 'application/x-www-form-urlencoded') == 'application/json':
            # to help with verifying later on we are going to load this json and then dump it again but with sorted keys
            # to ensure stable serializing so then we can do string comparison on the results
            self.server.rest_server.rest_params.append(json.dumps(json.loads(post_body), sort_keys=True))
        else:
            post_body = post_body.split('&')
            for e in post_body:
                if e == "":
                    continue
                self.server.rest_server.rest_params.append(e)

        code, response = fn(post_body, self.server.rest_server.args, path, params)
        self.send_response(code)

        if response is not None:
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            if sys.version_info >= (3, 0):
                self.wfile.write(bytes(json.dumps(response), 'utf-8'))
            else:
                self.wfile.write(bytes(json.dumps(response)))
        else:
            self.end_headers()


class MockHTTPServer(HTTPServer):
    def __init__(self, host_port, handler, rest_server):
        self.rest_server = rest_server  # Instance of MockRESTServer.
        HTTPServer.__init__(self, host_port, handler)

    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        HTTPServer.server_bind(self)


class MockHTTPSServer(HTTPServer):
    def __init__(self, host_port, handler, rest_server):
        self.rest_server = rest_server  # Instance of MockRESTServer.
        HTTPServer.__init__(self, host_port, handler)

    def server_bind(self):
        self.socket = ssl.wrap_socket(self.socket,
                                      keyfile="./test/key.pem",
                                      certfile="./test/cert.pem",
                                      server_side=True)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        HTTPServer.server_bind(self)


class MockRESTServer(object):
    def __init__(self, host, port, args={}):
        self.args = args
        self.host = host
        self.port = port
        self.https_port = port + 1
        self.trace = []
        self.rest_params = []
        self.queries = []
        self.stop = False
        self.server = MockHTTPServer((host, self.port), RequestHandler, self)
        self.https_server = MockHTTPSServer((host, self.https_port), RequestHandler, self)
        self.t1 = threading.Thread(target=self._run_http)
        self.t2 = threading.Thread(target=self._run_https)

    def set_args(self, args):
        self.args = args

    def host_port(self):
        return f'{self.host}:{self.port!s}'

    def url(self, https=False):
        return f"{'https://' if https else 'http://'}{self.host}:{self.https_port if https else self.port!s}"

    def _run(self, server):
        while not self.stop:
            try:
                server.handle_request()
            except BaseException:
                self.stop = True

    def _run_http(self):
        self._run(self.server)

    def _run_https(self):
        self._run(self.https_server)

    def run(self):
        self.stop = False
        self.t1.start()
        self.t2.start()

    def shutdown(self):
        self.stop = True
        self._close(self.url(), self.server, self.t1)
        self._close(self.url(https=True), self.https_server, self.t2)

    def _close(self, url, server, t):
        try:
            requests.get(f'{url}/close', timeout=0.2, verify=False)
        except Exception:
            pass

        server.server_close()
        t.join()


# ------------ Below functions that mock a superficial level of the couchbase server


def get_pools(rest_params=None, server_args=None, path="", endpoint_match=None):
    is_admin = True
    enterprise = True
    version = '0.0.0-0000-enterprise'
    init = True
    if 'init' in server_args:
        init = server_args['init']
    if 'is_admin' in server_args:
        is_admin = server_args['is_admin']
    if 'enterprise' in server_args:
        enterprise = server_args['enterprise']

    response_init = {'uuid': '5f8b140987f4e46c950e3fa82e7fcb48', 'settings':
                     {'viewUpdateDaemon': '/settings/viewUpdateDaemon?uuid=5f8b140987f4e46c950e3fa82e7fcb48',
                      'maxParallelIndexers': '/settings/maxParallelIndexers?uuid=5f8b140987f4e46c950e3fa82e7fcb48'},
                     'pools': [{'streamingUri': '/poolsStreaming/default?uuid=5f8b140987f4e46c950e3fa82e7fcb48',
                                'name': 'default', 'uri': '/pools/default?uuid=5f8b140987f4e46c950e3fa82e7fcb48'}],
                     'isEnterprise': enterprise,
                     'componentsVersion': {'kernel': '2.16.4', 'ale': version, 'ssl': '5.3.3',
                                           'os_mon': '2.2.14', 'stdlib': '1.19.4', 'inets': '5.9.8',
                                           'public_key': '0.21', 'ns_server': version, 'crypto': '3.2', 'asn1': '2.0.4',
                                           'lhttpc': '1.3.0', 'sasl': '2.3.4'}, 'implementationVersion': version,
                     'isAdminCreds': is_admin, 'isROAdminCreds': False}

    response_no_init = {'uuid': [], 'settings': [], 'pools': [], 'isEnterprise': enterprise,
                        'componentsVersion': {'kernel': '5.4.3.2', 'ale': version, 'ssl': '8.2.6.2', 'os_mon': '2.4.4',
                                              'stdlib': '3.4.5', 'inets': '6.5.2.4', 'public_key': '1.5.2',
                                              'ns_server': version, 'crypto': '4.2.2.2', 'asn1': '5.0.5.1',
                                              'lhttpc': '1.3.0', 'sasl': '3.1.2'}, 'implementationVersion': version,
                        'isAdminCreds': is_admin, 'isROAdminCreds': False}

    if init:
        return 200, response_init

    return 200, response_no_init


def do_nothing(rest_params=None, server_args=None, path="", endpoint_match=None):
    return 200, None


def set_analytics_link(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'fail' in server_args and server_args['fail']:
        return 409, "CBAS0054: Operation cannot be performed while the link is connected"
    return 200, None


def get_buckets(rest_params=None, server_args=None, path="", endpointMatch=None):
    if server_args is not None and 'buckets' in server_args:
        return 200, server_args['buckets']

    return 200, []


def get_ddocs(rest_params=None, server_args=None, path="", endpoint_match=None):
    return 200, {'rows': []}


def delete_bucket(rest_params=None, server_args=None, path="", endpoint_match=None):
    bucket = path[path.rfind('/') + 1:]
    if 'buckets' not in server_args:
        return 404, ['Bucket not found']

    for b in server_args['buckets']:
        if b['name'] == bucket:
            server_args['delete-bucket'] = bucket
            return 200, []

        return 404, ['Bucket not found']


def start_log_collection(rest_params=None, server_args=None, path="", endpoint_match=None):
    server_args['log-collection-started'] = True
    return 200, None


def get_tasks(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'tasks' in server_args:
        return 200, server_args['tasks']

    return 200, []


def get_default_pool(rest_params=None, server_args=None, path="", endpoint_match=None):
    response = {}
    if 'pools_default' in server_args:
        response = server_args['pools_default']

    return 200, response


def get_server_groups(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'server-group' in server_args:
        return 200, server_args['server-group']
    return 200, []


def server_group_action(rest_params=None, server_args=None, path="", endpoint_match=None):
    return 200, None


def get_indexes_settings(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'indexes-settings' in server_args:
        return 200, server_args['indexes-settings']

    return 200, {}


def get_node_info(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'node-info' in server_args:
        return 200, server_args['node-info']
    return 200, {}


def get_password_policy(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'password-policy' in server_args:
        return 200, server_args['password-policy']
    return 200, {}


def get_remote_cluster(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'remote-clusters' in server_args:
        return 200, server_args['remote-clusters']
    return 200, []


def get_rbac_user(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'rbac-users' in server_args:
        return 200, server_args['rbac-users']
    return 200, []


def get_my_roles(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'whoami' in server_args:
        return 200, server_args['whoami']
    return 200, []


def get_collection_manifest(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'collection_manifest' in server_args:
        return 200, server_args['collection_manifest']
    return 200, []


def get_groups(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'rbac-groups' in server_args:
        return 200, server_args['rbac-groups']
    return 200, {}


def get_user_groups(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'user-group' in server_args:
        return 200, server_args['user-group']
    return 200, {}


def get_user(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'rbac-users' in server_args:
        res = re.search(r'/settings/rbac/users/([^/]+)/([^/]+)$', path)
        if res is None:
            return 404, 'Unknown user.'
        domain = res.groups()[0]
        name = res.groups()[1]
        for u in server_args['rbac-users']:
            if u['id'] == name and u['domain'] == domain:
                return 200, u
        return 404, 'Unknown user.'
    return 200, {}


def get_group(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'rbac-groups' in server_args:
        res = re.search(r'/settings/rbac/groups/([^/]+)$', path)
        if res is None:
            return 404, 'Unknown group.'
        group = res.groups()[0]
        for g in server_args['rbac-groups']:
            if g['id'] == group:
                return 200, g
        return 404, 'Unknown group.'
    return 200, {}


def get_ldap_settings(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'ldap' in server_args:
        return 200, server_args['ldap']
    return 200, {}


def get_by_path(rest_params=None, server_args=None, path="", endpoint_match=None):
    status = 200
    if 'override-status' in server_args:
        status = int(server_args['override-status'])

    if path in server_args:
        if endpoint_match:
            server_args['query'] = endpoint_match
        return status, server_args[path]
    return status, {}


def get_audit_settings(rest_params=None, server_args=None, path="", endpoint_match=None):
    if 'audit_settings' in server_args:
        return 200, server_args['audit_settings']
    return 200, {}


endpoints = [
    (r'/close$', {'GET': do_nothing}),
    (r'/whoami', {'GET': get_my_roles}),
    (r'/pools$', {'GET': get_pools}),
    (r'/pools/default$', {'POST': do_nothing, 'GET': get_default_pool}),
    (r'/pools/default/certificates', {'GET': get_by_path}),
    (r'/pools/default/certificate/node/[\w\d.:]+$', {'GET': get_by_path}),
    (r'/pools/default/remoteClusters(/)?$', {'POST': do_nothing, 'GET': get_remote_cluster}),
    (r'/pools/default/remoteClusters/\w+$', {'DELETE': do_nothing, 'POST': do_nothing}),
    (r'/pools/default/tasks$', {'GET': get_tasks}),
    (r'/pools/default/trustedCAs', {'GET': get_by_path}),
    (r'/pools/default/nodeServices$', {'GET': get_by_path}),
    (r'/pools/default/serverGroups$', {'POST': do_nothing, 'GET': get_server_groups}),
    (r'/pools/default/serverGroups/\d+$', {'DELETE': do_nothing, 'PUT': do_nothing}),
    (r'/pools/default/serverGroups/\d+/addNode$', {'POST': do_nothing}),
    (r'/pools/default/serverGroups/rev=\d+$', {'PUT': do_nothing}),
    (r'/pools/default/buckets$', {'GET': get_buckets, 'POST': do_nothing}),
    (r'/pools/default/buckets/\w+$', {'DELETE': delete_bucket, 'POST': do_nothing}),
    (r'/pools/default/buckets/\w+/controller/doFlush$', {'POST': do_nothing}),
    (r'/pools/default/buckets/\w+/controller/compactDatabases$', {'POST': do_nothing}),
    (r'/pools/default/buckets/\w+/ddocs$', {'GET': get_ddocs}),
    (r'/pools/default/buckets/\w+/controller/compactBucket$', {'POST': do_nothing}),
    (r'/pools/default/buckets/\w+/scopes$', {'GET': get_collection_manifest, 'POST': do_nothing}),
    (r'/pools/default/buckets/\w+/scopes/\w+$', {'DELETE': do_nothing}),
    (r'/pools/default/buckets/\w+/scopes/\w+/collections$', {'POST': do_nothing}),
    (r'/pools/default/buckets/\w+/scopes/\w+/collections/\w+$', {'DELETE': do_nothing}),
    (r'/pools/default/trustedCAs/\d+$', {'DELETE': do_nothing}),
    (r'/pools/nodes', {'GET': get_by_path}),
    (r'/settings/indexes$', {'POST': do_nothing, 'GET': get_indexes_settings}),
    (r'/settings/passwordPolicy$', {'POST': do_nothing, 'GET': get_password_policy}),
    (r'/settings/querySettings$', {'POST': do_nothing}),
    (r'/settings/querySettings/curlWhitelist$', {'POST': do_nothing}),
    (r'/settings/rbac/users$', {'POST': do_nothing, 'GET': get_rbac_user}),
    (r'/settings/rbac/users/\w+$', {'PUT': do_nothing, 'GET': get_user_groups}),
    (r'/settings/rbac/groups/(\w|-)+', {'PUT': do_nothing, 'DELETE': do_nothing, 'GET': get_group}),
    (r'/settings/rbac/groups', {'GET': get_groups}),
    (r'/settings/rbac/users/\w+/\w+$', {'DELETE': do_nothing, 'PUT': do_nothing, 'GET': get_user}),
    (r'/settings/saslauthdAuth$', {'POST': do_nothing}),
    (r'/settings/ldap', {'POST': do_nothing, 'GET': get_ldap_settings}),
    (r'/settings/alerts$', {'POST': do_nothing}),
    (r'/settings/security$', {'POST': do_nothing, 'GET': get_by_path}),
    (r'/settings/audit$', {'POST': do_nothing, 'GET': get_audit_settings}),
    (r'/settings/audit/descriptors$', {'GET': get_by_path}),
    (r'/settings/stats$', {'POST': do_nothing}),
    (r'/settings/license$', {'POST': do_nothing, 'GET': get_by_path}),
    (r'/settings/license/validate$', {'POST': do_nothing, 'GET': get_by_path}),
    (r'/settings/web$', {'POST': do_nothing}),
    (r'/settings/autoFailover', {'POST': do_nothing}),
    (r'/settings/autoReprovision', {'POST': do_nothing}),
    (r'/settings/clientCertAuth', {'GET': get_by_path, 'POST': do_nothing}),
    (r'/settings/replications$', {'POST': do_nothing}),
    (r'/settings/rebalance$', {'GET': get_by_path, 'POST': do_nothing}),
    (r'/settings/retryRebalance$', {'GET': get_by_path, 'POST': do_nothing}),
    (r'/settings/replications/(\d|\w)+$', {'POST': do_nothing, 'GET': get_by_path}),
    (r'/node/controller/setupServices$', {'POST': do_nothing}),
    (r'/nodes/self/controller/settings$', {'POST': do_nothing}),
    (r'/nodes/self$', {'GET': get_node_info}),
    (r'/nodeInit', {'POST': do_nothing}),
    (r'/node/controller/enableExternalListener', {'POST': do_nothing}),
    (r'/node/controller/disableUnusedExternalListeners', {'POST': do_nothing}),
    (r'/node/controller/loadTrustedCAs', {'POST': do_nothing}),
    (r'/node/controller/reloadCertificate', {'POST': do_nothing}),
    (r'/node/controller/setupNetConfig', {'POST': do_nothing}),
    (r'/controller/failOver$', {'POST': do_nothing}),
    (r'/controller/rebalance$', {'POST': do_nothing}),
    (r'/controller/changePassword', {'POST': do_nothing}),
    (r'/controller/reAddNode$', {'POST': do_nothing}),
    (r'/controller/startGracefulFailover$', {'POST': do_nothing}),
    (r'/controller/cancelLogsCollection$', {'POST': do_nothing}),
    (r'/controller/createReplication$', {'POST': do_nothing}),
    (r'/controller/cancelXDCR/(\d|\w)+$', {'DELETE': do_nothing}),
    (r'/controller/setAutoCompaction$', {'POST': do_nothing}),
    (r'/controller/startLogsCollection$', {'POST': start_log_collection}),
    (r'/controller/uploadClusterCA', {'POST': do_nothing}),
    (r'/controller/regenerateCertificate', {'POST': get_by_path}),


    # index api
    (r'/getIndexMetadata$', {'GET': get_by_path}),
    (r'/api/index', {'GET': get_by_path}),

    # analytics api
    (r'/analytics/link',
     {'GET': get_by_path,
      'POST': set_analytics_link,
      'PUT': set_analytics_link,
      'DELETE': do_nothing}),

    # eventing api
    (r'/api/v1/functions/(.*)/settings', {'POST': do_nothing}),

    # backup server API
    (r'/api/v1/config', {'GET': get_by_path, 'PATCH': do_nothing}),
    (r'/api/v1/cluster/self/repository/(:?active|archived|imported)$', {'GET': get_by_path}),
    (r'/api/v1/cluster/self/repository/active/\w+/archive$', {'POST': do_nothing}),
    (r'/api/v1/plan$', {'GET': get_by_path}),
    (r'/api/v1/plan/\w+$', {'GET': get_by_path, 'DELETE': do_nothing, 'POST': do_nothing}),
    (r'/api/v1/cluster/self/repository/(:?active|archived|imported)/\w+(:?\?remove_repository=\w+)?$',
     {'GET': get_by_path, 'POST': do_nothing, 'DELETE': do_nothing})
]
