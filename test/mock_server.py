"""Mock server only emulates CB rest endpoints but has no functionality"""
import socket
import threading
import requests
import re
import json
import sys
from urllib.parse import urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append('GET:'+parsed.path)
        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'GET' in fns:
                return self.handle_fn(fns['GET'], parsed.path)

        self.not_found()

    def do_POST(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append('POST:' + parsed.path)
        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'POST' in fns:
                return self.handle_fn(fns['POST'], parsed.path)

        self.not_found()

    def do_PUT(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append('PUT:' + parsed.path)
        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'PUT' in fns:
                return self.handle_fn(fns['PUT'], parsed.path)

        self.not_found()

    def  do_DELETE(self):
        parsed = urlparse(self.path)
        self.server.rest_server.trace.append('DELETE:' + parsed.path)
        for (endpoint, fns) in endpoints:
            if re.search(endpoint, parsed.path) is not None and 'DELETE' in fns:
                return self.handle_fn(fns['DELETE'], parsed.path)
        self.not_found()

    def not_found(self):
        self.send_response(404)
        self.finish()

    def handle_fn(self, fn, path):
        content_len = int(self.headers.get('content-length', 0))
        post_body = self.rfile.read(content_len).decode('utf-8')
        post_body = post_body.split('&')
        for e in post_body:
            if e == "":
                continue
            self.server.rest_server.rest_params.append(e)
        code, response = fn(post_body, self.server.rest_server.args, path)
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


class MockRESTServer(object):
    def __init__(self, host, port, args={}):
        self.args = args
        self.host = host
        self.port = port
        self.trace = []
        self.rest_params = []
        self.stop = False
        self.server = MockHTTPServer((host, port), RequestHandler, self)
        self.t1 = threading.Thread(target=self._run)

    def set_args(self, args):
        self.args = args

    def host_port(self):
        return self.host + ":" + str(self.port)

    def url(self):
        return "http://" + self.host_port()

    def _run(self):
        while not self.stop:
            try:
                self.server.handle_request()
            except:
                self.stop = True

    def run(self):
        self.stop = False
        self.t1.start()

    def shutdown(self):
        self.stop = True
        try:
            requests.get(self.url() + "/close", timeout=0.2)
        except Exception:
            pass

        try:
            self.server.server_close()
            self.t1.join()
        except Exception:
            pass

# ------------ Below functions that mock a superficial level of the couchbase server


def get_pools(rest_params=None, server_args=None, path="", endpointMatch=None):
    is_admin = True
    enterprise = True,
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
                     'isAdminCreds': is_admin, 'isIPv6': False, 'isROAdminCreds': False}

    response_no_init = {'uuid': [], 'settings': [], 'pools': [], 'isEnterprise': enterprise,
                        'componentsVersion': {'kernel': '5.4.3.2', 'ale': version, 'ssl': '8.2.6.2', 'os_mon': '2.4.4',
                                              'stdlib': '3.4.5', 'inets': '6.5.2.4', 'public_key': '1.5.2',
                                              'ns_server': version, 'crypto': '4.2.2.2', 'asn1': '5.0.5.1',
                                              'lhttpc': '1.3.0', 'sasl': '3.1.2'}, 'implementationVersion': version,
                        'isAdminCreds': is_admin, 'isIPv6': False, 'isROAdminCreds': False}

    if init:
        return 200, response_init

    return 200, response_no_init


def do_nothing(rest_params=None, server_args=None, path="", endpointMatch=None):
    return 200, None


def get_buckets(rest_params=None, server_args=None, path="", endpointMatch=None):
    if server_args is not None and 'buckets' in server_args:
        return 200, server_args['buckets']

    return 200, []


def get_ddocs(rest_params=None, server_args=None, path="", endpointMatch=None):
    return 200, {'rows': []}


def delete_bucket(rest_params=None, server_args=None, path="", endpointMatch=None):
    bucket = path[path.rfind('/')+1:]
    if 'buckets' not in server_args:
        return 404, ['Bucket not found']

    for b in server_args['buckets']:
        if b['name'] == bucket:
            server_args['delete-bucket'] = bucket
            return 200, []

        return 404, ['Bucket not found']


def start_log_collection(rest_params=None, server_args=None, path="", endpointMatch=None):
    server_args['log-collection-started'] = True
    return 200, None


def get_tasks(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'tasks' in server_args:
        return 200, server_args['tasks']

    return 200, []


def get_default_pool(rest_params=None, server_args=None, path="", endpointMatch=None):
    response = {}
    if 'pools_default' in server_args:
        response = server_args['pools_default']

    return 200, response


def get_server_groups(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'server-group' in server_args:
        return 200, server_args['server-group']
    return 200, []


def server_group_action(rest_params=None, server_args=None, path="", endpointMatch=None):
    return 200, None


def get_indexes_settings(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'indexes-settings' in server_args:
        return 200, server_args['indexes-settings']

    return 200, {}


def get_node_info(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'node-info' in server_args:
        return 200, server_args['node-info']
    return 200, {}


def get_password_policy(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'password-policy' in server_args:
        return 200, server_args['password-policy']
    return 200, {}


def get_remote_cluster(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'remote-clusters' in server_args:
        return 200, server_args['remote-clusters']
    return 200, []


def get_rbac_user(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'rbac-users' in server_args:
        return 200, server_args['rbac-users']
    return 200, []


def get_my_roles(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'whoami' in server_args:
        return 200, server_args['whoami']
    return 200, []

def get_collection_manifest(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'collection_manifest'in server_args:
        return 200, server_args['collection_manifest']
    return 200, []

def get_group(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'group' in server_args:
        return 200, server_args['group']
    return 200, {}


def get_user_groups(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'user-group' in server_args:
        return 200, server_args['user-group']
    return 200, {}

def get_ldap_settings(rest_params=None, server_args=None, path="", endpointMatch=None):
    if 'ldap' in server_args:
        return 200, server_args['ldap']
    return 200, {}

endpoints = [
    ('/close$', {'GET': do_nothing}),
    ('/whoami', {'GET': get_my_roles}),
    ('/pools$', {'GET': get_pools}),
    ('/pools/default$', {'POST': do_nothing, 'GET': get_default_pool}),
    ('/pools/default/remoteClusters(/)?$', {'POST': do_nothing, 'GET': get_remote_cluster}),
    ('/pools/default/remoteClusters/\w+$', {'DELETE': do_nothing, 'POST': do_nothing}),
    ('/pools/default/tasks$', {'GET': get_tasks}),
    ('/pools/default/serverGroups$', {'POST': do_nothing, 'GET': get_server_groups}),
    ('/pools/default/serverGroups/\d+$', {'DELETE': do_nothing, 'PUT': do_nothing}),
    ('/pools/default/serverGroups/\d+/addNode$', {'POST': do_nothing}),
    ('/pools/default/serverGroups/rev=\d+$', {'PUT': do_nothing}),
    ('/pools/default/buckets$', {'GET': get_buckets, 'POST': do_nothing}),
    ('/pools/default/buckets/\w+$', {'DELETE': delete_bucket, 'POST': do_nothing}),
    ('/pools/default/buckets/\w+/controller/doFlush$', {'POST': do_nothing}),
    ('/pools/default/buckets/\w+/controller/compactDatabases$', {'POST': do_nothing}),
    ('/pools/default/buckets/\w+/ddocs$', {'GET': get_ddocs}),
    ('/pools/default/buckets/\w+/controller/compactBucket$', {'POST': do_nothing}),
    ('/pools/default/buckets/\w+/collections$', {'GET': get_collection_manifest, 'POST': do_nothing}),
    ('/pools/default/buckets/\w+/collections/\w+$', {'POST': do_nothing, 'DELETE': do_nothing}),
    ('/pools/default/buckets/\w+/collections/\w+/\w+$', {'DELETE': do_nothing}),
    ('/settings/indexes$', {'POST': do_nothing, 'GET': get_indexes_settings}),
    ('/settings/passwordPolicy$', {'POST': do_nothing, 'GET': get_password_policy}),
    ('/settings/rbac/users$', {'POST': do_nothing, 'GET': get_rbac_user}),
    ('/settings/rbac/users/\w+$', {'PUT': do_nothing, 'GET': get_user_groups}),
    ('/settings/rbac/groups/(\w|-)+', {'PUT': do_nothing, 'DELETE': do_nothing, 'GET': get_group}),
    ('/settings/rbac/groups', {'GET': get_group}),
    ('/settings/rbac/users/\w+/\w+$', {'DELETE': do_nothing, 'PUT': do_nothing}),
    ('/settings/saslauthdAuth$', {'POST': do_nothing}),
    ('/settings/ldap', {'POST': do_nothing, 'GET': get_ldap_settings}),
    ('/settings/alerts$', {'POST': do_nothing}),
    ('/settings/security$', {'POST': do_nothing}),
    ('/settings/audit$', {'POST': do_nothing}),
    ('/settings/stats$', {'POST': do_nothing}),
    ('/settings/web$', {'POST': do_nothing}),
    ('/settings/autoFailover', {'POST': do_nothing}),
    ('/settings/autoReprovision', {'POST': do_nothing}),
    ('/settings/replications$', {'POST': do_nothing}),
    ('/settings/replications/(\d|\w)+$', {'POST': do_nothing}),
    ('/node/controller/setupServices$', {'POST': do_nothing}),
    ('/nodes/self/controller/settings$', {'POST': do_nothing}),
    ('/nodes/self$', {'GET': get_node_info}),
    ('/node/controller/rename', {'POST': do_nothing}),
    ('/controller/failOver$', {'POST': do_nothing}),
    ('/controller/rebalance$', {'POST': do_nothing}),
    ('/controller/changePassword', {'POST': do_nothing}),
    ('/controller/reAddNode$', {'POST': do_nothing}),
    ('/controller/startGracefulFailover$', {'POST': do_nothing}),
    ('/controller/cancelLogsCollection$', {'POST': do_nothing}),
    ('/controller/createReplication$', {'POST': do_nothing}),
    ('/controller/cancelXDCR/(\d|\w)+$', {'DELETE': do_nothing}),
    ('/controller/setAutoCompaction$', {'POST': do_nothing}),
    ('/controller/startLogsCollection', {'POST': start_log_collection})
]
