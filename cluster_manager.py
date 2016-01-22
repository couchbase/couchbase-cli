"""Management API's for Couchbase Cluster"""

import requests

N1QL_SERVICE = 'n1ql'
INDEX_SERVICE = 'index'
MGMT_SERVICE = 'mgmt'
FTS_SERVICE = 'fts'

class ServiceNotAvailableException(Exception):
    """An exception taised when a service does not exist in the target cluster"""

    def __init__(self, service):
        Exception.__init__(self, "Service %s not available in target cluster" % service)

class ClusterManager(object):
    """A set of REST API's for managing a Couchbase cluster"""

    def __init__(self, hostname, username, password, ssl=False):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.ssl = ssl

    def n1ql_query(self, stmt, args=None):
        """Sends a N1QL query

        Sends a N1QL query and returns the result of the query. Raises a
        ServiceNotAvailable exception if the target cluster is no running the n1ql
        service."""

        hosts, errors = self.get_hostnames_for_service(N1QL_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(N1QL_SERVICE)

        url = hosts[0] + '/query/service'
        body = {'statement': str(stmt)}

        if args:
            body['args'] = str(args)

        result, errors = self._post_form_encoded(url, body)
        if errors:
            return None, errors

        return result, None

    def get_hostnames_for_service(self, service_name):
        """ Gets all hostnames that run a service

        Gets all hostnames for specified service and returns a list of strings
        in the form "http://hostname:port". If the ClusterManager is configured
        to use SSL/TLS then "https://" is prefixed to each name instead of
        "http://"."""
        url = self.hostname + '/pools/default/nodeServices'
        data, errors = self._get(url)
        if errors:
            return None, errors

        hosts = []
        for node in data['nodesExt']:
            node_host = '127.0.0.1'
            if 'hostname' in node:
                node_host = node['hostname']

            http_prefix = 'http://'
            fts_port_name = 'fts'
            n1ql_port_name = 'n1ql'
            mgmt_port_name = 'mgmt'
            index_port_name = 'indexHttp'

            if self.ssl:
                http_prefix = 'https://'
                n1ql_port_name = 'n1qlSSL'
                mgmt_port_name = 'mgmtSSL'
                # The is no ssl port for the index or fts services

            if service_name == MGMT_SERVICE and mgmt_port_name in node['services']:
                hosts.append(http_prefix + node_host + ':' + str(node['services'][mgmt_port_name]))

            if service_name == N1QL_SERVICE and n1ql_port_name in node['services']:
                hosts.append(http_prefix + node_host + ':' + str(node['services'][n1ql_port_name]))

            if service_name == INDEX_SERVICE and index_port_name in node['services']:
                hosts.append(http_prefix + node_host + ':' + str(node['services'][index_port_name]))

            if service_name == FTS_SERVICE and fts_port_name in node['services']:
                hosts.append(http_prefix + node_host + ':' + str(node['services'][fts_port_name]))

        return hosts, None

    def _get(self, url):
        response = requests.get(url, auth=(self.username, self.password))
        return _handle_response(response)

    def _post_form_encoded(self, url, params):
        response = requests.post(url, auth=(self.username, self.password), data=params)
        return _handle_response(response)

def _handle_response(response):
    if response.status_code == 200:
        if 'application/json' in response.headers['Content-Type']:
            return response.json(), None
        else:
            return response.text, None
    elif response.status_code in [400, 404]:
        return None, [response.text]
    elif response.status_code == 401:
        return None, ['ERROR: unable to access the REST API - please check your username' +
                      '(-u) and password (-p)']
    else:
        return None, ['Error: Recieved unexpectd status %d' % response.status_code]
