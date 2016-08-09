"""Management API's for Couchbase Cluster"""

import requests
import csv
import StringIO
import time

MAX_LEN_PASSWORD = 24

N1QL_SERVICE = 'n1ql'
INDEX_SERVICE = 'index'
MGMT_SERVICE = 'mgmt'
FTS_SERVICE = 'fts'

ERR_AUTH = 'unable to access the REST API - please check your username (-u) and password (-p)'
ERR_INTERNAL = 'Internal server error, please retry your request'

DEFAULT_REQUEST_TIMEOUT = 60

# Remove this once we can verify SSL certificates
requests.packages.urllib3.disable_warnings()

def request(f):
    def g(*args, **kwargs):
        cm = args[0]
        url = args[1]
        try:
            return f(*args, **kwargs)
        except requests.exceptions.ConnectionError, e:
            return None, ['Unable to connect to host at %s' % cm.hostname]
        except requests.exceptions.ReadTimeout, e:
            return None, ['Request to host `%s` timed out after %d seconds' % (url, cm.timeout)]
    return g


class ServiceNotAvailableException(Exception):
    """An exception raised when a service does not exist in the target cluster"""

    def __init__(self, service):
        Exception.__init__(self, "Service %s not available in target cluster" % service)

class ClusterManager(object):
    """A set of REST API's for managing a Couchbase cluster"""

    def __init__(self, host, port, username, password, ssl=False, timeout=DEFAULT_REQUEST_TIMEOUT):
        if ssl:
            self.hostname = 'https://%s:%s' % (host, str(port))
        else:
            self.hostname = 'http://%s:%s' % (host, str(port))

        self.username = username
        self.password = password
        self.timeout = timeout
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

    def is_cluster_initialized(self):
        data, errors = self.pools()
        if (errors and len(errors) == 1 and errors[0] == ERR_AUTH) or \
            (data and data['pools'] and len(data['pools']) > 0):
            return True, None
        return False, errors

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

    def pools(self, pool=None):
        """ Retrieves information about Couchbase management pools

        Returns Couchbase pools data"""
        url = self.hostname + '/pools'
        if pool:
            url += '/' + pool
        return self._get(url)

    def set_pools_default(self, data_ramsize, index_ramsize, fts_ramsize, cluster_name):
        """ Sets Couchbase RAM Quotas for various services

        Options:
        data_ramsize - An integer denoting the size in MB, None skips the parameter
        index_ramsize - An integer denoting the size in MB, None skips the parameter
        fts_ramsize - An integer denoting the size in MB, None skips the parameter
        cluster_name - Sets a name for the cluster, None skips the parameter
        """
        url = self.hostname + '/pools/default'
        params = {}
        if data_ramsize:
            params["memoryQuota"] = data_ramsize
        if index_ramsize:
            params["indexMemoryQuota"] = index_ramsize
        if fts_ramsize:
            params["ftsMemoryQuota"] = fts_ramsize
        if cluster_name is not None:
            params["clusterName"] = cluster_name

        return self._post_form_encoded(url, params)

    def setup_services(self, services):
        """ Sets the services on a node

        Options:
        services - A string containing a comma separated list of services
        """
        url = self.hostname + '/node/controller/setupServices'
        params = { "services": services }

        return self._post_form_encoded(url, params)

    def set_admin_credentials(self, username, password, port):
        """Sets the admin credentials and port for a cluster

        Options:
        username - The username for the cluster
        password - The password for the cluster
        port - The port number for the admin console to listen on. If set to
               None then the port is kept the same as it currently is.
        """
        url = self.hostname + '/settings/web'
        params = {}

        if username:
            params["username"] = username
        if password:
            if len(password) > MAX_LEN_PASSWORD:
                return None, ["Password length %s exceeds maximum length of %s characters" \
                    % (len(password), MAX_LEN_PASSWORD)]
            params["password"] = password
        if port:
            params["port"] = port
        else:
            params["port"] = "SAME"

        return self._post_form_encoded(url, params)

    def enable_notifications(self, enable):
        url = self.hostname + '/settings/stats'
        params = { "sendStats": "false"}

        if enable:
            params["sendStats"] = "true"

        return self._post_form_encoded(url, params)

    def get_server_groups(self):
        url = self.hostname + '/pools/default/serverGroups'
        return self._get(url)

    def get_server_group(self, groupName):
        groups, errors = self.get_server_groups()
        if errors:
            return None, error

        if not groups or not groups["groups"] or groups["groups"] == 0:
            return None, ["No server groups found"]

        if groupName:
            for group in groups["groups"]:
                if group["name"] == groupName:
                    return group, None
            return None, ["Group `%s` not found" % groupName]
        else:
            return groups["groups"][0], None

    def add_server(self, add_server, groupName, username, password, services):
        group, errors = self.get_server_group(groupName)
        if errors:
            return None, errors

        url = self.hostname + group["addNodeURI"]
        params = { "hostname": add_server,
                   "user": username,
                   "password": password,
                   "services": services }

        return self._post_form_encoded(url, params)

    def create_bucket(self, name, password, bucket_type, memory_quota,
                      eviction_policy, replicas, replica_indexes,
                      threads_number, flush_enabled, sync, timeout=60):
        url = self.hostname + '/pools/default/buckets'

        if name is None:
            return None ["The bucket name is required when creating a bucket"]
        if password is None:
            return None ["The bucket password is required when creating a bucket"]
        if bucket_type is None:
            return None ["The bucket type is required when creating a bucket"]
        if memory_quota is None:
            return None ["The bucket memory quota is required when creating a bucket"]

        params = { "name": name,
                   "authType": "sasl",
                   "bucketType": bucket_type,
                   "saslPassword": password,
                   "ramQuotaMB": memory_quota }

        if eviction_policy is not None:
            params["evictionPolicy"] = eviction_policy
        if replicas is not None:
            params["replicaNumber"] = replicas
        if replica_indexes is not None:
            params["replicaIndex"] = replica_indexes
        if threads_number is not None:
            params["threadsNumber"] = threads_number
        if flush_enabled is not None:
            params["flushEnabled"] = flush_enabled

        result, errors = self._post_form_encoded(url, params)
        if errors:
            return None, errors

        if sync:
            all_node_ready = False
            start = time.time()
            while (time.time() - start) <= timeout and not all_node_ready:
                buckets, errors = self.list_buckets()
                if name not in buckets:
                    time.sleep(1)
                    continue

                url = self.hostname + '/pools/default/buckets/' + name
                content, errors = self._get(url)
                if errors:
                    return None, errors

                all_node_ready = True
                for node in content["nodes"]:
                    if node["status"] != "healthy":
                        all_node_ready = False
                        break
                if not all_node_ready:
                    time.sleep(1)

            if not all_node_ready:
                return None, ["Bucket created, but not ready after %d seconds" % timeout]

        return result, None


    def edit_bucket(self, name, password, memory_quota, eviction_policy,
                    replicas, threads_number, flush_enabled):
        url = self.hostname + '/pools/default/buckets/' + name

        if name is None:
            return None ["The bucket name is required when editing a bucket"]

        params = {}
        if password is not None:
            params["saslPassword"] = password
        if memory_quota is not None:
            params["ramQuotaMB"] = memory_quota
        if eviction_policy is not None:
            params["evictionPolicy"] = eviction_policy
        if replicas is not None:
            params["replicaNumber"] = replicas
        if threads_number is not None:
            params["threadsNumber"] = threads_number
        if flush_enabled is not None:
            params["flushEnabled"] = flush_enabled

        return self._post_form_encoded(url, params)

    def delete_bucket(self, name):
        url = self.hostname + '/pools/default/buckets/' + name
        return self._delete(url)

    def flush_bucket(self, name):
        if name is None:
            return None ["The bucket name is required when flushing a bucket"]

        url = self.hostname + '/pools/default/buckets/' + name + '/controller/doFlush'
        return self._post_form_encoded(url, None)

    def compact_bucket(self, name, view_only, data_only):
        if data_only and not view_only:
            url = self.hostname + '/pools/default/buckets/' + name + \
                '/controller/compactDatabases'
            return self._post_form_encoded(url, None)
        elif view_only and not data_only:
            url = self.hostname + '/pools/default/buckets/' + name + '/ddocs'
            ddocs, errors = self._get(url)
            if errors:
                return None, errors

            for row in ddocs["rows"]:
                url = self.hostname + row["controllers"]["compact"]
                _, errors = self._post_form_encoded(url, None)
                if errors:
                    return None, errors
            return None, None
        elif not data_only and not view_only:
            url = self.hostname + '/pools/default/buckets/' + name + \
                '/controller/compactBucket'
            return self._post_form_encoded(url, None)
        else:
            return None, ["Cannot compact data only and view only, pick one or neither"]

    def list_buckets(self, extended=False):
        url = self.hostname + '/pools/default/buckets'
        result, errors = self._get(url)
        if errors:
            return None, errors

        if extended:
            return result, errors

        names = list()
        for bucket in result:
            names.append(bucket["name"])

        return names, None

    def get_bucket(self, name):
        url = self.hostname + '/pools/default/buckets'
        result, errors = self._get(url)
        if errors:
            return None, errors

        for bucket in result:
            if bucket["name"] == name:
                return bucket, None

        return None, ["Bucket not found"]

    def stop_rebalance(self):
        url = self.hostname + '/controller/stopRebalance'
        return self._post_form_encoded(url, dict())

    def set_audit_settings(self, enabled, log_path, rotate_interval):
        url = self.hostname + '/settings/audit'

        params, errors = self._get(url)
        if errors:
            return None, errors

        if enabled:
            params["auditdEnabled"] = enabled
        if log_path:
            params["logPath"] = log_path
        if rotate_interval:
            params["rotateInterval"] = rotate_interval

        if "logPath" not in params and params["auditdEnabled"] == "true":
            return None, ["The audit log path must be specified when auditing is first set up"]

        return self._post_form_encoded(url, params)

    def set_autofailover_settings(self, enabled, timeout):
        url = self.hostname + '/settings/autoFailover'

        if timeout and timeout < 30:
            return None, ["Timeout value must be at least 30 seconds"]

        params = dict()
        if enabled:
            params["enabled"] = enabled
        if timeout:
            params["timeout"] = timeout

        return self._post_form_encoded(url, params)

    def set_index_settings(self, storageMode, maxRollbackPoints, stableSnapInterval,
                           memSnapInterval, threads, logLevel):
        """ Sets global index settings"""
        params = dict()
        if storageMode:
            params["storageMode"] = storageMode
        if maxRollbackPoints:
            params["maxRollbackPoints"] = maxRollbackPoints
        if stableSnapInterval:
            params["stableSnapshotInterval"] = stableSnapInterval
        if memSnapInterval:
            params["memorySnapshotInterval"] = memSnapInterval
        if threads:
            params["indexerThreads"] = threads
        if logLevel:
            params["logLevel"] = logLevel

        url = self.hostname + '/settings/indexes'
        return self._post_form_encoded(url, params)

    def index_settings(self):
        """ Retrieves the index settings

            Returns a map of all global index settings"""
        url = self.hostname + '/settings/indexes'
        return self._get(url)

    def setRoles(self,userList,roleList,userNameList):
        # we take a comma-delimited list of roles that needs to go into a dictionary
        paramDict = {"roles" : roleList}
        userIds = []
        userNames = []
        userF = StringIO.StringIO(userList)
        for idList in csv.reader(userF, delimiter=','):
            userIds.extend(idList)

        # did they specify user names?
        if userNameList != None:
            userNameF = StringIO.StringIO(userNameList)
            for nameList in csv.reader(userNameF, delimiter=','):
                userNames.extend(nameList)
            if len(userNames) != len(userIds):
                return None, ["Error: specified %d user ids and %d user names, must have the same number of each." %  (len(userIds),len(userNames))]

        # did they specify user names?
        # but we need a separate REST call for each user in the comma-delimited user list
        for index in range(len(userIds)):
            user = userIds[index]
            paramDict["id"] = user
            if len(userNames) > 0:
                paramDict["name"] = userNames[index]
            url = self.hostname + '/settings/rbac/users/' + user
            data, errors = self._put(url,paramDict)
            if errors:
                return data, errors

        return data, errors

    def deleteRoles(self,userList):
        # need a separate REST call for each user in the comma-delimited user list
        userF = StringIO.StringIO(userList)
        reader = csv.reader(userF, delimiter=',')
        for users in reader:
            for user in users:
                url = self.hostname + '/settings/rbac/users/' + user
                data, errors = self._delete(url)
                if errors:
                    return data, errors

        return data, errors

    def getRoles(self):
        url = self.hostname + '/settings/rbac/users'
        data, errors = self._get(url)

        return data, errors

    def myRoles(self):
        url = self.hostname + '/whoami'
        data, errors = self._get(url)

        return data, errors

    def retrieve_cluster_certificate(self, extended=False):
        """ Retrieves the current cluster certificate

        Gets the current cluster certificate. If extended is set tot True then
        we return the extended certificate which contains the certificate type,
        certicicate key, expiration, subject, and warnings."""
        url = self.hostname + '/pools/default/certificate'
        if extended:
            url += '?extended=true'
        return self._get(url)

    def regenerate_cluster_certificate(self):
        """ Regenerates the cluster certificate

        Regenerates the cluster certificate and returns the new certificate."""
        url = self.hostname + '/controller/regenerateCertificate'
        return self._post_form_encoded(url, None)

    def upload_cluster_certificate(self, certificate):
        """ Uploads a new cluster certificate"""
        url = self.hostname + '/controller/uploadClusterCA'
        return self._post_form_encoded(url, certificate)

    def retrieve_node_certificate(self, node):
        """ Retrieves the current node certificate

        Returns the current node certificate"""
        url = self.hostname + '/pools/default/certificate/node/' + node
        return self._get(url)

    def set_node_certificate(self):
        """Activates the current node certificate

        Grabs chain.pem and pkey.pem from the <data folder>/inbox/ directory and
        applies them to the node. chain.pem contains the chain encoded certificates
        starting from the node certificat and ending with the last intermediate
        certificate before cluster CA. pkey.pem contains the pem encoded private
        key for node certifiactes. Both files should exist on the server before
        this API is called."""
        url = self.hostname + '/node/controller/reloadCertificate'
        return self._post_form_encoded(url, None)

    # Low level methods for basic HTML operations

    @request
    def _get(self, url):
        response = requests.get(url, auth=(self.username, self.password), verify=False,
                                timeout=self.timeout)
        return _handle_response(response)

    @request
    def _post_form_encoded(self, url, params):
        response = requests.post(url, auth=(self.username, self.password), data=params,
                                 verify=False, timeout=self.timeout)
        return _handle_response(response)

    @request
    def _put(self, url, params):
        response = requests.put(url, params, auth=(self.username, self.password),
                                verify=False, timeout=self.timeout)
        return _handle_response(response)

    @request
    def _delete(self, url):
        response = requests.delete(url, auth=(self.username, self.password),
                                   verify=False, timeout=self.timeout)
        return _handle_response(response)


def _handle_response(response):
    if response.status_code in [200, 202]:
        if 'Content-Type' not in response.headers:
            return "", None
        if 'application/json' in response.headers['Content-Type']:
            return response.json(), None
        else:
            return response.text, None
    elif response.status_code in [400, 404]:
        if 'application/json' in response.headers['Content-Type']:
            errors = response.json()
            if isinstance(errors, list):
                return None, errors
            if isinstance(errors, dict) and "errors" in errors:
                if isinstance(errors["errors"], dict):
                    rv = list()
                    for key, value in errors["errors"].iteritems():
                        rv.append(key + " - " + value)
                    return None, rv
        return None, [response.text]
    elif response.status_code == 401:
        return None, [ERR_AUTH]
    elif response.status_code == 500:
        return None, [ERR_INTERNAL]
    else:
        return None, ['Recieved unexpected status %d' % response.status_code]
