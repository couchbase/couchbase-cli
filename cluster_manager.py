"""Management API's for Couchbase Cluster"""


import csv
import json
import requests
import os
import io
import sys
import time
import urllib.request, urllib.parse, urllib.error

N1QL_SERVICE = 'n1ql'
INDEX_SERVICE = 'index'
MGMT_SERVICE = 'mgmt'
FTS_SERVICE = 'fts'
EVENT_SERVICE = 'eventing'

ERR_AUTH = 'unable to access the REST API - please check your username (-u) and password (-p)'
ERR_INTERNAL = 'Internal server error, please retry your request'

DEFAULT_REQUEST_TIMEOUT = 60

try:
    from cb_version import VERSION
except ImportError:
    VERSION = '0.0.0-0000'

# Remove this once we can verify SSL certificates
requests.packages.urllib3.disable_warnings()

def request(f):
    def g(*args, **kwargs):
        cm = args[0]
        url = args[1]
        try:
            return f(*args, **kwargs)
        except requests.exceptions.ConnectionError as e:
            if '[SSL: CERTIFICATE_VERIFY_FAILED]' in str(e):
                return None, ['Certificate verification failed.\n' +
                'If you are using self-signed certificates you can re-run this command with\n' +
                'the --no-ssl-verify flag. Note however that disabling ssl verification\n' +
                'means that couchbase-cli will be vulnerable to man-in-the-middle attacks.\n\n' +
                'For the most secure access to Couchbase make sure that you have X.509\n' +
                'certificates set up in your cluster and use the --cacert flag to specify\n' +
                'your client certificate.']
            elif str(e).startswith('[SSL]'):
                return None, [f'Unable to connect with the given CA certificate: {str(e)}']
            return None, [f'Unable to connect to host at {cm.hostname}: {str(e)}']
        except requests.exceptions.ReadTimeout as e:
            return None, [f'Request to host `{url}` timed out after {cm.timeout} seconds']
    return g


class ServiceNotAvailableException(Exception):
    """An exception raised when a service does not exist in the target cluster"""

    def __init__(self, service):
        Exception.__init__(self, f'Service {service} not available in target cluster')


class ClusterManager(object):
    """A set of REST API's for managing a Couchbase cluster"""

    def __init__(self, hostname, username, password, sslFlag=False, verifyCert=True,
                 caCert=True, debug=False, timeout=DEFAULT_REQUEST_TIMEOUT, cert=None):
        hostname = hostname.replace("couchbase://", "http://", 1)
        hostname = hostname.replace("couchbases://", "https://", 1)

        self.hostname = hostname
        # verify argument on Request functions can take boolean or a path to a CA if
        # a path is not provide but the cert still needs to be verified it should use
        # the system provided CAs
        self.verifyCert = verifyCert
        self.caCert = caCert
        if not verifyCert:
            self.caCert = False
        # This is for client side certs which is currently not used.
        self.cert = cert

        parsed = urllib.parse.urlparse(hostname)
        if sslFlag:
            hostport = parsed.hostname.split(':')
            if parsed.scheme == 'http://':
                if parsed.port == 8091:
                    self.hostname = f'https://{parsed.hostname}:18091'
                else:
                    self.hostname = f'https://{parsed.hostname}:{parsed.port}'

            # Certificates and verification are not used when the ssl flag is
            # specified.
            self.verifyCert = False
            self.caCert = False

        self.username = username.encode('utf-8').decode('latin1')
        self.password = password.encode('utf-8').decode('latin1')
        self.timeout = timeout
        self.ssl = self.hostname.startswith("https://")
        self.debug = debug
        self.headers = requests.utils.default_headers()
        self.headers.update(
            {
                'User-Agent': f'{os.path.basename(sys.argv[0])}  {VERSION}',
            }
        )

    def restore_index_metadata(self, bucket, index_defs):
        hosts, errors = self.get_hostnames_for_service(INDEX_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(INDEX_SERVICE)

        url = f'{hosts[0]}/restoreIndexMetadata?bucket={bucket}'
        return self._post_json(url, index_defs)

    def get_index_metadata(self, bucket):
        hosts, errors = self.get_hostnames_for_service(INDEX_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(INDEX_SERVICE)

        url = f'{hosts[0]}/getIndexMetadata?bucket={bucket}'
        return self._get(url)

    def restore_fts_index_metadata(self, index_defs):
        hosts, errors = self.get_hostnames_for_service(FTS_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(FTS_SERVICE)

        for index_def in index_defs:
            url = f'{hosts[0]}/api/index/{index_def["name"]}?prevIndexUUID=*'
            if "sourceUUID" in index_def:
                del index_def["sourceUUID"]
            result, errors = self._put_json(url, index_def)
            if errors:
                return None, errors

        return None, None

    def get_fts_index_alias(self):
        hosts, errors = self.get_hostnames_for_service(FTS_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(FTS_SERVICE)

        url = f'{hosts[0]}/api/index'
        result, errors = self._get(url)
        if errors:
            return None, errors

        index_defs = []
        if "indexDefs" in result and result["indexDefs"] is not None:
            for _, index in result['indexDefs']['indexDefs'].items():
                if index['type'] == 'fulltext-alias':
                    index_defs.append(index)

        return index_defs, None

    def get_fts_index_metadata(self, bucket):
        hosts, errors = self.get_hostnames_for_service(FTS_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(FTS_SERVICE)

        url = f'{hosts[0]}/api/index'
        result, errors = self._get(url)
        if errors:
            return None, errors


        bucket_index_defs = []
        if "indexDefs" in result and result["indexDefs"] is not None:
            for _, index_def in result["indexDefs"]["indexDefs"].items():
                if index_def["type"] == "fulltext-index" and index_def["sourceName"] == bucket:
                    bucket_index_defs.append(index_def)
        return bucket_index_defs, None

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

        url = f'{hosts[0]}/query/service'
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

    def is_enterprise(self):
        data, errors = self.pools()
        if errors:
            return None, errors
        return data["isEnterprise"], None

    def get_hostnames_for_service(self, service_name):
        """ Gets all hostnames that run a service

        Gets all hostnames for specified service and returns a list of strings
        in the form "http://hostname:port". If the ClusterManager is configured
        to use SSL/TLS then "https://" is prefixed to each name instead of
        "http://"."""
        url = f'{self.hostname}/pools/default/nodeServices'
        data, errors = self._get(url)
        if errors:
            return None, errors

        # this block of code will check if we are using internal or external address
        # first get the host being used to get the node services info
        used_host = urllib.parse.urlparse(self.hostname).hostname
        useAlt = False
        # next check if its external or internal
        for node in data['nodesExt']:
            if not 'hostname' in node and (used_host == '127.0.0.1' or used_host == 'localhost'):
                useAlt = False
                break
            if 'hostname' in node and used_host == node['hostname']:
                useAlt = False
                break
            if 'alternateAddresses' in node and node['alternateAddresses']['external']['hostname'] == used_host:
                useAlt = True
                break

        hosts = []
        for node in data['nodesExt']:
            node_host = '127.0.0.1'
            if 'hostname' in node:
                node_host = node['hostname']

            # Check for Raw IPv6 address
            if ':' in node_host:
                node_host = '[' + node_host + ']'

            http_prefix = 'http://'
            fts_port_name = 'fts'
            n1ql_port_name = 'n1ql'
            mgmt_port_name = 'mgmt'
            index_port_name = 'indexHttp'
            event_port_name = 'eventingAdminPort'

            if self.ssl:
                http_prefix = 'https://'
                n1ql_port_name = 'n1qlSSL'
                mgmt_port_name = 'mgmtSSL'
                event_port_name = 'eventingSSL'
                index_port_name = 'indexHttps'
                fts_port_name = 'ftsSSL'

            services = node['services']

            if useAlt and 'alternateAddresses' not in node:
                continue

            if 'alternateAddresses' in node and useAlt:
                alt_node_host = node['alternateAddresses']['external']['hostname']
                # Check for Raw IPv6 address
                if ':' in alt_node_host:
                    alt_node_host = '[' + alt_node_host + ']'
                node_host = alt_node_host
                services = node['alternateAddresses']['external']['ports']

            if service_name == MGMT_SERVICE and mgmt_port_name in services:
                hosts.append(http_prefix + node_host + ':' + str(services[mgmt_port_name]))

            if service_name == N1QL_SERVICE and n1ql_port_name in services:
                hosts.append(http_prefix + node_host + ':' + str(services[n1ql_port_name]))

            if service_name == INDEX_SERVICE and index_port_name in services:
                hosts.append(http_prefix + node_host + ':' + str(services[index_port_name]))

            if service_name == FTS_SERVICE and fts_port_name in services:
                hosts.append(http_prefix + node_host + ':' + str(services[fts_port_name]))

            if service_name == EVENT_SERVICE and event_port_name in services:
                hosts.append(http_prefix + node_host + ':' + str(services[event_port_name]))

        return hosts, None

    def pools(self, pool=None):
        """ Retrieves information about Couchbase management pools

        Returns Couchbase pools data"""
        url = f'{self.hostname}/pools'
        if pool:
            url += '/' + pool
        return self._get(url)

    def set_admin_password(self, password):
        url = f'{self.hostname}/controller/resetAdminPassword'
        params = { "password": password }

        return self._post_form_encoded(url, params)

    def regenerate_admin_password(self):
        url = f'{self.hostname}/controller/resetAdminPassword?generate=1'

        return self._post_form_encoded(url, None)

    def rotate_master_pwd(self):
        url = f'{self.hostname}/node/controller/rotateDataKey'
        return self._post_form_encoded(url, None)

    def set_master_pwd(self, password):
        url = f'{self.hostname}/node/controller/changeMasterPassword'
        params = { "newPassword": password }
        return self._post_form_encoded(url, params)

    def user_change_passsword(self, new_password):
        url = f'{self.hostname}/controller/changePassword'
        params = {'password': new_password}
        return self._post_form_encoded(url, params)

    def set_pools_default(self, data_ramsize, index_ramsize, fts_ramsize, cbas_ramsize, eventing_ramsize, cluster_name):
        """ Sets Couchbase RAM Quotas for various services

        Options:
        data_ramsize - An integer denoting the size in MB, None skips the parameter
        index_ramsize - An integer denoting the size in MB, None skips the parameter
        fts_ramsize - An integer denoting the size in MB, None skips the parameter
        cbas_ramsize - An integer denoting the size in MB, None skips the parameter
        eventing_ramsize - An integer denoting the size in MB, None skips the parameter
        cluster_name - Sets a name for the cluster, None skips the parameter
        """
        url = f'{self.hostname}/pools/default'
        params = {}
        if data_ramsize:
            params["memoryQuota"] = data_ramsize
        if index_ramsize:
            params["indexMemoryQuota"] = index_ramsize
        if fts_ramsize:
            params["ftsMemoryQuota"] = fts_ramsize
        if cbas_ramsize:
            params["cbasMemoryQuota"] = cbas_ramsize
        if eventing_ramsize:
            params["eventingMemoryQuota"] = eventing_ramsize
        if cluster_name:
            params["clusterName"] = cluster_name

        return self._post_form_encoded(url, params)

    def setup_services(self, services):
        """ Sets the services on a node

        Options:
        services - A string containing a comma separated list of services
        """
        url = f'{self.hostname}/node/controller/setupServices'
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
        url = f'{self.hostname}/settings/web'
        params = {}

        if username:
            params["username"] = username
        if password:
            params["password"] = password
        if port:
            params["port"] = port
        else:
            params["port"] = "SAME"

        return self._post_form_encoded(url, params)

    def enable_notifications(self, enable):
        url = f'{self.hostname}/settings/stats'
        params = { "sendStats": "false"}

        if enable:
            params["sendStats"] = "true"

        return self._post_form_encoded(url, params)

    def get_server_groups(self):
        url = f'{self.hostname}/pools/default/serverGroups'
        return self._get(url)

    def get_server_group(self, groupName):
        groups, errors = self.get_server_groups()
        if errors:
            return None, errors

        if not groups or not groups["groups"] or groups["groups"] == 0:
            return None, ["No server groups found"]

        if groupName:
            for group in groups["groups"]:
                if group["name"] == groupName:
                    return group, None
            return None, [f'Group `{groupName}` not found']
        else:
            return groups["groups"][0], None

    def add_server(self, add_server, groupName, username, password, services):
        group, errors = self.get_server_group(groupName)
        if errors:
            return None, errors

        url = f'{self.hostname}{group["addNodeURI"]}'
        params = { "hostname": add_server,
                   "user": username,
                   "password": password,
                   "services": services }

        return self._post_form_encoded(url, params)

    def readd_server(self, server):
        _, _, _, readd, _, errors = self._get_otps_names(readd_nodes=[server])
        if errors:
            return None, errors

        if len(readd) != 1:
            return None, ["Server not found %s" % server]

        url = f'{self.hostname}/controller/reAddNode'
        params = { "otpNode": readd[0] }

        return self._post_form_encoded(url, params)

    def get_tasks(self):
        url = f'{self.hostname}/pools/default/tasks'
        return self._get(url)

    def collect_logs_start(self, servers, redaction_level, salt, log_dir, tmp_dir, upload, upload_host, upload_proxy,
                           upload_customer, upload_ticket):
        url = f'{self.hostname}/controller/startLogsCollection'
        params = dict()

        if servers == "*":
            params["nodes"] = servers
        else:
            nodes = servers.split(",")
            known, _, _, readd, _, errors = self._get_otps_names(readd_nodes=nodes)
            if errors:
                return None, errors

            if len(nodes) != len(readd):
                return None, ["Servers list contains invalid servers"]

            params["nodes"] = ",".join(readd)

        if redaction_level:
            params["logRedactionLevel"] = redaction_level
        if log_dir:
            params["logDir"] = log_dir
        if tmp_dir:
            params["tmpDir"] = tmp_dir
        if salt:
            params["logRedactionSalt"] = salt

        if upload:
            if upload_host:
                params["uploadHost"] = upload_host
            if upload_proxy:
                params["uploadProxy"] = upload_proxy
            if upload_customer:
                params["customer"] = upload_customer
            if upload_ticket:
                params["ticket"] = upload_ticket

        return self._post_form_encoded(url, params)

    def collect_logs_stop(self):
        url = f'{self.hostname}/controller/cancelLogsCollection'
        return self._post_form_encoded(url, dict())

    def failover(self, servers_to_failover, force):
        _, _, failover, _, _, errors = self._get_otps_names(failover_nodes=servers_to_failover)
        if errors:
            return None, errors


        if len(failover) != len(servers_to_failover):
            if len(servers_to_failover) == 1:
                return None, ["Server can't be failed over because it's not part of the cluster"]
            return None, ["Some nodes specified to be failed over are not part of the cluster"]

        params = {"otpNode": [server for server, _ in failover]}

        if force:
            params["allowUnsafe"] = "true"
            url = f'{self.hostname}/controller/failOver'
            return self._post_form_encoded(url, params)
        else:
            for server, server_status in failover:
                if server_status != 'healthy':
                    return None, ["% can't be gracefully failed over because it is not healthy", server]
            url = f'{self.hostname}/controller/startGracefulFailover'
            return self._post_form_encoded(url, params)

    def recovery(self, server, recovery_type):
        _, _, _, readd, _, errors = self._get_otps_names(readd_nodes=[server])
        if errors:
            return None, errors

        if len(readd) != 1:
            return None, [f'Server not found {server}']

        url = f'{self.hostname}/controller/setRecoveryType'
        params = { "otpNode": readd[0],
                   "recoveryType": recovery_type }

        return self._post_form_encoded(url, params)

    def rebalance(self, remove_nodes):
        url = f'{self.hostname}/controller/rebalance'
        all, eject, _, _, _, errors = self._get_otps_names(eject_nodes=remove_nodes)
        if errors:
            return None, errors

        if len(eject) != len(remove_nodes):
            return None, ["Some nodes specified to be removed are not part of the cluster"]

        params = { "knownNodes": ','.join(all),
                   "ejectedNodes": ','.join(eject) }

        return self._post_form_encoded(url, params)

    def set_settings_rebalance_retry(self, enabled, wait_for, max_attempts):
        url = f'{self.hostname}/settings/retryRebalance'

        params = {'enabled': enabled}
        if wait_for:
            params['afterTimePeriod'] = wait_for
        if max_attempts:
            params['maxAttempts'] = max_attempts

        return self._post_form_encoded(url, params)

    def get_settings_rebalance_retry(self):
        url = f'{self.hostname}/settings/retryRebalance'
        return self._get(url)

    def cancel_rebalance_retry(self, rebalance_id):
        url = f'{self.hostname}/controller/cancelRebalanceRetry/{rebalance_id}'
        return self._post_form_encoded(url, {})

    def get_rebalance_info(self):
        url = f'{self.hostname}/pools/default/pendingRetryRebalance'
        return self._get(url)

    def rebalance_status(self):
        data, errors = self.get_tasks()
        if errors:
            return (None, None), errors

        rv = {
            "status": "unknown",
            "msg": "unknown state",
            "details": {}
        }

        for task in data:
            if task["type"] != "rebalance":
                continue

            err_msg = None
            if "errorMessage" in task:
                rv["status"] = "errored"
                rv["msg"] = task['errorMessage']
                break
            elif task["status"] == "running":
                rv["status"] = task["status"]
                rv["msg"] = "Rebalance is running"
                rv["details"]["progress"] = task["progress"]
                rv["details"]["refresh"] = task["recommendedRefreshPeriod"]
                rv["details"]["totalBuckets"] = 0
                rv["details"]["curBucket"] = 0
                rv["details"]["curBucketName"] = ""

                if "bucketsCount" in task["detailedProgress"]:
                    rv["details"]["totalBuckets"] = task["detailedProgress"]["bucketsCount"]

                if "bucketNumber" in task["detailedProgress"]:
                    rv["details"]["curBucket"] = task["detailedProgress"]["bucketNumber"]

                if "bucket" in task["detailedProgress"]:
                    rv["details"]["curBucketName"] = task["detailedProgress"]["bucket"]

                acc = 0
                if "perNode" in task["detailedProgress"]:

                    for _, node in task["detailedProgress"]["perNode"].items():
                        acc += node["ingoing"]["docsTotal"] - node["ingoing"]["docsTransferred"]
                        acc += node["outgoing"]["docsTotal"] - node["outgoing"]["docsTransferred"]

                rv["details"]["docsRemaining"] = acc
            elif task["status"] == "notRunning":
                rv["status"] = task["status"]
                rv["msg"] = "Rebalance is not running"
                if "statusIsStale" in task:
                    if task["statusIsStale"] or task["statusIsStale"] == "true":
                        rv["status"] = "stale"
                        rv["msg"] = "Current status is stale, please retry"

            break

        return rv, None

    # otpNode should only be printed out or handed back to ns_server
    # It should never be used to create a connection to a node
    def _get_otps_names(self, eject_nodes=[], failover_nodes=[], readd_nodes=[]):
        result, errors = self.pools('default')
        if errors:
            return None, None, None, None, None, errors

        all = list()
        eject = list()
        failover = list()
        readd = list()
        hostnames = list()
        for node in result["nodes"]:
            if "otpNode" not in node:
                return [], [], [], [], [], ["Unable to get otp names"]
            all.append(node['otpNode'])
            hostnames.append(node['hostname'])
            if node['hostname'] in eject_nodes:
                eject.append(node['otpNode'])
            if node['hostname'] in failover_nodes:
                if node['clusterMembership'] != 'active':
                    return [], [], [], [], [], ["Can't failover a node that isn't in the cluster"]
                else:
                    failover.append((node['otpNode'], node['status']))
            _, host = node['otpNode'].split('@')
            hostport = f'{host}:8091'
            if node['hostname'] in readd_nodes or hostport in readd_nodes:
                readd.append(node['otpNode'])

        return all, eject, failover, readd, hostnames, None

    def create_bucket(self, name, bucket_type, memory_quota,
                      eviction_policy, replicas, replica_indexes,
                      threads_number, conflict_resolution, flush_enabled,
                      max_ttl, compression_mode, sync, db_frag_perc, db_frag_size, view_frag_perc,
                      view_frag_size, from_hour, from_min, to_hour, to_min,
                      abort_outside, paralleldb_and_view_compact, purge_interval, timeout=60):
        url = f'{self.hostname}/pools/default/buckets'

        if name is None:
            return None, ["The bucket name is required when creating a bucket"]
        if bucket_type is None:
            return None, ["The bucket type is required when creating a bucket"]
        if memory_quota is None:
            return None, ["The bucket memory quota is required when creating a bucket"]

        params = {"name": name,
                  "bucketType": bucket_type,
                  "ramQuotaMB": memory_quota}

        if eviction_policy is not None:
            params["evictionPolicy"] = eviction_policy
        if replicas is not None:
            params["replicaNumber"] = replicas
        if replica_indexes is not None:
            params["replicaIndex"] = replica_indexes
        if threads_number is not None:
            params["threadsNumber"] = threads_number
        if conflict_resolution is not None:
            params["conflictResolutionType"] = conflict_resolution
        if flush_enabled is not None:
            params["flushEnabled"] = flush_enabled
        if max_ttl is not None:
            params["maxTTL"] = max_ttl
        if compression_mode is not None:
            params["compressionMode"] = compression_mode

        if bucket_type == "couchbase":
            if (db_frag_perc is not None or db_frag_size is not None or view_frag_perc is not None or
                    view_frag_size is not None or from_hour is not None or from_min is not None or
                    to_hour is not None or to_min is not None or abort_outside is not None or
                    paralleldb_and_view_compact is not None or purge_interval is not None):
                params["autoCompactionDefined"] = "true"
                params["parallelDBAndViewCompaction"] = "false"
            if db_frag_perc is not None:
                params["databaseFragmentationThreshold[percentage]"] = db_frag_perc
            if db_frag_size is not None:
                params["databaseFragmentationThreshold[size]"] = db_frag_size * 1024 ** 2
            if view_frag_perc is not None:
                params["viewFragmentationThreshold[percentage]"] = view_frag_perc
            if view_frag_size is not None:
                params["viewFragmentationThreshold[size]"] = view_frag_size * 1024 ** 2
            if to_min is not None:
                params["allowedTimePeriod[toMinute]"] = to_min
            if to_hour is not None:
                params["allowedTimePeriod[toHour]"] = to_hour
            if from_min is not None:
                params["allowedTimePeriod[fromMinute]"] = from_min
            if from_hour is not None:
                params["allowedTimePeriod[fromHour]"] = from_hour
            if abort_outside is not None:
                params["allowedTimePeriod[abortOutside]"] = abort_outside
            if paralleldb_and_view_compact is not None:
                params["parallelDBAndViewCompaction"] = paralleldb_and_view_compact

        if bucket_type != "memcached" and purge_interval is not None:
                params["purgeInterval"] = purge_interval

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

                url = f'{self.hostname}/pools/default/buckets/{name}'
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
                return None, [f'Bucket created, but not ready after {timeout} seconds']

        return result, None

    def edit_bucket(self, name, memory_quota, eviction_policy,
                    replicas, threads_number, flush_enabled, max_ttl,
                    compression_mode, remove_port, db_frag_perc, db_frag_size, view_frag_perc,
                    view_frag_size, from_hour, from_min, to_hour, to_min,
                    abort_outside, paralleldb_and_view_compact, purge_interval):
        url = f'{self.hostname}/pools/default/buckets/{name}'

        if name is None:
            return None, ["The bucket name is required when editing a bucket"]

        params = {}
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
        if max_ttl is not None:
            params["maxTTL"] = max_ttl
        if compression_mode is not None:
            params["compressionMode"] = compression_mode
        if remove_port:
            params["proxyPort"] = "none"
        if (db_frag_perc is not None or db_frag_size is not None or view_frag_perc is not None or
                view_frag_size is not None or from_hour is not None or from_min is not None or
                to_hour is not None or to_min is not None or abort_outside is not None or
                paralleldb_and_view_compact is not None or purge_interval is not None):
            params["autoCompactionDefined"] = "true"
            params["parallelDBAndViewCompaction"] = "false"
        if db_frag_perc is not None:
            params["databaseFragmentationThreshold[percentage]"] = db_frag_perc
        if db_frag_size is not None:
            params["databaseFragmentationThreshold[size]"] = db_frag_size * 1024 ** 2
        if view_frag_perc is not None:
            params["viewFragmentationThreshold[percentage]"] = view_frag_perc
        if view_frag_size is not None:
            params["viewFragmentationThreshold[size]"] = view_frag_size * 1024 ** 2
        if to_min is not None:
            params["allowedTimePeriod[toMinute]"] = to_min
        if to_hour is not None:
            params["allowedTimePeriod[toHour]"] = to_hour
        if from_min is not None:
            params["allowedTimePeriod[fromMinute]"] = from_min
        if from_hour is not None:
            params["allowedTimePeriod[fromHour]"] = from_hour
        if abort_outside is not None:
            params["allowedTimePeriod[abortOutside]"] = abort_outside
        if paralleldb_and_view_compact is not None:
            params["parallelDBAndViewCompaction"] = paralleldb_and_view_compact
        if purge_interval is not None:
            params["purgeInterval"] = purge_interval

        return self._post_form_encoded(url, params)

    def delete_bucket(self, name):
        url = f'{self.hostname}/pools/default/buckets/{name}'
        return self._delete(url, None)

    def flush_bucket(self, name):
        if name is None:
            return None, ["The bucket name is required when flushing a bucket"]

        url = f'{self.hostname}/pools/default/buckets/{name}/controller/doFlush'
        return self._post_form_encoded(url, None)

    def compact_bucket(self, name, data_only, view_only):
        if data_only and not view_only:
            url = f'{self.hostname}/pools/default/buckets/{name}/controller/compactDatabases'
            return self._post_form_encoded(url, None)
        elif view_only and not data_only:
            url = f'{self.hostname}/pools/default/buckets/{name}/ddocs'
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
            url = f'{self.hostname}/pools/default/buckets/{name}/controller/compactBucket'
            return self._post_form_encoded(url, None)
        else:
            return None, ["Cannot compact data only and view only, pick one or neither"]

    def list_buckets(self, extended=False):
        url = f'{self.hostname}/pools/default/buckets'
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
        url = f'{self.hostname}/pools/default/buckets'
        result, errors = self._get(url)
        if errors:
            return None, errors

        for bucket in result:
            if bucket["name"] == name:
                return bucket, None

        return None, ["Bucket not found"]

    def set_data_paths(self, data_path, index_path, cbas_path, java_home):
        url = f'{self.hostname}/nodes/self/controller/settings'
        params = dict()

        if data_path is not None:
            params["path"] = data_path

        if index_path is not None:
            params["index_path"] = index_path

        if cbas_path is not None:
            params["cbas_path"] = cbas_path

        if java_home is not None:
            params["java_home"] = java_home

        return self._post_form_encoded(url, params)

    def node_info(self):
        url = f'{self.hostname}/nodes/self'
        return self._get(url)

    def get_babysitter_cookie(self):
        url = f'{self.hostname}/diag/eval'
        payload = \
            '{json, atom_to_binary(ns_server:get_babysitter_cookie(), latin1)}.'
        return self._post_form_encoded(url, payload)

    def set_hostname(self, hostname):
        url = f'{self.hostname}/node/controller/rename'
        params = { "hostname": hostname }
        return self._post_form_encoded(url, params)

    def stop_rebalance(self):
        params = {"allowUnsafe": "true"}
        url = f'{self.hostname}/controller/stopRebalance'
        return self._post_form_encoded(url, params)

    def create_server_group(self, name):
        url = f'{self.hostname}/pools/default/serverGroups'
        params = { "name": name }
        return self._post_form_encoded(url, params)

    def delete_server_group(self, name):
        uri, errors = self._get_server_group_uri(name)
        if errors:
            return None, errors

        url = self.hostname + uri
        params = { "name": name }
        return self._delete(url, params)

    def rename_server_group(self, name, new_name):
        uri, errors = self._get_server_group_uri(name)
        if errors:
            return None, errors

        url = self.hostname + uri
        params = { "name": new_name }
        return self._put(url, params)

    def move_servers_between_groups(self, servers, from_group, to_group):
        groups, errors = self.get_server_groups()
        if errors:
            return None, errors

        # Find the groups to move servers between
        move_from_group = None
        move_to_group = None
        for group in groups["groups"]:
            if from_group == group['name']:
                move_from_group = group
            if to_group == group['name']:
                move_to_group = group

        if move_from_group is None:
            return None, [f'Group to move servers from `{from_group}` not found']
        if move_to_group is None:
            return None, [f'Group to move servers to `{to_group}` not found']

        # Find the servers to move in the from group
        nodes_to_move = []
        for server in servers:
            found = False
            for node in move_from_group["nodes"]:
                if server == node["hostname"]:
                    nodes_to_move.append(node)
                    move_from_group["nodes"].remove(node)
                    found = True

            if not found:
                return None, [f"Can't move {server} because it doesn't exist in '{from_group}'"]

        # Move the servers to the to group
        for node in nodes_to_move:
            move_to_group["nodes"].append(node)

        url = self.hostname + groups["uri"]
        return self._put_json(url, groups)


    def _get_server_group_uri(self, name):
        groups, errors = self.get_server_groups()
        if errors:
            return errors

        for group in groups["groups"]:
            if name == group["name"]:
                return group["uri"], None
        return None, [f'Group `{name}` not found']

    def delete_rbac_user(self, username, auth_domain):
        url = f'{self.hostname}/settings/rbac/users/{auth_domain}/{username}'
        return self._delete(url, None)

    def list_rbac_users(self):
        url = f'{self.hostname}/settings/rbac/users'
        return self._get(url)

    def my_roles(self):
        url = f'{self.hostname}/whoami'
        return self._get(url)

    def set_rbac_user(self, username, password, name, roles, auth_domain, groups):
        if auth_domain is None:
            return None, ["The authentication type is required"]

        if username is None:
            return None, ["The username is required"]

        url = f'{self.hostname}/settings/rbac/users/{auth_domain}/{username}'

        defaults, errors = self._get(url)
        if errors and errors[0] == '"Unknown user."':
            defaults = {}
        elif errors:
            return None, errors

        params = {}
        if name is not None:
            params['name'] = name
        elif 'name' in defaults:
            params['name'] = defaults['name']
        if password is not None:
            params['password'] = password
        if roles is not None:
            params['roles'] = roles;
        elif 'roles' in defaults:
            params['roles'] = self._format_user_roles(defaults['roles'])
        if groups is not None:
            params['groups'] = groups
        elif 'groups' in defaults:
            params['groups'] = ','.join(defaults['groups'])

        return self._put(url, params)

    def _format_user_roles(self, roles):
        directly_assigned = filter(lambda r: any(o for o in r['origins'] if o['type'] == 'user'), roles)
        return ",".join(self._format_role(r) for r in directly_assigned)

    def _format_role(self, role):
        return f'{role["role"]}' + (f'[{role["bucket_name"]}]' if 'bucket_name' in role else '')

    def get_user_group(self, group):
        if group is None:
            return None, ['group name is required']

        url = f'{self.hostname}/settings/rbac/groups/{group}'
        return self._get(url)

    def delete_user_group(self, group):
        if group is None:
            return None, ['group name is required']

        url = f'{self.hostname}/settings/rbac/groups/{group}'
        return self._delete(url, dict())

    def set_user_group(self, group, roles, description, ldap_ref):
        if group is None:
            return None, ['Group name is required']

        url = f'{self.hostname}/settings/rbac/groups/{group}'

        defaults, errors = self._get(url)
        if errors and errors[0] == '"Unknown group."':
            defaults = {}
        elif errors:
            return None, errors

        params = {}
        if roles is not None:
            params['roles'] = roles
        elif 'roles' in defaults:
            params['roles'] = ",".join(self._format_role(r) for r in defaults['roles'])
        if description is not None:
            params['description'] = description
        elif 'description' in defaults:
            params['description'] = defaults['description']
        if ldap_ref is not None:
            params['ldap_group_ref'] = ldap_ref
        elif 'ldap_group_ref' in defaults:
            params['ldap_group_ref'] = defaults['ldap_group_ref']

        return self._put(url, params)

    def list_user_groups(self):
        url = f'{self.hostname}/settings/rbac/groups'
        return self._get(url)

    def get_password_policy(self):
        url = f'{self.hostname}/settings/passwordPolicy'
        return self._get(url)

    def set_security_settings(self, disable_http_ui, cluster_encryption_level, tls_min_version,
                              honor_order, cipher_suites):
        url = f'{self.hostname}/settings/security'
        params = {}

        if disable_http_ui:
            params['disableUIOverHttp'] = disable_http_ui
        if cluster_encryption_level:
            params['clusterEncryptionLevel'] = cluster_encryption_level
        if tls_min_version:
            params['tlsMinVersion'] = tls_min_version
        if honor_order:
            params['honorCipherOrder'] = honor_order
        if cipher_suites:
            params['cipherSuites'] = cipher_suites

        return self._post_form_encoded(url, params)

    def get_security_settings(self):
        return self._get(f'{self.hostname}/settings/security')

    def set_password_policy(self, min_length, upper_case, lower_case, digit,
                            special_char):
        url = self.hostname + '/settings/passwordPolicy'

        params = dict()
        if min_length:
            params["minLength"] = min_length
        if upper_case:
            if upper_case == "1":
                params["enforceUppercase"] = "true"
            else:
                params["enforceUppercase"] ="false"
        if lower_case:
            if lower_case == "1":
                params["enforceLowercase"] = "true"
            else:
                params["enforceLowercase"] ="false"
        if digit:
            if digit == "1":
                params["enforceDigits"] = "true"
            else:
                params["enforceDigits"] = "false"
        if special_char:
            if special_char == "1":
                params["enforceSpecialChars"] = "true"
            else:
                params["enforceSpecialChars"] ="false"

        return self._post_form_encoded(url, params)

    def set_audit_settings(self, enabled, log_path, rotate_interval, rotate_size):
        url = f'{self.hostname}/settings/audit'

        params = dict()
        if enabled:
            params["auditdEnabled"] = enabled
        if log_path:
            params["logPath"] = log_path
        if rotate_interval:
            params["rotateInterval"] = rotate_interval
        if rotate_size:
            params["rotateSize"] = rotate_size

        if "logPath" not in params and "auditdEnabled" in params and params["auditdEnabled"] == "true":
            return None, ["The audit log path must be specified when auditing is first set up"]

        return self._post_form_encoded(url, params)

    def set_autofailover_settings(self, enabled, timeout, failoverOfServerGroups, maxCount,
                                  failoverOnDataDiskIssuesEnabled, failoverOnDataDiskIssuesTimePeriod,
                                  canAbortRebalance):
        url = self.hostname + '/settings/autoFailover'

        params = dict()
        if enabled:
            params["enabled"] = enabled
        if timeout:
            params["timeout"] = timeout
        if failoverOfServerGroups:
            params["failoverServerGroup"] = failoverOfServerGroups
        if failoverOnDataDiskIssuesEnabled:
            params["failoverOnDataDiskIssues[enabled]"] = failoverOnDataDiskIssuesEnabled
        if maxCount:
            params["maxCount"] = maxCount
        if failoverOnDataDiskIssuesTimePeriod:
            params["failoverOnDataDiskIssues[timePeriod]"] = failoverOnDataDiskIssuesTimePeriod
        if canAbortRebalance:
            params["canAbortRebalance"] = canAbortRebalance

        return self._post_form_encoded(url, params)

    def set_dp_mode(self):
        url = f'{self.hostname}/settings/developerPreview'
        params = {'enabled': 'true'}
        return self._post_form_encoded(url, params)

    def set_autoreprovision_settings(self, enabled, max_nodes):
        url = self.hostname + '/settings/autoReprovision'

        params = dict()
        if enabled:
            params["enabled"] = enabled
        if max_nodes:
            params["maxNodes"] = max_nodes

        return self._post_form_encoded(url, params)

    def set_compaction_settings(self, dbFragPerc, dbFragSize, viewFragPerc, viewFragSize,
                                fromHour, fromMin, toHour, toMin, abortOutside,
                                parallelDBAndViewCompact, purgeInterval, gsiMode, gsiPerc,
                                gsiInterval, gsiFromHour, gsiFromMin, gsiToHour, gsiToMin,
                                enableGsiAbort):
        url = f'{self.hostname}/controller/setAutoCompaction'
        params = dict()

        if dbFragPerc is not None:
            params["databaseFragmentationThreshold[percentage]"] = dbFragPerc
        if dbFragSize is not None:
            params["databaseFragmentationThreshold[size]"] = dbFragSize
        if viewFragPerc is not None:
            params["viewFragmentationThreshold[percentage]"] = viewFragPerc
        if viewFragSize is not None:
            params["viewFragmentationThreshold[size]"] = viewFragSize
        if fromHour is not None:
            params["allowedTimePeriod[fromHour]"] = fromHour
        if fromMin is not None:
            params["allowedTimePeriod[fromMinute]"] = fromMin
        if toHour is not None:
            params["allowedTimePeriod[toHour]"] = toHour
        if toMin is not None:
            params["allowedTimePeriod[toMinute]"] = toMin
        if abortOutside is not None:
            params["allowedTimePeriod[abortOutside]"] = abortOutside
        if parallelDBAndViewCompact is not None:
            params["parallelDBAndViewCompaction"] = parallelDBAndViewCompact
        if purgeInterval is not None:
            params["purgeInterval"] = purgeInterval
        if gsiMode is not None:
            params["indexCompactionMode"] = gsiMode
        if gsiPerc is not None:
            params["indexFragmentationThreshold[percentage]"] = gsiPerc
        if gsiInterval is not None:
            params["indexCircularCompaction[daysOfWeek]"] = gsiInterval
        if gsiFromHour is not None:
            params["indexCircularCompaction[interval][fromHour]"] = gsiFromHour
        if gsiFromMin is not None:
            params["indexCircularCompaction[interval][fromMinute]"] = gsiFromMin
        if gsiToHour is not None:
            params["indexCircularCompaction[interval][toHour]"] = gsiToHour
        if gsiToMin is not None:
            params["indexCircularCompaction[interval][toMinute]"] = gsiToMin
        if enableGsiAbort is not None:
            params["indexCircularCompaction[interval][abortOutside]"] = enableGsiAbort

        return self._post_form_encoded(url, params)

    def set_index_settings(self, storageMode, maxRollbackPoints, stableSnapInterval,
                           memSnapInterval, threads, logLevel):
        """ Sets global index settings"""
        params = dict()
        if storageMode is not None:
            params["storageMode"] = storageMode
        if maxRollbackPoints is not None:
            params["maxRollbackPoints"] = maxRollbackPoints
        if stableSnapInterval is not None:
            params["stableSnapshotInterval"] = stableSnapInterval
        if memSnapInterval is not None:
            params["memorySnapshotInterval"] = memSnapInterval
        if threads is not None:
            params["indexerThreads"] = threads
        if logLevel is not None:
            params["logLevel"] = logLevel

        url = f'{self.hostname}/settings/indexes'
        return self._post_form_encoded(url, params)

    def set_alert_settings(self, enabled_email_alerts, email_recipients,
                           email_sender, email_user, email_pass, email_host,
                           email_port, email_encrypted, alerts):
        url = f'{self.hostname}/settings/alerts'
        params = dict()

        if enabled_email_alerts:
            params["enabled"] = enabled_email_alerts
        if email_recipients:
            params["recipients"] = email_recipients
        if email_sender:
            params["sender"] = email_sender
        if email_user:
            params["emailUser"] = email_user
        if email_pass:
            params["emailPass"] = email_pass
        if email_host:
            params["emailHost"] = email_host
        if email_port:
            params["emailPort"] = email_port
        if email_encrypted:
            params["emailEncrypt"] = email_encrypted
        if alerts is not None:
            params["alerts"] = alerts

        return self._post_form_encoded(url, params)

    def index_settings(self):
        """ Retrieves the index settings

            Returns a map of all global index settings"""
        url = f'{self.hostname}/settings/indexes'
        return self._get(url)

    def sasl_settings(self, enabled, read_only_admins, admins):
        """ Sets Sasl Settings

        enabled - The string "true" or "false"
        admins - A new line separated list or the string "asterisk"
        read_only_admins - A new line separated list or the string "asterisk"
        """

        url = f'{self.hostname}/settings/saslauthdAuth'
        params = { "enabled": enabled }

        if read_only_admins is not None:
            params["roAdmins"] = read_only_admins
        if admins is not None:
            params["admins"] = admins

        return self._post_form_encoded(url, params)

    def get_ldap(self):
        url = f'{self.hostname}/settings/ldap'
        return self._get(url)

    def ldap_settings(self, authentication_enabled, authorization_enabled, hosts, port, encryption, user_dn_mapping,
                    timeout, max_parallel, max_cache, cache_lifetime, query_dn, query_pass, group_query,
                    nested_groups, nested_groups_max_depth, server_ca_ver, ca):
        url = f'{self.hostname}/settings/ldap'

        params = {}

        if authentication_enabled is not None:
            params['authenticationEnabled'] = authentication_enabled
        if authorization_enabled is not None:
            params['authorizationEnabled'] = authorization_enabled
        if hosts is not None:
            params['hosts'] = hosts
        if port is not None:
            params['port'] = port
        if encryption is not None:
            params['encryption'] = encryption
        if user_dn_mapping is not None:
            params['userDNMapping'] = user_dn_mapping
        if timeout is not None:
            params['requestTimeout'] = timeout
        if max_parallel is not None:
            params['maxParallelConnections'] = max_parallel
        if max_cache is not None:
            params['maxCacheSize'] = max_cache
        if cache_lifetime is not None:
            params['cacheValueLifetime'] = cache_lifetime
        if query_dn is not None:
            params['bindDN'] = query_dn
        if query_pass is not None:
            params['bindPass'] = query_pass
        if group_query is not None:
            params['groupsQuery'] = group_query
        if nested_groups is not None:
            params['nestedGroupsEnabled'] = nested_groups
        if nested_groups_max_depth is not None:
            params['nestedGroupsMaxDepth'] = nested_groups_max_depth
        if server_ca_ver is not None:
            params['serverCertValidation'] = server_ca_ver
        if ca and server_ca_ver != 'false':
            params['cacert'] = ca

        if len(params) == 0:
            return None, ['Please provide at least one option to set']

        return self._post_form_encoded(url, params)

    def setRoles(self, userList, roleList, userNameList):
        # we take a comma-delimited list of roles that needs to go into a dictionary
        paramDict = {"roles" : roleList}
        userIds = []
        userNames = []
        userF = io.StringIO(userList)
        for idList in csv.reader(userF, delimiter=','):
            userIds.extend(idList)

        # did they specify user names?
        if userNameList != None:
            userNameF = io.StringIO(userNameList)
            for nameList in csv.reader(userNameF, delimiter=','):
                userNames.extend(nameList)
            if len(userNames) != len(userIds):
                return None, [f'Error: specified {len(userIds)} user ids and {len(userNames)} user names, must'
                              f' have the same number of each.']

        # did they specify user names?
        # but we need a separate REST call for each user in the comma-delimited user list
        for index in range(len(userIds)):
            user = userIds[index]
            paramDict["id"] = user
            if len(userNames) > 0:
                paramDict["name"] = userNames[index]
            url = f'{self.hostname}/settings/rbac/users/{user}'
            data, errors = self._put(url,paramDict)
            if errors:
                return data, errors

        return data, errors

    def deleteRoles(self,userList):
        # need a separate REST call for each user in the comma-delimited user list
        userF = io.StringIO(userList)
        reader = csv.reader(userF, delimiter=',')
        for users in reader:
            for user in users:
                url = f'{self.hostname}/settings/rbac/users/{user}'
                data, errors = self._delete(url, None)
                if errors:
                    return data, errors

        return data, errors

        url = f'{self.hostname}/settings/rbac/users'
        data, errors = self._get(url)

        return data, errors

    def retrieve_cluster_certificate(self, extended=False):
        """ Retrieves the current cluster certificate

        Gets the current cluster certificate. If extended is set tot True then
        we return the extended certificate which contains the certificate type,
        certicicate key, expiration, subject, and warnings."""
        url = f'{self.hostname}/pools/default/certificate'
        if extended:
            url += '?extended=true'
        return self._get(url)

    def regenerate_cluster_certificate(self):
        """ Regenerates the cluster certificate

        Regenerates the cluster certificate and returns the new certificate."""
        url = f'{self.hostname}/controller/regenerateCertificate'
        return self._post_form_encoded(url, None)

    def upload_cluster_certificate(self, certificate):
        """ Uploads a new cluster certificate"""
        url = f'{self.hostname}/controller/uploadClusterCA'
        return self._post_form_encoded(url, certificate)

    def retrieve_node_certificate(self, node):
        """ Retrieves the current node certificate

        Returns the current node certificate"""
        url = f'{self.hostname}/pools/default/certificate/node/{node}'
        return self._get(url)

    def set_node_certificate(self):
        """Activates the current node certificate

        Grabs chain.pem and pkey.pem from the <data folder>/inbox/ directory and
        applies them to the node. chain.pem contains the chain encoded certificates
        starting from the node certificat and ending with the last intermediate
        certificate before cluster CA. pkey.pem contains the pem encoded private
        key for node certifiactes. Both files should exist on the server before
        this API is called."""
        url = f'{self.hostname}/node/controller/reloadCertificate'
        return self._post_form_encoded(url, None)

    def set_client_cert_auth(self, config):
        """Enable/disable the client cert auth"""
        url = f'{self.hostname}/settings/clientCertAuth'
        return self._post_json(url, config)

    def retrieve_client_cert_auth(self):
        url = f'{self.hostname}/settings/clientCertAuth'
        return self._get(url)

    def create_xdcr_reference(self, name, hostname, username, password, encrypted,
                              encryptionType, certificate, clientCertificate, clientKey):
        return self._set_xdcr_reference(False, name, hostname, username,
                                        password, encrypted, encryptionType,
                                        certificate, clientCertificate, clientKey)

    def edit_xdcr_reference(self, name, hostname, username, password, encrypted,
                            encryptionType, certificate, clientCertificate, clientKey):
        return self._set_xdcr_reference(True, name, hostname, username,
                                        password, encrypted, encryptionType,
                                        certificate, clientCertificate, clientKey)

    def _set_xdcr_reference(self, edit, name, hostname, username, password,
                            encrypted, encryptionType, certificate, clientCertificate, clientKey):
        url = f'{self.hostname}/pools/default/remoteClusters'
        params = {}

        if edit:
            url += f'/{urllib.parse.quote(name)}'

        if name is not None:
            params["name"] = name
        if hostname is not None:
            params["hostname"] = hostname
        if username is not None:
            params["username"] = username
        if password is not None:
            params["password"] = password
        if encrypted is not None:
            params["demandEncryption"] = encrypted
        if encryptionType is not None:
            params["encryptionType"] = encryptionType
        if certificate is not None:
            params["certificate"] = certificate
        if clientCertificate:
            params['clientCertificate'] = clientCertificate
        if clientKey:
            params['clientKey'] = clientKey

        return self._post_form_encoded(url, params)

    def delete_xdcr_reference(self, name):
        url = f'{self.hostname}/pools/default/remoteClusters/{urllib.parse.quote(name)}'
        return self._delete(url, None)

    def list_xdcr_references(self):
        url = f'{self.hostname}/pools/default/remoteClusters/'
        return self._get(url)

    def xdcr_replicator_settings(self, chk_interval, worker_batch_size,
                                 doc_batch_size, fail_interval, replication_thresh,
                                 src_nozzles, dst_nozzles, usage_limit, compression,
                                 log_level, stats_interval, replicator_id, filter, filter_skip, priority,
                                 reset_expiry, filter_del, filter_exp):

        url = f'{self.hostname}/settings/replications/{urllib.parse.quote_plus(replicator_id)}'
        params = self._get_xdcr_params(chk_interval, worker_batch_size, doc_batch_size,
                                       fail_interval, replication_thresh, src_nozzles,
                                       dst_nozzles, usage_limit, compression, log_level,
                                       stats_interval)
        if filter is not None:
            params['filterExpression'] = filter
            filter_numeric = "0"
            if filter_skip:
                filter_numeric = "1"
            params['filterSkipRestream'] = filter_numeric
        if priority:
            params['priority'] = priority
        if reset_expiry:
            params['filterBypassExpiry'] = 'true' if reset_expiry == '1' else 'false'
        if filter_del:
            params['filterDeletion'] = 'true' if filter_del == '1' else 'false'
        if filter_exp:
            params['filterExpiration'] = 'true' if filter_exp == '1' else 'false'

        return self._post_form_encoded(url, params)

    def xdcr_global_settings(self, chk_interval, worker_batch_size, doc_batch_size,
                             fail_interval, replication_threshold, src_nozzles,
                             dst_nozzles, usage_limit, compression, log_level, stats_interval, max_proc):
        url = f'{self.hostname}/settings/replications'
        params = self._get_xdcr_params(chk_interval, worker_batch_size, doc_batch_size,
                                       fail_interval, replication_threshold, src_nozzles,
                                       dst_nozzles, usage_limit, compression, log_level, stats_interval)
        if max_proc:
            params['goMaxProcs'] = max_proc

        return self._post_form_encoded(url, params)

    def _get_xdcr_params(self, chk_interval, worker_batch_size, doc_batch_size,
                         fail_interval, replication_threshold, src_nozzles,
                         dst_nozzles, usage_limit, compression, log_level, stats_interval):
        params = {}
        if chk_interval is not None:
            params["checkpointInterval"] = chk_interval
        if worker_batch_size is not None:
            params["workerBatchSize"] = worker_batch_size
        if doc_batch_size is not None:
            params["docBatchSizeKb"] = doc_batch_size
        if fail_interval is not None:
            params["failureRestartInterval"] = fail_interval
        if replication_threshold is not None:
            params["optimisticReplicationThreshold"] = replication_threshold
        if src_nozzles is not None:
            params["sourceNozzlePerNode"] = src_nozzles
        if dst_nozzles is not None:
            params["targetNozzlePerNode"] = dst_nozzles
        if usage_limit is not None:
            params["networkUsageLimit"] = usage_limit
        if compression is not None:
            params["compressionType"] = compression
        if log_level is not None:
            params["logLevel"] = log_level
        if stats_interval is not None:
            params["statsInterval"] = stats_interval
        return params

    def  create_xdcr_replication(self, name, to_bucket, from_bucket, filter, rep_mode, compression,
                                 reset_expiry, filter_del, filter_exp):
        url = f'{self.hostname}/controller/createReplication'
        params = { "replicationType": "continuous" }

        if to_bucket is not None:
            params["toBucket"] = to_bucket
        if name is not None:
            params["toCluster"] = name
        if from_bucket is not None:
            params["fromBucket"] = from_bucket
        if rep_mode is not None:
            params["type"] = rep_mode
        if filter is not None:
            params["filterExpression"] = filter
        if compression is not None:
            params["compressionType"] = compression
        if reset_expiry:
            params['filterBypassExpiry'] = 'true' if reset_expiry == '1' else 'false'
        if filter_del:
            params['filterDeletion'] = 'true' if filter_del == '1' else 'false'
        if filter_exp:
            params['filterExpiration'] = 'true' if filter_exp == '1' else 'false'

        return self._post_form_encoded(url, params)

    def delete_xdcr_replicator(self, replicator_id):
        url =f'{self.hostname}/controller/cancelXDCR/{urllib.parse.quote_plus(replicator_id)}'
        return self._delete(url, None)

    def pause_xdcr_replication(self, replicator_id):
        url = f'{self.hostname}/settings/replications/{urllib.parse.quote_plus(replicator_id)}'
        params = { "pauseRequested": "true" }
        return self._post_form_encoded(url, params)

    def resume_xdcr_replication(self, replicator_id):
        url = f'{self.hostname}/settings/replications/{urllib.parse.quote_plus(replicator_id)}'
        params = { "pauseRequested": "false" }
        return self._post_form_encoded(url, params)

    def get_query_settings(self):
        url = f'{self.hostname}/settings/querySettings'
        return self._get(url)

    def post_query_settings(self, pipeline_batch, pipeline_cap, scan_cap, timeout, prepared_limit, completed_limit,
                            complete_threshold, log_level, max_parallelism, n1ql_feature_control):
        url = f'{self.hostname}/settings/querySettings'
        params = {}
        if pipeline_batch is not None:
            params['queryPipelineBatch'] = pipeline_batch
        if pipeline_cap is not None:
            params['queryPipelineCap'] = pipeline_cap
        if scan_cap is not None:
            params['queryScanCap'] = scan_cap
        if timeout is not None:
            params['queryTimeout'] = timeout
        if prepared_limit is not None:
            params['queryPreparedLimit'] = prepared_limit
        if completed_limit is not None:
            params['queryCompletedLimit'] = completed_limit
        if complete_threshold is not None:
            params['queryCompletedThreshold'] = complete_threshold
        if log_level is not None:
            params['queryLogLevel'] = log_level
        if max_parallelism is not None:
            params['queryMaxParallelism'] = max_parallelism
        if n1ql_feature_control is not None:
            params['queryN1QLFeatCtrl'] = n1ql_feature_control

        return self._post_form_encoded(url, params)

    def list_functions(self):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)
        url = f'{hosts[0]}/api/v1/functions'
        return self._get(url)

    def export_functions(self):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)
        url = f'{hosts[0]}/api/v1/export'
        return self._get(url)

    def import_functions(self, parms):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors
        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)
        url = f'{hosts[0]}/api/v1/import'
        return self._post_json(url, parms)

    def delete_function(self, function):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)
        url = f'{hosts[0]}/api/v1/functions/{urllib.parse.quote_plus(function)}'
        return self._delete(url, None)

    def pause_resume_function(self, function_name, pause):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)

        url = f"{hosts[0]}/api/v1/functions/{urllib.parse.quote_plus(function_name)}/{'pause' if pause else 'resume'}"
        return self._post_json(url, None)

    def deploy_undeploy_function(self, function, deploy, boundary):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)

        params = {}
        params["deployment_status"] = deploy
        params["processing_status"] = deploy

        if deploy:
            params["feed-boundary"] = boundary

        url = f'{hosts[0]}/api/v1/functions/{urllib.parse.quote_plus(function)}/settings'
        return self._post_json(url, params)

    def create_scope(self, bucket, scope):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/collections'
        params = {"name": scope}
        return self._post_form_encoded(url, params)

    def drop_scope(self, bucket, scope):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/collections/'\
            f'{urllib.parse.quote_plus(scope)}'
        return self._delete(url, None)

    def create_collection(self, bucket, scope, collection, max_ttl):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/collections/' \
            f'{urllib.parse.quote_plus(scope)}'
        params = {"name": collection}
        if max_ttl:
            params["maxTTL"] = max_ttl
        return self._post_form_encoded(url, params)

    def drop_collection(self, bucket, scope, collection):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/collections/'\
            f'{urllib.parse.quote_plus(scope)}/{urllib.parse.quote_plus(collection)}'
        return self._delete(url, None)

    def get_manifest(self, bucket):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/collections'
        return self._get(url)

    def set_alternate_address(self, hostname, ports):
        url = f'{self.hostname}/node/controller/setupAlternateAddresses/external'
        params = {}
        if hostname:
            params['hostname'] = hostname
        if ports:
            for (name, value) in ports:
                params[name] = value

        return self._put(url, params)

    def delete_alternate_address(self):
        url = f'{self.hostname}/node/controller/setupAlternateAddresses/external'
        return self._delete(url, {})

    def get_alternate_address(self):
        url = f'{self.hostname}/pools/default/nodeServices'
        node_service, error = self._get(url)
        if error:
            return None, error
        return node_service['nodesExt'], None

    def enable_external_listener(self, host=None, ipfamily=None, encryption=None):
        hostname = host if host else self.hostname
        url = f'{hostname}/node/controller/enableExternalListener'
        params = {}
        if ipfamily:
            params['afamily'] = ipfamily
        if encryption:
            params['nodeEncryption'] = encryption
        return self._post_form_encoded(url, params)

    def disable_external_listener(self, host=None, ipfamily=None, encryption=None):
        hostname = host if host else self.hostname
        url = f'{hostname}/node/controller/disableExternalListener'
        params = {}
        if ipfamily:
            params['afamily'] = ipfamily
        if encryption:
            params['nodeEncryption'] = encryption
        return self._post_form_encoded(url, params)

    def setup_net_config(self, host=None, ipfamily=None, encryption=None):
        hostname = host if host else self.hostname
        url = f'{hostname}/node/controller/setupNetConfig'
        params = {}
        if ipfamily:
            params['afamily'] = ipfamily
        if encryption:
            params['nodeEncryption'] = encryption

        return self._post_form_encoded(url, params)

    def node_get_address_family(self, host):
        node_data, err = self._get(f'{host}/pools/nodes')
        if err:
            return '', err

        for n in node_data['nodes']:
            if 'thisNode' in n and n['thisNode']:
                if 'addressFamily' in n:
                    return n['addressFamily'], None
                return '', [f'Node {host} must be version 6.5 or higher']

        return '', [f'Could not get data for {host}']

    def reset_cipher_suites(self):
        url = f'{self.hostname}/controller/resetCipherSuites'
        return self._post_form_encoded(url, None)

    # returns info about nodes even if cluster is not initialized yet
    def nodes_info(self):
        node_data, err = self.pools('nodes')
        if err and err[0] == '"unknown pool"':
            result, err = self.node_info()
            if err:
                return None, err
            else:
                return [result], None
        elif err:
            return None, err

        return node_data['nodes'], None

    # Low level methods for basic HTML operations

    @request
    def _get(self, url):
        if self.debug:
            print(f'GET {url}')
        response = requests.get(url, auth=(self.username, self.password), verify=self.caCert,
                                cert=self.cert, timeout=self.timeout,
                                headers=self.headers)
        return _handle_response(response, self.debug)

    @request
    def _post_form_encoded(self, url, params):
        if self.debug:
            if params is None:
                params = {}
            print(f'POST {url} {urllib.parse.urlencode(params)}')
        response = requests.post(url, auth=(self.username, self.password), data=params,
                                 cert=self.cert, verify=self.caCert, timeout=self.timeout,
                                 headers=self.headers)
        return _handle_response(response, self.debug)

    @request
    def _post_json(self, url, params):
        if self.debug:
            if params is None:
                params = {}
            print(f'POST {url} {json.dumps(params)}')
        response = requests.post(url, auth=(self.username, self.password), json=params,
                                 cert=self.cert, verify=self.caCert, timeout=self.timeout,
                                 headers=self.headers)
        return _handle_response(response, self.debug)

    @request
    def _put(self, url, params):
        if self.debug:
            if params is None:
                params = {}
            print(f'PUT {url} {urllib.parse.urlencode(params)}')
        response = requests.put(url, params, auth=(self.username, self.password),
                                cert=None, verify=self.caCert, timeout=self.timeout,
                                headers=self.headers)
        return _handle_response(response, self.debug)

    @request
    def _put_json(self, url, params):
        if self.debug:
            if params is None:
                params = {}
            print(f'PUT {url} {json.dumps(params)}')
        response = requests.put(url, auth=(self.username, self.password), json=params,
                                cert=None, verify=self.caCert, timeout=self.timeout,
                                headers = self.headers)
        return _handle_response(response, self.debug)

    @request
    def _delete(self, url, params):
        if self.debug:
            if params is None:
                params = {}
            print(f'DELETE {url} {urllib.parse.urlencode(params)}')
        response = requests.delete(url, auth=(self.username, self.password), data=params,
                                   cert=None, verify=self.caCert, timeout=self.timeout,
                                   headers=self.headers)
        return _handle_response(response, self.debug)


def _handle_response(response, debug):
    if debug:
        output = str(response.status_code)
        if response.headers:
            output += f', {response.headers}'
        if response.content:
            response.encoding = 'utf-8'
            output += f', {response.content}'
        print(output)
    if response.status_code in [200, 202]:
        if 'Content-Type' not in response.headers:
            return "", None
        if not response.content:
            return "", None
        if 'application/json' in response.headers['Content-Type']:
            return response.json(), None
        else:
            response.encoding = 'utf-8'
            return response.text, None
    elif response.status_code in [400, 404]:
        if 'application/json' in response.headers['Content-Type']:
            errors = response.json()
            if isinstance(errors, list):
                return None, errors
            if "errors" in errors and isinstance(errors["errors"], list):
                return None, errors["errors"]
            if isinstance(errors, dict):
                if "errors" in errors and isinstance(errors["errors"], dict):
                    errors = errors["errors"]
                rv = list()
                for key, value in errors.items():
                    rv.append(key + " - " + str(value))
                return None, rv
        return None, [response.text]
    elif response.status_code == 401:
        return None, [ERR_AUTH]
    elif response.status_code == 403:
        errors = response.json()
        return None, [errors["message"] + ": " + ", ".join(errors["permissions"])]
    # Error codes from Eventing service
    elif response.status_code  in [406, 422, 423]:
        errors = response.json()
        if "description" in errors:
            return None, [errors["description"]]
        return None, ['Received unexpected status %d' % response.status_code]
    # Error code from Eventing Service
    elif response.status_code == 207:
        errors = response.json()
        if isinstance(errors, list):
            rv = list()
            for error in errors:
                if error['code'] == 20:
                    rv.append(error['info'])
            return None, rv
        else:
            return None, ['Received unexpected status %d' % response.status_code]
    elif response.status_code == 500:
        return None, [ERR_INTERNAL]
    else:
        return None, ['Received unexpected status %d' % response.status_code]
