"""Management API's for Couchbase Cluster"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
import urllib3

import couchbaseConstants
from x509_adapter import X509Adapter, X509AdapterFactory

N1QL_SERVICE = 'n1ql'
INDEX_SERVICE = 'index'
MGMT_SERVICE = 'mgmt'
FTS_SERVICE = 'fts'
EVENT_SERVICE = 'eventing'
CBAS_SERVICE = 'cbas'
BACKUP_SERVICE = 'backup'

ERR_AUTH = 'unable to access the REST API - please check your username (-u) and password (-p)'
ERR_INTERNAL = 'Internal server error, please retry your request'

DEFAULT_REQUEST_TIMEOUT = 60

try:
    from cb_version import VERSION
except ImportError:
    VERSION = '0.0.0-0000'


def one_zero_boolean_to_string(value: str) -> str:
    """Helper function to convert arguments with 1/0 string values to true or false"""
    return 'true' if value == '1' else 'false'


# Remove this once we can verify SSL certificates
urllib3.disable_warnings()


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
        except requests.exceptions.ReadTimeout:
            return None, [f'Request to host `{url}` timed out after {cm.timeout} seconds']
    return g


class ServiceNotAvailableException(Exception):
    """An exception raised when a service does not exist in the target cluster"""

    def __init__(self, service):
        Exception.__init__(self, f'Service {service} not available in target cluster')


class ClusterManager(object):
    """A set of REST API's for managing a Couchbase cluster"""

    def __init__(self, hostname, username, password, ssl_flag=False, verify_cert=True, ca_cert=True, debug=False,
                 timeout=DEFAULT_REQUEST_TIMEOUT, client_ca: Optional[Path] = None,
                 client_ca_password: Optional[str] = None,
                 client_pk: Optional[Path] = None, client_pk_password: Optional[str] = None):
        hostname = hostname.replace("couchbase://", "http://", 1)
        hostname = hostname.replace("couchbases://", "https://", 1)

        self.hostname = hostname
        # verify argument on Request functions can take boolean or a path to a CA if
        # a path is not provide but the cert still needs to be verified it should use
        # the system provided CAs
        self.verify_cert = verify_cert
        self.ca_cert = ca_cert
        if not verify_cert:
            self.ca_cert = False

        parsed = urllib.parse.urlparse(hostname)
        if ssl_flag:
            if parsed.scheme == 'http://':
                if parsed.port == 8091:
                    self.hostname = f'https://{parsed.hostname}:18091'
                else:
                    self.hostname = f'https://{parsed.hostname}:{parsed.port}'

            # Certificates and verification are not used when the ssl flag is
            # specified.
            self.verify_cert = False
            self.ca_cert = False

        self.username = username.encode('utf-8').decode('latin1') if username is not None else ""
        self.password = password.encode('utf-8').decode('latin1') if password is not None else ""
        self.timeout = timeout
        self.ssl = self.hostname.startswith("https://")
        self.debug = debug
        self.headers = requests.utils.default_headers()
        self.headers.update(
            {
                'User-Agent': f'{os.path.basename(sys.argv[0])}  {VERSION}',
            }
        )

        self.session = requests.Session()

        adapter = self._generate_x509_adapter(hostname, client_ca, client_ca_password, client_pk, client_pk_password)
        if adapter:
            self.session.mount("https://", adapter)

    @classmethod
    def _generate_x509_adapter(cls,
                               hostname: str,
                               client_ca: Optional[Path] = None,
                               client_ca_password: Optional[str] = None,
                               client_pk: Optional[Path] = None,
                               client_pk_password: Optional[str] = None) -> Optional[X509Adapter]:
        if client_ca is None:
            return None

        password = client_pk_password if client_pk_password is not None else client_ca_password
        password = password if password is not None else ""

        return X509AdapterFactory(hostname, client_ca, client_pk=client_pk, password=password).generate()

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
            _, errors = self._put_json(url, index_def)
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

    def is_enterprise_columnar(self):
        data, errors = self.pools()
        if errors:
            return None, None, errors
        # use data["isColumnar"] once MB-65393 is completed
        return data["isEnterprise"], data["implementationVersion"].endswith("-columnar"), None

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
        use_alt = False
        # next check if its external or internal
        for node in data['nodesExt']:
            if 'hostname' not in node and used_host in ['127.0.0.1', 'localhost']:
                use_alt = False
                break
            if 'hostname' in node and used_host == node['hostname']:
                use_alt = False
                break
            if 'alternateAddresses' in node and node['alternateAddresses']['external']['hostname'] == used_host:
                use_alt = True
                break

        hosts = []
        for node in data['nodesExt']:
            # Single node cluster will not have a hostname, default to the hostname specified
            # to work with remote clusters
            node_host = used_host
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
            cbas_port_name = 'cbas'
            backup_port_name = 'backupAPI'

            if self.ssl:
                http_prefix = 'https://'
                n1ql_port_name = 'n1qlSSL'
                mgmt_port_name = 'mgmtSSL'
                event_port_name = 'eventingSSL'
                index_port_name = 'indexHttps'
                fts_port_name = 'ftsSSL'
                cbas_port_name = 'cbasSSL'
                backup_port_name = 'backupAPIHTTPS'

            services = node['services']

            if use_alt and 'alternateAddresses' not in node:
                continue

            if 'alternateAddresses' in node and use_alt:
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

            if service_name == CBAS_SERVICE and cbas_port_name in services:
                hosts.append(http_prefix + node_host + ':' + str(services[cbas_port_name]))

            if service_name == BACKUP_SERVICE and backup_port_name in services:
                hosts.append(f'{http_prefix}{node_host}:{services[backup_port_name]}')

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
        params = {"password": password}

        return self._post_form_encoded(url, params)

    def regenerate_admin_password(self):
        url = f'{self.hostname}/controller/resetAdminPassword?generate=1'

        return self._post_form_encoded(url, None)

    def lock_admin(self):
        url = f'{self.hostname}/controller/lockAdmin'

        return self._post_form_encoded(url, None)

    def unlock_admin(self):
        url = f'{self.hostname}/controller/unlockAdmin'

        return self._post_form_encoded(url, None)

    def rotate_master_pwd(self):
        url = f'{self.hostname}/node/controller/rotateDataKey'
        return self._post_form_encoded(url, None)

    def set_master_pwd(self, password):
        url = f'{self.hostname}/node/controller/changeMasterPassword'
        params = {"newPassword": password}
        return self._post_form_encoded(url, params)

    def user_change_passsword(self, new_password):
        url = f'{self.hostname}/controller/changePassword'
        params = {'password': new_password}
        return self._post_form_encoded(url, params)

    def set_pools_default(self, data_ramsize, index_ramsize, fts_ramsize, cbas_ramsize, eventing_ramsize, query_ramsize,
                          cluster_name):
        """ Sets Couchbase RAM Quotas for various services

        Options:
        data_ramsize - An integer denoting the size in MiB, None skips the parameter
        index_ramsize - An integer denoting the size in MiB, None skips the parameter
        fts_ramsize - An integer denoting the size in MiB, None skips the parameter
        cbas_ramsize - An integer denoting the size in MiB, None skips the parameter
        eventing_ramsize - An integer denoting the size in MiB, None skips the parameter
        query_ramsize - An integer denoting the size in MiB, None skips the parameter
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
        if query_ramsize:
            params["queryMemoryQuota"] = query_ramsize
        if cluster_name:
            params["clusterName"] = cluster_name

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
        params = {"sendStats": "false"}

        if enable:
            params["sendStats"] = "true"

        return self._post_form_encoded(url, params)

    def get_server_groups(self):
        url = f'{self.hostname}/pools/default/serverGroups'
        return self._get(url)

    def get_server_group(self, group_name):
        groups, errors = self.get_server_groups()
        if errors:
            return None, errors

        if not groups or not groups["groups"] or groups["groups"] == 0:
            return None, ["No server groups found"]

        for group in groups["groups"]:
            if group["name"] == group_name:
                return group, None

        return None, [f'Group `{group_name}` not found']

    def get_add_node_uri(self, group_name):
        # Not given a group, use the default 'addNode' API; this avoids using the 'serverGroups' API on CE. See MB-51094
        # for more information.
        if group_name is None or group_name == "":
            return f"{self.hostname}/controller/addNode", None

        group, errors = self.get_server_group(group_name)
        if errors:
            return None, errors

        return f'{self.hostname}{group["addNodeURI"]}', None

    def add_server(self, add_server, group_name, username, password, services):
        uri, errors = self.get_add_node_uri(group_name)
        if errors:
            return None, errors

        return self._post_form_encoded(
            uri,
            {"hostname": add_server, "user": username, "password": password, "services": services},
        )

    def readd_server(self, server):
        all_cluster_nodes_info, errors = self._get_all_cluster_nodes_info()
        if errors:
            return None, errors

        readd, errors = self._get_otp_names_of_matched_nodes(all_cluster_nodes_info, [server])
        if errors:
            return None, errors

        if len(readd) != 1:
            return None, ["Server not found %s" % server]

        url = f'{self.hostname}/controller/reAddNode'
        params = {"otpNode": readd[0]}

        return self._post_form_encoded(url, params)

    def get_tasks(self):
        url = f'{self.hostname}/pools/default/tasks'
        return self._get(url)

    def collect_logs_start(self, servers, redaction_level, salt, log_dir, tmp_dir, upload, upload_host, upload_proxy,
                           upload_customer, upload_ticket, encryption_password):
        url = f'{self.hostname}/controller/startLogsCollection'
        params = dict()

        if servers == "*":
            params["nodes"] = servers
        else:
            nodes = servers.split(",")

            all_cluster_nodes_info, errors = self._get_all_cluster_nodes_info()
            if errors:
                return None, errors

            readd, errors = self._get_otp_names_of_matched_nodes(all_cluster_nodes_info, nodes)
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
        if encryption_password:
            params["encryptionPassword"] = encryption_password

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

    def failover(self, servers_to_failover, hard, force):
        all_cluster_nodes_info, errors = self._get_all_cluster_nodes_info()
        if errors:
            return None, errors

        failover, errors = self._get_otp_names_of_matched_nodes(all_cluster_nodes_info, servers_to_failover)
        if errors:
            return None, errors

        if len(failover) != len(servers_to_failover):
            if len(servers_to_failover) == 1:
                return None, [f"Server {servers_to_failover[0]} can't be failed over because it's not part of the "
                              "cluster"]
            return None, ["Some nodes specified to be failed over are not part of the cluster"]

        failover_with_statuses, errors = self._get_nodes_to_failover_statuses_by_otp(all_cluster_nodes_info, failover,
                                                                                     get_inactive=force)
        if errors:
            return None, errors

        params = {"otpNode": [server for server, _ in failover_with_statuses]}

        if hard:
            if force:
                params["allowUnsafe"] = "true"
            url = f'{self.hostname}/controller/failOver'
            return self._post_form_encoded(url, params)

        for server, server_status in failover_with_statuses:
            if server_status != 'healthy':
                return None, [f"{server} can't be gracefully failed over because it is not healthy"]
        url = f'{self.hostname}/controller/startGracefulFailover'
        return self._post_form_encoded(url, params)

    def recovery(self, server, recovery_type):
        all_cluster_nodes_info, errors = self._get_all_cluster_nodes_info()
        if errors:
            return None, errors

        readd, errors = self._get_otp_names_of_matched_nodes(all_cluster_nodes_info, [server])
        if errors:
            return None, errors

        if len(readd) != 1:
            return None, [f'Server not found {server}']

        url = f'{self.hostname}/controller/setRecoveryType'
        params = {"otpNode": readd[0],
                  "recoveryType": recovery_type}

        return self._post_form_encoded(url, params)

    def rebalance(self, remove_nodes):
        all_cluster_nodes_info, errors = self._get_all_cluster_nodes_info()
        if errors:
            return None, errors

        all_nodes, errors = self._get_all_nodes_otp_names(all_cluster_nodes_info)
        if errors:
            return None, errors

        eject, errors = self._get_otp_names_of_matched_nodes(all_cluster_nodes_info, remove_nodes)
        if errors:
            return None, errors

        if len(eject) != len(remove_nodes):
            return None, ["Some nodes specified to be removed are not part of the cluster"]

        url = f'{self.hostname}/controller/rebalance'
        params = {"knownNodes": ','.join(all_nodes),
                  "ejectedNodes": ','.join(eject)}

        return self._post_form_encoded(url, params)

    @staticmethod
    def map_services_to_nodes(nodes):
        """Returns a dictionary that maps services to a set of nodes that provide that service

        nodes - The 'nodes' entry from /pools/default"""

        service_map = defaultdict(set, {
            FTS_SERVICE: set(),
            INDEX_SERVICE: set(),
            N1QL_SERVICE: set(),
            BACKUP_SERVICE: set(),
            CBAS_SERVICE: set()
        })

        for node in nodes:
            otp_node = node.get('otpNode')
            for service in node.get('services', []):
                if service not in service_map:
                    service_map[service] = set()
                service_map[service].add(otp_node)
        return service_map

    @classmethod
    def get_otp_names_and_verify(cls, cluster_info, nodes, list_name):
        """Verifies the nodes in the list exist in the cluster and returns their OTP names"""
        otp_names, errors = cls._get_otp_names_of_matched_nodes(cluster_info, nodes)
        if errors:
            return None, errors

        if len(nodes) != len(otp_names):
            return None, [f"Invalid node(s) in the {list_name} list not found in the cluster"]

        return otp_names, None

    @staticmethod
    def add_service_to_node(nodes, service_set, service_name):
        for node in nodes:
            if node in service_set:
                print(f"Node {node} already provides the {service_name} service")
                continue
            service_set.add(node)

    @staticmethod
    def remove_service_from_node(nodes, service_set, service_name):
        for node in nodes:
            if node not in service_set:
                return [f"Node {node} does not provide the {service_name} service"]
            service_set.remove(node)

    def rebalance_services(self,
                           fts_add, fts_remove,
                           index_add, index_remove,
                           n1ql_add, n1ql_remove,
                           backup_add, backup_remove,
                           cbas_add, cbas_remove):

        all_cluster_nodes_info, errors = self._get_all_cluster_nodes_info()
        if errors:
            return None, errors

        # Check if the same node is being added and removed from the same service
        if len(set(fts_add) & set(fts_remove)) > 0:
            return None, ["Nodes can't be added and removed from the Search Service at the same time"]

        if len(set(index_add) & set(index_remove)) > 0:
            return None, ["Nodes can't be added and removed from the Index Service at the same time"]

        if len(set(n1ql_add) & set(n1ql_remove)) > 0:
            return None, ["Nodes can't be added and removed from the Query Service at the same time"]

        if len(set(backup_add) & set(backup_remove)) > 0:
            return None, ["Nodes can't be added and removed from the Backup Service at the same time"]

        if len(set(cbas_add) & set(cbas_remove)) > 0:
            return None, ["Nodes can't be added and removed from the Analytics Service at the same time"]

        # Verify the nodes in the inputted lists exist in the cluster, and get their OTP names
        otp_fts_add, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, fts_add, "fts_add")
        if errors:
            return None, errors

        otp_fts_remove, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, fts_remove, "fts_remove")
        if errors:
            return None, errors

        otp_index_add, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, index_add, "index_add")
        if errors:
            return None, errors

        otp_index_remove, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, index_remove, "index_remove")
        if errors:
            return None, errors

        otp_n1ql_add, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, n1ql_add, "query_add")
        if errors:
            return None, errors

        otp_n1ql_remove, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, n1ql_remove, "query_remove")
        if errors:
            return None, errors

        otp_backup_add, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, backup_add, "backup_add")
        if errors:
            return None, errors

        otp_backup_remove, errors = self.get_otp_names_and_verify(
            all_cluster_nodes_info, backup_remove, "backup_remove")
        if errors:
            return None, errors

        otp_cbas_add, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, cbas_add, "analytics_add")
        if errors:
            return None, errors

        otp_cbas_remove, errors = self.get_otp_names_and_verify(all_cluster_nodes_info, cbas_remove, "analytics_remove")
        if errors:
            return None, errors

        # Map the services to the nodes that provide them
        services_to_nodes = self.map_services_to_nodes(all_cluster_nodes_info)

        # Add/remove the inputted nodes to the service_to_nodes dictionary
        self.add_service_to_node(otp_fts_add, services_to_nodes[FTS_SERVICE], FTS_SERVICE)
        errors = self.remove_service_from_node(otp_fts_remove, services_to_nodes[FTS_SERVICE], FTS_SERVICE)
        if errors:
            return None, errors

        self.add_service_to_node(otp_index_add, services_to_nodes[INDEX_SERVICE], INDEX_SERVICE)
        errors = self.remove_service_from_node(otp_index_remove, services_to_nodes[INDEX_SERVICE], INDEX_SERVICE)
        if errors:
            return None, errors

        self.add_service_to_node(otp_n1ql_add, services_to_nodes[N1QL_SERVICE], N1QL_SERVICE)
        errors = self.remove_service_from_node(otp_n1ql_remove, services_to_nodes[N1QL_SERVICE], N1QL_SERVICE)
        if errors:
            return None, errors

        self.add_service_to_node(otp_backup_add, services_to_nodes[BACKUP_SERVICE], BACKUP_SERVICE)
        errors = self.remove_service_from_node(otp_backup_remove, services_to_nodes[BACKUP_SERVICE], BACKUP_SERVICE)
        if errors:
            return None, errors

        self.add_service_to_node(otp_cbas_add, services_to_nodes[CBAS_SERVICE], CBAS_SERVICE)
        errors = self.remove_service_from_node(otp_cbas_remove, services_to_nodes[CBAS_SERVICE], CBAS_SERVICE)
        if errors:
            return None, errors

        all_nodes, errors = self._get_all_nodes_otp_names(all_cluster_nodes_info)
        if errors:
            return None, errors

        url = f'{self.hostname}/controller/rebalance'
        params = {"knownNodes": ','.join(all_nodes),
                  f"topology[{FTS_SERVICE}]": ','.join(services_to_nodes[FTS_SERVICE]),
                  f"topology[{INDEX_SERVICE}]": ','.join(services_to_nodes[INDEX_SERVICE]),
                  f"topology[{N1QL_SERVICE}]": ','.join(services_to_nodes[N1QL_SERVICE]),
                  f"topology[{BACKUP_SERVICE}]": ','.join(services_to_nodes[BACKUP_SERVICE]),
                  f"topology[{CBAS_SERVICE}]": ','.join(services_to_nodes[CBAS_SERVICE])}

        return self._post_form_encoded(url, params)

    def get_settings_rebalance(self):
        url = f'{self.hostname}/settings/rebalance'
        return self._get(url)

    def set_settings_rebalance(self, rebalance_moves_per_node):
        url = f'{self.hostname}/settings/rebalance'
        params = {'rebalanceMovesPerNode': rebalance_moves_per_node}
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
                if "statusIsStale" in task and (task["statusIsStale"] or task["statusIsStale"] == "true"):
                    rv["status"] = "stale"
                    rv["msg"] = "Current status is stale, please retry"
                elif "masterRequestTimedOut" in task and task["masterRequestTimedOut"]:
                    rv["status"] = "stale"
                    rv["msg"] = "Orchestrator request timed out, please retry"
            break

        return rv, None

    def _get_all_cluster_nodes_info(self):
        result, errors = self.pools('default')
        if errors:
            return None, errors

        if "nodes" not in result:
            return None, ["Could not get info about the nodes in the cluster"]

        return result["nodes"], None

    @classmethod
    def _get_all_nodes_otp_names(cls, all_cluster_nodes_info):
        all_cluster_nodes_otp_names = []

        for node in all_cluster_nodes_info:
            err = cls._check_otp_name_in_node(node)
            if err is not None:
                return None, [err]
            all_cluster_nodes_otp_names.append(node['otpNode'])

        return all_cluster_nodes_otp_names, None

    # otpNode should only be printed out or handed back to ns_server
    # It should never be used to create a connection to a node
    @classmethod
    def _get_otp_names_of_matched_nodes(cls, all_cluster_nodes_info, nodes_to_match):
        matched_nodes_otp_names = []
        nodes_to_match = cls._remove_schemes_from_nodes(nodes_to_match)

        for node in all_cluster_nodes_info:
            err = cls._check_otp_name_in_node(node)
            if err is not None:
                return None, [err]
            otp_name = node['otpNode']

            if "hostname" not in node:
                return None, [f"Unable to get the hostname of the {otp_name} node"]
            hostname = node['hostname']

            if 'ports' not in node:
                return None, [f"Unable to get the ports of the {hostname} node"]
            ports = node['ports']
            ports_to_check = []

            if 'httpsMgmt' in ports:
                ports_to_check.append(ports['httpsMgmt'])

            _, otp_hostname = otp_name.split('@')
            node_hostname, node_port = cls._get_hostname_and_port(hostname)
            if node_hostname is None or node_port is None:
                return None, [f"Failed to get hostname and port of the {hostname} node"]
            ports_to_check.append(node_port)

            for port in ports_to_check:
                if f'[{node_hostname}]:{port}' in nodes_to_match or f'[{otp_hostname}]:{port}' in nodes_to_match or \
                        f'{node_hostname}:{port}' in nodes_to_match or f'{otp_hostname}:{port}' in nodes_to_match:
                    matched_nodes_otp_names.append(otp_name)

        return matched_nodes_otp_names, None

    @classmethod
    def _check_otp_name_in_node(cls, node):
        if "otpNode" not in node:
            # Try to provide the hostname of the node to help with investigating why its Otp name could not be
            # retrieved from the default cluster management pool
            if "hostname" not in node:
                return "Unable to get the Otp name or the hostname of a cluster node"
            return f"Unable to get the Otp name of the {node['hostname']} node"
        return None

    @classmethod
    def _remove_schemes_from_nodes(cls, nodes):
        return [re.sub('.*//', '', node_url) for node_url in nodes]

    @classmethod
    def _get_hostname_and_port(cls, node):
        # We need to support node urls without schemes, prepending '//' makes urlparse() correctly identify hostnames
        if '//' not in node:
            node = f'//{node}'
        parse_result = urlparse(node)
        hostname = parse_result.hostname
        try:
            port = parse_result.port
        except ValueError:
            return None, None

        return hostname, port

    @classmethod
    def _get_nodes_to_failover_statuses_by_otp(cls, all_cluster_nodes_info, nodes_otp_names, get_inactive=False):
        nodes_otp_names_with_states = []

        for node in all_cluster_nodes_info:
            otp_name = node['otpNode']
            if otp_name in nodes_otp_names:
                valid_states = ['active', 'inactiveFailed', 'inactiveAdded'] if get_inactive else ['active']
                if node['clusterMembership'] not in valid_states:
                    return None, ["Can't failover a node that isn't in the cluster"]
                nodes_otp_names_with_states.append((otp_name, node['status']))

        return nodes_otp_names_with_states, None

    def get_encryption_settings(self):
        url = f'{self.hostname}/settings/security/encryptionAtRest'
        return self._get(url)

    def set_encryption_settings(self, target, typ, key, dek_rotation_interval, dek_lifetime):
        url = f'{self.hostname}/settings/security/encryptionAtRest'

        method = 'disabled'
        if typ == 'key':
            method = 'encryptionKey'
        elif typ == 'master-password':
            method = 'nodeSecretsManager'

        params = {'encryptionMethod': method}
        if key is not None:
            params['encryptionKeyId'] = key
        if dek_rotation_interval is not None:
            params['dekRotationInterval'] = dek_rotation_interval
        if dek_lifetime is not None:
            params['dekLifetime'] = dek_lifetime

        params = {f'{target}.{k}': v for k, v in params.items()}
        return self._post_form_encoded(url, params)

    def list_keys(self):
        url = f'{self.hostname}/settings/encryptionKeys'
        return self._get(url)

    def create_key(self, name, typ, usages, data):
        url = f'{self.hostname}/settings/encryptionKeys'
        params = {'name': name, 'type': typ, 'usage': usages, 'data': data}
        return self._post_json(url, params)

    def edit_key(self, keyid, name, typ, usages, data):
        url = f'{self.hostname}/settings/encryptionKeys/{keyid}'
        params = {'name': name, 'type': typ, 'usage': usages, 'data': data}
        return self._put_json(url, params)

    def rotate_key(self, keyid):
        url = f'{self.hostname}/controller/rotateEncryptionKey/{keyid}'
        return self._post_form_encoded(url, None)

    def delete_key(self, keyid):
        url = f'{self.hostname}/settings/encryptionKeys/{keyid}'
        return self._delete(url, None)

    def create_bucket(
            self,
            name,
            bucket_type,
            storage_type,
            memory_quota,
            durability_min_level,
            eviction_policy,
            replicas,
            replica_indexes,
            threads_number,
            conflict_resolution,
            flush_enabled,
            max_ttl,
            compression_mode,
            sync,
            db_frag_perc,
            db_frag_size,
            view_frag_perc,
            view_frag_size,
            from_hour,
            from_min,
            to_hour,
            to_min,
            abort_outside,
            paralleldb_and_view_compact,
            purge_interval,
            history_retention_bytes,
            history_retention_seconds,
            history_retention_default,
            rank,
            vbuckets,
            encryption_key,
            dek_rotation_interval,
            dek_lifetime,
            timeout=60):
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
        if storage_type is not None:
            params["storageBackend"] = storage_type
        if durability_min_level is not None:
            params["durabilityMinLevel"] = durability_min_level
        if history_retention_bytes is not None:
            params["historyRetentionBytes"] = history_retention_bytes
        if history_retention_seconds is not None:
            params["historyRetentionSeconds"] = history_retention_seconds
        if history_retention_default is not None:
            params["historyRetentionCollectionDefault"] = one_zero_boolean_to_string(history_retention_default)
        if rank is not None:
            params["rank"] = rank

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
                params["allowedTimePeriod[abortOutside]"] = one_zero_boolean_to_string(abort_outside)
            if paralleldb_and_view_compact is not None:
                params["parallelDBAndViewCompaction"] = one_zero_boolean_to_string(paralleldb_and_view_compact)

        if vbuckets is not None:
            params["numVBuckets"] = vbuckets

        if bucket_type != "memcached" and purge_interval is not None:
            params["purgeInterval"] = purge_interval

        if encryption_key is not None:
            params["encryptionAtRestKeyId"] = encryption_key
        if dek_rotation_interval is not None:
            params["encryptionAtRestDekRotationInterval"] = dek_rotation_interval
        if dek_lifetime is not None:
            params["encryptionAtRestDekLifetime"] = dek_lifetime

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

                nodes = content["nodes"]
                if len(nodes) == 0:
                    time.sleep(1)
                    continue

                all_node_ready = True
                for node in nodes:
                    if node["status"] != "healthy":
                        all_node_ready = False
                        break
                if not all_node_ready:
                    time.sleep(1)

            if not all_node_ready:
                return None, [f'Bucket created, but not ready after {timeout} seconds']

        return result, None

    def edit_bucket(self, name, memory_quota, durability_min_level, eviction_policy,
                    replicas, threads_number, flush_enabled, max_ttl,
                    compression_mode, remove_port, db_frag_perc, db_frag_size, view_frag_perc,
                    view_frag_size, from_hour, from_min, to_hour, to_min,
                    abort_outside, paralleldb_and_view_compact, purge_interval,
                    history_retention_bytes, history_retention_seconds, history_retention_default,
                    rank, encryption_key, dek_rotation_interval, dek_lifetime,
                    couchbase_bucket: bool = True):
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
                paralleldb_and_view_compact is not None or purge_interval is not None) and couchbase_bucket:
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
            params["allowedTimePeriod[abortOutside]"] = one_zero_boolean_to_string(abort_outside)
        if paralleldb_and_view_compact is not None:
            params["parallelDBAndViewCompaction"] = one_zero_boolean_to_string(paralleldb_and_view_compact)
        if purge_interval is not None:
            params["purgeInterval"] = purge_interval
        if durability_min_level is not None:
            params["durabilityMinLevel"] = durability_min_level
        if history_retention_bytes is not None:
            params["historyRetentionBytes"] = history_retention_bytes
        if history_retention_seconds is not None:
            params["historyRetentionSeconds"] = history_retention_seconds
        if history_retention_default is not None:
            params["historyRetentionCollectionDefault"] = one_zero_boolean_to_string(history_retention_default)
        if rank is not None:
            params["rank"] = rank

        if encryption_key is not None:
            params["encryptionAtRestKeyId"] = encryption_key
        if dek_rotation_interval is not None:
            params["encryptionAtRestDekRotationInterval"] = dek_rotation_interval
        if dek_lifetime is not None:
            params["encryptionAtRestDekLifetime"] = dek_lifetime

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
        if view_only and not data_only:
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
        if not data_only and not view_only:
            url = f'{self.hostname}/pools/default/buckets/{name}/controller/compactBucket'
            return self._post_form_encoded(url, None)

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

    def node_init(self, hostname=None, afamily=None, data_path=None,
                  index_path=None, cbas_path=None, eventing_path=None,
                  java_home=None):
        url = f'{self.hostname}/nodeInit'
        params = dict()

        if hostname is not None:
            params["hostname"] = hostname

        if afamily is not None:
            params["afamily"] = afamily

        if data_path is not None:
            params["dataPath"] = data_path

        if index_path is not None:
            params["indexPath"] = index_path

        if cbas_path is not None:
            params["analyticsPath"] = cbas_path

        if eventing_path is not None:
            params["eventingPath"] = eventing_path

        if java_home is not None:
            params["javaHome"] = java_home

        return self._post_form_encoded(url, params)

    def cluster_init(self, services=None, username=None, password=None, port=None,
                     cluster_name=None, data_ramsize=None, index_ramsize=None,
                     fts_ramsize=None, cbas_ramsize=None, eventing_ramsize=None, query_ramsize=None,
                     ipfamily=None, ipfamilyonly=None, encryption=None,
                     indexer_storage_mode=None,
                     send_stats=None):
        url = f'{self.hostname}/clusterInit'
        params = dict()

        if services is not None:
            params["services"] = services

        if username is not None:
            params["username"] = username

        if password is not None:
            params["password"] = password

        if port is not None:
            params["port"] = port
        else:
            params["port"] = "SAME"

        if cluster_name is not None:
            params["clusterName"] = cluster_name

        if data_ramsize is not None:
            params["memoryQuota"] = data_ramsize

        if index_ramsize is not None:
            params["indexMemoryQuota"] = index_ramsize

        if fts_ramsize is not None:
            params["ftsMemoryQuota"] = fts_ramsize

        if cbas_ramsize is not None:
            params["cbasMemoryQuota"] = cbas_ramsize

        if eventing_ramsize is not None:
            params["eventingMemoryQuota"] = eventing_ramsize

        if query_ramsize is not None:
            params['queryMemoryQuota'] = query_ramsize

        if ipfamily is not None:
            params["afamily"] = ipfamily

        if ipfamilyonly is not None:
            params["afamilyOnly"] = "true" if ipfamilyonly else "false"

        if encryption is not None:
            params["nodeEncryption"] = encryption

        if indexer_storage_mode is not None:
            params["indexerStorageMode"] = indexer_storage_mode

        if send_stats is not None:
            params["sendStats"] = "true" if send_stats else "false"

        return self._post_form_encoded(url, params)

    def node_info(self):
        url = f'{self.hostname}/nodes/self'
        return self._get(url)

    def reset_node(self):
        url = f'{self.hostname}/controller/hardResetNode'
        return self._post_form_encoded(url, None)

    def get_babysitter_cookie(self):
        url = f'{self.hostname}/diag/eval'
        payload = \
            '{json, atom_to_binary(ns_server:get_babysitter_cookie(), latin1)}.'
        return self._post_form_encoded(url, payload)

    def get_ns_server_cookie(self):
        url = f'{self.hostname}/diag/eval'
        payload = \
            '{json, atom_to_binary(erlang:get_cookie(), latin1)}.'
        return self._post_form_encoded(url, payload)

    def stop_rebalance(self):
        params = {"allowUnsafe": "true"}
        url = f'{self.hostname}/controller/stopRebalance'
        return self._post_form_encoded(url, params)

    def create_server_group(self, name):
        url = f'{self.hostname}/pools/default/serverGroups'
        params = {"name": name}
        return self._post_form_encoded(url, params)

    def delete_server_group(self, name):
        uri, errors = self._get_server_group_uri(name)
        if errors:
            return None, errors

        url = self.hostname + uri
        params = {"name": name}
        return self._delete(url, params)

    def rename_server_group(self, name, new_name):
        uri, errors = self._get_server_group_uri(name)
        if errors:
            return None, errors

        url = self.hostname + uri
        params = {"name": new_name}
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
        if errors and errors[0] == 'Unknown user.':
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
            params['roles'] = roles
        elif 'roles' in defaults:
            params['roles'] = self._format_user_roles(defaults['roles'])
        if groups is not None:
            params['groups'] = groups
        elif 'groups' in defaults:
            params['groups'] = ','.join(defaults['groups'])

        return self._put(url, params)

    def lock_rbac_user(self, username, lock):
        url = f'{self.hostname}/settings/rbac/users/local/{username}'
        params = {'locked': 'true' if lock else 'false'}
        return self._patch_form_encoded(url, params)

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
        if errors and errors[0] == 'Unknown group.':
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

    def set_security_settings(self, disable_http_ui, cluster_encryption_level, tls_min_version, honor_order,
                              cipher_suites, disable_www_authenticate, hsts_max_age, hsts_preload,
                              hsts_includeSubDomains):
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
        if disable_www_authenticate:
            params['disableWWWAuthenticate'] = disable_www_authenticate

        if any([hsts_max_age, hsts_preload, hsts_includeSubDomains]):
            hsts = []
            if hsts_max_age:
                hsts.append(f'max-age={hsts_max_age}')
            if hsts_preload:
                hsts.append('preload')
            if hsts_includeSubDomains:
                hsts.append('includeSubDomains')
            params['responseHeaders'] = f'{{"Strict-Transport-Security":"{";".join(hsts)}"}}'

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
                params["enforceUppercase"] = "false"
        if lower_case:
            if lower_case == "1":
                params["enforceLowercase"] = "true"
            else:
                params["enforceLowercase"] = "false"
        if digit:
            if digit == "1":
                params["enforceDigits"] = "true"
            else:
                params["enforceDigits"] = "false"
        if special_char:
            if special_char == "1":
                params["enforceSpecialChars"] = "true"
            else:
                params["enforceSpecialChars"] = "false"

        return self._post_form_encoded(url, params)

    def set_audit_settings(self, enabled, log_path, rotate_interval, rotate_size, disabled, disabled_users, prune_age):
        url = f'{self.hostname}/settings/audit'

        params = dict()
        if enabled:
            params["auditdEnabled"] = one_zero_boolean_to_string(enabled)
        if log_path:
            params["logPath"] = log_path
        if rotate_interval:
            params["rotateInterval"] = rotate_interval
        if rotate_size:
            params["rotateSize"] = rotate_size
        if disabled is not None:
            params["disabled"] = disabled
        if disabled_users is not None:
            params["disabledUsers"] = disabled_users
        if prune_age is not None:
            params["pruneAge"] = prune_age

        return self._post_form_encoded(url, params)

    def get_audit_settings(self):
        return self._get(f'{self.hostname}/settings/audit')

    def get_id_descriptors(self):
        return self._get(f'{self.hostname}/settings/audit/descriptors')

    def set_autofailover_settings(self, enabled, timeout, max_count, failover_on_data_disk_issues_enabled,
                                  failover_on_data_disk_issues_time_period,
                                  failover_on_data_disk_non_responsive_enabled,
                                  failover_on_data_disk_non_responsive_time_period,
                                  can_abort_rebalance, allow_failover_for_ephemeral_without_replica):
        url = self.hostname + '/settings/autoFailover'

        params = dict()
        if enabled:
            params["enabled"] = enabled
        if timeout:
            params["timeout"] = timeout
        if failover_on_data_disk_issues_enabled:
            params["failoverOnDataDiskIssues[enabled]"] = failover_on_data_disk_issues_enabled
        if failover_on_data_disk_issues_time_period:
            params["failoverOnDataDiskIssues[timePeriod]"] = failover_on_data_disk_issues_time_period
        if failover_on_data_disk_non_responsive_enabled:
            params["failoverOnDataDiskNonResponsiveness[enabled]"] = \
                failover_on_data_disk_non_responsive_enabled
        if failover_on_data_disk_non_responsive_time_period:
            params["failoverOnDataDiskNonResponsiveness[timePeriod]"] = \
                failover_on_data_disk_non_responsive_time_period
        if max_count:
            params["maxCount"] = max_count
        if can_abort_rebalance:
            params["canAbortRebalance"] = can_abort_rebalance
        if allow_failover_for_ephemeral_without_replica:
            params["allowFailoverEphemeralNoReplicas"] = allow_failover_for_ephemeral_without_replica

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

    def set_compaction_settings(self, db_frag_perc, db_frag_size, view_frag_perc, view_frag_size,
                                from_hour, from_min, to_hour, to_min, abortOutside,
                                parallel_db_and_view_compact, purge_interval, gsi_mode, gsi_perc,
                                gsi_interval, gsi_from_hour, gsi_from_min, gsi_to_hour, gsi_to_min,
                                enable_gsi_abort):
        url = f'{self.hostname}/controller/setAutoCompaction'
        params = dict()

        if db_frag_perc is not None:
            params["databaseFragmentationThreshold[percentage]"] = db_frag_perc
        if db_frag_size is not None:
            params["databaseFragmentationThreshold[size]"] = db_frag_size
        if view_frag_perc is not None:
            params["viewFragmentationThreshold[percentage]"] = view_frag_perc
        if view_frag_size is not None:
            params["viewFragmentationThreshold[size]"] = view_frag_size
        if from_hour is not None:
            params["allowedTimePeriod[fromHour]"] = from_hour
        if from_min is not None:
            params["allowedTimePeriod[fromMinute]"] = from_min
        if to_hour is not None:
            params["allowedTimePeriod[toHour]"] = to_hour
        if to_min is not None:
            params["allowedTimePeriod[toMinute]"] = to_min
        if abortOutside is not None:
            params["allowedTimePeriod[abortOutside]"] = abortOutside
        if parallel_db_and_view_compact is not None:
            params["parallelDBAndViewCompaction"] = parallel_db_and_view_compact
        if purge_interval is not None:
            params["purgeInterval"] = purge_interval
        if gsi_mode is not None:
            params["indexCompactionMode"] = gsi_mode
        if gsi_perc is not None:
            params["indexFragmentationThreshold[percentage]"] = gsi_perc
        if gsi_interval is not None:
            params["indexCircularCompaction[daysOfWeek]"] = gsi_interval
        if gsi_from_hour is not None:
            params["indexCircularCompaction[interval][fromHour]"] = gsi_from_hour
        if gsi_from_min is not None:
            params["indexCircularCompaction[interval][fromMinute]"] = gsi_from_min
        if gsi_to_hour is not None:
            params["indexCircularCompaction[interval][toHour]"] = gsi_to_hour
        if gsi_to_min is not None:
            params["indexCircularCompaction[interval][toMinute]"] = gsi_to_min
        if enable_gsi_abort is not None:
            params["indexCircularCompaction[interval][abortOutside]"] = enable_gsi_abort

        return self._post_form_encoded(url, params)

    def set_index_settings(self, storage_mode, max_rollback_points, stable_snap_interval,
                           mem_snap_interval, threads, log_level, replicas, optimize):
        """ Sets global index settings"""
        params = dict()
        if storage_mode is not None:
            params["storageMode"] = storage_mode
        if max_rollback_points is not None:
            params["maxRollbackPoints"] = max_rollback_points
        if stable_snap_interval is not None:
            params["stableSnapshotInterval"] = stable_snap_interval
        if mem_snap_interval is not None:
            params["memorySnapshotInterval"] = mem_snap_interval
        if threads is not None:
            params["indexerThreads"] = threads
        if log_level is not None:
            params["logLevel"] = log_level
        if replicas is not None:
            params["numReplica"] = replicas
        if optimize is not None:
            params["redistributeIndexes"] = one_zero_boolean_to_string(optimize)

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
        params = {"enabled": enabled}

        if read_only_admins is not None:
            params["roAdmins"] = read_only_admins
        if admins is not None:
            params["admins"] = admins

        return self._post_form_encoded(url, params)

    def get_ldap(self):
        url = f'{self.hostname}/settings/ldap'
        return self._get(url)

    def ldap_settings(self, authentication_enabled, authorization_enabled, hosts, port, encryption, user_dn_mapping,
                      timeout, max_parallel, max_cache, cache_lifetime, query_dn, query_pass, cert, key, group_query,
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
        if cert:
            params['clientTLSCert'] = cert
        if key:
            params['clientTLSKey'] = key
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

    def retrieve_cluster_certificates(self):
        """ Retrieves the current cluster certificates"""
        url = f'{self.hostname}/pools/default/certificates'
        return self._get(url)

    def retrieve_cluster_ca(self):
        """ Retrieves the current cluster CAs"""
        url = f'{self.hostname}/pools/default/trustedCAs'
        return self._get(url)

    def load_cluster_ca(self, node):
        """ Load a CA from ./inbox/CA on a node. Making it available to all nodes in the cluster"""
        url = f'{node}/node/controller/loadTrustedCAs'
        return self._post_form_encoded(url, None)

    def delete_cluster_ca(self, ca_id):
        """ Deletes ta CA from the cluster via ID"""
        url = f'{self.hostname}/pools/default/trustedCAs/{ca_id}'
        return self._delete(url, None)

    def upload_cluster_certificate(self, certificate):
        """ Uploads a new cluster certificate, In 7.1 this endpoint is deprecated"""
        url = f'{self.hostname}/controller/uploadClusterCA'
        return self._post_form_encoded(url, certificate)

    def regenerate_cluster_certificate(self):
        """ Regenerates the cluster certificate

        Regenerates the cluster certificate and returns the new certificate."""
        url = f'{self.hostname}/controller/regenerateCertificate'
        return self._post_form_encoded(url, None)

    def retrieve_node_certificate(self, node):
        """ Retrieves the current node certificate

        Returns the current node certificate"""
        url = f'{self.hostname}/pools/default/certificate/node/{node}'
        return self._get(url)

    def set_node_certificate(self, pkey_settings):
        """Activates the current node certificate

        Grabs chain.pem and pkey.pem from the <data folder>/inbox/ directory and
        applies them to the node. chain.pem contains the chain encoded certificates
        starting from the node certificat and ending with the last intermediate
        certificate before cluster CA. pkey.pem contains the pem encoded private
        key for node certifiactes. Both files should exist on the server before
        this API is called."""
        params = {}

        if pkey_settings:
            params["privateKeyPassphrase"] = pkey_settings

        return self._post_json(f'{self.hostname}/node/controller/reloadCertificate', params)

    def set_client_cert_auth(self, config):
        """Enable/disable the client cert auth"""
        url = f'{self.hostname}/settings/clientCertAuth'
        return self._post_json(url, config)

    def retrieve_client_cert_auth(self):
        url = f'{self.hostname}/settings/clientCertAuth'
        return self._get(url)

    def create_xdcr_reference(self, name, hostname, hostname_external, username, password, encrypted,
                              encryption_type, certificate, client_certificate, client_key):
        return self._set_xdcr_reference(False, name, hostname, hostname_external, username,
                                        password, encrypted, encryption_type,
                                        certificate, client_certificate, client_key)

    def edit_xdcr_reference(self, name, hostname, hostname_external, username, password, encrypted,
                            encryption_type, certificate, client_certificate, client_key):
        return self._set_xdcr_reference(True, name, hostname, hostname_external, username,
                                        password, encrypted, encryption_type,
                                        certificate, client_certificate, client_key)

    def _set_xdcr_reference(self, edit, name, hostname, hostname_external, username, password,
                            encrypted, encryption_type, certificate, client_certificate, client_key):
        url = f'{self.hostname}/pools/default/remoteClusters'
        params = {}

        if edit:
            url += f'/{urllib.parse.quote(name)}'

        if name is not None:
            params["name"] = name
        if hostname is not None:
            params["hostname"] = hostname
        if hostname_external:
            params["network_type"] = "external"
        if username is not None:
            params["username"] = username
        if password is not None:
            params["password"] = password
        if encrypted is not None:
            params["demandEncryption"] = encrypted
        if encryption_type is not None:
            params["encryptionType"] = encryption_type
        if certificate is not None:
            params["certificate"] = certificate
        if client_certificate:
            params['clientCertificate'] = client_certificate
        if client_key:
            params['clientKey'] = client_key

        return self._post_form_encoded(url, params)

    def delete_xdcr_reference(self, name):
        url = f'{self.hostname}/pools/default/remoteClusters/{urllib.parse.quote(name)}'
        return self._delete(url, None)

    def list_xdcr_references(self):
        url = f'{self.hostname}/pools/default/remoteClusters/'
        return self._get(url)

    def get_xdcr_replicator_settings(self, replicator_id: str):
        """Retrieves the settings for a XDCR replication with id 'replicator_id'"""
        return self._get(f'{self.hostname}/settings/replications/{urllib.parse.quote_plus(replicator_id)}')

    def xdcr_replicator_settings(
            self,
            chk_interval,
            worker_batch_size,
            doc_batch_size,
            fail_interval,
            replication_thresh,
            src_nozzles,
            dst_nozzles,
            usage_limit,
            compression,
            log_level,
            stats_interval,
            replicator_id,
            filter_expression,
            filter_skip,
            priority,
            reset_expiry,
            filter_del,
            filter_exp,
            filter_binary,
            col_explicit_mappings,
            col_migration_mode,
            col_mapping_rule):

        url = f'{self.hostname}/settings/replications/{urllib.parse.quote_plus(replicator_id)}'
        params = self._get_xdcr_params(chk_interval, worker_batch_size, doc_batch_size,
                                       fail_interval, replication_thresh, src_nozzles,
                                       dst_nozzles, usage_limit, compression, log_level,
                                       stats_interval)
        if filter is not None:
            params['filterExpression'] = filter_expression
            filter_numeric = "0"
            if filter_skip:
                filter_numeric = "1"
            params['filterSkipRestream'] = filter_numeric
        if priority:
            params['priority'] = priority
        if reset_expiry:
            params['filterBypassExpiry'] = one_zero_boolean_to_string(reset_expiry)
        if filter_del:
            params['filterDeletion'] = one_zero_boolean_to_string(filter_del)
        if filter_exp:
            params['filterExpiration'] = one_zero_boolean_to_string(filter_exp)
        if filter_binary:
            params['filterBinary'] = one_zero_boolean_to_string(filter_binary)
        if col_explicit_mappings is not None:
            params['collectionsExplicitMapping'] = one_zero_boolean_to_string(col_explicit_mappings)
        if col_migration_mode is not None:
            params['collectionsMigrationMode'] = one_zero_boolean_to_string(col_migration_mode)
        if col_mapping_rule is not None:
            params['colMappingRules'] = col_mapping_rule

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

    @staticmethod
    def _get_xdcr_params(chk_interval, worker_batch_size, doc_batch_size,
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

    def create_xdcr_replication(self, name, to_bucket, from_bucket, chk_interval, worker_batch_size, doc_batch_size,
                                fail_interval, replication_thresh, src_nozzles, dst_nozzles, usage_limit, compression,
                                log_level, stats_interval, filter_expression, priority, reset_expiry, filter_del,
                                filter_exp, filter_binary, col_explicit_mappings, col_migration_mode, col_mapping_rule):
        url = f'{self.hostname}/controller/createReplication'
        params = self._get_xdcr_params(chk_interval, worker_batch_size, doc_batch_size,
                                       fail_interval, replication_thresh, src_nozzles,
                                       dst_nozzles, usage_limit, compression, log_level,
                                       stats_interval)

        params['replicationType'] = 'continuous'
        if to_bucket is not None:
            params["toBucket"] = to_bucket
        if name is not None:
            params["toCluster"] = name
        if from_bucket is not None:
            params["fromBucket"] = from_bucket
        if filter is not None:
            params['filterExpression'] = filter_expression
        if priority:
            params['priority'] = priority
        if reset_expiry:
            params['filterBypassExpiry'] = one_zero_boolean_to_string(reset_expiry)
        if filter_del:
            params['filterDeletion'] = one_zero_boolean_to_string(filter_del)
        if filter_binary:
            params['filterBinary'] = one_zero_boolean_to_string(filter_binary)
        if filter_exp:
            params['filterExpiration'] = one_zero_boolean_to_string(filter_exp)
        if col_explicit_mappings is not None:
            params['collectionsExplicitMapping'] = one_zero_boolean_to_string(col_explicit_mappings)
        if col_migration_mode is not None:
            params['collectionsMigrationMode'] = one_zero_boolean_to_string(col_migration_mode)
        if col_mapping_rule is not None:
            params['colMappingRules'] = col_mapping_rule

        return self._post_form_encoded(url, params)

    def delete_xdcr_replicator(self, replicator_id):
        url = f'{self.hostname}/controller/cancelXDCR/{urllib.parse.quote_plus(replicator_id)}'
        return self._delete(url, None)

    def pause_xdcr_replication(self, replicator_id):
        url = f'{self.hostname}/settings/replications/{urllib.parse.quote_plus(replicator_id)}'
        params = {"pauseRequested": "true"}
        return self._post_form_encoded(url, params)

    def resume_xdcr_replication(self, replicator_id):
        url = f'{self.hostname}/settings/replications/{urllib.parse.quote_plus(replicator_id)}'
        params = {"pauseRequested": "false"}
        return self._post_form_encoded(url, params)

    def get_query_settings(self):
        url = f'{self.hostname}/settings/querySettings'
        return self._get(url)

    def post_query_settings(self, pipeline_batch, pipeline_cap, scan_cap, timeout, prepared_limit, completed_limit,
                            complete_threshold, log_level, max_parallelism, n1ql_feature_control, temp_dir,
                            temp_dir_max_size, cbo, memory_quota, tx_timeout, node_quota, node_quota_val_percent,
                            use_replica, num_cpus):
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
        if temp_dir is not None:
            params['queryTmpSpaceDir'] = temp_dir
        if temp_dir_max_size is not None:
            params['queryTmpSpaceSize'] = temp_dir_max_size
        if cbo is not None:
            params['queryUseCBO'] = one_zero_boolean_to_string(cbo)
        if memory_quota is not None:
            params['queryMemoryQuota'] = memory_quota
        if tx_timeout is not None:
            params['queryTxTimeout'] = tx_timeout
        if node_quota is not None:
            params['queryNodeQuota'] = node_quota
        if node_quota_val_percent is not None:
            params['queryNodeQuotaValPercent'] = node_quota_val_percent
        if use_replica is not None:
            params['queryUseReplica'] = use_replica
        if num_cpus is not None:
            params['queryNumCpus'] = num_cpus

        return self._post_form_encoded(url, params)

    def post_query_curl_access_settings(self, restricted: bool, allowed_urls: Optional[List[str]],
                                        disallowed_urls: Optional[List[str]]):
        """POST query curl access settings."""
        url = f'{self.hostname}/settings/querySettings/curlAllowlist'
        params: Dict[str, Any] = {'all_access': not restricted}
        if allowed_urls is not None:
            params['allowed_urls'] = allowed_urls
        if disallowed_urls is not None:
            params['disallowed_urls'] = disallowed_urls

        return self._post_json(url, params)

    def list_functions(self):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)
        url = f'{hosts[0]}/api/v1/functions'
        return self._get(url)

    def get_functions_status(self):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)
        url = f'{hosts[0]}/api/v1/status'
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

    def delete_function(self, function, bucket, scope):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)
        url = f'{hosts[0]}/api/v1/functions/{urllib.parse.quote_plus(function)}' \
            f'?bucket={urllib.parse.quote_plus(bucket)}&scope={urllib.parse.quote_plus(scope)}'
        return self._delete(url, None)

    def pause_resume_function(self, function_name, bucket, scope, pause):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)

        url = f"{hosts[0]}/api/v1/functions/{urllib.parse.quote_plus(function_name)}/{'pause' if pause else 'resume'}" \
            f"?bucket={urllib.parse.quote_plus(bucket)}&scope={urllib.parse.quote_plus(scope)}"
        return self._post_json(url, None)

    def deploy_undeploy_function(self, function, bucket, scope, deploy, boundary):
        hosts, errors = self.get_hostnames_for_service(EVENT_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(EVENT_SERVICE)

        params = {}

        if deploy and boundary:
            params["feed-boundary"] = boundary

        url = f'{hosts[0]}/api/v1/functions/{urllib.parse.quote_plus(function)}/{"deploy" if deploy else "undeploy"}' \
            f'?bucket={urllib.parse.quote_plus(bucket)}&scope={urllib.parse.quote_plus(scope)}'
        return self._post_json(url, params)

    def create_analytics_link(self, opts):
        return self._set_analytics_link(False, opts.scope, opts.name, self._get_analytics_link_params(opts))

    def edit_analytics_link(self, opts):
        return self._set_analytics_link(True, opts.scope, opts.name, self._get_analytics_link_params(opts))

    @staticmethod
    def _get_analytics_link_params(opts):
        params = {}
        if opts.type:
            params["type"] = opts.type

        # couchbase link params
        if opts.hostname:
            params["hostname"] = opts.hostname
        if opts.encryption:
            params["encryption"] = opts.encryption
        if opts.certificates:
            params["certificate"] = '\n'.join(opts.certificates)
        if opts.user_key:
            params['clientKey'] = opts.user_key
        if opts.user_key_passphrase:
            params['clientKeyPassphrase'] = opts.user_key_passphrase
        if opts.user_certificate:
            params['clientCertificate'] = opts.user_certificate
        if opts.link_username:
            params["username"] = opts.link_username
        if opts.link_password:
            params["password"] = opts.link_password

        # s3 link params
        if opts.access_key_id:
            params['accessKeyId'] = opts.access_key_id
        if opts.secret_access_key:
            params['secretAccessKey'] = opts.secret_access_key
        if opts.session_token:
            params['sessionToken'] = opts.session_token
        if opts.region:
            params['region'] = opts.region
        if opts.service_endpoint:
            params['serviceEndpoint'] = opts.service_endpoint

        # azure blob and datalake link params
        if opts.account_name:
            params['accountName'] = opts.account_name
        if opts.account_key:
            params['accountKey'] = opts.account_key
        if opts.shared_access_signature:
            params['sharedAccessSignature'] = opts.shared_access_signature
        if opts.managed_identity_id:
            params['managedIdentityId'] = opts.managed_identity_id
        if opts.client_id:
            params['clientId'] = opts.client_id
        if opts.client_secret:
            params['clientSecret'] = opts.client_secret
        if opts.client_certificate:
            params['clientCertificate'] = opts.client_certificate
        if opts.client_certificate_password:
            params['clientCertificatePassword'] = opts.client_certificate_password
        if opts.tenant_id:
            params['tenantId'] = opts.tenant_id
        if opts.endpoint:
            params['endpoint'] = opts.endpoint

        # gcs link params
        if opts.application_default_credentials:
            params['applicationDefaultCredentials'] = opts.application_default_credentials
        if opts.json_credentials:
            params['jsonCredentials'] = opts.json_credentials

        return params

    def _set_analytics_link(self, edit, scope, name, params):
        hosts, errors = self.get_hostnames_for_service(CBAS_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(CBAS_SERVICE)

        url = f'{hosts[0]}/analytics/link/{urllib.parse.quote_plus(scope)}/{urllib.parse.quote_plus(name)}'

        return self._put(url, params) if edit else self._post_form_encoded(url, params)

    def delete_analytics_link(self, scope, name):
        hosts, errors = self.get_hostnames_for_service(CBAS_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(CBAS_SERVICE)

        url = f'{hosts[0]}/analytics/link/{urllib.parse.quote_plus(scope)}/{urllib.parse.quote_plus(name)}'

        return self._delete(url, None)

    def list_analytics_links(self, scope, name, link_type):
        hosts, errors = self.get_hostnames_for_service(CBAS_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(CBAS_SERVICE)

        url = f'{hosts[0]}/analytics/link'

        params = {}
        if scope:
            url += f'/{urllib.parse.quote_plus(scope)}'
            if name:
                url += f'/{urllib.parse.quote_plus(name)}'

        if link_type:
            params["type"] = link_type

        return self._get(url, params)

    def get_backup_service_settings(self):
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        return self._get(f'{hosts[0]}/api/v1/config')

    def patch_backup_service_settings(self, rotation_period=None, rotation_size=None):
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        params = {}
        if rotation_period is not None:
            params['history_rotation_period'] = rotation_period
        if rotation_size is not None:
            params['history_rotation_size'] = rotation_size

        return self._patch_json(f'{hosts[0]}/api/v1/config', params)

    def get_backup_service_repositories(self, cluster='self', state=None):
        """List all backup repositories in the given state

        Args:
            cluster (str): Only 'self' is supported.
            state (str): The state of repositories to retrieve

        Returns:
            A list of repositories and None if successful. Otherwise none a list of strings denoting the errors.
        """
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        if state not in ['active', 'archived', 'imported']:
            return None, [f'Invalid backup repository state {state}']

        return self._get(f'{hosts[0]}/api/v1/cluster/{cluster}/repository/{state}')

    def get_backup_service_repository(self, repository_id, state, cluster='self'):
        """Retrieves a single repository from the backup service

        Args:
            repository_id (str): The repository id to be retrieved
            state (str): The state of the repository to retrieve
            cluster (str): Only 'self' is supported.
        """
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        return self._get(f'{hosts[0]}/api/v1/cluster/{cluster}/repository/{state}/{repository_id}')

    def archive_backup_repository(self, repository_id, new_id, cluster='self'):
        """Archive an active repository

        Args:
            repository_id (str): The repository id to be retrieved
            new_id (str): The id that will be given to the archived repository
            cluster (str): Only 'self' is supported.
        """
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        return self._post_json(f'{hosts[0]}/api/v1/cluster/{cluster}/repository/active/{repository_id}/archive',
                               {'id': new_id})

    def add_backup_active_repository(self, repository_id: str, body: Dict[str, Any], cluster: str = 'self'):
        """Archive an active repository

        Args:
            repository_id (str): The id to be given to the new repository.
            body (dict): The add active repository request.
            cluster (str): Only 'self' is supported.
        """
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        return self._post_json(f'{hosts[0]}/api/v1/cluster/{cluster}/repository/active/{repository_id}', body)

    def list_backup_plans(self):
        """Retrieves all the backup plans from the backup service"""
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        return self._get(f'{hosts[0]}/api/v1/plan')

    def get_backup_plan(self, name: str):
        """Retrieves a plan by name"""
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        return self._get(f'{hosts[0]}/api/v1/plan/{name}')

    def delete_backup_plan(self, name: str):
        """Deletes a backup plan by name"""
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        return self._delete(f'{hosts[0]}/api/v1/plan/{name}', None)

    def delete_backup_repository(self, repository_id: str, state: str, delete_repo: bool, cluster: str = 'self'):
        """Delete a backup repository
        Args:
            repository_id (str): The id to be deleted.
            state (str): The state in which the isntance to be deleted is.
            delete_repo (bool): Wheter or not to delete the backup Repository.
            cluster (str): Only 'self' is supported.
        """
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)
        url = f'{hosts[0]}/api/v1/cluster/{cluster}/repository/{state}/{repository_id}?remove_repository={delete_repo!s}'
        return self._delete(url, None)

    def add_backup_plan(self, name: str, plan: Dict[str, Any]):
        """Adds a new backup plan

        Args:
            name (str): The name to give to the new plan if it already exists it will fail.
            plan (object): The plan to add.
        """
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        return self._post_json(f'{hosts[0]}/api/v1/plan/{name}', plan)

    def get_backup_node_threads_map(self):
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        url = f'{hosts[0]}/api/v1/nodesThreadsMap'
        return self._get(url)

    def post_backup_node_threads_map(self, threadsMap: Dict[str, int]):
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        url = f'{hosts[0]}/api/v1/nodesThreadsMap'
        return self._post_json(url, threadsMap)

    def patch_backup_node_threads_map(self, threadsMap: Dict[str, int]):
        hosts, errors = self.get_hostnames_for_service(BACKUP_SERVICE)
        if errors:
            return None, errors

        if not hosts:
            raise ServiceNotAvailableException(BACKUP_SERVICE)

        url = f'{hosts[0]}/api/v1/nodesThreadsMap'

        return self._patch_json(url, threadsMap)

    def create_scope(self, bucket, scope):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/scopes'
        params = {"name": scope}
        return self._post_form_encoded(url, params)

    def drop_scope(self, bucket, scope):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/scopes/'\
            f'{urllib.parse.quote_plus(scope)}'
        return self._delete(url, None)

    def create_collection(self, bucket, scope, collection, max_ttl, enable_history):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/scopes/' \
            f'{urllib.parse.quote_plus(scope)}/collections'
        params = {"name": collection}
        if max_ttl is not None:
            params["maxTTL"] = max_ttl
        if enable_history:
            params["history"] = one_zero_boolean_to_string(enable_history)

        return self._post_form_encoded(url, params)

    def edit_collection(self, bucket, scope, collection, max_ttl, enable_history):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/scopes/' \
            f'{urllib.parse.quote_plus(scope)}/collections/{urllib.parse.quote_plus(collection)}'
        params = {}
        if enable_history:
            params["history"] = one_zero_boolean_to_string(enable_history)
        if max_ttl is not None:
            params["maxTTL"] = max_ttl

        return self._patch_form_encoded(url, params)

    def drop_collection(self, bucket, scope, collection):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/scopes/' \
            f'{urllib.parse.quote_plus(scope)}/collections/{urllib.parse.quote_plus(collection)}'
        return self._delete(url, None)

    def get_manifest(self, bucket):
        url = f'{self.hostname}/pools/default/buckets/{urllib.parse.quote_plus(bucket)}/scopes'
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

    def disable_unused_external_listeners(self, host=None, ipfamily=None, encryption=None):
        hostname = host if host else self.hostname
        url = f'{hostname}/node/controller/disableUnusedExternalListeners'
        res, err = self._post_form_encoded(url, None)
        if err is None or err[0] != 'Requested resource not found.\r\n':
            return res, err

        # Backward compatibility: 6.5 nodes don't have
        # disableUnusedExternalListeners api yet. Call previous api then.
        return self.disable_external_listener(host=host, ipfamily=ipfamily, encryption=encryption)

    def setup_net_config(self, host=None, ipfamily=None, encryption=None, ipfamilyonly=None):
        hostname = host if host else self.hostname
        url = f'{hostname}/node/controller/setupNetConfig'
        params = {}
        if ipfamily:
            params['afamily'] = ipfamily
            params['afamilyOnly'] = 'true' if ipfamilyonly else 'false'
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
        if err and err[0] == 'unknown pool':
            result, err = self.node_info()
            if err:
                return None, err
            return [result], None
        if err:
            return None, err

        return node_data['nodes'], None

    def node_version(self):
        """
        Returns node implementationVersion from /pools.
        Should work even if cluster is not initialised yet.
        """
        node_info, err = self.pools()
        if err:
            return None, err

        return node_info["implementationVersion"].split(
            "-")[0], None if "implementationVersion" in node_info else (None, "unexpected JSON returned from /pools")

    def min_version(self):
        data, err = self.pools("default")
        if err:
            return None, err

        min_version = data["nodes"][0]["version"].split("-")[0]

        for node in data["nodes"][1:]:
            node_version = node["version"].split("-")[0]

            if node_version < min_version:
                min_version = node_version

        return min_version, None

    def get_analytics_settings(self):
        """Gets the Analytics Service Settings """
        url = f'{self.hostname}/settings/analytics'
        return self._get(url)

    def set_analytics_settings(self, replicas=None):
        """ Set the Analytics Settings
        Args:
            replicas (int): The number of replicas.
        """
        url = f'{self.hostname}/settings/analytics'
        params = {
            'numReplicas': replicas
        }
        params = dict(filter(lambda x: x[1], params.items()))
        return self._post_form_encoded(url, params)

    # Low level methods for basic HTML operations

    @classmethod
    def _url_encode_params(cls, params):
        return urllib.parse.urlencode(params if params is not None else {})

    @classmethod
    def _json_encode_params(cls, params):
        return json.dumps(params if params is not None else {})

    @request
    def _get(self, url, params=None):
        if self.debug:
            print(f'GET {url} {self._url_encode_params(params)}')

        return self._handle_response(self.session.get(url, params=params, auth=(self.username, self.password),
                                                      verify=self.ca_cert, timeout=self.timeout, headers=self.headers))

    @request
    def _post_form_encoded(self, url, params):
        if self.debug:
            if params is not None and isinstance(params, str):
                print(f'POST {url} {params}')
            else:
                print(f'POST {url} {self._url_encode_params(params)}')

        return self._handle_response(self.session.post(url, auth=(self.username, self.password), data=params,
                                                       verify=self.ca_cert, timeout=self.timeout, headers=self.headers))

    @request
    def _post_json(self, url, params):
        if self.debug:
            print(f'POST {url} {self._json_encode_params(params)}')

        return self._handle_response(self.session.post(url, auth=(self.username, self.password), json=params,
                                                       verify=self.ca_cert, timeout=self.timeout, headers=self.headers))

    @request
    def _patch_form_encoded(self, url, params):
        if self.debug:
            print(f'PATCH {url} {self._url_encode_params(params)}')

        return self._handle_response(
            self.session.patch(
                url, auth=(
                    self.username,
                    self.password),
                data=params,
                verify=self.ca_cert,
                timeout=self.timeout,
                headers=self.headers))

    @request
    def _patch_json(self, url, params):
        if self.debug:
            print(f'PATCH {url} {self._json_encode_params(params)}')

        return self._handle_response(self.session.patch(url, auth=(self.username, self.password), json=params,
                                                        verify=self.ca_cert, timeout=self.timeout,
                                                        headers=self.headers))

    @request
    def _put(self, url, params):
        if self.debug:
            print(f'PUT {url} {self._url_encode_params(params)}')

        return self._handle_response(self.session.put(url, params, auth=(self.username, self.password),
                                                      verify=self.ca_cert, timeout=self.timeout, headers=self.headers))

    @request
    def _put_json(self, url, params):
        if self.debug:
            print(f'PUT {url} {self._json_encode_params(params)}')

        return self._handle_response(self.session.put(url, None, auth=(self.username, self.password), json=params,
                                                      verify=self.ca_cert, timeout=self.timeout, headers=self.headers))

    @request
    def _delete(self, url, params):
        if self.debug:
            print(f'DELETE {url} {self._url_encode_params(params)}')

        return self._handle_response(self.session.delete(url, auth=(self.username, self.password), data=params,
                                                         verify=self.ca_cert, timeout=self.timeout,
                                                         headers=self.headers))

    def _handle_response(self, response):
        if self.debug:
            output = str(response.status_code)
            if response.headers:
                output += f', {response.headers}'
            if response.content:
                response.encoding = 'utf-8'
                output += f', {response.content}'
            print(output)
        if response.status_code in [200, 202, 204]:
            if 'Content-Type' not in response.headers:
                return "", None
            if not response.content:
                return "", None
            if 'application/json' in response.headers['Content-Type']:
                return response.json(), None
            else:
                response.encoding = 'utf-8'
                return response.text, None
        elif response.status_code in [400, 404, 405, 409]:
            if 'Content-Type' not in response.headers:
                return None, ["Not a Couchbase Server, please check hostname and port"]
            if 'application/json' in response.headers['Content-Type']:
                errors = response.json()
                if isinstance(errors, list):
                    return None, errors
                if "errors" in errors and isinstance(errors["errors"], list):
                    return None, errors["errors"]
                if "errors" in errors and isinstance(errors["errors"], dict):
                    return None, [f"{key} - {str(value)}" for key, value in errors["errors"].items()]
                return None, [errors]
            return None, [response.text]
        elif response.status_code == 401:
            return None, [ERR_AUTH]
        elif response.status_code == 403:
            if 'Content-Type' not in response.headers:
                return None, ["Not a Couchbase Server, please check hostname and port"]
            if 'application/json' in response.headers['Content-Type']:
                errors = response.json()
                if 'message' in errors and 'permissions' in errors:
                    return None, [errors['message'] + ": " + ", ".join(errors["permissions"])]
                elif 'runtime_info' in errors and 'info' in errors['runtime_info']:
                    return None, [errors['runtime_info']['info']]
            return None, [f"Unexpected response for error 403: {response.text}"]
        # Error codes from Eventing service
        elif response.status_code in [406, 422, 423]:
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
                    if 'code' in error and error['code'] == couchbaseConstants.ERR_APP_DEPLOYED:
                        rv.append(error['info'])
                return None, rv
            else:
                return None, ['Received unexpected status %d' % response.status_code]
        elif response.status_code == 500:
            return None, [ERR_INTERNAL]
        else:
            return None, ['Received unexpected status %d' % response.status_code]
