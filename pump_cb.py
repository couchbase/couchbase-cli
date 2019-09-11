#!/usr/bin/env python3

import logging
import json
import time
import urllib.request, urllib.parse, urllib.error

from typing import Optional, Tuple, List, Dict, Union, Any

import couchbaseConstants
import cb_bin_client
import pump
import pump_mc

from cluster_manager import ClusterManager, ServiceNotAvailableException


def _to_string(str_or_bytes: Union[str, bytes]) -> str:
    if isinstance(str_or_bytes, bytes):
        return str_or_bytes.decode()
    return str_or_bytes


class CBSink(pump_mc.MCSink):
    DDOC_HEAD = "_design/"

    """Smart client sink to couchbase cluster."""
    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        if spec.startswith("https://"):
            setattr(opts, "ssl", True)
        super(CBSink, self).__init__(opts, spec, source_bucket, source_node,
                                     source_map, sink_map, ctl, cur)

        self.rehash = opts.extra.get("rehash", 0)

    def add_start_event(self, conn: Optional[cb_bin_client.MemcachedClient]) -> couchbaseConstants.PUMP_ERROR:
        sasl_user = str(self.source_bucket.get("name", pump.get_username(self.opts.username)))
        event = {"timestamp": self.get_timestamp(),
                 "real_userid": {"source": "internal",
                                 "user": pump.returnString(sasl_user),
                                },
                 "mode": getattr(self.opts, "mode", "diff"),
                 "source_bucket": pump.returnString(self.source_bucket['name']),
                 "source_node": pump.returnString(self.source_node['hostname']),
                 "target_bucket": pump.returnString(self.sink_map['buckets'][0]['name'])
                }
        if conn:
            try:
                conn.audit(couchbaseConstants.AUDIT_EVENT_RESTORE_SINK_START, json.dumps(event))
            except Exception as e:
                logging.warning(f'auditing error: {e}')
        return 0

    def add_stop_event(self, conn: Optional[cb_bin_client.MemcachedClient]) -> couchbaseConstants.PUMP_ERROR:
        sasl_user = str(self.source_bucket.get("name", pump.get_username(self.opts.username)))
        event = {"timestamp": self.get_timestamp(),
                 "real_userid": {"source": "internal",
                                 "user": pump.returnString(sasl_user)
                                },
                 "source_bucket": pump.returnString(self.source_bucket['name']),
                 "source_node": pump.returnString(self.source_node['hostname']),
                 "target_bucket": pump.returnString(self.sink_map['buckets'][0]['name'])
                }
        if conn:
            try:
                conn.audit(couchbaseConstants.AUDIT_EVENT_RESTORE_SINK_STOP, json.dumps(event))
            except Exception as e:
                logging.warning(f'auditing error: {e}')
        return 0

    def scatter_gather(self, mconns: Dict[str, cb_bin_client.MemcachedClient], batch: pump.Batch) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch], Optional[bool]]:
        sink_map_buckets = self.sink_map['buckets']
        if len(sink_map_buckets) != 1:
            return "error: CBSink.run() expected 1 bucket in sink_map", None, None

        vbuckets_num = len(sink_map_buckets[0]['vBucketServerMap']['vBucketMap'])
        vbuckets = batch.group_by_vbucket_id(vbuckets_num, self.rehash)

        # Scatter or send phase.
        for vbucket_id, msgs in vbuckets.items():
            rv, conn = self.find_conn(mconns, vbucket_id, msgs)
            if rv != 0:
                return rv, None, None
            if conn is not None:
                rv, skipped = self.send_msgs(conn, msgs, self.operation(),
                                    vbucket_id=vbucket_id)
            if rv != 0:
                return rv, None, None

        # Yield to let other threads do stuff while server's processing.
        time.sleep(0.01)

        retry_batch = None
        need_refresh = False

        # Gather or recv phase.
        for vbucket_id, msgs in vbuckets.items():
            rv, conn = self.find_conn(mconns, vbucket_id, msgs)
            if rv != 0:
                return rv, None, None
            if conn is not None:
                rv, retry, refresh = self.recv_msgs(conn, msgs, skipped, vbucket_id=vbucket_id)
            if rv != 0:
                return rv, None, None
            if retry:
                retry_batch = batch
            if refresh:
                need_refresh = True

        if need_refresh:
            self.refresh_sink_map()

        return 0, retry_batch, retry_batch is not None and not need_refresh

    @staticmethod
    def map_recovery_buckets(sink_map: Dict[str, Any], bucket_name: str, vbucket_list: str):
        """When we do recovery of vbuckets the vbucket map is not up to date, but
        ns_server does tell us where the destination vbuckets are. This function
        looks at the recovery plan and modifies the vbucket map in order to ensure
        that we send the recovery data to the right server."""
        vbucket_list_dict: Dict[Any, Any] = json.loads(vbucket_list)
        if type(vbucket_list_dict) is not dict:
            return "Expected recovery map to be a dictionary"

        server_vb_map = None
        bucket_info = None
        for bucket in sink_map["buckets"]:
            if bucket["name"] == bucket_name:
                bucket_info = bucket
                server_vb_map = bucket["vBucketServerMap"]

        if bucket_info is not None:
            for node in bucket_info["nodes"]:
                if "direct" not in node["ports"]:
                    continue
                otpNode = _to_string(node["otpNode"])
                mcdHost = otpNode.split("@")[1] + ":" + str(node["ports"]["direct"])
                for remap_node, remap_vbs in vbucket_list_dict.items():
                    if _to_string(remap_node) == otpNode and mcdHost in server_vb_map["serverList"]:  # type: ignore
                        idx = server_vb_map["serverList"].index(mcdHost)  # type: ignore
                        for vb in remap_vbs:  # type: ignore
                            server_vb_map["vBucketMap"][vb][0] = idx  # type: ignore

        return None

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        return (spec.startswith("http://") or
                spec.startswith("couchbase://") or
                spec.startswith("https://"))

    @staticmethod
    def check_source(opts, source_class, source_spec: str, sink_class, sink_spec: str) -> couchbaseConstants.PUMP_ERROR:
        if source_spec.startswith("http://") or source_spec.startswith("couchbase://"):
            return 0
        return pump.Sink.check_source(opts, source_class, source_spec,
                                      sink_class, sink_spec)

    @staticmethod
    def check(opts, spec: str, source_map: Dict[str, Any]) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                    Optional[Dict[str, Any]]]:
        rv, sink_map = pump.rest_couchbase(opts, spec,
                                           opts.username_dest is not None and opts.password_dest is not None)
        if rv != 0:
            return rv, None

        if sink_map is None:
            return rv, None

        rv, source_bucket_name = pump.find_source_bucket_name(opts, source_map)
        if rv != 0:
            return rv, None
        rv, sink_bucket_name = pump.find_sink_bucket_name(opts, source_bucket_name)
        if rv != 0:
            return rv, None

        # Adjust sink_map['buckets'] to have only our sink_bucket.
        sink_buckets = [bucket for bucket in sink_map['buckets']
                        if pump.returnString(bucket['name']) == pump.returnString(sink_bucket_name)]
        if not sink_buckets:
            return f'error: missing bucket-destination: {sink_bucket_name} at destination: {spec};' \
                f' perhaps your username/password is missing or incorrect', None
        if len(sink_buckets) != 1:
            return f'error: multiple buckets with name:{sink_bucket_name} at destination: {spec}', None
        sink_map['buckets'] = sink_buckets
        if opts.extra.get("allow_recovery_vb_remap", 0) == 1:
            error = CBSink.map_recovery_buckets(sink_map, sink_bucket_name, opts.vbucket_list)
            if error is not None:
                return error, None

        return 0, sink_map

    def refresh_sink_map(self) -> couchbaseConstants.PUMP_ERROR:
        """Grab a new vbucket-server-map."""
        logging.warning(f'refreshing sink map: {self.spec}')
        rv, new_sink_map = CBSink.check(self.opts, self.spec, self.source_map)
        if rv == 0:
            self.sink_map = new_sink_map
        return rv

    @staticmethod
    def consume_fts_index(opts, sink_spec: str, sink_map, source_bucket,
                          source_map, source_design: Union[str, bytes]) -> couchbaseConstants.PUMP_ERROR:
        if not source_design:
            return 0

        try:
            index_defs = json.loads(source_design)
            if not index_defs:
                return 0
        except ValueError as e:
            return f'error: could not parse fts index definitions; exception: {e!s}'

        try:
            username = opts.username
            password = opts.password
            if opts.username_dest is not None and opts.password_dest is not None:
                username = opts.username_dest
                password = opts.password_dest
            rest = ClusterManager(sink_spec, username, password, opts.ssl, opts.no_ssl_verify,
                                  opts.cacert, False)
            _, errors = rest.restore_fts_index_metadata(index_defs)
            return errors
        except ServiceNotAvailableException as e:
            return "No fts service in cluster, skipping restore of aliases"

    @staticmethod
    def consume_fts_alias(opts, sink_spec: str, sink_map, source_bucket,
                          source_map, source_design: Union[str, bytes]) -> couchbaseConstants.PUMP_ERROR:
        if not source_design:
            return 0
        try:
            index_defs = json.loads(source_design)
            if not index_defs:
                return 0
        except ValueError as e:
            return f'error: could not parse fts index definitions; exception: {e!s}'

        try:
            username = opts.username
            password = opts.password
            if opts.username_dest is not None and opts.password_dest is not None:
                username = opts.username_dest
                password = opts.password_dest
            rest = ClusterManager(sink_spec, username, password, opts.ssl, opts.no_ssl_verify,
                                  opts.cacert, False)
            _, errors = rest.restore_fts_index_metadata(index_defs)
            return errors
        except ServiceNotAvailableException as e:
            return "No fts service in cluster, skipping restore of indexes"

    @staticmethod
    def consume_index(opts, sink_spec: str, sink_map, source_bucket,
                      source_map, source_design: Union[str, bytes]) -> couchbaseConstants.PUMP_ERROR:
        if not source_design:
            return 0

        try:
            sd = json.loads(source_design)
            if not sd:
               return 0
        except ValueError as e:
            return f'error: could not parse source design; exception: {e!s}'

        try:
            sink_bucket = sink_map['buckets'][0]
            username = opts.username
            password = opts.password
            if opts.username_dest is not None and opts.password_dest is not None:
                username = opts.username_dest
                password = opts.password_dest
            rest = ClusterManager(sink_spec, username, password, opts.ssl, opts.no_ssl_verify,
                                  opts.cacert, False)
            _, errors = rest.restore_index_metadata(sink_bucket['name'], sd)
            return errors
        except ServiceNotAvailableException as e:
            return "No index service in cluster, skipping restore of indexes"

    @staticmethod
    def consume_design(opts, sink_spec: str, sink_map, source_bucket,
                       source_map, source_design: Union[str, bytes]) -> couchbaseConstants.PUMP_ERROR:
        if not source_design:
            return 0
        try:
            sd = json.loads(source_design)
        except ValueError as e:
            return f'error: could not parse source design; exception: {e!s}'
        if not sd:
            return 0

        if (not sink_map['buckets'] or
            len(sink_map['buckets']) != 1 or
            not sink_map['buckets'][0] or
            not sink_map['buckets'][0]['name']):
            return "error: design sink incorrect sink_map bucket"
        spec_parts = pump.parse_spec(opts, sink_spec, 8091)
        if not spec_parts:
            return "error: design sink no spec_parts: " + sink_spec
        sink_bucket = sink_map['buckets'][0]
        sink_nodes = pump.filter_bucket_nodes(sink_bucket, spec_parts) or \
            sink_bucket['nodes']
        if not sink_nodes:
            return "error: design sink nodes missing"
        couch_api_base = sink_nodes[0].get('couchApiBase')
        if not couch_api_base:
            return f'error: cannot restore bucket design on a couchbase cluster that does not support couch API;' \
                f' the couchbase cluster may be an older, pre-2.0 version; please check your cluster URL: {sink_spec}'
        host, port, user, pswd, path = \
            pump.parse_spec(opts, couch_api_base, 8092)
        if user is None:
            user = spec_parts[2] # Default to the main REST user/pwsd.
            pswd = spec_parts[3]

        if opts.username_dest is not None and opts.password_dest is not None:
            user = opts.username_dest
            user = opts.password_dest
        if type(sd) is dict:

            id = sd.get('_id', None)
            if id:
                str_source = _to_string(source_design)
                err, conn, response = \
                    pump.rest_request(host, int(port), user, pswd, opts.ssl,
                                      f'{path}/{id}', method='PUT', body=str_source,
                                      reason="consume_design", verify=opts.no_ssl_verify, ca_cert=opts.cacert)
                if conn:
                    conn.close()
                if err:
                    return f'error: could not restore design doc id: {id}; response: {response}; err: {err}'
            else:
                stmts = sd.get('statements', [])
                hostname = f'http://{spec_parts[0]}:{spec_parts[1]!s}'
                cm = ClusterManager(hostname, user, pswd, opts.ssl)
                try:
                    for stmt in stmts:
                        result, errors = cm.n1ql_query(stmt['statement'], stmt.get('args', None))
                        if errors:
                            logging.error(f'N1QL query {stmt["statement"]} failed due to {errors}')

                        if result and 'errors' in result:
                            for error in result['errors']:
                                logging.error(f'N1QL query {stmt["statement"]} failed due to error `{error["msg"]}`')
                except ServiceNotAvailableException as e:
                    logging.error("Failed to restore indexes, cluster does not contain a query node")
        elif type(sd) is list:
            for row in sd:
                logging.debug(f'design_doc row: {row!s}')

                doc = row.get('doc', None)
                if not doc:
                    stmt = row.get('statement', None)
                    if not stmt:
                        return f'error: missing design doc or index statement in row: {row}'
                    else:
                        #publish index
                        return 0

                if 'json' in doc and 'meta' in doc:
                    js = doc['json']
                    id = doc['meta'].get('id', None)
                    if not id:
                        return f'error: missing id for design doc: {row}'
                else:
                    # Handle design-doc from 2.0DP4.
                    js = doc
                    if '_rev' in js:
                        del js['_rev']
                    id = row.get('id', None)
                    if not id:
                        return f'error: missing id for row: {row}'

                js_doc = json.dumps(js)
                if id.startswith(CBSink.DDOC_HEAD):
                    id = CBSink.DDOC_HEAD + urllib.parse.quote(id[len(CBSink.DDOC_HEAD):], '')
                else:
                    id = urllib.parse.quote(id, '')
                logging.debug(f'design_doc: {js_doc}')
                logging.debug(f'design_doc id: {id} at: {path}/{id}')

                try:
                    err, conn, response = \
                        pump.rest_request(host, int(port), user, pswd, opts.ssl,
                                          f'{path}/{id}', method='PUT', body=js_doc,
                                          reason="consume_design", verify=opts.no_ssl_verify, ca_cert=opts.cacert)
                    if conn:
                        conn.close()
                    if err:
                        return f'error: could not restore design doc id: {id}; response: {response}; err: {err}'
                except Exception as e:
                    return f'error: design sink exception: {e}; couch_api_base: {couch_api_base}'

                logging.debug(f'design_doc created at: {path}/{id}')

        return 0

    def find_conn(self, mconns: Dict[str, cb_bin_client.MemcachedClient], vbucket_id: int, msgs) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[cb_bin_client.MemcachedClient]]:
        bucket = self.sink_map['buckets'][0]

        vBucketMap = bucket['vBucketServerMap']['vBucketMap']
        serverList = bucket['vBucketServerMap']['serverList']

        if vbucket_id > len(vBucketMap):
            return f'error: map missing vbucket_id: {vbucket_id!s}' \
                f'; perhaps your source does not have vbuckets; if so, try using moxi (HOST:11211) as a destination',\
                   None

        # Primary server for a vbucket_id is the 0'th entry.
        host_port = serverList[vBucketMap[vbucket_id][0]]
        server_host, _ = pump.hostport(host_port)
        if getattr(self, 'alt_add', None):
            host = None
            port = None
            for n in bucket['nodes']:
                if 'alternateAddresses' not in n:
                    # we could raise an error if one of the nodes does not have an alternate address
                    continue

                node_host = '127.0.0.1'
                if 'hostname' in n:
                    node_host, _ = pump.hostport(n['hostname'])

                # if server does not match continue
                if node_host != server_host:
                    continue

                # if correct node check alternate address
                host = n['alternateAddresses']['external']['hostname']
                if self.opts.ssl:
                    if 'kvSSL' in n['alternateAddresses']['external']['ports']:
                        port = n['alternateAddresses']['external']['ports']['kvSSL']
                    else:
                        return 'Host does not have a secure data port', None
                elif 'kv' in n['alternateAddresses']['external']['ports']:
                    port = n['alternateAddresses']['external']['ports']['kv']
                else:
                    return 'Host does not have data service', None
                break

            if host is None or port is None:
                return 'No alternate address information found for host "{}"'.format(host_port), None

            # wrap ipv6 address with []
            if ':' in host and not host.startswith('['):
                host = '[' + host + ']'

            try:
                port_int = int(port)
            except ValueError:
                return 'Invalid port "{}"'.format(port), None

            host_port = host + ":" + str(port)

        logging.debug('Conencting to host "{}" for vbucket {}'.format(host_port, vbucket_id))

        conn = mconns.get(host_port, None)
        if conn is None:
            host, port = pump.hostport(host_port, 11210)
            if self.opts.ssl:
                port = couchbaseConstants.SSL_PORT
            bucket = bucket['name']
            username = self.opts.username
            password = self.opts.password
            if self.opts.username_dest is not None and self.opts.password_dest is not None:
                username = self.opts.username_dest
                password = self.opts.password_dest
            rv, conn = CBSink.connect_mc(host, port, username, password, bucket, self.opts.ssl,
                                         self.opts.no_ssl_verify, self.opts.cacert,
                                         collections=self.opts.collection!=None)
            if rv != 0:
                logging.error(f'error: CBSink.connect() for send: {rv}')
                return rv, None
            if conn is not None:
                mconns[host_port] = conn
                self.add_start_event(conn)
        return 0, conn
