#!/usr/bin/env python3

import logging
import os
import queue
import random
import json
import socket
import struct
import time
import threading
import select

from typing import Dict, List, Any, Optional, Tuple, Union

import cb_bin_client
import couchbaseConstants
import pump
import pump_mc
import pump_cb

from cluster_manager import ClusterManager, ServiceNotAvailableException

bool_to_str = lambda value: str(bool(int(value))).lower()


class DCPStreamSource(pump.Source, threading.Thread):
    """Can read from cluster/server/bucket via DCP streaming."""
    HIGH_SEQNO = "high_seqno"
    VB_UUID = "uuid"
    ABS_HIGH_SEQNO = "abs_high_seqno"
    PURGE_SEQNO = "purge_seqno"

    HIGH_SEQNO_BYTE = 8
    UUID_BYTE = 8

    def __init__(self, opts, spec: str, source_bucket, source_node: Dict[str, Any],
                 source_map, sink_map, ctl: Dict[str, Any], cur: Dict[str, Any]):
        if spec.startswith("https://"):
            setattr(opts, "ssl", True)
        super(DCPStreamSource, self).__init__(opts, spec, source_bucket, source_node,
                                            source_map, sink_map, ctl, cur)
        threading.Thread.__init__(self)

        self.dcp_done: bool = False
        self.dcp_conn: Optional[cb_bin_client.MemcachedClient] = None
        self.mem_conn: Optional[cb_bin_client.MemcachedClient] = None
        self.dcp_name: str = opts.process_name
        self.ack_last: bool = False
        self.cmd_last: Optional[int] = None
        self.num_msg: int = 0
        # change made to support development version
        self.version_supported: bool = self.source_node['version'].split(".") >= ["3", "0", "0"] or \
                                 (self.source_node['version'].split(".") == ['0', '0', '0-0000-enterprise'])
        self.recv_min_bytes: int = int(opts.extra.get("recv_min_bytes", 4096))
        self.batch_max_bytes: int = int(opts.extra.get("batch_max_bytes", 400000))
        self.flow_control = int(opts.extra.get("flow_control", 1))
        self.vbucket_list = getattr(opts, "vbucket_list", None)
        self.r=random.Random()
        self.queue_size = int(opts.extra.get("dcp_consumer_queue_length", 1000))
        self.response: queue.Queue = queue.Queue(self.queue_size)
        self.running = False
        self.stream_list: Dict[Any, Any] = {}
        self.unack_size = 0
        self.node_vbucket_map: Optional[List[int]] = None

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        return (spec.startswith("http://") or
                spec.startswith("couchbase://") or
                spec.startswith("https://")
                )

    @staticmethod
    def check(opts, spec: str)-> Tuple[couchbaseConstants.PUMP_ERROR, Optional[Dict[str, Any]]]:
        err, map = pump.rest_couchbase(opts, spec)
        if err:
            return err, map
        if not map or not map.get('buckets'):
            return f'error: no buckets supporting DCP at source: {spec};' \
                f' please check your username/password to the cluster', None
        return 0, map

    @staticmethod
    def provide_design(opts, source_spec: str, source_bucket: Dict[str, Any], source_map) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        # Ephemeral buckets do not have design docs
        if source_bucket['bucketType'] == 'ephemeral':
            return 0, None
        spec_parts = source_map.get('spec_parts')
        if not spec_parts:
            return "error: no design spec_parts", None
        host, port, user, pswd, path = spec_parts

        source_nodes = pump.filter_bucket_nodes(source_bucket, spec_parts)
        if not source_nodes:
            source_nodes = source_bucket['nodes']
            if not source_nodes:
                return f'error: no design source node; spec_parts: {spec_parts}', None

        couch_api_base = source_nodes[0].get('couchApiBase')
        if not couch_api_base:
            return 0, None  # No couchApiBase; probably not 2.0.

        err, ddocs_json, ddocs = \
            pump.rest_request_json(host, int(port), user, pswd, opts.ssl,
                                   "/pools/default/buckets/%s/ddocs" %
                                   (source_bucket['name']),
                                   reason="provide_design", verify=opts.no_ssl_verify, cacert=opts.cacert)
        if err and "response: 404" in err: # A 404/not-found likely means 2.0-DP4.
            ddocs_json = None
            ddocs_url = f'{couch_api_base}/_all_docs'
            ddocs_qry = "?startkey=\"_design/\"&endkey=\"_design0\"&include_docs=true"

            host, port, user, pswd, path = \
                pump.parse_spec(opts, ddocs_url, 8092)
            # Not using user/pwd as 2.0-DP4 CAPI did not support auth.
            err, ddocs_json, ddocs = \
                pump.rest_request_json(host, int(port), None, None, opts.ssl,
                                       path + ddocs_qry,
                                       reason="provide_design-2.0DP4", verify=opts.no_ssl_verify, cacert=opts.cacert)
        if err is not None:
            return err, None

        if ddocs is None:
            return 0, None

        if not ddocs.get('rows', None):
            return 0, None
        else:
            return 0, json.dumps(ddocs.get('rows', []))

    @staticmethod
    def provide_index(opts, source_spec: str, source_bucket: Dict[str, Any], source_map) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        try:
            rest = ClusterManager(source_spec, opts.username, opts.password, opts.ssl, opts.no_ssl_verify,
                                  opts.cacert, False)
            result, errors = rest.get_index_metadata(source_bucket['name'])
            if errors:
                return errors, None
            return 0, json.dumps(result["result"])
        except ServiceNotAvailableException as e:
            return 0, None

    @staticmethod
    def provide_fts_index(opts, source_spec: str, source_bucket: Dict[str, Any], source_map) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        try:
            rest = ClusterManager(source_spec, opts.username, opts.password, opts.ssl, opts.no_ssl_verify,
                                  opts.cacert, False)
            result, errors = rest.get_fts_index_metadata(source_bucket['name'])
            if errors:
                return errors, None
            return 0, json.dumps(result)
        except ServiceNotAvailableException as e:
            return 0, None

    @staticmethod
    def provide_fts_alias(opts, source_spec: str, source_bucket: Dict[str, Any], source_map) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        try:
            rest = ClusterManager(source_spec, opts.username, opts.password, opts.ssl, opts.no_ssl_verify,
                                  opts.cacert, False)
            result, errors = rest.get_fts_index_alias()
            if errors:
                return errors, None
            return 0, json.dumps(result)
        except ServiceNotAvailableException as e:
            return 0, None

    def get_conflict_resolution_type(self) -> str:
        confResType = "seqno"
        if "conflictResolutionType" in self.source_bucket:
            confResType = self.source_bucket["conflictResolutionType"]
        return confResType

    def add_start_event(self, conn: Optional[cb_bin_client.MemcachedClient]) -> couchbaseConstants.PUMP_ERROR:
        sasl_user = str(self.source_bucket.get("name"))
        event = {"timestamp": self.get_timestamp(),
                 "real_userid": {"source": "internal",
                                 "user": pump.returnString(sasl_user),
                                },
                 "mode": pump.returnString(getattr(self.opts, "mode", "diff")),
                 "source_bucket": pump.returnString(self.source_bucket['name']),
                 "source_node": pump.returnString(self.source_node['hostname'])
                }
        if conn:
            try:
                conn.audit(couchbaseConstants.AUDIT_EVENT_BACKUP_START, json.dumps(event))
            except Exception as e:
                logging.warning(f'auditing error: {e}')
        return 0

    def add_stop_event(self, conn: Optional[cb_bin_client.MemcachedClient]) -> couchbaseConstants.PUMP_ERROR:
        sasl_user = str(self.source_bucket.get("name"))
        event = {"timestamp": self.get_timestamp(),
                 "real_userid": {"source": "internal",
                                 "user": pump.returnString(sasl_user)
                                },
                 "source_bucket": pump.returnString(self.source_bucket['name']),
                 "source_node": pump.returnString(self.source_node['hostname'])
                }
        if conn:
            try:
                conn.audit(couchbaseConstants.AUDIT_EVENT_BACKUP_STOP, json.dumps(event))
            except Exception as e:
                logging.warning(f'auditing error: {e}')
        return 0

    def build_node_vbucket_map(self) -> Optional[List[int]]:
        if "vBucketServerMap" in self.source_bucket:
            server_list = self.source_bucket["vBucketServerMap"]["serverList"]
            vbucket_map = self.source_bucket["vBucketServerMap"]["vBucketMap"]
        else:
            return None

        node_vbucket_map = []
        nodename = f'{self.source_node.get("hostname", "N/A").split(":")[0]}:{self.source_node["ports"]["direct"]!s}'
        nodeindex = -1
        for index, node in enumerate(server_list):
            if nodename == node:
                nodeindex = index
                break
        for vbucket_id, vblist in enumerate(vbucket_map):
            if vblist[0] >= 0 and vblist[0] == nodeindex:
                node_vbucket_map.append(vbucket_id)
        return node_vbucket_map

    def provide_batch(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch]]:
        alternate_add = getattr(self, 'alt_add', None)
        if not self.version_supported:
            return "error: cannot back up 2.x or older clusters with 3.x tools", None

        cur_sleep = 0.2
        cur_retry = 0
        max_retry = self.opts.extra['max_retry']
        if not self.node_vbucket_map:
            self.node_vbucket_map = self.build_node_vbucket_map()

        while True:
            if self.dcp_done:
                if self.dcp_conn:
                    self.add_stop_event(self.dcp_conn)
                    self.dcp_conn.close()
                    self.dcp_conn = None
                return 0, None

            rv = self.get_dcp_conn()
            if rv != 0:
                self.dcp_done = True
                return rv, None

            rv, batch = self.provide_dcp_batch_actual()
            if rv == 0:
                return 0, batch

            if self.dcp_conn:
                self.dcp_conn.close()
                self.dcp_conn = None

            if cur_retry > max_retry:
                self.dcp_done = True
                return rv, batch

            logging.warning(f'backoff: {cur_retry}, sleeping: {cur_sleep}, on error: {rv}')
            time.sleep(cur_sleep)
            cur_sleep = min(cur_sleep * 2, 20) # Max backoff sleep 20 seconds.
            cur_retry = cur_retry + 1

    def provide_dcp_batch_actual(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch]]:
        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']
        delta_ack_size = batch_max_bytes * 10 / 4 #ack every 25% of buffer size
        last_processed = 0
        total_bytes_read = 0

        vbid: int = 0
        start_seqno: int = 0
        end_seqno: int = 0
        vb_uuid: int = 0
        hi_seqno: int = 0
        ss_start_seqno: int = 0
        ss_end_seqno: int = 0
        no_response_count: int = 0
        try:
            while (not self.dcp_done and
                   batch.size() < batch_max_size and
                   batch.bytes < batch_max_bytes):

                if self.response.empty():
                    if len(self.stream_list) > 0:
                        logging.debug(f'no response while there {len(self.stream_list)} active streams')
                        time.sleep(.25)
                        no_response_count = no_response_count + 1
                        # if not had a response after a minimum of 30 seconds then state we are done
                        if no_response_count == 120:
                            logging.warning(f'no response for 30 seconds while there {len(self.stream_list)} active streams')
                            self.dcp_done = True
                    else:
                        self.dcp_done = True
                    continue

                no_response_count = 0
                unprocessed_size = total_bytes_read - last_processed
                if unprocessed_size > delta_ack_size:
                    rv = self.ack_buffer_size(unprocessed_size)
                    if rv:
                        logging.error(rv)
                    else:
                        last_processed = total_bytes_read

                cmd, errcode, opaque, cas, keylen, extlen, data, datalen, dtype, bytes_read = \
                    self.response.get()  # type: int, int, int, int, int, int, bytes, int, int, int
                total_bytes_read += bytes_read
                rv = 0
                metalen = flags = flg = exp = 0
                key = val = ext = b''
                need_ack = False
                seqno = 0
                if cmd == couchbaseConstants.CMD_DCP_REQUEST_STREAM:
                    total_bytes_read -= bytes_read
                    if errcode == couchbaseConstants.ERR_SUCCESS:
                        pair_index = (self.source_bucket['name'], self.source_node['hostname'])
                        start = 0
                        step = DCPStreamSource.HIGH_SEQNO_BYTE + DCPStreamSource.UUID_BYTE
                        while start+step <= datalen:
                            uuid, seqno = struct.unpack(
                                            couchbaseConstants.DCP_VB_UUID_SEQNO_PKT_FMT,
                                            data[start:start + step])
                            if pair_index not in self.cur['failoverlog']:
                                self.cur['failoverlog'][pair_index] = {}
                            if opaque not in self.cur['failoverlog'][pair_index] or \
                               not self.cur['failoverlog'][pair_index][opaque]:
                                self.cur['failoverlog'][pair_index][opaque] = [(uuid, seqno)]
                            else:
                                self.cur['failoverlog'][pair_index][opaque].append((uuid, seqno))
                            start = start + step
                    elif errcode == couchbaseConstants.ERR_KEY_ENOENT:
                        logging.warn("producer doesn't know about the vbucket uuid, rollback to 0")
                        vbid, flags, start_seqno, end_seqno, vb_uuid, ss_start_seqno, ss_end_seqno = \
                            self.stream_list[opaque]
                        del self.stream_list[opaque]
                    elif errcode == couchbaseConstants.ERR_KEY_EEXISTS:
                       logging.warning(f'a stream exists on the connection for vbucket: {opaque}')
                    elif errcode ==  couchbaseConstants.ERR_NOT_MY_VBUCKET:
                        logging.warning(f'Vbucket is not active anymore, skip it:{vbid!s}')
                        del self.stream_list[opaque]
                    elif errcode == couchbaseConstants.ERR_ERANGE:
                        logging.warning(f'Start or end sequence numbers specified incorrectly,({start_seqno},'
                                        f' {end_seqno})')
                        del self.stream_list[opaque]
                    elif errcode == couchbaseConstants.ERR_ROLLBACK:
                        vbid, flags, start_seqno, end_seqno, vb_uuid, ss_start_seqno, ss_stop_seqno = \
                            self.stream_list[opaque]
                        start_seqno, = struct.unpack(couchbaseConstants.DCP_VB_SEQNO_PKT_FMT, data)
                        #find the most latest uuid, hi_seqno that fit start_seqno
                        if self.cur['failoverlog']:
                            pair_index = (self.source_bucket['name'], self.source_node['hostname'])
                            if self.cur['failoverlog'][pair_index].get("vbid"):
                                for uuid, seqno in self.cur['failoverlog'][pair_index][vbid]:
                                    if start_seqno >= seqno:
                                        vb_uuid = uuid
                                        break
                        ss_start_seqno = start_seqno
                        ss_end_seqno = start_seqno
                        self.request_dcp_stream(vbid, flags, start_seqno, end_seqno, vb_uuid, ss_start_seqno,
                                                ss_end_seqno)

                        del self.stream_list[opaque]
                        self.stream_list[opaque] = \
                            (vbid, flags, start_seqno, end_seqno, vb_uuid, ss_start_seqno, ss_end_seqno)
                    else:
                        logging.error(f'unprocessed errcode: {errcode}')
                        del self.stream_list[opaque]
                elif cmd == couchbaseConstants.CMD_DCP_MUTATION:
                    vbucket_id = errcode
                    seqno, rev_seqno, flg, exp, locktime, metalen, nru = \
                        struct.unpack(couchbaseConstants.DCP_MUTATION_PKT_FMT, data[0:extlen])
                    key_start = extlen
                    val_start = key_start + keylen
                    val_len = datalen- keylen - metalen - extlen
                    meta_start = val_start + val_len
                    key = data[extlen:val_start]
                    val = data[val_start:meta_start]
                    conf_res = 0
                    if meta_start < datalen:
                        # handle extra conflict resolution fields
                        extra_meta = data[meta_start:]
                        extra_index = 0
                        version = extra_meta[extra_index]
                        extra_index += 1
                        while extra_index < metalen:
                            id, extlen = struct.unpack(couchbaseConstants.DCP_EXTRA_META_PKG_FMT, extra_meta[extra_index:extra_index+3])
                            extra_index += 3
                            if id == couchbaseConstants.DCP_EXTRA_META_CONFLICT_RESOLUTION:
                                if extlen == 1:
                                    conf_res, = struct.unpack(">B", extra_meta[extra_index:extra_index+1])
                                elif extlen == 2:
                                    conf_res, = struct.unpack(">H", extra_meta[extra_index:extra_index+2])
                                elif extlen == 4:
                                    conf_res, = struct.unpack(">I", extra_meta[extra_index:extra_index+4])
                                elif extlen == 8:
                                    conf_res, = struct.unpack(">Q", extra_meta[extra_index:extra_index+8])
                                else:
                                    logging.error(f'unsupported extra meta data format: {extlen:d}')
                                    conf_res = 0
                            extra_index += extlen

                    if not self.skip(key, vbucket_id):
                        msg = (cmd, vbucket_id, key, flg, exp, cas, rev_seqno.to_bytes(4, 'big'), val, seqno, dtype, metalen,
                               conf_res)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                elif cmd == couchbaseConstants.CMD_DCP_DELETE or cmd == couchbaseConstants.CMD_DCP_EXPIRATION:
                    vbucket_id = errcode
                    seqno, rev_seqno, metalen = struct.unpack(couchbaseConstants.DCP_DELETE_PKT_FMT, data[0:extlen])
                    key_start = extlen
                    val_start = key_start + keylen
                    key = data[extlen:val_start]
                    # If the delete has the Xattr data type get the Xattrs from the body
                    if dtype & couchbaseConstants.DATATYPE_HAS_XATTR:
                        val = data[val_start:]
                    if not self.skip(key, vbucket_id):
                        msg = (cmd, vbucket_id, key, flg, exp, cas, rev_seqno.to_bytes(4, 'big'), val, seqno, dtype, metalen, 0)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                    if cmd == couchbaseConstants.CMD_DCP_DELETE:
                        batch.adjust_size += 1
                elif cmd == couchbaseConstants.CMD_DCP_FLUSH:
                    total_bytes_read -= bytes_read
                    logging.warning("stopping: saw CMD_DCP_FLUSH")
                    self.dcp_done = True
                    break
                elif cmd == couchbaseConstants.CMD_DCP_END_STREAM:
                    del self.stream_list[opaque]
                    if not len(self.stream_list):
                        self.dcp_done = True
                elif cmd == couchbaseConstants.CMD_DCP_SNAPSHOT_MARKER:
                    ss_start_seqno, ss_end_seqno, _ = struct.unpack(couchbaseConstants.DCP_SNAPSHOT_PKT_FMT,
                                                                    data[0:extlen])
                    pair_index = (self.source_bucket['name'], self.source_node['hostname'])
                    if not self.cur['snapshot']:
                        self.cur['snapshot'] = {}
                    if pair_index not in self.cur['snapshot']:
                        self.cur['snapshot'][pair_index] = {}
                    self.cur['snapshot'][pair_index][opaque] = (ss_start_seqno, ss_end_seqno)
                elif cmd == couchbaseConstants.CMD_DCP_NOOP:
                    total_bytes_read -= bytes_read
                    need_ack = True
                elif cmd == couchbaseConstants.CMD_DCP_BUFFER_ACK:
                    total_bytes_read -= bytes_read
                    if errcode != couchbaseConstants.ERR_SUCCESS:
                        logging.warning(f'buffer ack response errcode: {errcode}')
                    continue
                else:
                    total_bytes_read -= bytes_read
                    logging.warning(f'warning: unexpected DCP message: {cmd}')
                    return f'unexpected DCP message: {cmd}', batch

                if need_ack:
                    self.ack_last = True
                    try:
                        if self.dcp_conn is not None:
                            self.dcp_conn._sendMsg(cmd, b'', b'', opaque, vbucketId=0,
                                                   fmt=couchbaseConstants.RES_PKT_FMT,
                                                   magic=couchbaseConstants.RES_MAGIC_BYTE)
                    except socket.error:
                        return f'error: socket.error on sendall(); perhaps the source server:' \
                                   f' {self.source_node["hostname"]} was rebalancing or had' \
                                   f' connectivity/server problems', batch
                    except EOFError:
                        self.dcp_done = True
                        return f'error: EOFError on socket sendall(); perhaps the source server:' \
                                   f' {self.source_node["hostname"]} was rebalancing or had ' \
                                   f'connectivity/server problems', batch

                    # Close the batch when there's an ACK handshake, so
                    # the server can concurrently send us the next batch.
                    # If we are slow, our slow ACK's will naturally slow
                    # down the server.
                    self.ack_buffer_size(total_bytes_read - last_processed)
                    return 0, batch

                self.ack_last = False
                self.cmd_last = cmd

        except EOFError:
            if batch.size() <= 0 and self.ack_last:
                # A closed conn after an ACK means clean end of TAP dump.
                self.dcp_done = True

        if batch.size() <= 0:
            return 0, None
        self.ack_buffer_size(total_bytes_read - last_processed)
        return 0, batch

    def get_dcp_conn(self):
        """Return previously connected dcp conn."""

        if not self.dcp_conn:
            host, _  = pump.hostport(self.source_node['hostname'])
            port = self.source_node['ports']['direct']
            username = self.opts.username
            password = self.opts.password
            bucket = str(self.source_bucket.get("name"))
            if self.opts.ssl:
                port = couchbaseConstants.SSL_PORT

            if getattr(self, 'alt_add', None):
                if 'alternateAddresses' in self.source_node:
                    host = self.source_node['alternateAddresses']['external']['hostname']
                    if 'ports' not in self.source_node['alternateAddresses']['external']:
                        return ['no data port available in external address: {}'. format(host)], None

                    alterante_ports = self.source_node['alternateAddresses']['external']['ports']
                    if self.opts.ssl:
                        if 'kvSSL' not in alterante_ports:
                            return ['Secure data port not available in external host: {}'.format(host)]
                        port = alterante_ports['kvSSL']
                    elif 'kv' not in alterante_ports:
                        return ['Data port not available in external host: {}'.format(host)]
                    else:
                        port = alterante_ports['kv']
                else:
                    return ['alterante address not available in node: {}'.format(self.source_node['otpNode'])], None

            logging.debug(f'  DCPStreamSource connecting mc: {host}:{port!s}')

            err, self.dcp_conn = pump.get_mcd_conn(host, port, username, password, bucket, self.opts.ssl,
                                                   self.opts.no_ssl_verify, self.opts.cacert)
            if err:
                return err, None

            err, self.mem_conn = pump.get_mcd_conn(host, port, username, password, bucket, self.opts.ssl,
                                                   self.opts.no_ssl_verify, self.opts.cacert)
            if err:
                return err, None

            flags = couchbaseConstants.FLAG_DCP_PRODUCER | couchbaseConstants.FLAG_DCP_XATTRS
            extra = struct.pack(couchbaseConstants.DCP_CONNECT_PKT_FMT, 0, flags)
            try:
                opaque = self.r.randint(0, 2**32)
                self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_CONNECT, self.dcp_name.encode() , b'', opaque, extra)
                self.dcp_conn._handleSingleResponse(opaque)

                buff_size = 0
                if self.flow_control:
                    # set connection buffer size. Considering header size, we roughly
                    # set the total received package size as 10 times as value size.
                    buff_size = self.batch_max_bytes * 10

                opaque=self.r.randint(0, 2**32)
                self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_CONTROL,
                                       couchbaseConstants.KEY_DCP_CONNECTION_BUFFER_SIZE,
                                       str(self.batch_max_bytes * 10).encode(), opaque)
                self.dcp_conn._handleSingleResponse(opaque)

                opaque=self.r.randint(0, 2**32)
                self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_CONTROL,
                                       couchbaseConstants.KEY_DCP_NOOP, b'true', opaque)
                self.dcp_conn._handleSingleResponse(opaque)

                opaque=self.r.randint(0, 2**32)
                self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_CONTROL,
                                       couchbaseConstants.KEY_DCP_NOOP_INTERVAL,
                                       b'180', opaque)
                self.dcp_conn._handleSingleResponse(opaque)
            except EOFError:
                return "error: Fail to set up DCP connection"
            except cb_bin_client.MemcachedError:
                return "error: DCP connect memcached error"
            except socket.error:
                return "error: DCP connection error"
            try:
                opaque=self.r.randint(0, 2**32)
                self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_CONTROL,
                                       couchbaseConstants.KEY_DCP_EXT_METADATA,
                                       bool_to_str(True).encode(), opaque)
                self.dcp_conn._handleSingleResponse(opaque)
            except EOFError:
                return "error: Fail to set up DCP connection"
            except cb_bin_client.MemcachedError:
                pass
            except socket.error:
                return "error: DCP connection error"

            self.running = True
            self.start()

            self.add_start_event(self.dcp_conn)
            self.setup_dcp_streams()
        return 0

    def ack_buffer_size(self, buf_size: int) -> couchbaseConstants.PUMP_ERROR:
        if self.flow_control:
            try:
                opaque = 0
                if self.dcp_conn is not None:
                    self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_BUFFER_ACK, b'', b'', opaque,
                                            struct.pack(">I", int(buf_size)))
                    logging.debug(f'Send buffer size: {buf_size:d}')
            except socket.error:
                return "error: socket error during sending buffer ack msg"
            except EOFError:
                return "error: send buffer ack msg"

        return 0

    def run(self):
        if not self.dcp_conn:
            logging.error("socket to memcached server is not created yet.")
            return

        bytes_read = b''
        rd_timeout = 1
        desc = [self.dcp_conn.s]
        while self.running:
            try:
                if self.dcp_done:
                    time.sleep(1)
                    continue
                readers, writers, errors = select.select(desc, [], [], rd_timeout)
                rd_timeout = .25
                if len(self.stream_list) == 0:
                    time.sleep(1)
                    continue

                for reader in readers:
                    data = reader.recv(self.recv_min_bytes)
                    logging.debug(f'Read {len(data)} bytes off the wire')
                    if len(data) == 0:
                        raise EOFError("Got empty data (remote died?).")
                    bytes_read += data
                while len(bytes_read) >= couchbaseConstants.MIN_RECV_PACKET:
                    magic, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas= \
                        struct.unpack(couchbaseConstants.RES_PKT_FMT, \
                                      bytes_read[0:couchbaseConstants.MIN_RECV_PACKET])

                    if len(bytes_read) < (couchbaseConstants.MIN_RECV_PACKET+bodylen):
                        break

                    rd_timeout = 0
                    body = bytes_read[couchbaseConstants.MIN_RECV_PACKET:couchbaseConstants.MIN_RECV_PACKET+bodylen]
                    bytes_read = bytes_read[couchbaseConstants.MIN_RECV_PACKET+bodylen:]
                    self.response.put((opcode, status, opaque, cas, keylen, extlen, body, \
                                       bodylen, datatype, \
                                       couchbaseConstants.MIN_RECV_PACKET+bodylen))
            except socket.error:
                break
            except Exception as e:
                pass

    def setup_dcp_streams(self):
        # send request to retrieve vblist and uuid for the node
        stats = self.mem_conn.stats(b'vbucket-seqno')
        if not stats:
            return "error: fail to retrive vbucket seqno", None
        self.mem_conn.close()

        uuid_list = {}
        seqno_list = {}
        vb_list = {}
        for key, val in stats.items():
            vb, counter = key.decode().split(":")
            if counter in [DCPStreamSource.VB_UUID,
                           DCPStreamSource.HIGH_SEQNO,
                           DCPStreamSource.ABS_HIGH_SEQNO,
                           DCPStreamSource.PURGE_SEQNO]:
                if vb[3:] not in vb_list:
                    vb_list[vb[3:]] = {}
                vb_list[vb[3:]][counter] = int(val)
        flags = 0
        pair_index = (self.source_bucket['name'], self.source_node['hostname'])
        vbucketsOrDict = None
        vbuckets = None
        if self.vbucket_list:
            vbucketsOrDict = json.loads(self.vbucket_list)
            if type(vbucketsOrDict) is dict:
                vbuckets = []
                for key, value in vbucketsOrDict.items():
                    vbuckets += value
            else:
                vbuckets = vbucketsOrDict
        for vbid in vb_list:
            if int(vbid) not in self.node_vbucket_map:
                #skip nonactive vbucket
                continue
            if vbuckets and int(vbid) not in vbuckets:
                #skip vbuckets that are not in this run
                continue

            if self.cur['seqno'] and self.cur['seqno'][pair_index]:
                start_seqno = self.cur['seqno'][pair_index][vbid]
            else:
                start_seqno = 0
            uuid = 0
            if self.cur['failoverlog'] and self.cur['failoverlog'][pair_index]:
                if vbid in self.cur['failoverlog'][pair_index] and \
                   self.cur['failoverlog'][pair_index][vbid]:
                    #Use the latest failover log
                    self.cur['failoverlog'][pair_index][vbid] = \
                        sorted(self.cur['failoverlog'][pair_index][vbid], \
                               key=lambda tup: tup[1],
                               reverse=True)
                    uuid, _ = self.cur['failoverlog'][pair_index][vbid][0]
            ss_start_seqno = start_seqno
            ss_end_seqno = start_seqno
            if self.cur['snapshot'] and self.cur['snapshot'][pair_index]:
                if vbid in self.cur['snapshot'][pair_index] and \
                   self.cur['snapshot'][pair_index][vbid]:
                    ss_start_seqno, ss_end_seqno = self.cur['snapshot'][pair_index][vbid]
                    if start_seqno == ss_end_seqno:
                        ss_start_seqno = start_seqno
            self.request_dcp_stream(int(vbid), flags, start_seqno,
                                    vb_list[vbid][DCPStreamSource.HIGH_SEQNO],
                                    uuid, ss_start_seqno, ss_end_seqno)

    def request_dcp_stream(self,
                           vbid: int,
                           flags: int,
                           start_seqno: int,
                           end_seqno: int,
                           vb_uuid: int,
                           ss_start_seqno: int,
                           ss_end_seqno: int):
        if not self.dcp_conn:
            return "error: no dcp connection setup yet.", None
        extra = struct.pack(couchbaseConstants.DCP_STREAM_REQ_PKT_FMT,
                            int(flags), 0,
                            int(start_seqno),
                            int(end_seqno),
                            int(vb_uuid),
                            int(ss_start_seqno),
                            int(ss_end_seqno))
        self.dcp_conn._sendMsg(couchbaseConstants.CMD_DCP_REQUEST_STREAM, b'', b'', vbid, extra, 0, 0, vbid)
        self.stream_list[vbid] = (vbid, flags, start_seqno, end_seqno, vb_uuid, ss_start_seqno, ss_end_seqno)

    @staticmethod
    def total_msgs(opts, source_bucket: Dict[str, Any], source_node, source_map: Dict[str, Any]) ->\
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[int]]:
        source_name = source_node.get("hostname", None)
        if not source_name:
            return 0, None

        spec = source_map['spec']
        name = source_bucket['name']
        vbuckets_num = len(source_bucket['vBucketServerMap']['vBucketMap'])
        if not vbuckets_num:
            return 0, None

        vbucket_list = getattr(opts, "vbucket_list", None)

        stats_vals = {}
        host, port, user, pswd, _ = pump.parse_spec(opts, spec, 8091)
        for stats in ["curr_items", "vb_active_resident_items_ratio"]:
            path = f'/pools/default/buckets/{name}/stats/{stats}'
            err, json, data = pump.rest_request_json(host, int(port),
                                                     user, pswd, opts.ssl, path,
                                                     reason="total_msgs", verify=opts.no_ssl_verify, cacert=opts.cacert)
            if err:
                return 0, None

            if data is not None:
                nodeStats = data.get("nodeStats", None)

            if not nodeStats:
                return 0, None
            vals = nodeStats.get(source_name, None)
            if not vals:
                return 0, None
            stats_vals[stats] = vals[-1]

        total_msgs = stats_vals["curr_items"]
        resident_ratio = stats_vals["vb_active_resident_items_ratio"]
        if 0 < resident_ratio < 100:
            # for DGM case, server will transfer both in-memory items and
            # backfill all items on disk
            total_msgs += (resident_ratio/100.0) * stats_vals["curr_items"]
        return 0, int(total_msgs)
