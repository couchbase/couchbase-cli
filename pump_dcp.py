#!/usr/bin/env python

import logging
import os
import Queue
import random
import json
import socket
import struct
import time
import threading
import select
import exceptions

import cb_bin_client
import couchbaseConstants

import pump
import pump_mc
import pump_cb
import pump_tap

try:
    import ctypes
except ImportError:
    cb_path = '/opt/couchbase/lib/python'
    while cb_path in sys.path:
        sys.path.remove(cb_path)
    try:
        import ctypes
    except ImportError:
        sys.exit('error: could not import ctypes module')
    else:
        sys.path.insert(0, cb_path)

bool_to_str = lambda value: str(bool(int(value))).lower()

class DCPStreamSource(pump_tap.TAPDumpSource, threading.Thread):
    """Can read from cluster/server/bucket via DCP streaming."""
    HIGH_SEQNO = "high_seqno"
    VB_UUID = "uuid"
    ABS_HIGH_SEQNO = "abs_high_seqno"
    PURGE_SEQNO = "purge_seqno"

    HIGH_SEQNO_BYTE = 8
    UUID_BYTE = 8

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(DCPStreamSource, self).__init__(opts, spec, source_bucket, source_node,
                                            source_map, sink_map, ctl, cur)
        threading.Thread.__init__(self)

        self.dcp_done = False
        self.dcp_conn = None
        self.mem_conn = None
        self.dcp_name = opts.process_name
        self.ack_last = False
        self.cmd_last = None
        self.num_msg = 0
        self.version_supported = self.source_node['version'].split(".") >= ["3", "0", "0"]
        self.recv_min_bytes = int(opts.extra.get("recv_min_bytes", 4096))
        self.batch_max_bytes = int(opts.extra.get("batch_max_bytes", 400000))
        self.flow_control = int(opts.extra.get("flow_control", 1))
        self.vbucket_list = getattr(opts, "vbucket_list", None)
        self.r=random.Random()
        self.queue_size = int(opts.extra.get("dcp_consumer_queue_length", 1000))
        self.response = Queue.Queue(self.queue_size)
        self.running = False
        self.stream_list = {}
        self.unack_size = 0
        self.node_vbucket_map = None

    @staticmethod
    def can_handle(opts, spec):
        return (spec.startswith("http://") or
                spec.startswith("couchbase://") or
                spec.startswith("https://")
                )

    @staticmethod
    def check(opts, spec):
        return pump_tap.TAPDumpSource.check(opts, spec)

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        return pump_tap.TAPDumpSource.provide_design(opts, source_spec, source_bucket, source_map)

    @staticmethod
    def provide_index(opts, source_spec, source_bucket, source_map):
        err, index_server = pump.filter_server(opts, source_spec, 'index')
        if err or not index_server:
            logging.warning("could not find index server:%s" % err)
            return 0, None

        spec_parts = source_map.get('spec_parts')
        if not spec_parts:
            return "error: no design spec_parts", None
        host, port, user, pswd, path = spec_parts
        host, port = pump.hostport(index_server)
        err, ddocs_json, ddocs = \
            pump.rest_request_json(host, couchbaseConstants.INDEX_PORT, user, pswd, opts.ssl,
                                   "/getIndexMetadata?bucket=%s" % (source_bucket['name']),
                                   reason="provide_index")
        if err:
            return err, None

        return 0, json.dumps(ddocs["result"])

    def add_start_event(self, conn):
        sasl_user = str(self.source_bucket.get("name"))
        event = {"timestamp": self.get_timestamp(),
                 "real_userid": {"source": "internal",
                                 "user": sasl_user,
                                },
                 "mode": getattr(self.opts, "mode", "diff"),
                 "source_bucket": self.source_bucket['name'],
                 "source_node": self.source_node['hostname']
                }
        if conn:
            try:
                conn.audit(couchbaseConstants.AUDIT_EVENT_BACKUP_START, json.dumps(event))
            except Exception, e:
                logging.warn("auditing error: %s" % e)
        return 0

    def add_stop_event(self, conn):
        sasl_user = str(self.source_bucket.get("name"))
        event = {"timestamp": self.get_timestamp(),
                 "real_userid": {"source": "internal",
                                 "user": sasl_user
                                },
                 "source_bucket": self.source_bucket['name'],
                 "source_node": self.source_node['hostname']
                }
        if conn:
            try:
                conn.audit(couchbaseConstants.AUDIT_EVENT_BACKUP_STOP, json.dumps(event))
            except Exception, e:
                logging.warn("auditing error: %s" % e)
        return 0

    def build_node_vbucket_map(self):
        if self.source_bucket.has_key("vBucketServerMap"):
            server_list = self.source_bucket["vBucketServerMap"]["serverList"]
            vbucket_map = self.source_bucket["vBucketServerMap"]["vBucketMap"]
        else:
            return None

        node_vbucket_map = []
        nodename = self.source_node.get('hostname', 'N/A').split(":")[0]
        nodeindex = -1
        for index, node in enumerate(server_list):
            if nodename in node:
                nodeindex = index
                break
        for index, vblist in enumerate(vbucket_map):
            if vblist[0] >= 0 and vblist[0] == nodeindex:
                node_vbucket_map.append(index)
        return node_vbucket_map

    def provide_batch(self):
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

            logging.warn("backoff: %s, sleeping: %s, on error: %s" %
                         (cur_retry, cur_sleep, rv))
            time.sleep(cur_sleep)
            cur_sleep = min(cur_sleep * 2, 20) # Max backoff sleep 20 seconds.
            cur_retry = cur_retry + 1

    def provide_dcp_batch_actual(self):
        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']
        delta_ack_size = batch_max_bytes * 10 / 4 #ack every 25% of buffer size
        last_processed = 0
        total_bytes_read = 0

        vbid = 0
        cmd = 0
        start_seqno = 0
        end_seqno = 0
        vb_uuid = 0
        hi_seqno = 0
        ss_start_seqno = 0
        ss_end_seqno = 0
        try:
            while (not self.dcp_done and
                   batch.size() < batch_max_size and
                   batch.bytes < batch_max_bytes):

                if self.response.empty():
                    if len(self.stream_list) > 0:
                        logging.debug("no response while there %s active streams" % len(self.stream_list))
                        time.sleep(.25)
                    else:
                        self.dcp_done = True
                    continue
                unprocessed_size = total_bytes_read - last_processed
                if unprocessed_size > delta_ack_size:
                    rv = self.ack_buffer_size(unprocessed_size)
                    if rv:
                        logging.error(rv)
                    else:
                        last_processed = total_bytes_read

                cmd, errcode, opaque, cas, keylen, extlen, data, datalen, dtype, bytes_read = \
                    self.response.get()
                total_bytes_read += bytes_read
                rv = 0
                metalen = flags = flg = exp = 0
                key = val = ext = ''
                need_ack = False
                seqno = 0
                if cmd == couchbaseConstants.CMD_DCP_REQUEST_STREAM:
                    if errcode == couchbaseConstants.ERR_SUCCESS:
                        pair_index = (self.source_bucket['name'], self.source_node['hostname'])
                        start = 0
                        step = DCPStreamSource.HIGH_SEQNO_BYTE + DCPStreamSource.UUID_BYTE
                        while start+step <= datalen:
                            uuid, seqno = struct.unpack(
                                            couchbaseConstants.DCP_VB_UUID_SEQNO_PKT_FMT, \
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
                       logging.warn("a stream exists on the connection for vbucket:%s" % opaque)
                    elif errcode ==  couchbaseConstants.ERR_NOT_MY_VBUCKET:
                        logging.warn("Vbucket is not active anymore, skip it:%s" % vbid)
                        del self.stream_list[opaque]
                    elif errcode == couchbaseConstants.ERR_ERANGE:
                        logging.warn("Start or end sequence numbers specified incorrectly,(%s, %s)" % \
                                     (start_seqno, end_seqno))
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
                        self.request_dcp_stream(vbid, flags, start_seqno, end_seqno, vb_uuid, ss_start_seqno, ss_end_seqno)

                        del self.stream_list[opaque]
                        self.stream_list[opaque] = \
                            (vbid, flags, start_seqno, end_seqno, vb_uuid, ss_start_seqno, ss_end_seqno)
                    else:
                        logging.error("unprocessed errcode:%s" % errcode)
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
                                    conf_res, = struct.unpack(">B",extra_meta[extra_index:extra_index+1])
                                elif extlen == 2:
                                    conf_res, = struct.unpack(">H",extra_meta[extra_index:extra_index+2])
                                elif extlen == 4:
                                    conf_res, = struct.unpack(">I", extra_meta[extra_index:extra_index+4])
                                elif extlen == 8:
                                    conf_res, = struct.unpack(">Q", extra_meta[extra_index:extra_index+8])
                                else:
                                    logging.error("unsupported extra meta data format:%d" % extlen)
                                    conf_res = 0
                            extra_index += extlen

                    if not self.skip(key, vbucket_id):
                        msg = (cmd, vbucket_id, key, flg, exp, cas, rev_seqno, val, seqno, dtype, \
                               metalen, conf_res)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                elif cmd == couchbaseConstants.CMD_DCP_DELETE or \
                     cmd == couchbaseConstants.CMD_DCP_EXPIRATION:
                    vbucket_id = errcode
                    seqno, rev_seqno, metalen = \
                        struct.unpack(couchbaseConstants.DCP_DELETE_PKT_FMT, data[0:extlen])
                    key_start = extlen
                    val_start = key_start + keylen
                    key = data[extlen:val_start]
                    if not self.skip(key, vbucket_id):
                        msg = (cmd, vbucket_id, key, flg, exp, cas, rev_seqno, val, seqno, dtype, \
                               metalen, 0)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                    if cmd == couchbaseConstants.CMD_DCP_DELETE:
                        batch.adjust_size += 1
                elif cmd == couchbaseConstants.CMD_DCP_FLUSH:
                    logging.warn("stopping: saw CMD_DCP_FLUSH")
                    self.dcp_done = True
                    break
                elif cmd == couchbaseConstants.CMD_DCP_END_STREAM:
                    del self.stream_list[opaque]
                    if not len(self.stream_list):
                        self.dcp_done = True
                elif cmd == couchbaseConstants.CMD_DCP_SNAPSHOT_MARKER:
                    ss_start_seqno, ss_end_seqno, _ = \
                        struct.unpack(couchbaseConstants.DCP_SNAPSHOT_PKT_FMT, data[0:extlen])
                    pair_index = (self.source_bucket['name'], self.source_node['hostname'])
                    if not self.cur['snapshot']:
                        self.cur['snapshot'] = {}
                    if pair_index not in self.cur['snapshot']:
                        self.cur['snapshot'][pair_index] = {}
                    self.cur['snapshot'][pair_index][opaque] = (ss_start_seqno, ss_end_seqno)
                elif cmd == couchbaseConstants.CMD_DCP_NOOP:
                    need_ack = True
                elif cmd == couchbaseConstants.CMD_DCP_BUFFER_ACK:
                    if errcode != couchbaseConstants.ERR_SUCCESS:
                        logging.warning("buffer ack response errcode:%s" % errcode)
                    continue
                else:
                    logging.warn("warning: unexpected DCP message: %s" % cmd)
                    return "unexpected DCP message: %s" % cmd, batch

                if need_ack:
                    self.ack_last = True
                    try:
                        self.dcp_conn._sendMsg(cmd, '', '', opaque, vbucketId=0,
                                          fmt=couchbaseConstants.RES_PKT_FMT,
                                          magic=couchbaseConstants.RES_MAGIC_BYTE)
                    except socket.error:
                        return ("error: socket.error on send();"
                                " perhaps the source server: %s was rebalancing"
                                " or had connectivity/server problems" %
                                (self.source_node['hostname'])), batch
                    except EOFError:
                        self.dcp_done = True
                        return ("error: EOFError on socket send();"
                                " perhaps the source server: %s was rebalancing"
                                " or had connectivity/server problems" %
                                (self.source_node['hostname'])), batch

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
            host = self.source_node['hostname'].split(':')[0]
            port = self.source_node['ports']['direct']
            if self.opts.ssl:
                port = couchbaseConstants.SSL_PORT
            version = self.source_node['version']

            logging.debug("  DCPStreamSource connecting mc: " +
                          host + ":" + str(port))

            self.dcp_conn = cb_bin_client.MemcachedClient(host, port)
            if not self.dcp_conn:
                return "error: could not connect to memcached: " + \
                    host + ":" + str(port)
            self.mem_conn = cb_bin_client.MemcachedClient(host, port)
            if not self.mem_conn:
                return "error: could not connect to memcached: " + \
                    host + ":" + str(port)
            sasl_user = str(self.source_bucket.get("name"))
            sasl_pswd = str(self.source_bucket.get("saslPassword"))
            if sasl_user:
                try:
                    self.dcp_conn.sasl_auth_cram_md5(sasl_user, sasl_pswd)
                    self.mem_conn.sasl_auth_cram_md5(sasl_user, sasl_pswd)
                except cb_bin_client.MemcachedError:
                    try:
                        self.dcp_conn.sasl_auth_plain(sasl_user, sasl_pswd)
                        self.mem_conn.sasl_auth_plain(sasl_user, sasl_pswd)
                    except EOFError:
                        return "error: SASL auth error: %s:%s, user: %s" % \
                            (host, port, sasl_user)
                    except cb_bin_client.MemcachedError:
                        return "error: SASL auth failed: %s:%s, user: %s" % \
                            (host, port, sasl_user)
                    except socket.error:
                        return "error: SASL auth socket error: %s:%s, user: %s" % \
                            (host, port, sasl_user)
                except EOFError:
                    return "error: SASL auth error: %s:%s, user: %s" % \
                        (host, port, sasl_user)
                except socket.error:
                    return "error: SASL auth socket error: %s:%s, user: %s" % \
                        (host, port, sasl_user)
            extra = struct.pack(couchbaseConstants.DCP_CONNECT_PKT_FMT, 0, \
                                couchbaseConstants.FLAG_DCP_PRODUCER)
            try:
                opaque=self.r.randint(0, 2**32)
                self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_CONNECT, self.dcp_name, \
                                       '', opaque, extra)
                self.dcp_conn._handleSingleResponse(opaque)

                buff_size = 0
                if self.flow_control:
                    # set connection buffer size. Considering header size, we roughly
                    # set the total received package size as 10 times as value size.
                    buff_size = self.batch_max_bytes * 10

                opaque=self.r.randint(0, 2**32)
                self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_CONTROL,
                                       couchbaseConstants.KEY_DCP_CONNECTION_BUFFER_SIZE,
                                       str(self.batch_max_bytes * 10), opaque)
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
                                       bool_to_str(True), opaque)
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

    def ack_buffer_size(self, buf_size):
        if self.flow_control:
            try:
                opaque=self.r.randint(0, 2**32)
                self.dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_BUFFER_ACK, '', '', \
                    opaque, struct.pack(">I", int(buf_size)))
                logging.debug("Send buffer size: %d" % buf_size)
            except socket.error:
                return "error: socket error during sending buffer ack msg"
            except EOFError:
                return "error: send buffer ack msg"

        return None

    def run(self):
        if not self.dcp_conn:
            logging.error("socket to memcached server is not created yet.")
            return

        bytes_read = ''
        rd_timeout = 1
        desc = [self.dcp_conn.s]
        extra_bytes = 0
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
                    logging.debug("Read %d bytes off the wire" % len(data))
                    if len(data) == 0:
                        raise exceptions.EOFError("Got empty data (remote died?).")
                    bytes_read += data
                while len(bytes_read) >= couchbaseConstants.MIN_RECV_PACKET:
                    magic, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas= \
                        struct.unpack(couchbaseConstants.RES_PKT_FMT, \
                                      bytes_read[0:couchbaseConstants.MIN_RECV_PACKET])

                    if len(bytes_read) < (couchbaseConstants.MIN_RECV_PACKET+bodylen):
                        extra_bytes = len(bytes_read)
                        break

                    rd_timeout = 0
                    body = bytes_read[couchbaseConstants.MIN_RECV_PACKET : \
                                      couchbaseConstants.MIN_RECV_PACKET+bodylen]
                    bytes_read = bytes_read[couchbaseConstants.MIN_RECV_PACKET+bodylen:]
                    self.response.put((opcode, status, opaque, cas, keylen, extlen, body, \
                                       bodylen, datatype, \
                                       couchbaseConstants.MIN_RECV_PACKET+bodylen+extra_bytes))
                    extra_bytes = 0
            except socket.error:
                break
            except Exception, e:
                pass

    def setup_dcp_streams(self):
        #send request to retrieve vblist and uuid for the node
        stats = self.mem_conn.stats("vbucket-seqno")
        if not stats:
            return "error: fail to retrive vbucket seqno", None
        self.mem_conn.close()

        uuid_list = {}
        seqno_list = {}
        vb_list = {}
        for key, val in stats.items():
            vb, counter = key.split(":")
            if counter in [DCPStreamSource.VB_UUID,
                           DCPStreamSource.HIGH_SEQNO,
                           DCPStreamSource.ABS_HIGH_SEQNO,
                           DCPStreamSource.PURGE_SEQNO]:
                if not vb_list.has_key(vb[3:]):
                    vb_list[vb[3:]] = {}
                vb_list[vb[3:]][counter] = int(val)
        flags = 0
        pair_index = (self.source_bucket['name'], self.source_node['hostname'])
        vbuckets = None
        if self.vbucket_list:
            vbuckets = json.loads(self.vbucket_list)
        for vbid in vb_list.iterkeys():
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
                           vbid,
                           flags,
                           start_seqno,
                           end_seqno,
                           vb_uuid,
                           ss_start_seqno,
                           ss_end_seqno):
        if not self.dcp_conn:
            return "error: no dcp connection setup yet.", None
        extra = struct.pack(couchbaseConstants.DCP_STREAM_REQ_PKT_FMT,
                            int(flags), 0,
                            int(start_seqno),
                            int(end_seqno),
                            int(vb_uuid),
                            int(ss_start_seqno),
                            int(ss_end_seqno))
        self.dcp_conn._sendMsg(couchbaseConstants.CMD_DCP_REQUEST_STREAM, '', '', vbid, \
                               extra, 0, 0, vbid)
        self.stream_list[vbid] = (vbid, flags, start_seqno, end_seqno, vb_uuid, ss_start_seqno, ss_end_seqno)

    def _read_dcp_conn(self, dcp_conn):
        buf, cmd, errcode, opaque, cas, keylen, extlen, data, datalen = \
            self.recv_dcp_msg(dcp_conn.s)
        dcp_conn.buf = buf

        rv = 0
        metalen = flags = ttl = flg = exp = 0
        meta = key = val = ext = ''
        need_ack = False
        extlen = seqno = 0
        if data:
            ext = data[0:extlen]
            if cmd == couchbaseConstants.CMD_DCP_MUTATION:
                seqno, rev_seqno, flg, exp, locktime, meta = \
                    struct.unpack(couchbaseConstants.DCP_MUTATION_PKT_FMT, ext)
                key_start = extlen
                val_start = key_start + keylen
                key = data[extlen:val_start]
                val = data[val_start:]
            elif cmd == couchbaseConstants.CMD_DCP_DELETE or \
                 cmd == couchbaseConstants.CMD_DCP_EXPIRATION:
                seqno, rev_seqno, meta = \
                    struct.unpack(couchbaseConstants.DCP_DELETE_PKT_FMT, ext)
                key_start = extlen
                val_start = key_start + keylen
                key = data[extlen:val_start]
            else:
                rv = "error: uninterpreted DCP commands:%s" % cmd
        elif datalen:
            rv = "error: could not read full TAP message body"

        return rv, cmd, errcode, key, flg, exp, cas, meta, val, opaque, need_ack, seqno

    def _recv_dcp_msg_error(self, sock, buf):
        pkt, buf = self.recv_dcp(sock, couchbaseConstants.MIN_RECV_PACKET, buf)
        if not pkt:
            raise EOFError()
        magic, cmd, keylen, extlen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(couchbaseConstants.REQ_PKT_FMT, pkt)
        if magic != couchbaseConstants.REQ_MAGIC_BYTE:
            raise Exception("unexpected recv_msg magic: " + str(magic))
        data, buf = self.recv_dcp(sock, datalen, buf)
        return buf, cmd, errcode, opaque, cas, keylen, extlen, data, datalen

    def _recv_dcp_msg(self, sock):
        response = ""
        while len(response) < couchbaseConstants.MIN_RECV_PACKET:
            data = sock.recv(couchbaseConstants.MIN_RECV_PACKET - len(response))
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?).")
            response += data
        assert len(response) == couchbaseConstants.MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas=\
            struct.unpack(couchbaseConstants.RES_PKT_FMT, response)

        datalen = remaining
        rv = ""
        while remaining > 0:
            data = sock.recv(remaining)
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?).")
            rv += data
            remaining -= len(data)

        assert (magic in (couchbaseConstants.RES_MAGIC_BYTE, couchbaseConstants.REQ_MAGIC_BYTE)), \
               "Got magic: %d" % magic
        return cmd, errcode, opaque, cas, keylen, extralen, rv, datalen

    def _recv_dcp(self, skt, nbytes, buf):
        recv_arr = [ buf ]
        recv_tot = len(buf) # In bytes.

        while recv_tot < nbytes:
            data = None
            try:
                data = skt.recv(max(nbytes - len(buf), self.recv_min_bytes))
            except socket.timeout:
                logging.error("error: recv socket.timeout")
            except Exception, e:
                msg = str(e)
                if msg.find("Connection reset by peer") >= 0:
                    logging.error("error: recv exception: " + \
                        "%s, %s may be inactive" % (msg, self.source_node["hostname"]))
                else:
                    logging.error("error: recv exception: " + str(e))


            if not data:
                return None, ''

            recv_arr.append(data)
            recv_tot += len(data)

        joined = ''.join(recv_arr)

        return joined[:nbytes], joined[nbytes:]

    @staticmethod
    def total_msgs(opts, source_bucket, source_node, source_map):
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
            path = "/pools/default/buckets/%s/stats/%s" % (name, stats)
            err, json, data = pump.rest_request_json(host, int(port),
                                                     user, pswd, opts.ssl, path,
                                                     reason="total_msgs")
            if err:
                return 0, None

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
            #for DGM case, server will transfer both in-memory items and
            #backfill all items on disk
            total_msgs += (resident_ratio/100.0) * stats_vals["curr_items"]
        return 0, int(total_msgs)

class DCPSink(pump_cb.CBSink):
    """Smart client using DCP protocol to steam data to couchbase cluster."""
    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(DCPSink, self).__init__(opts, spec, source_bucket, source_node,
                                     source_map, sink_map, ctl, cur)

        self.vbucket_list = getattr(opts, "vbucket_list", None)

    @staticmethod
    def check_base(opts, spec):
        #Overwrite CBSink.check_base() to allow replica vbucket state

        op = getattr(opts, "destination_operation", None)
        if op not in [None, 'set', 'add', 'get']:
            return ("error: --destination-operation unsupported value: %s" +
                    "; use set, add, get") % op

        return pump.EndPoint.check_base(opts, spec)

    def scatter_gather(self, mconns, batch):
        sink_map_buckets = self.sink_map['buckets']
        if len(sink_map_buckets) != 1:
            return "error: CBSink.run() expected 1 bucket in sink_map", None, None

        vbuckets_num = len(sink_map_buckets[0]['vBucketServerMap']['vBucketMap'])
        vbuckets = batch.group_by_vbucket_id(vbuckets_num, self.rehash)

        # Scatter or send phase.
        for vbucket_id, msgs in vbuckets.iteritems():
            rv, conn = self.find_conn(mconns, vbucket_id)
            if rv != 0:
                return rv, None, None
            rv = self.send_msgs(conn, msgs, self.operation(),
                                vbucket_id=vbucket_id)
            if rv != 0:
                return rv, None, None

        # Yield to let other threads do stuff while server's processing.
        time.sleep(0.01)

        # Gather or recv phase.
        # Only process the last msg which requires ack request
        last_msg = []
        retry_batch = None
        need_refresh = False
        for vbucket_id, msgs in vbuckets.iteritems():
            last_msg.append(msgs[-1])
            rv, conn = self.find_conn(mconns, vbucket_id)
            if rv != 0:
                return rv, None, None
            rv, retry, refresh = \
                self.recv_msgs(conn, last_msg, vbucket_id=vbucket_id, verify_opaque=False)
            if rv != 0:
                return rv, None, None
            if retry:
                retry_batch = batch
            if refresh:
                need_refresh = True

        return 0, retry_batch, retry_batch and not need_refresh

    def connect_mc(self, node, sasl_user, sasl_pswd):
        """Return previously connected dcp conn."""

        host = node.split(':')[0]
        port = node.split(':')[1]
        if self.opts.ssl:
            port = couchbaseConstants.SSL_PORT
        logging.debug("  DCPSink connecting mc: " +
                      host + ":" + str(port))

        dcp_conn = cb_bin_client.MemcachedClient(host, port)
        if not dcp_conn:
            return "error: could not connect to memcached: " + \
                host + ":" + str(port), None
        if sasl_user:
            try:
                dcp_conn.sasl_auth_plain(sasl_user, sasl_pswd)
            except EOFError:
                return "error: SASL auth error: %s:%s, user: %s" % \
                    (host, port, sasl_user), None
            except cb_bin_client.MemcachedError:
                return "error: SASL auth failed: %s:%s, user: %s" % \
                    (host, port, sasl_user), None
            except socket.error:
                return "error: SASL auth socket error: %s:%s, user: %s" % \
                    (host, port, sasl_user), None
        extra = struct.pack(couchbaseConstants.DCP_CONNECT_PKT_FMT, 0, \
                            couchbaseConstants.FLAG_DCP_CONSUMER)
        try:
            opaque=self.r.randint(0, 2**32)
            dcp_conn._sendCmd(couchbaseConstants.CMD_DCP_CONNECT, self.dcp_name, '', opaque, extra)
            dcp_conn._handleSingleResponse(opaque)
        except EOFError:
            return "error: Fail to set up DCP connection", None
        except cb_bin_client.MemcachedError:
            return "error: DCP connect memcached error", None
        except socket.error:
            return "error: DCP connection error", None

        return 0, dcp_conn

    def find_conn(self, mconns, vbucket_id):
        if not self.vbucket_list:
            return super(DCPSink, self).find_conn(mconns, vbucket_id)

        vbuckets = json.loads(self.vbucket_list)
        if isinstance(vbuckets, list):
            if vbucket_id not in vbuckets:
                return "error: unexpected vbucket id:" + str(vbucket_id), None
            return super(DCPSink, self).find_conn(mconns, vbucket_id)

        bucket = self.sink_map['buckets'][0]
        serverList = bucket['vBucketServerMap']['serverList']
        if serverList:
            port_number = serverList[0].split(":")[-1]
        else:
            port_number = 11210

        conn = None
        if isinstance(vbuckets, dict):
            for node, vblist in vbuckets.iteritems():
                for vbucket in vblist:
                    if int(vbucket) == int(vbucket_id):
                        host_port = "%s:%s" % (node, port_number)
                        conn = mconns.get(host_port, None)
                        if not conn:
                            user = bucket['name']
                            pswd = bucket['saslPassword']
                            rv, conn = DCPSink.connect_mc(host_port, user, pswd)
                            if rv != 0:
                                logging.error("error: CBSink.connect() for send: " + rv)
                                return rv, None
                            mconns[host_port] = conn
                        break
        return 0, conn

    def send_msgs(self, conn, msgs, operation, vbucket_id=None):
        m = []
        #Ask for acknowledgement for the last msg of batch
        for i, msg in enumerate(msgs):
            cmd, vbucket_id_msg, key, flg, exp, cas, meta, val, seqno, dtype, nmeta, conf_res = msg
            if vbucket_id is not None:
                vbucket_id_msg = vbucket_id

            if self.skip(key, vbucket_id_msg):
                continue

            if cmd == couchbaseConstants.CMD_GET:
                val, flg, exp, cas = '', 0, 0, 0
            if cmd == couchbaseConstants.CMD_NOOP:
                key, val, flg, exp, cas = '', '', 0, 0, 0
            need_ack = (i == len(msgs)-1)
            rv, req = self.cmd_request(cmd, vbucket_id_msg, key, val,
                                       ctypes.c_uint32(flg).value,
                                       exp, cas, meta, i, seqno, dtype, nmeta, need_ack)
            if rv != 0:
                return rv
            self.append_req(m, req)
        if m:
            try:
                conn.s.send(''.join(m))
            except socket.error, e:
                return "error: conn.send() exception: %s" % (e)
        return 0

    def cmd_request(self, cmd, vbucket_id, key, val, flg, exp, cas, meta, opaque, seqno, dtype,\
                    nmeta, need_ack):
        if meta:
            seq_no = str(meta)
            if len(seq_no) > 8:
                seq_no = seq_no[0:8]
            if len(seq_no) < 8:
                # The seq_no might be 32-bits from 2.0DP4, so pad with 0x00's.
                seq_no = ('\x00\x00\x00\x00\x00\x00\x00\x00' + seq_no)[-8:]
            check_seqno, = struct.unpack(">Q", seq_no)
            if not check_seqno:
                seq_no = ('\x00\x00\x00\x00\x00\x00\x00\x00' + 1)[-8:]
        else:
            seq_no = ('\x00\x00\x00\x00\x00\x00\x00\x00' + 1)[-8:]
        metalen = len(str(seq_no))
        locktime = 0
        rev_seqno = 0
        nru = 0
        if cmd == couchbaseConstants.CMD_DCP_MUTATION:
            ext = struct.pack(couchbaseConstants.DCP_MUTATION_PKT_FMT,
                              seqno, rev_seqno, flg, exp, locktime, metalen, nru)
        elif cmd == couchbaseConstants.CMD_DCP_DELETE:
            ext = struct.pack(couchbaseConstants.DCP_DELETE_PKT_FMT,
                              seqno, rev_seqno, metalen)
        else:
            return "error: MCSink - unknown tap cmd for request: " + str(cmd), None

        hdr = self.cmd_header(cmd, vbucket_id, key, val, ext, cas, opaque, metalen, dtype)
        return 0, (hdr, ext, seq_no, key, val)

    def cmd_header(self, cmd, vbucket_id, key, val, ext, cas, opaque, metalen,
                   dtype=0,
                   fmt=couchbaseConstants.REQ_PKT_FMT,
                   magic=couchbaseConstants.REQ_MAGIC_BYTE):
        #MB-11902
        dtype = 0
        return struct.pack(fmt, magic, cmd,
                           len(key), len(ext), dtype, vbucket_id,
                           metalen + len(key) + len(val) + len(ext), opaque, cas)

    #TODO : where is the seq_no? same or different from tap seq_no?
    def append_req(self, m, req):
        hdr, ext, seq_no, key, val = req
        m.append(hdr)
        if ext:
            m.append(ext)
        if seq_no:
            m.append(seq_no)
        if key:
            m.append(str(key))
        if val:
            m.append(str(val))
