#!/usr/bin/env python

import logging
import os
import random
import simplejson as json
import socket
import string
import struct
import time
import threading
import select
import exceptions

import cb_bin_client
import couchbaseConstants
from cbqueue import PumpQueue

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

class UPRStreamSource(pump_tap.TAPDumpSource, threading.Thread):
    """Can read from cluster/server/bucket via UPR streaming."""
    HIGH_SEQNO = "high_seqno"
    HIGH_SEQNO_BYTE = 8
    VB_UUID = "uuid"
    UUID_BYTE = 8

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(UPRStreamSource, self).__init__(opts, spec, source_bucket, source_node,
                                            source_map, sink_map, ctl, cur)
        threading.Thread.__init__(self)

        self.upr_done = False
        self.upr_conn = None
        self.mem_conn = None
        self.upr_name = "".join(random.sample(string.letters, 16))
        self.ack_last = False
        self.cmd_last = None
        self.num_msg = 0
        self.version_supported = self.source_node['version'].split(".") >= ["3", "0", "0"]
        self.recv_min_bytes = int(getattr(opts, "recv_min_bytes", 4096))
        self.vbucket_list = getattr(opts, "vbucket_list", None)
        self.r=random.Random()
        self.queue = PumpQueue()
        self.response = PumpQueue()
        self.running = False
        self.stream_list = {}

    @staticmethod
    def can_handle(opts, spec):
        return (spec.startswith("http://") or
                spec.startswith("couchbase://"))

    @staticmethod
    def check(opts, spec):
        return pump_tap.TAPDumpSource.check(opts, spec)

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        return pump_tap.TAPDumpSource.provide_design(opts, source_spec, source_bucket, source_map);

    def provide_batch(self):
        if not self.version_supported:
            return super(UPRStreamSource, self).provide_batch()
        cur_sleep = 0.2
        cur_retry = 0
        max_retry = self.opts.extra['max_retry']
        while True:
            if self.upr_done:
                return 0, None

            rv, upr_conn = self.get_upr_conn()
            if rv != 0:
                self.upr_done = True
                return rv, None

            rv, batch = self.provide_upr_batch_actual(upr_conn)
            if rv == 0:
                return rv, batch

            if self.upr_conn:
                self.upr_conn.close()
                self.upr_conn = None

            if cur_retry > max_retry:
                self.upr_done = True
                return rv, batch

            logging.warn("backoff: %s, sleeping: %s, on error: %s" %
                         (cur_retry, cur_sleep, rv))
            time.sleep(cur_sleep)
            cur_sleep = min(cur_sleep * 2, 20) # Max backoff sleep 20 seconds.
            cur_retry = cur_retry + 1

    def provide_upr_batch_actual(self, upr_conn):
        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']
        vbid = 0
        cmd = 0
        start_seqno = 0
        end_seqno = 0
        vb_uuid = 0
        hi_seqno = 0
        try:
            while (not self.upr_done and
                   batch.size() < batch_max_size and
                   batch.bytes < batch_max_bytes):

                if self.response.empty():
                    if len(self.stream_list) > 0:
                        time.sleep(.25)
                    else:
                        self.upr_done = True
                    continue
                cmd, errcode, opaque, cas, keylen, extlen, data, datalen, dtype = \
                    self.response.get()
                rv = 0
                metalen = flags = flg = exp = 0
                key = val = ext = ''
                need_ack = False
                seqno = 0
                if cmd == couchbaseConstants.CMD_UPR_REQUEST_STREAM:
                    if errcode == couchbaseConstants.ERR_SUCCESS:
                        pair_index = (self.source_bucket['name'], self.source_node['hostname'])
                        start = 0
                        step = UPRStreamSource.HIGH_SEQNO_BYTE + UPRStreamSource.UUID_BYTE
                        while start+step <= datalen:
                            uuid, seqno = struct.unpack(
                                            couchbaseConstants.UPR_VB_UUID_SEQNO_PKT_FMT, \
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
                        vbid, flags, start_seqno, end_seqno, vb_uuid, hi_seqno = \
                            self.stream_list[opaque]
                        del self.stream_list[opaque]
                        #self.request_upr_stream(vbid, flags, start_seqno, end_seqno, 0, hi_seqno)
                    elif errcode == couchbaseConstants.ERR_KEY_EEXISTS:
                       logging.warn("a stream exists on the connection for vbucket:%s" % opaque)
                    elif errcode ==  couchbaseConstants.ERR_NOT_MY_VBUCKET:
                        logging.warn("Vbucket is not active anymore, skip it:%s" % vbid)
                        del self.stream_list[opaque]
                    elif errcode == couchbaseConstants.ERR_ERANGE:
                        #logging.warn("Start or end sequence numbers specified incorrectly,(%s, %s)" % \
                        #             (start_seqno, end_seqno))
                        del self.stream_list[opaque]
                    elif errcode == couchbaseConstants.ERR_ROLLBACK:
                        vbid, flags, start_seqno, end_seqno, vb_uuid, hi_seqno = \
                            self.stream_list[opaque]
                        start_seqno, = struct.unpack(couchbaseConstants.UPR_VB_SEQNO_PKT_FMT, data)
                        #find the most latest uuid, hi_seqno that fit start_seqno
                        if self.cur['failoverlog']:
                            pair_index = (self.source_bucket['name'], self.source_node['hostname'])
                            if vbid in self.cur['failoverlog'][pair_index]:
                                for uuid, seqno in self.cur['failoverlog'][pair_index][vbid]:
                                    if start_seqno >= seqno:
                                        vb_uuid = uuid
                                        hi_seqno = seqno
                                        break
                        self.request_upr_stream(vbid, flags, start_seqno, end_seqno, vb_uuid, hi_seqno)

                        del self.stream_list[opaque]
                        self.stream_list[opaque] = \
                            (vbid, flags, start_seqno, end_seqno, vb_uuid, hi_seqno)
                elif cmd == couchbaseConstants.CMD_UPR_MUTATION:
                    vbucket_id = errcode
                    seqno, rev_seqno, flg, exp, locktime, metalen, nru = \
                        struct.unpack(couchbaseConstants.UPR_MUTATION_PKT_FMT, data[0:extlen])
                    key_start = extlen
                    val_start = key_start + keylen
                    key = data[extlen:val_start]
                    val = data[val_start:]
                    if not self.skip(key, vbucket_id):
                        msg = (cmd, vbucket_id, key, flg, exp, cas, rev_seqno, val, seqno, dtype, \
                               metalen)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                elif cmd == couchbaseConstants.CMD_UPR_DELETE or \
                     cmd == couchbaseConstants.CMD_UPR_EXPIRATION:
                    vbucket_id = errcode
                    seqno, rev_seqno, metalen = \
                        struct.unpack(couchbaseConstants.UPR_DELETE_PKT_FMT, data[0:extlen])
                    key_start = extlen
                    val_start = key_start + keylen
                    key = data[extlen:val_start]
                    if not self.skip(key, vbucket_id):
                        msg = (cmd, vbucket_id, key, flg, exp, cas, rev_seqno, val, seqno, dtype, \
                               metalen)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                    if cmd == couchbaseConstants.CMD_UPR_DELETE:
                        batch.adjust_size += 1
                elif cmd == couchbaseConstants.CMD_UPR_FLUSH:
                    logging.warn("stopping: saw CMD_UPR_FLUSH")
                    self.upr_done = True
                    break
                elif cmd == couchbaseConstants.CMD_UPR_END_STREAM:
                    del self.stream_list[opaque]
                    if not len(self.stream_list):
                        self.upr_done = True
                elif cmd == couchbaseConstants.CMD_UPR_SNAPSHOT_MARKER:
                    logging.info("snapshot marker received, simply ignored:")
                else:
                    logging.warn("warning: unexpected UPR message: %s" % cmd)
                    return "unexpected UPR message: %s" % cmd, batch

                if need_ack:
                    self.ack_last = True
                    try:
                        upr_conn._sendMsg(cmd, '', '', opaque, vbucketId=0,
                                          fmt=couchbaseConstants.RES_PKT_FMT,
                                          magic=couchbaseConstants.RES_MAGIC_BYTE)
                    except socket.error:
                        return ("error: socket.error on send();"
                                " perhaps the source server: %s was rebalancing"
                                " or had connectivity/server problems" %
                                (self.source_node['hostname'])), batch
                    except EOFError:
                        self.upr_done = True
                        return ("error: EOFError on socket send();"
                                " perhaps the source server: %s was rebalancing"
                                " or had connectivity/server problems" %
                                (self.source_node['hostname'])), batch

                    # Close the batch when there's an ACK handshake, so
                    # the server can concurrently send us the next batch.
                    # If we are slow, our slow ACK's will naturally slow
                    # down the server.
                    return 0, batch

                self.ack_last = False
                self.cmd_last = cmd

        except EOFError:
            if batch.size() <= 0 and self.ack_last:
                # A closed conn after an ACK means clean end of TAP dump.
                self.upr_done = True

        if batch.size() <= 0:
            return 0, None
        return 0, batch

    def get_upr_conn(self):
        """Return previously connected upr conn."""

        if not self.upr_conn:
            host = self.source_node['hostname'].split(':')[0]
            port = self.source_node['ports']['direct']
            version = self.source_node['version']

            logging.debug("  UPRStreamSource connecting mc: " +
                          host + ":" + str(port))

            self.upr_conn = cb_bin_client.MemcachedClient(host, port)
            if not self.upr_conn:
                return "error: could not connect to memcached: " + \
                    host + ":" + str(port), None
            self.mem_conn = cb_bin_client.MemcachedClient(host, port)
            if not self.upr_conn:
                return "error: could not connect to memcached: " + \
                    host + ":" + str(port), None
            sasl_user = str(pump.get_username(self.source_bucket.get("name", self.opts.username)))
            sasl_pswd = str(pump.get_password(self.source_bucket.get("saslPassword", self.opts.password)))
            if sasl_user:
                try:
                    self.upr_conn.sasl_auth_cram_md5(sasl_user, sasl_pswd)
                    self.mem_conn.sasl_auth_cram_md5(sasl_user, sasl_pswd)
                except cb_bin_client.MemcachedError:
                    try:
                        self.upr_conn.sasl_auth_plain(sasl_user, sasl_pswd)
                        self.mem_conn.sasl_auth_plain(sasl_user, sasl_pswd)
                    except EOFError:
                        return "error: SASL auth error: %s:%s, user: %s" % \
                            (host, port, sasl_user), None
                    except cb_bin_client.MemcachedError:
                        return "error: SASL auth failed: %s:%s, user: %s" % \
                            (host, port, sasl_user), None
                    except socket.error:
                        return "error: SASL auth socket error: %s:%s, user: %s" % \
                            (host, port, sasl_user), None
                except EOFError:
                    return "error: SASL auth error: %s:%s, user: %s" % \
                        (host, port, sasl_user), None
                except socket.error:
                    return "error: SASL auth socket error: %s:%s, user: %s" % \
                        (host, port, sasl_user), None
            extra = struct.pack(couchbaseConstants.UPR_CONNECT_PKT_FMT, 0, \
                                couchbaseConstants.FLAG_UPR_PRODUCER)
            try:
                opaque=self.r.randint(0, 2**32)
                self.upr_conn._sendCmd(couchbaseConstants.CMD_UPR_CONNECT, self.upr_name, \
                                       '', opaque, extra)
                self.upr_conn._handleSingleResponse(opaque)
            except EOFError:
                return "error: Fail to set up UPR connection", None
            except cb_bin_client.MemcachedError:
                return "error: UPR connect memcached error", None
            except socket.error:
                return "error: UPR connection error", None
            self.running = True
            self.start()

            self.setup_upr_streams()
        return 0, self.upr_conn

    def run(self):
        if not self.upr_conn:
            logging.error("socket to memcached server is not created yet.")
            return

        bytes_read = ''
        rd_timeout = 1
        desc = [self.upr_conn.s]
        while self.running:
            readers, writers, errors = select.select(desc, [], [], rd_timeout)
            rd_timeout = .25

            for reader in readers:
                data = reader.recv(1024)
                logging.debug("Read %d bytes off the wire" % len(data))
                if len(data) == 0:
                    raise exceptions.EOFError("Got empty data (remote died?).")
                bytes_read += data

            while len(bytes_read) >= couchbaseConstants.MIN_RECV_PACKET:
                magic, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas= \
                    struct.unpack(couchbaseConstants.RES_PKT_FMT, \
                                  bytes_read[0:couchbaseConstants.MIN_RECV_PACKET])

                if len(bytes_read) < (couchbaseConstants.MIN_RECV_PACKET+bodylen):
                    break

                rd_timeout = 0
                body = bytes_read[couchbaseConstants.MIN_RECV_PACKET : \
                                  couchbaseConstants.MIN_RECV_PACKET+bodylen]
                bytes_read = bytes_read[couchbaseConstants.MIN_RECV_PACKET+bodylen:]

                self.response.put((opcode, status, opaque, cas, keylen, extlen, body, \
                                   bodylen, datatype))

    def setup_upr_streams(self):
        #send request to retrieve vblist and uuid for the node
        stats = self.mem_conn.stats("vbucket-seqno")
        if not stats:
            return "error: fail to retrive vbucket seqno", None
        self.mem_conn.close()

        uuid_list = {}
        seqno_list = {}
        vb_list = {}
        for key, val in stats.items():
            i = key.find(UPRStreamSource.VB_UUID)
            if i > 0:
                if not vb_list.has_key(key[3:i-1]):
                    vb_list[key[3:i-1]] = {}
                vb_list[key[3:i-1]][UPRStreamSource.VB_UUID] = int(val)
            i = key.find(UPRStreamSource.HIGH_SEQNO)
            if i > 0:
                if not vb_list.has_key(key[3:i-1]):
                    vb_list[key[3:i-1]] = {}
                vb_list[key[3:i-1]][UPRStreamSource.HIGH_SEQNO] = int(val)

        flags = 0
        pair_index = (self.source_bucket['name'], self.source_node['hostname'])
        for vbid in vb_list.iterkeys():
            if self.cur['seqno'] and self.cur['seqno'][pair_index]:
                start_seqno = self.cur['seqno'][pair_index][int(vbid)]
            else:
                start_seqno = 0
            hi_seqno = 0
            uuid = 0
            if self.cur['failoverlog'] and self.cur['failoverlog'][pair_index]:
                if vbid in self.cur['failoverlog'][pair_index] and \
                   self.cur['failoverlog'][pair_index][vbid]:
                    #Use the latest failover log
                    self.cur['failoverlog'][pair_index][vbid] = \
                        sorted(self.cur['failoverlog'][pair_index][vbid], \
                               lambda tup: tup[1], reverse=True)
                    uuid, hi_seqno = self.cur['failoverlog'][pair_index][vbid][0]

            self.request_upr_stream(int(vbid), flags, start_seqno,
                                    vb_list[vbid][UPRStreamSource.HIGH_SEQNO],
                                    uuid, hi_seqno)

    def request_upr_stream(self, vbid, flags, start_seqno, end_seqno, vb_uuid, hi_seqno):
        if not self.upr_conn:
            return "error: no upr connection setup yet.", None
        extra = struct.pack(couchbaseConstants.UPR_STREAM_REQ_PKT_FMT,
                            int(flags), 0,
                            int(start_seqno),
                            int(end_seqno),
                            int(vb_uuid),
                            int(hi_seqno))
        self.upr_conn._sendMsg(couchbaseConstants.CMD_UPR_REQUEST_STREAM, '', '', vbid, \
                               extra, 0, 0, vbid)
        self.stream_list[vbid] = (vbid, flags, start_seqno, end_seqno, vb_uuid, hi_seqno)

    def _read_upr_conn(self, upr_conn):
        buf, cmd, errcode, opaque, cas, keylen, extlen, data, datalen = \
            self.recv_upr_msg(upr_conn.s)
        upr_conn.buf = buf

        rv = 0
        metalen = flags = ttl = flg = exp = 0
        meta = key = val = ext = ''
        need_ack = False
        extlen = seqno = 0
        if data:
            ext = data[0:extlen]
            if cmd == couchbaseConstants.CMD_UPR_MUTATION:
                seqno, rev_seqno, flg, exp, locktime, meta = \
                    struct.unpack(couchbaseConstants.UPR_MUTATION_PKT_FMT, ext)
                key_start = extlen
                val_start = key_start + keylen
                key = data[extlen:val_start]
                val = data[val_start:]
            elif cmd == couchbaseConstants.CMD_UPR_DELETE or \
                 cmd == couchbaseConstants.CMD_UPR_EXPIRATION:
                seqno, rev_seqno, meta = \
                    struct.unpack(couchbaseConstants.UPR_DELETE_PKT_FMT, ext)
                key_start = extlen
                val_start = key_start + keylen
                key = data[extlen:val_start]
            else:
                rv = "error: uninterpreted UPR commands:%s" % cmd
        elif datalen:
            rv = "error: could not read full TAP message body"

        return rv, cmd, errcode, key, flg, exp, cas, meta, val, opaque, need_ack, seqno

    def _recv_upr_msg_error(self, sock, buf):
        pkt, buf = self.recv_upr(sock, couchbaseConstants.MIN_RECV_PACKET, buf)
        if not pkt:
            raise EOFError()
        magic, cmd, keylen, extlen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(couchbaseConstants.REQ_PKT_FMT, pkt)
        if magic != couchbaseConstants.REQ_MAGIC_BYTE:
            raise Exception("unexpected recv_msg magic: " + str(magic))
        data, buf = self.recv_upr(sock, datalen, buf)
        return buf, cmd, errcode, opaque, cas, keylen, extlen, data, datalen

    def _recv_upr_msg(self, sock):
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

    def _recv_upr(self, skt, nbytes, buf):
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
                                                     user, pswd, path,
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

class UPRSink(pump_cb.CBSink):
    """Smart client using UPR protocol to steam data to couchbase cluster."""
    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(UPRSink, self).__init__(opts, spec, source_bucket, source_node,
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
        """Return previously connected upr conn."""

        host = node.split(':')[0]
        port = node.split(':')[1]

        logging.debug("  UPRSink connecting mc: " +
                      host + ":" + str(port))

        upr_conn = cb_bin_client.MemcachedClient(host, port)
        if not upr_conn:
            return "error: could not connect to memcached: " + \
                host + ":" + str(port), None
        if sasl_user:
            try:
                upr_conn.sasl_auth_plain(sasl_user, sasl_pswd)
            except EOFError:
                return "error: SASL auth error: %s:%s, user: %s" % \
                    (host, port, sasl_user), None
            except cb_bin_client.MemcachedError:
                return "error: SASL auth failed: %s:%s, user: %s" % \
                    (host, port, sasl_user), None
            except socket.error:
                return "error: SASL auth socket error: %s:%s, user: %s" % \
                    (host, port, sasl_user), None
        extra = struct.pack(couchbaseConstants.UPR_CONNECT_PKT_FMT, 0, \
                            couchbaseConstants.FLAG_UPR_CONSUMER)
        try:
            opaque=self.r.randint(0, 2**32)
            upr_conn._sendCmd(couchbaseConstants.CMD_UPR_CONNECT, self.upr_name, '', opaque, extra)
            upr_conn._handleSingleResponse(opaque)
        except EOFError:
            return "error: Fail to set up UPR connection", None
        except cb_bin_client.MemcachedError:
            return "error: UPR connect memcached error", None
        except socket.error:
            return "error: UPR connection error", None

        return 0, upr_conn

    def find_conn(self, mconns, vbucket_id):
        if not self.vbucket_list:
            return super(UPRSink, self).find_conn(mconns, vbucket_id)

        vbuckets = json.loads(self.vbucket_list)
        if isinstance(vbuckets, list):
            if vbucket_id not in vbuckets:
                return "error: unexpected vbucket id:" + str(vbucket_id), None
            return super(UPRSink, self).find_conn(mconns, vbucket_id)

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
                            rv, conn = UPRSink.connect_mc(host_port, user, pswd)
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
            cmd, vbucket_id_msg, key, flg, exp, cas, meta, val, seqno, dtype, nmeta = msg
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
        if cmd == couchbaseConstants.CMD_UPR_MUTATION:
            ext = struct.pack(couchbaseConstants.UPR_MUTATION_PKT_FMT,
                              seqno, rev_seqno, flg, exp, locktime, metalen, nru)
        elif cmd == couchbaseConstants.CMD_UPR_DELETE:
            ext = struct.pack(couchbaseConstants.UPR_DELETE_PKT_FMT,
                              seqno, rev_seqno, metalen)
        else:
            return "error: MCSink - unknown tap cmd for request: " + str(cmd), None

        hdr = self.cmd_header(cmd, vbucket_id, key, val, ext, cas, opaque, metalen, dtype)
        return 0, (hdr, ext, seq_no, key, val)

    def cmd_header(self, cmd, vbucket_id, key, val, ext, cas, opaque, metalen,
                   dtype=0,
                   fmt=couchbaseConstants.REQ_PKT_FMT,
                   magic=couchbaseConstants.REQ_MAGIC_BYTE):
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
