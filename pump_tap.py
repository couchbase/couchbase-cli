#!/usr/bin/env python

import logging
import random
import simplejson as json
import socket
import string
import struct
import time

import cb_bin_client
import couchbaseConstants

import pump
import pump_mc
import pump_cb

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

# TODO: (1) TAPDumpSource - handle TAP_FLAG_NETWORK_BYTE_ORDER.

class TAPDumpSource(pump.Source):
    """Can read from cluster/server/bucket via TAP dump."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(TAPDumpSource, self).__init__(opts, spec, source_bucket, source_node,
                                            source_map, sink_map, ctl, cur)
        self.tap_done = False
        self.tap_conn = None
        self.tap_name = "".join(random.sample(string.letters, 16))
        self.ack_last = False # True when the last TAP msg had TAP_FLAG_ACK.
        self.cmd_last = None
        self.num_msg = 0

        self.recv_min_bytes = int(getattr(opts, "recv_min_bytes", 4096))
        self.vbucket_list = getattr(opts, "vbucket_list", None)

    @staticmethod
    def can_handle(opts, spec):
        return (spec.startswith("http://") or
                spec.startswith("couchbase://"))

    @staticmethod
    def check(opts, spec):
        err, map = pump.rest_couchbase(opts, spec)
        if err:
            return err, map
        if not map or not map.get('buckets'):
            return ("error: no TAP'able buckets at source: %s;"
                    " please check your username/password to the cluster" %
                    (spec)), None
        return 0, map

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        spec_parts = source_map.get('spec_parts')
        if not spec_parts:
            return "error: no design spec_parts", None
        host, port, user, pswd, path = spec_parts

        source_nodes = pump.filter_bucket_nodes(source_bucket, spec_parts)
        if not source_nodes:
            if spec_parts[0] not in ['localhost', '127.0.0.1']:
                return ("error: no design source node; spec_parts: %s" %
                        (spec_parts,), None)
            else:
                source_nodes = source_bucket['nodes']

        couch_api_base = source_nodes[0].get('couchApiBase')
        if not couch_api_base:
            return 0, None # No couchApiBase; probably not 2.0.

        err, ddocs_json, ddocs = \
            pump.rest_request_json(host, int(port), user, pswd,
                                   "/pools/default/buckets/%s/ddocs" %
                                   (source_bucket['name']),
                                   reason="provide_design")
        if err and "response: 404" in err: # A 404/not-found likely means 2.0-DP4.
            ddocs_json = None
            ddocs_url = couch_api_base + "/_all_docs"
            ddocs_qry = "?startkey=\"_design/\"&endkey=\"_design0\"&include_docs=true"

            host, port, user, pswd, path = \
                pump.parse_spec(opts, ddocs_url, 8092)
            # Not using user/pwd as 2.0-DP4 CAPI did not support auth.
            err, ddocs_json, ddocs = \
                pump.rest_request_json(host, int(port), None, None,
                                       path + ddocs_qry,
                                       reason="provide_design-2.0DP4")
        if err:
            return err, None

        return 0, json.dumps(ddocs.get('rows', []))

    def provide_batch(self):
        cur_sleep = 0.2
        cur_retry = 0
        max_retry = self.opts.extra['max_retry']

        while True:
            if self.tap_done:
                return 0, None

            rv, tap_conn = self.get_tap_conn()
            if rv != 0:
                self.tap_done = True
                return rv, None

            rv, batch = self.provide_batch_actual(tap_conn)
            if rv == 0:
                return rv, batch

            if self.tap_conn:
                self.tap_conn.close()
                self.tap_conn = None

            if cur_retry >= max_retry:
                self.tap_done = True
                return rv, batch

            logging.warn("backoff: %s, sleeping: %s, on error: %s" %
                         (cur_retry, cur_sleep, rv))
            time.sleep(cur_sleep)
            cur_sleep = min(cur_sleep * 2, 20) # Max backoff sleep 20 seconds.
            cur_retry = cur_retry + 1

    def provide_batch_actual(self, tap_conn):
        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        try:
            while (not self.tap_done and
                   batch.size() < batch_max_size and
                   batch.bytes < batch_max_bytes):
                # TODO: (1) TAPDumpSource - provide_batch timeout on inactivity.

                rv, cmd, vbucket_id, key, flg, exp, cas, meta, val, \
                    opaque, need_ack = self.read_tap_conn(tap_conn)
                if rv != 0:
                    self.tap_done = True
                    return rv, batch

                if (cmd == couchbaseConstants.CMD_TAP_MUTATION or
                    cmd == couchbaseConstants.CMD_TAP_DELETE):
                    if not self.skip(key, vbucket_id):
                        msg = (cmd, vbucket_id, key, flg, exp, cas, meta, val)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                elif cmd == couchbaseConstants.CMD_TAP_OPAQUE:
                    pass
                elif cmd == couchbaseConstants.CMD_NOOP:
                    # 1.8.x servers might not end the TAP dump on an empty bucket,
                    # so we treat 2 NOOP's in a row as the end and proactively close.
                    # Only do this when there've been no msgs to avoid closing
                    # during a slow backfill.
                    if (self.cmd_last == couchbaseConstants.CMD_NOOP and
                        self.num_msg == 0 and
                        batch.size() <= 0):
                        self.tap_done = True
                        return 0, batch
                elif cmd == couchbaseConstants.CMD_TAP_FLUSH:
                    logging.warn("stopping: saw CMD_TAP_FLUSH")
                    self.tap_done = True
                    break
                else:
                    s = str(pump.CMD_STR.get(cmd, cmd))
                    logging.warn("warning: unexpected TAP message: " + s)
                    return "unexpected TAP message: " + s, batch

                if need_ack:
                    self.ack_last = True
                    try:
                        tap_conn._sendMsg(cmd, '', '', opaque, vbucketId=0,
                                          fmt=couchbaseConstants.RES_PKT_FMT,
                                          magic=couchbaseConstants.RES_MAGIC_BYTE)
                    except socket.error:
                        return ("error: socket.error on send();"
                                " perhaps the source server: %s was rebalancing"
                                " or had connectivity/server problems" %
                                (self.source_node['hostname'])), batch
                    except EOFError:
                        self.tap_done = True
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
                self.tap_done = True

        if batch.size() <= 0:
            return 0, None

        return 0, batch

    def get_tap_conn(self):
        """Return previously connected tap conn."""

        if not self.tap_conn:
            host = self.source_node['hostname'].split(':')[0]
            port = self.source_node['ports']['direct']
            version = self.source_node['version'] # Ex: '2.0.0-1944-rel-community' or '1.8.1-937-rel-community'.

            logging.debug("  TAPDumpSource connecting mc: " +
                          host + ":" + str(port))

            self.tap_conn = cb_bin_client.MemcachedClient(host, port)
            if not self.tap_conn:
                return "error: could not connect to memcached: " + \
                    host + ":" + str(port), None

            sasl_user = str(self.source_bucket.get("name",
                                                   self.opts.username) or "")
            sasl_pswd = str(self.source_bucket.get("saslPassword",
                                                   self.opts.password) or "")
            if sasl_user:
                try:
                    self.tap_conn.sasl_auth_plain(sasl_user, sasl_pswd)
                except EOFError:
                    return "error: SASL auth error: %s:%s, user: %s" % \
                        (host, port, sasl_user), None
                except cb_bin_client.MemcachedError:
                    return "error: SASL auth failed: %s:%s, user: %s" % \
                        (host, port, sasl_user), None
                except socket.error:
                    return "error: SASL auth socket error: %s:%s, user: %s" % \
                        (host, port, sasl_user), None

            # We explicitly do not use TAP_FLAG_REGISTERED_CLIENT,
            # as that is for checkpoint/incremental backup only.
            #
            tap_opts = {couchbaseConstants.TAP_FLAG_DUMP: '',
                        couchbaseConstants.TAP_FLAG_SUPPORT_ACK: ''}

            self.tap_conn.tap_fix_flag_byteorder = version.split(".") >= ["2", "0", "0"]
            if self.tap_conn.tap_fix_flag_byteorder:
                tap_opts[couchbaseConstants.TAP_FLAG_TAP_FIX_FLAG_BYTEORDER] = ''

            if self.vbucket_list:
                tap_opts[couchbaseConstants.TAP_FLAG_LIST_VBUCKETS] = ''

            ext, val = TAPDumpSource.encode_tap_connect_opts(tap_opts, vblist=self.vbucket_list)

            self.tap_conn._sendCmd(couchbaseConstants.CMD_TAP_CONNECT,
                                   self.tap_name, val, 0, ext)

        return 0, self.tap_conn

    def read_tap_conn(self, tap_conn):
        buf, cmd, vbucket_id, opaque, cas, keylen, extlen, data, datalen = \
            self.recv_msg(tap_conn.s, getattr(tap_conn, 'buf', ''))
        tap_conn.buf = buf

        rv = 0
        metalen = flags = ttl = flg = exp = 0
        meta = key = val = ext = ''
        need_ack = False

        if data:
            ext = data[0:extlen]
            if extlen == 8:
                metalen, flags, ttl = \
                    struct.unpack(couchbaseConstants.TAP_GENERAL_PKT_FMT, ext)
            elif extlen == 16:
                metalen, flags, ttl, flg, exp = \
                    struct.unpack(couchbaseConstants.TAP_MUTATION_PKT_FMT, ext)
                if not tap_conn.tap_fix_flag_byteorder:
                    flg = socket.ntohl(flg)
            need_ack = flags & couchbaseConstants.TAP_FLAG_ACK
            meta_start = extlen
            key_start = meta_start + metalen
            val_start = key_start + keylen

            meta = data[meta_start:key_start]
            key = data[key_start:val_start]
            val = data[val_start:]
        elif datalen:
            rv = "error: could not read full TAP message body"

        return rv, cmd, vbucket_id, key, flg, exp, cas, meta, val, opaque, need_ack

    def recv_msg(self, sock, buf):
        pkt, buf = self.recv(sock, couchbaseConstants.MIN_RECV_PACKET, buf)
        if not pkt:
            raise EOFError()
        magic, cmd, keylen, extlen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(couchbaseConstants.REQ_PKT_FMT, pkt)
        if magic != couchbaseConstants.REQ_MAGIC_BYTE:
            raise Exception("unexpected recv_msg magic: " + str(magic))
        data, buf = self.recv(sock, datalen, buf)
        return buf, cmd, errcode, opaque, cas, keylen, extlen, data, datalen

    def recv(self, skt, nbytes, buf):
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
    def encode_tap_connect_opts(opts, backfill=False, vblist=None):
        header = 0
        val = []
        for op in sorted(opts.keys()):
            header |= op
            if op in couchbaseConstants.TAP_FLAG_TYPES:
                val.append(struct.pack(couchbaseConstants.TAP_FLAG_TYPES[op],
                                       opts[op]))
            elif backfill and op == couchbaseConstants.TAP_FLAG_CHECKPOINT:
                if opts[op][2] >= 0:
                    val.append(struct.pack(">HHQ",
                                           opts[op][0], opts[op][1], opts[op][2]))
            elif vblist and op == couchbaseConstants.TAP_FLAG_LIST_VBUCKETS:
                vblist = json.loads(vblist)
                if isinstance(vblist, dict):
                    for node, vbucketlist in vblist.iteritems():
                        val.append(struct.pack(">H", len(vbucketlist)))
                        for v in vbucketlist:
                            val.append(struct.pack(">H", int(v)))
                elif isinstance(vblist, list):
                    val.append(struct.pack(">H", len(vblist)))
                    for v in vblist:
                        val.append(struct.pack(">H", int(v)))
            else:
                val.append(opts[op])

        return struct.pack(">I", header), ''.join(val)

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
            #for DGM case, server will transfer both in-memory items and backfill all items on disk
            total_msgs += (resident_ratio/100.0) * stats_vals["curr_items"]
        return 0, int(total_msgs)

class TapSink(pump_cb.CBSink):
    """Smart client using tap protocol to steam data to couchbase cluster."""
    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(TapSink, self).__init__(opts, spec, source_bucket, source_node,
                                     source_map, sink_map, ctl, cur)

        self.tap_flags = couchbaseConstants.TAP_FLAG_NETWORK_BYTE_ORDER
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

    def find_conn(self, mconns, vbucket_id):
        if not self.vbucket_list:
            return super(TapSink, self).find_conn(mconns, vbucket_id)

        vbuckets = json.loads(self.vbucket_list)
        if isinstance(vbuckets, list):
            if vbucket_id not in vbuckets:
                return "error: unexpected vbucket id:" + str(vbucket_id), None
            return super(TapSink, self).find_conn(mconns, vbucket_id)

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
                            rv, conn = TapSink.connect_mc(node, port_number, user, pswd)
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
            cmd, vbucket_id_msg, key, flg, exp, cas, meta, val = msg
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
                                       exp, cas, meta, i, need_ack)
            if rv != 0:
                return rv
            self.append_req(m, req)
        if m:
            try:
                conn.s.send(''.join(m))
            except socket.error, e:
                return "error: conn.send() exception: %s" % (e)
        return 0

    def cmd_request(self, cmd, vbucket_id, key, val, flg, exp, cas, meta, opaque, need_ack):
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
        ttl = -1
        tap_flags = self.tap_flags
        if need_ack:
            tap_flags |= couchbaseConstants.TAP_FLAG_ACK
        if cmd == couchbaseConstants.CMD_TAP_MUTATION:
            ext = struct.pack(couchbaseConstants.TAP_MUTATION_PKT_FMT,
                              metalen,
                              tap_flags,
                              ttl, flg, exp)
        elif cmd == couchbaseConstants.CMD_TAP_DELETE:
            ext = struct.pack(couchbaseConstants.TAP_GENERAL_PKT_FMT,
                              metalen,
                              tap_flags, ttl)
        else:
            return "error: MCSink - unknown tap cmd for request: " + str(cmd), None

        hdr = self.cmd_header(cmd, vbucket_id, key, val, ext, 0, opaque, metalen)
        return 0, (hdr, ext, seq_no, key, val)

    def cmd_header(self, cmd, vbucket_id, key, val, ext, cas, opaque, metalen,
                   dtype=0,
                   fmt=couchbaseConstants.REQ_PKT_FMT,
                   magic=couchbaseConstants.REQ_MAGIC_BYTE):
        return struct.pack(fmt, magic, cmd,
                           len(key), len(ext), dtype, vbucket_id,
                           metalen + len(key) + len(val) + len(ext), opaque, cas)

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
