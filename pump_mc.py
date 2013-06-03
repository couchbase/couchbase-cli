#!/usr/bin/env python

import logging
import socket
import struct
import time
import sys

import cb_bin_client
import couchbaseConstants
import pump

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

OP_MAP = {
    'get': couchbaseConstants.CMD_GET,
    'set': couchbaseConstants.CMD_SET,
    'add': couchbaseConstants.CMD_ADD,
    'delete': couchbaseConstants.CMD_DELETE,
    }

OP_MAP_WITH_META = {
    'get': couchbaseConstants.CMD_GET,
    'set': couchbaseConstants.CMD_SET_WITH_META,
    'add': couchbaseConstants.CMD_ADD_WITH_META,
    'delete': couchbaseConstants.CMD_DELETE_WITH_META
    }

class MCSink(pump.Sink):
    """Dumb client sink using binary memcached protocol.
       Used when moxi or memcached is destination."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(MCSink, self).__init__(opts, spec, source_bucket, source_node,
                                     source_map, sink_map, ctl, cur)

        self.op_map = OP_MAP
        if opts.extra.get("try_xwm", 1):
            self.op_map = OP_MAP_WITH_META
        self.conflict_resolve = opts.extra.get("conflict_resolve", 1)
        self.init_worker(MCSink.run)

    def close(self):
        self.push_next_batch(None, None)

    @staticmethod
    def check_base(opts, spec):
        if getattr(opts, "destination_vbucket_state", "active") != "active":
            return ("error: only --destination-vbucket-state=active" +
                    " is supported by this destination: %s") % (spec)

        op = getattr(opts, "destination_operation", None)
        if not op in [None, 'set', 'add', 'get']:
            return ("error: --destination-operation unsupported value: %s" +
                    "; use set, add, get") % (op)

        # Skip immediate superclass Sink.check_base(),
        # since MCSink can handle different destination operations.
        return pump.EndPoint.check_base(opts, spec)

    @staticmethod
    def run(self):
        """Worker thread to asynchronously store batches into sink."""

        mconns = {} # State kept across scatter_gather() calls.

        while not self.ctl['stop']:
            batch, future = self.pull_next_batch()
            if not batch:
                self.future_done(future, 0)
                self.close_mconns(mconns)
                return

            backoff = 0.1 # Reset backoff after a good batch.

            while batch:  # Loop in case retry is required.
                rv, batch, need_backoff = self.scatter_gather(mconns, batch)
                if rv != 0:
                    self.future_done(future, rv)
                    self.close_mconns(mconns)
                    return

                if batch:
                    self.cur["tot_sink_retry_batch"] = \
                        self.cur.get("tot_sink_retry_batch", 0) + 1

                if need_backoff:
                    backoff = min(backoff * 2.0, 10.0)
                    logging.warn("backing off, secs: %s" % (backoff))
                    time.sleep(backoff)

            self.future_done(future, 0)

        self.close_mconns(mconns)

    def close_mconns(self, mconns):
        for k, conn in mconns.items():
            conn.close()

    def scatter_gather(self, mconns, batch):
        conn = mconns.get("conn")
        if not conn:
            rv, conn = self.connect()
            if rv != 0:
                return rv, None
            mconns["conn"] = conn

        # TODO: (1) MCSink - run() handle --data parameter.

        # Scatter or send phase.
        rv = self.send_msgs(conn, batch.msgs, self.operation())
        if rv != 0:
            return rv, None, None

        # Gather or recv phase.
        rv, retry, refresh = self.recv_msgs(conn, batch.msgs)
        if refresh:
            self.refresh_sink_map()
        if retry:
            return rv, batch, True

        return rv, None, None

    def send_msgs(self, conn, msgs, operation, vbucket_id=None):
        m = []

        for i, msg in enumerate(msgs):
            cmd, vbucket_id_msg, key, flg, exp, cas, meta, val = msg
            if vbucket_id is not None:
                vbucket_id_msg = vbucket_id

            if self.skip(key, vbucket_id_msg):
                continue

            rv, cmd = self.translate_cmd(cmd, operation, meta)
            if rv != 0:
                return rv

            if cmd == couchbaseConstants.CMD_GET:
                val, flg, exp, cas = '', 0, 0, 0
            if cmd == couchbaseConstants.CMD_NOOP:
                key, val, flg, exp, cas = '', '', 0, 0, 0
            if cmd in (couchbaseConstants.CMD_DELETE, couchbaseConstants.CMD_DELETE_WITH_META):
                val = ''
            rv, req = self.cmd_request(cmd, vbucket_id_msg, key, val,
                                       ctypes.c_uint32(flg).value,
                                       exp, cas, meta, i)
            if rv != 0:
                return rv

            self.append_req(m, req)

        if m:
            try:
                conn.s.send(''.join(m))
            except socket.error, e:
                return "error: conn.send() exception: %s" % (e)

        return 0

    def recv_msgs(self, conn, msgs, vbucket_id=None, verify_opaque=True):
        refresh = False
        retry = False

        for i, msg in enumerate(msgs):
            cmd, vbucket_id_msg, key, flg, exp, cas, meta, val = msg
            if vbucket_id is not None:
                vbucket_id_msg = vbucket_id

            if self.skip(key, vbucket_id_msg):
                continue

            try:
                r_cmd, r_status, r_ext, r_key, r_val, r_cas, r_opaque = \
                    self.read_conn(conn)
                if verify_opaque and i != r_opaque:
                    return "error: opaque mismatch: %s %s" % (i, r_opaque), None, None

                if r_status == couchbaseConstants.ERR_SUCCESS:
                    continue
                elif r_status == couchbaseConstants.ERR_KEY_EEXISTS:
                    logging.warn("item exists: %s, key: %s" %
                                 (self.spec, key))
                    continue
                elif r_status == couchbaseConstants.ERR_KEY_ENOENT:
                    if (cmd != couchbaseConstants.CMD_TAP_DELETE and
                        cmd != couchbaseConstants.CMD_GET):
                        logging.warn("item not found: %s, key: %s" %
                                     (self.spec, key))
                    continue
                elif (r_status == couchbaseConstants.ERR_ETMPFAIL or
                      r_status == couchbaseConstants.ERR_EBUSY or
                      r_status == couchbaseConstants.ERR_ENOMEM):
                    retry = True # Retry the whole batch again next time.
                    continue     # But, finish recv'ing current batch.
                elif r_status == couchbaseConstants.ERR_NOT_MY_VBUCKET:
                    msg = ("received NOT_MY_VBUCKET;"
                           " perhaps the cluster is/was rebalancing;"
                           " vbucket_id: %s, key: %s, spec: %s, host:port: %s:%s"
                           % (vbucket_id_msg, key, self.spec,
                              conn.host, conn.port))
                    if self.opts.extra.get("nmv_retry", 1):
                        logging.warn("warning: " + msg)
                        refresh = True
                        retry = True
                        self.cur["tot_sink_not_my_vbucket"] = \
                            self.cur.get("tot_sink_not_my_vbucket", 0) + 1
                    else:
                        return "error: " + msg, None, None
                elif r_status == couchbaseConstants.ERR_UNKNOWN_COMMAND:
                    if self.op_map == OP_MAP:
                        if not retry:
                            return "error: unknown command: %s" % (r_cmd), None, None
                    else:
                        if not retry:
                            logging.warn("destination does not take XXX-WITH-META"
                                         " commands; will use META-less commands")
                        self.op_map = OP_MAP
                        retry = True
                else:
                    return "error: MCSink MC error: " + str(r_status), None, None

            except Exception, e:
                logging.error("MCSink exception: %s", e)
                return "error: MCSink exception: " + str(e), None, None
        return 0, retry, refresh

    def translate_cmd(self, cmd, op, meta):
        if len(str(meta)) <= 0:
            # The source gave no meta, so use regular commands.
            self.op_map = OP_MAP

        if cmd == couchbaseConstants.CMD_TAP_MUTATION:
            m = self.op_map.get(op, None)
            if m:
                return 0, m
            return "error: MCSink.translate_cmd, unsupported op: " + op, None

        if cmd == couchbaseConstants.CMD_TAP_DELETE:
            if op == 'get':
                return 0, couchbaseConstants.CMD_NOOP
            return 0, self.op_map['delete']

        if cmd == couchbaseConstants.CMD_GET:
            return 0, cmd

        return "error: MCSink - unknown cmd: %s, op: %s" % (cmd, op), None

    def append_req(self, m, req):
        hdr, ext, key, val = req
        m.append(hdr)
        if ext:
            m.append(ext)
        if key:
            m.append(str(key))
        if val:
            m.append(str(val))

    @staticmethod
    def can_handle(opts, spec):
        return (spec.startswith("memcached://") or
                spec.startswith("memcached-binary://"))

    @staticmethod
    def check(opts, spec, source_map):
        host, port, user, pswd, path = \
            pump.parse_spec(opts, spec, int(getattr(opts, "port", 11211)))
        rv, conn = MCSink.connect_mc(host, port, user, pswd)
        if rv != 0:
            return rv, None
        conn.close()
        return 0, None

    def refresh_sink_map(self):
        return 0

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design):
        if source_design:
            logging.warn("warning: cannot restore bucket design"
                         " on a memached destination")
        return 0

    def consume_batch_async(self, batch):
        return self.push_next_batch(batch, pump.SinkBatchFuture(self, batch))

    def connect(self):
        host, port, user, pswd, path = \
            pump.parse_spec(self.opts, self.spec,
                            int(getattr(self.opts, "port", 11211)))
        return MCSink.connect_mc(host, port, user, pswd)

    @staticmethod
    def connect_mc(host, port, user, pswd):
        mc = cb_bin_client.MemcachedClient(host, int(port))
        if user:
            try:
                mc.sasl_auth_plain(str(user), str(pswd))
            except EOFError:
                return "error: SASL auth error: %s:%s, user: %s" % \
                    (host, port, user), None
            except cb_bin_client.MemcachedError:
                return "error: SASL auth failed: %s:%s, user: %s" % \
                    (host, port, user), None
            except socket.error:
                return "error: SASL auth exception: %s:%s, user: %s" % \
                    (host, port, user), None
        return 0, mc

    def cmd_request(self, cmd, vbucket_id, key, val, flg, exp, cas, meta, opaque):
        if (cmd == couchbaseConstants.CMD_SET_WITH_META or
            cmd == couchbaseConstants.CMD_ADD_WITH_META or
            cmd == couchbaseConstants.CMD_DELETE_WITH_META):
            cr = int(self.conflict_resolve)
            if meta:
                seq_no = str(meta)
                if len(seq_no) > 8:
                    seq_no = seq_no[0:8]
                if len(seq_no) < 8:
                    # The seq_no might be 32-bits from 2.0DP4, so pad with 0x00's.
                    seq_no = ('\x00\x00\x00\x00\x00\x00\x00\x00' + seq_no)[-8:]
                check_seqno, = struct.unpack(">Q", seq_no)
                if check_seqno:
                    ext = (struct.pack(">II", flg, exp) + seq_no +
                           struct.pack(">Q", cas) + struct.pack(">I", cr))
                else:
                    ext = struct.pack(">IIQQI", flg, exp, 1, cas, cr)
            else:
                ext = struct.pack(">IIQQI", flg, exp, 1, cas, cr)
        elif (cmd == couchbaseConstants.CMD_SET or
              cmd == couchbaseConstants.CMD_ADD):
            ext = struct.pack(couchbaseConstants.SET_PKT_FMT, flg, exp)
        elif (cmd == couchbaseConstants.CMD_DELETE or
              cmd == couchbaseConstants.CMD_GET or
              cmd == couchbaseConstants.CMD_NOOP):
            ext = ''
        else:
            return "error: MCSink - unknown cmd for request: " + str(cmd), None

        hdr = self.cmd_header(cmd, vbucket_id, key, val, ext, 0, opaque)
        return 0, (hdr, ext, key, val)

    def cmd_header(self, cmd, vbucket_id, key, val, ext, cas, opaque,
                   dtype=0,
                   fmt=couchbaseConstants.REQ_PKT_FMT,
                   magic=couchbaseConstants.REQ_MAGIC_BYTE):
        return struct.pack(fmt, magic, cmd,
                           len(key), len(ext), dtype, vbucket_id,
                           len(key) + len(ext) + len(val), opaque, cas)

    def read_conn(self, conn):
        ext = ''
        key = ''
        val = ''

        buf, cmd, errcode, extlen, keylen, data, cas, opaque = \
            self.recv_msg(conn.s, getattr(conn, 'buf', ''))
        conn.buf = buf

        if data:
            ext = data[0:extlen]
            key = data[extlen:extlen+keylen]
            val = data[extlen+keylen:]

        return cmd, errcode, ext, key, val, cas, opaque

    def recv_msg(self, sock, buf):
        pkt, buf = self.recv(sock, couchbaseConstants.MIN_RECV_PACKET, buf)
        if not pkt:
            raise EOFError()
        magic, cmd, keylen, extlen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(couchbaseConstants.RES_PKT_FMT, pkt)
        if magic != couchbaseConstants.RES_MAGIC_BYTE:
            raise Exception("unexpected recv_msg magic: " + str(magic))
        data, buf = self.recv(sock, datalen, buf)
        return buf, cmd, errcode, extlen, keylen, data, cas, opaque

    def recv(self, skt, nbytes, buf):
        while len(buf) < nbytes:
            data = None
            try:
                data = skt.recv(max(nbytes - len(buf), 4096))
            except socket.timeout:
                logging.error("error: recv socket.timeout")
            except Exception, e:
                logging.error("error: recv exception: " + str(e))

            if not data:
                return None, ''
            buf += data

        return buf[:nbytes], buf[nbytes:]
