#!/usr/bin/env python

import logging
import random
import simplejson as json
import socket
import string
import struct
import time

import mc_bin_client
import memcacheConstants

import pump

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

                if (cmd == memcacheConstants.CMD_TAP_MUTATION or
                    cmd == memcacheConstants.CMD_TAP_DELETE):
                    if not self.skip(key, vbucket_id):
                        msg = (cmd, vbucket_id, key, flg, exp, cas, meta, val)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                elif cmd == memcacheConstants.CMD_TAP_OPAQUE:
                    pass
                elif cmd == memcacheConstants.CMD_NOOP:
                    # 1.8.x servers might not end the TAP dump on an empty bucket,
                    # so we treat 2 NOOP's in a row as the end and proactively close.
                    # Only do this when there've been no msgs to avoid closing
                    # during a slow backfill.
                    if (self.cmd_last == memcacheConstants.CMD_NOOP and
                        self.num_msg == 0 and
                        batch.size() <= 0):
                        self.tap_done = True
                        return 0, batch
                elif cmd == memcacheConstants.CMD_TAP_FLUSH:
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
                                          fmt=memcacheConstants.RES_PKT_FMT,
                                          magic=memcacheConstants.RES_MAGIC_BYTE)
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

            self.tap_conn = mc_bin_client.MemcachedClient(host, port)
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
                except mc_bin_client.MemcachedError:
                    return "error: SASL auth failed: %s:%s, user: %s" % \
                        (host, port, sasl_user), None
                except socket.error:
                    return "error: SASL auth socket error: %s:%s, user: %s" % \
                        (host, port, sasl_user), None

            # We explicitly do not use TAP_FLAG_REGISTERED_CLIENT,
            # as that is for checkpoint/incremental backup only.
            #
            tap_opts = {memcacheConstants.TAP_FLAG_DUMP: '',
                        memcacheConstants.TAP_FLAG_SUPPORT_ACK: ''}

            self.tap_conn.tap_fix_flag_byteorder = version.split(".") >= ["2", "0", "0"]
            if self.tap_conn.tap_fix_flag_byteorder:
                tap_opts[memcacheConstants.TAP_FLAG_TAP_FIX_FLAG_BYTEORDER] = ''

            ext, val = TAPDumpSource.encode_tap_connect_opts(tap_opts)

            self.tap_conn._sendCmd(memcacheConstants.CMD_TAP_CONNECT,
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
                    struct.unpack(memcacheConstants.TAP_GENERAL_PKT_FMT, ext)
            elif extlen == 16:
                metalen, flags, ttl, flg, exp = \
                    struct.unpack(memcacheConstants.TAP_MUTATION_PKT_FMT, ext)
                if not tap_conn.tap_fix_flag_byteorder:
                    flg = socket.ntohl(flg)

            need_ack = flags & memcacheConstants.TAP_FLAG_ACK

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
        pkt, buf = self.recv(sock, memcacheConstants.MIN_RECV_PACKET, buf)
        if not pkt:
            raise EOFError()
        magic, cmd, keylen, extlen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(memcacheConstants.REQ_PKT_FMT, pkt)
        if magic != memcacheConstants.REQ_MAGIC_BYTE:
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
    def encode_tap_connect_opts(opts, backfill=False):
        header = 0
        val = []

        for op in sorted(opts.keys()):
            header |= op
            if op in memcacheConstants.TAP_FLAG_TYPES:
                val.append(struct.pack(memcacheConstants.TAP_FLAG_TYPES[op],
                                       opts[op]))
            elif backfill and op == memcacheConstants.TAP_FLAG_CHECKPOINT:
                if opts[op][2] >= 0:
                    val.append(struct.pack(">HHQ",
                                           opts[op][0], opts[op][1], opts[op][2]))
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
        path = "/pools/default/buckets/%s/stats/curr_items" % (name)
        host, port, user, pswd, _ = pump.parse_spec(opts, spec, 8091)
        err, json, data = pump.rest_request_json(host, int(port),
                                                 user, pswd, path,
                                                 reason="total_msgs")
        if err:
            return 0, None

        nodeStats = data.get("nodeStats", None)
        if not nodeStats:
            return 0, None

        curr_items = nodeStats.get(source_name, None)
        if not curr_items:
            return 0, None

        return 0, curr_items[-1]
