#!/usr/bin/env python

import base64
import logging
import random
import re
import socket
import string
import struct

import mc_bin_client
import memcacheConstants

import pump

class TAPDumpSource(pump.Source):
    """Can read from cluster/server/bucket via TAP dump."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        pump.Source.__init__(self, opts, spec, source_bucket, source_node,
                             source_map, sink_map, ctl, cur)

        self.tap_done = False
        self.tap_conn = None
        self.tap_name = "".join(random.sample(string.letters, 16))

        self.recv_min_bytes = int(getattr(opts, "recv_min_bytes", 4096))

    @staticmethod
    def can_handle(opts, spec):
        return (spec.startswith("http://") or
                spec.startswith("couchbase://"))

    @staticmethod
    def check(opts, spec):
        return pump.rest_couchbase(opts, spec)

    @staticmethod
    def provide_config(opts, spec, bucket):
        # TODO: (4) TAPDumpSource - provide_config implementation.
        return 0, None

    @staticmethod
    def provide_design(opts, spec, bucket):
        # TODO: (4) TAPDumpSource - provide_design implementation.
        return 0, None

    def provide_batch(self):
        if self.tap_done:
            return 0, None

        rv, tap_conn = self.get_tap_conn()
        if rv != 0:
            return rv, None

        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        try:
            while (not self.tap_done and
                   batch.size() < batch_max_size and
                   batch.bytes < batch_max_bytes):
                # TODO: (1) TAPDumpSource - provide_batch timeout on inactivity.

                need_ack = False

                cmd, opaque, cas, vbucket_id, key, ext, val = \
                    self.read_tap_conn(tap_conn)

                if (cmd == memcacheConstants.CMD_TAP_MUTATION or
                    cmd == memcacheConstants.CMD_TAP_DELETE):
                    rv, eng_length, flags, ttl, flg, exp, need_ack = \
                        self.parse_tap_ext(ext)
                    if rv != 0:
                        logging.warn("stopping:"
                                     " received partial message;"
                                     " perhaps some item(s) not transferred;"
                                     " key: %s, cmd %s, vbucket_id %s"
                                     % (key, pump.CMD_STR.get(cmd, cmd),
                                        vbucket_id))
                        self.tap_done = True
                        break

                    if not self.skip(key, vbucket_id):
                        item = (cmd, vbucket_id, key, flg, exp, cas, val)
                        batch.append(item, len(val))
                elif cmd == memcacheConstants.CMD_TAP_OPAQUE:
                    if len(ext) > 0:
                        rv, eng_length, flags, ttl, flg, exp, need_ack = \
                            self.parse_tap_ext(ext)
                        if rv != 0:
                            logging.warn("stopping:"
                                         " received partial TAP_OPAQUE msg;"
                                         " perhaps some item(s) missed")
                            self.tap_done = True
                            break
                elif cmd == memcacheConstants.CMD_NOOP:
                    continue # NOOP's are ignorable; used to keep conn alive.
                elif cmd == memcacheConstants.CMD_TAP_FLUSH:
                    logging.warn("stopping: saw CMD_TAP_FLUSH")
                    self.tap_done = True
                    break
                else:
                    logging.warn("stopping: unexpected, saw " +
                                 str(pump.CMD_STR.get(cmd, cmd)))
                    self.tap_done = True
                    break

                if need_ack:
                    tap_conn._sendMsg(cmd, '', '', opaque, vbucketId=0,
                                      fmt=memcacheConstants.RES_PKT_FMT,
                                      magic=memcacheConstants.RES_MAGIC_BYTE)

                    # Close the batch when there's an ACK handshake, so
                    # the server can concurrently send us the next batch.
                    # If we are slow, our slow ACK's will naturally slow
                    # down the server.
                    return 0, batch

        except EOFError:
            self.tap_done = True

        return 0, batch

    def get_tap_conn(self):
        """Return previously connected tap conn."""

        if not self.tap_conn:
            host = self.source_node['hostname'].split(':')[0]
            port = self.source_node['ports']['direct']

            logging.debug("  TAPDumpSource connecting mc: " +
                          host + ":" + str(port))

            self.tap_conn = mc_bin_client.MemcachedClient(host, port)
            if not self.tap_conn:
                return "error: could not connect to memcached: " + \
                    host + ":" + str(port), None

            if self.source_bucket.get("authType", None) == "sasl":
                sasl_user = str(self.source_bucket.get("name",
                                                       self.opts.username) or "")
                sasl_pswd = str(self.source_bucket.get("saslPassword",
                                                       self.opts.password) or "")
                if sasl_user:
                    try:
                        self.tap_conn.sasl_auth_plain(sasl_user, sasl_pswd)
                    except EOFError:
                        return "error: SASL auth error: %s, user: %s" % \
                            (host, sasl_user), None
                    except mc_bin_client.MemcachedError:
                        return "error: SASL auth failed: %s, user: %s" % \
                            (host, sasl_user), None

            # We explicitly do not use TAP_FLAG_REGISTERED_CLIENT,
            # as that is for checkpoint/incremental backup only.
            #
            ext, val = \
                TAPDumpSource.encode_tap_connect_opts({
                    # TODO: (1) TAPDumpSource - validate TAP_FLAG_XXX usage.
                    memcacheConstants.TAP_FLAG_DUMP: '',
                    memcacheConstants.TAP_FLAG_SUPPORT_ACK: '',
                    })

            self.tap_conn._sendCmd(memcacheConstants.CMD_TAP_CONNECT,
                                   self.tap_name, val, 0, ext)

        return 0, self.tap_conn

    def read_tap_conn(self, tap_conn):
        buf, cmd, vbucket_id, opaque, cas, keylen, extlen, data = \
            self.recv_msg(tap_conn.s, getattr(tap_conn, 'buf', ''))
        tap_conn.buf = buf

        ext = ''
        key = ''
        val = ''
        if data:
            ext = data[0:extlen]
            key = data[extlen:extlen+keylen]
            val = data[extlen+keylen:]

        return cmd, opaque, cas, vbucket_id, key, ext, val

    def recv_msg(self, sock, buf):
        pkt, buf = self.recv(sock, memcacheConstants.MIN_RECV_PACKET, buf)
        if not pkt:
            raise EOFError()
        magic, cmd, keylen, extlen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(memcacheConstants.REQ_PKT_FMT, pkt)
        if magic != memcacheConstants.REQ_MAGIC_BYTE:
            raise Exception("unexpected recv_msg magic: " + str(magic))
        data, buf = self.recv(sock, datalen, buf)
        return buf, cmd, errcode, opaque, cas, keylen, extlen, data

    def recv(self, skt, nbytes, buf):
        while len(buf) < nbytes:
            data = None
            try:
                data = skt.recv(max(nbytes - len(buf), self.recv_min_bytes))
            except socket.timeout:
                logging.error("error: recv socket.timeout")
            except Exception as e:
                logging.error("error: recv exception: " + str(e))

            if not data:
                return None, ''

            buf += data

        return buf[:nbytes], buf[nbytes:]

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

    def parse_tap_ext(self, ext):
        if len(ext) == 8:
            flg = exp = 0
            eng_length, flags, ttl = \
                struct.unpack(memcacheConstants.TAP_GENERAL_PKT_FMT, ext)
        elif len(ext) == 16:
            eng_length, flags, ttl, flg, exp = \
                struct.unpack(memcacheConstants.TAP_MUTATION_PKT_FMT, ext)
        else:
            return "error: unexpected tap ext length: " + str(len(ext)), \
                0, 0, 0, 0, 0, False

        need_ack = flags & memcacheConstants.TAP_FLAG_ACK

        return 0, eng_length, flags, ttl, flg, exp, need_ack


