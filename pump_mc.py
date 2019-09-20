#!/usr/bin/env python3

import logging
import json
import re
import socket
import struct
import time
import sys
from typing import Tuple, Any, Optional, Dict, List, Union
import cb_bin_client
import couchbaseConstants
import pump
import snappy # pylint: disable=import-error
from cb_util import tag_user_data

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

ATR_EXP = re.compile(r'_txn:atr-\d+-#([a-f1-9]+)$')

def to_bytes(bytes_or_str):
    if isinstance(bytes_or_str, str):
        value = bytes_or_str.encode()  # uses 'utf-8' for encoding
    else:
        value = bytes_or_str
    return value  # Instance of bytes


class txnMarker:
    def __init__(self, start: int, length: int, body: bytes):
        self.start = start
        self.length = length
        self.body = body


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
        self.lww_restore = 0
        self.init_worker(MCSink.run)
        self.uncompress = opts.extra.get("uncompress", 0)
        self.txn_warning_issued = False
        if self.get_conflict_resolution_type() == "lww":
            self.lww_restore = 1

    def close(self):
        self.push_next_batch(None, None)

    @staticmethod
    def check_base(opts, spec: str) -> couchbaseConstants.PUMP_ERROR:
        if getattr(opts, "destination_vbucket_state", "active") != "active":
            return f'error: only --destination-vbucket-state=active is supported by this destination: {spec}'

        op = getattr(opts, "destination_operation", None)
        if not op in [None, 'set', 'add', 'get']:
            return f'error: --destination-operation unsupported value: {op}; use set, add, get'
        # Skip immediate superclass Sink.check_base(),
        # since MCSink can handle different destination operations.
        return pump.EndPoint.check_base(opts, spec)

    @staticmethod
    def run(self):
        """Worker thread to asynchronously store batches into sink."""

        mconns: Dict[str, cb_bin_client.MemcachedClient] = {}  # State kept across scatter_gather() calls.
        backoff_cap: int = self.opts.extra.get("backoff_cap", 10)
        while not self.ctl['stop']:
            batch, future = self.pull_next_batch()  # type: Optional[pump.Batch], pump.SinkBatchFuture
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
                    backoff = min(backoff * 2.0, backoff_cap)
                    logging.warning(f'backing off, secs: {backoff}')
                    time.sleep(backoff)

            self.future_done(future, 0)

        self.close_mconns(mconns)

    def get_conflict_resolution_type(self) -> str:
        bucket = self.sink_map["buckets"][0]
        confResType = "seqno"
        if "conflictResolutionType" in bucket:
            confResType = bucket["conflictResolutionType"]
        return confResType

    def close_mconns(self, mconns: Dict[str, cb_bin_client.MemcachedClient]):
        for k, conn in mconns.items():
            self.add_stop_event(conn)
            conn.close()

    def scatter_gather(self, mconns: Dict[str, cb_bin_client.MemcachedClient], batch: pump.Batch) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch], Optional[bool]]:
        conn: Optional[cb_bin_client.MemcachedClient] = mconns.get("conn")
        if not conn:
            rv, conn = self.connect()
            if rv != 0:
                return rv, None, None
            mconns["conn"] = conn  # type: ignore

        # TODO: (1) MCSink - run() handle --data parameter.

        # Scatter or send phase.
        rv, skipped = self.send_msgs(conn, batch.msgs, self.operation())  # type: ignore
        if rv != 0:
            return rv, None, None

        # Gather or recv phase.
        rv, retry, refresh = self.recv_msgs(conn, batch.msgs, skipped)  # type: ignore
        if refresh:
            self.refresh_sink_map()
        if retry:
            return rv, batch, True

        return rv, None, None

    def send_msgs(self, conn: cb_bin_client.MemcachedClient, msgs: List[couchbaseConstants.BATCH_MSG], operation: str,
                  vbucket_id: Optional[int] = None) -> Tuple[couchbaseConstants.PUMP_ERROR, List[int]]:
        m: List[bytes] = []
        skipped: List[int] = []

        msg_format_length = 0
        for i, msg in enumerate(msgs):
            if not msg_format_length:
                msg_format_length = len(msg)
            cmd, vbucket_id_msg, key, flg, exp, cas, meta, val = msg[:8]
            seqno = dtype = nmeta = conf_res = 0
            if msg_format_length > 8:
                seqno, dtype, nmeta, conf_res = msg[8:12]
            if vbucket_id is not None:
                vbucket_id_msg = vbucket_id

            if self.skip(key, vbucket_id_msg):
                continue

            if cmd == couchbaseConstants.CMD_SUBDOC_MULTIPATH_MUTATION:
                err, req = self.format_multipath_mutation(key, val, vbucket_id_msg, cas, i)
                if err:
                    return err, skipped
                self.append_req(m, req)
                continue
            if cmd == couchbaseConstants.CMD_SUBDOC_MULTIPATH_LOOKUP:
                err, req = self.format_multipath_lookup(key, val, vbucket_id_msg, cas, i)
                if err:
                    return err, skipped
                self.append_req(m, req)
                continue

            rv, translated_cmd = self.translate_cmd(cmd, operation, meta)
            if translated_cmd is None:
                return rv, skipped
            if dtype > 2:
                if self.uncompress and val:
                    try:
                        val = snappy.uncompress(val)
                    except Exception as err:
                        pass
            if translated_cmd == couchbaseConstants.CMD_GET:
                val, flg, exp, cas = b'', 0, 0, 0
            if translated_cmd == couchbaseConstants.CMD_NOOP:
                key, val, flg, exp, cas = b'', b'', 0, 0, 0
            if translated_cmd == couchbaseConstants.CMD_DELETE:
                val = b''
            # A tombstone can contain Xattrs
            if translated_cmd == couchbaseConstants.CMD_DELETE_WITH_META and not dtype & couchbaseConstants.DATATYPE_HAS_XATTR:
                val = b''
            # on mutations filter txn related data
            if translated_cmd == couchbaseConstants.CMD_SET_WITH_META or translated_cmd == couchbaseConstants.CMD_SET:
                if not getattr(self.opts, 'force_txn', False):
                    skip, val, cas, exp, meta, dtype = self.filter_out_txn(key, val, cas, exp, meta, dtype)
                    if skip:
                        skipped.append(i)
                        if not self.txn_warning_issued:
                            self.txn_warning_issued = True
                            print(f'Mid-transaction data have being rolled backed and restored, but transactional atomicity cannot'
                                  f' be guaranteed.')
                        continue

            rv, req = self.cmd_request(translated_cmd, vbucket_id_msg, key, val,  # type: ignore
                                       ctypes.c_uint32(flg).value,
                                       exp, cas, meta, i, dtype, nmeta,
                                       conf_res)  # type: ignore
            if rv != 0:
                return rv, skipped

            self.append_req(m, req)

        if m:
            try:
                conn.s.sendall(self.join_str_and_bytes(m))  # type: ignore
            except socket.error as e:
                return f'error: conn.sendall() exception: {e}', skipped

        return 0, skipped

    @staticmethod
    def filter_out_txn(key: bytes, val: bytes, cas: int, exp: int, revid: bytes,
                       data_type: int) -> Tuple[bool, bytes, int, int, bytes, int]:
        """This function return signature is [skip:bool, val:bytes, cas:num, exp:num, revid:num, data_type:num]
           skip is true if the key matches an ATR or a client record, otherwise it will be false.
           If the value has XATTRS they will be checked fot txn object that if it exists will be removed"""

        # If it does not have XATTRS return all values as they where
        if not (data_type & couchbaseConstants.DATATYPE_HAS_XATTR > 0):
            return False, val, cas, exp, revid, data_type

        str_key = key.decode()
        if str_key == '_txn:client-record':
            logging.info('(TXN) Skipped the transfer of the txn-client-record')
            return True, val, cas, exp, revid, data_type

        if ATR_EXP.match(str_key):
            logging.info(f'(TXN) Skipped the transfer of the ATR: {str_key}')
            return True, val, cas, exp, revid, data_type

        # Get the length of the xattrs
        xattr_len_bytes = val[:4]
        xattr_len_int = int.from_bytes(xattr_len_bytes, byteorder='big', signed=False)

        # extended attributes length is invalid return as normal and let the server deal with it
        if xattr_len_int > len(val):
            return False, val, cas, exp, revid, data_type

        # look for a txn on the xattrs
        xattrs = val[4:xattr_len_int+7]
        txn_marker: Optional[txnMarker] = None
        ix = 0
        while ix < len(xattrs):
            pair_len = int.from_bytes(xattrs[ix: ix + 4], byteorder='big', signed=False)
            ix += 4
            # get the key for the xattr object
            xattr_key = ''
            while ix + len(xattr_key) < len(xattrs) and xattrs[ix + len(xattr_key)] != 0:
                xattr_key += chr(xattrs[ix + len(xattr_key)])

            # check if the key is txn if it matches create a marker and break
            if xattr_key == 'txn':
                txn_marker = txnMarker(ix, pair_len, xattrs[ix+len(xattr_key)+1: pair_len+ix-len(xattr_key)+2])
                break

            # otherwise skip body and move on to next xattr
            ix += pair_len

        if txn_marker is None:
            return False, val, cas, exp, revid, data_type

        # check if txn is valid JSON if not let the server handle it
        try:
            txn_xattr = json.loads(txn_marker.body.decode())
        except Exception as e:
            logging.debug('(TXN) txn is invalid json')
            return False, val, cas, exp, revid, data_type

        # get op type from the txn
        if 'op' not in txn_xattr or 'type' not in txn_xattr['op']:
            logging.info('(TXN) Transaction extended attributes is missing required field "op"')
            return False, val, cas, exp, revid, data_type

        if txn_xattr['op']['type'] == 'insert':
            logging.info(f'(TXN) Skip document with key {tag_user_data(str_key)} as it is a TXN insert')
            return True, val, cas, exp, revid, data_type

        # get cas, revid and expiry from the txn
        if 'restore' not in txn_xattr:
            logging.info('(TXN) Transaction extended attributes is missing required field "restore"')
            return False, val, cas, exp, revid, data_type

        if 'CAS' in txn_xattr['restore']:
            cas = int(txn_xattr['restore']['CAS'], 16)
        if 'exptime' in txn_xattr['restore']:
            exp = int(txn_xattr['restore']['exptime'])
        if 'revid' in txn_xattr['restore']:
            revid = struct.pack('>Q', int(txn_xattr['restore']['revid']))

        # Remove txn xattrs from value
        logging.info(f'(TXN) Removing transaction extended attributes "{tag_user_data(txn_marker.body)}" '
                     f'from key {tag_user_data(str_key)}')

        new_xattr_len = xattr_len_int - txn_marker.length - 4
        # check if the are any xattrs left
        if new_xattr_len <= 0:
            # copy everything after the xattrs
            val = val[xattr_len_int+4:]
            data_type = 0x00  # Raw bytes
            try:
                valid_json = json.loads(val.decode())
                data_type = 0x01  # Raw json
            except Exception as e:
                data_type = 0x00
            return False, val, cas, exp, revid, data_type

        # create new value package starting with new xattr len
        new_val = new_xattr_len.to_bytes(4, 'big', signed=False)
        if txn_marker.start != 4:
            new_val += val[4: txn_marker.start]

        new_val += val[4+txn_marker.start + txn_marker.length:]
        return False, new_val, cas, exp, revid, data_type

    @staticmethod
    def format_multipath_mutation(key: bytes, value: bytes, vbucketId: int, cas: int = 0, opaque: int = 0) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, couchbaseConstants.REQUEST]:
        empty_tuple = (b'', None, None, None, None)
        try:
            json_value: Dict[str, Any] = json.loads(value)
        except Exception:
            return 'value has invalid format for multipath mutation', empty_tuple

        if 'obj' not in json_value:
            return 'value has invalid format for multipath mutation', empty_tuple
        if 'xattr_f' not in json_value:
            return 'value has invalid format for multipath mutation', empty_tuple
        if 'xattr_v' not in json_value:
            return 'value has invalid format for multipath mutation', empty_tuple

        obj: bytes = to_bytes(json_value['obj'])
        xattr_f: bytes = to_bytes(json_value['xattr_f'])
        xattr_v: bytes = to_bytes(json_value['xattr_v'])

        subop_format = ">BBHI"
        sbcmd_len = 8 * 2 + len(obj) + len(xattr_f) + len(xattr_v)
        total_body_len = sbcmd_len + 1 + len(key)
        subcmd_msg0 = struct.pack(subop_format, couchbaseConstants.CMD_SUBDOC_DICT_UPSERT,
                                  couchbaseConstants.SUBDOC_FLAG_XATTR_PATH, len(xattr_f), len(xattr_v))
        subcmd_msg0 += xattr_f + xattr_v
        subcmd_msg1 = struct.pack(subop_format, couchbaseConstants.CMD_SET, 0, 0, len(obj))
        subcmd_msg1 += obj

        msg_head = struct.pack(couchbaseConstants.REQ_PKT_FMT, couchbaseConstants.REQ_MAGIC_BYTE,
                               couchbaseConstants.CMD_SUBDOC_MULTIPATH_MUTATION, len(key),
                               1, 0, vbucketId, total_body_len, opaque, cas)
        extras = struct.pack(">B", couchbaseConstants.SUBDOC_FLAG_MKDOC)

        return 0, (msg_head+extras+key+subcmd_msg0+subcmd_msg1, None, None, None, None)

    @staticmethod
    def format_multipath_lookup(key: bytes, value: bytes, vbucketId: int, cas: int = 0, opaque: int = 0) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, couchbaseConstants.REQUEST]:
        empty_tuple = (b'', None, None, None, None)
        try:
            json_value: Dict[str, Any] = json.loads(value)
        except Exception:
            return 'value has invalid format for multipath mutation', empty_tuple

        if 'xattr_f' not in json_value:
            return 'value has invalid format for multipath lookup', empty_tuple

        field: bytes = to_bytes(json_value['xattr_f'])

        subcmd_fmt = '>BBH'
        subcmd_msg0 = struct.pack(subcmd_fmt, couchbaseConstants.CMD_SUBDOC_GET,
                                  couchbaseConstants.SUBDOC_FLAG_XATTR_PATH, len(field))
        subcmd_msg0 += field
        subcmd_msg1 = struct.pack(subcmd_fmt, couchbaseConstants.CMD_GET, 0, 0)

        total_body_len = len(subcmd_msg0) + len(subcmd_msg1) + len(key)
        msg_head = struct.pack(couchbaseConstants.REQ_PKT_FMT, couchbaseConstants.REQ_MAGIC_BYTE,
                               couchbaseConstants.CMD_SUBDOC_MULTIPATH_LOOKUP, len(key),
                               0, 0, vbucketId, total_body_len, opaque, cas)

        return 0, (msg_head+key+subcmd_msg0+subcmd_msg1, None, None, None, None)

    @staticmethod
    def join_str_and_bytes(lst: List[bytes]) -> bytes:
        out = b''
        for x in lst:
            out += x
        return out

    def recv_msgs(self, conn: cb_bin_client.MemcachedClient, msgs: List[couchbaseConstants.BATCH_MSG],
                  skipped: List[int], vbucket_id: Optional[int] = None,
                  verify_opaque: bool = True) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[bool], Optional[bool]]:
        refresh = False
        retry = False
        skipped_counter = 0
        for i, msg in enumerate(msgs):
            # some messages of the original batch where not sent as they were filter out due to been half-way
            # transaction related documents. The check bellow is to avoid waiting for responses for elements
            # that where not sent
            if len(skipped) > 0 and i == skipped[0]:
                skipped = skipped[1:]
                skipped_counter += 1
                continue

            cmd, vbucket_id_msg, key, flg, exp, cas, meta, val = msg[:8]
            if vbucket_id is not None:
                vbucket_id_msg = vbucket_id

            if self.skip(key, vbucket_id_msg):
                continue

            try:
                r_cmd, r_status, r_ext, r_key, r_val, r_cas, r_opaque = \
                    self.read_conn(conn)  # type: ignore
                if verify_opaque and (i - skipped_counter) != r_opaque:
                    return f'error: opaque mismatch: {i} {r_opaque}', None, None

                if r_status == couchbaseConstants.ERR_SUCCESS:
                    continue
                elif r_status == couchbaseConstants.ERR_KEY_EEXISTS:
                    #logging.warn("item exists: %s, key: %s" %
                    #             (self.spec, tag_user_data(key)))
                    continue
                elif r_status == couchbaseConstants.ERR_KEY_ENOENT:
                    if (cmd != couchbaseConstants.CMD_TAP_DELETE and
                        cmd != couchbaseConstants.CMD_GET):
                        logging.warning(f'item not found: {self.spec}, key: {tag_user_data(key)}')
                    continue
                elif (r_status == couchbaseConstants.ERR_ETMPFAIL or
                      r_status == couchbaseConstants.ERR_EBUSY or
                      r_status == couchbaseConstants.ERR_ENOMEM):
                    retry = True # Retry the whole batch again next time.
                    continue     # But, finish recv'ing current batch.
                elif r_status == couchbaseConstants.ERR_NOT_MY_VBUCKET:
                    str_msg = f'received NOT_MY_VBUCKET; perhaps the cluster is/was rebalancing;' \
                        f' vbucket_id: {vbucket_id_msg}, key: {tag_user_data(key)}, spec: {self.spec},' \
                        f' host:port: {conn.host}:{conn.port}'
                    if self.opts.extra.get("nmv_retry", 1):
                        logging.warning(f'warning: {str_msg}')
                        refresh = True
                        retry = True
                        self.cur["tot_sink_not_my_vbucket"] = \
                            self.cur.get("tot_sink_not_my_vbucket", 0) + 1
                    else:
                        return f'error: {str_msg}', None, None
                elif r_status == couchbaseConstants.ERR_UNKNOWN_COMMAND:
                    if self.op_map == OP_MAP:
                        if not retry:
                            return f'error: unknown command: {r_cmd}', None, None
                    else:
                        if not retry:
                            logging.warn("destination does not take XXX-WITH-META"
                                         " commands; will use META-less commands")
                        self.op_map = OP_MAP
                        retry = True
                elif r_status == couchbaseConstants.ERR_ACCESS:
                    return json.loads(r_val)["error"]["context"], None, None
                else:
                    return "error: MCSink MC error: " + str(r_status), None, None

            except Exception as e:
                logging.error(f'MCSink exception: {e}')
                return f'error: MCSink exception: {e!s}', None, None
        return 0, retry, refresh

    def translate_cmd(self, cmd: int, op: str, meta: bytes) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[int]]:
        if len(meta) == 0:
            # The source gave no meta, so use regular commands.
            self.op_map = OP_MAP

        if cmd in[couchbaseConstants.CMD_TAP_MUTATION, couchbaseConstants.CMD_DCP_MUTATION]:
            m = self.op_map.get(op, None)
            if m:
                return 0, m
            return f'error: MCSink.translate_cmd, unsupported op: {op}', None

        if cmd in [couchbaseConstants.CMD_TAP_DELETE, couchbaseConstants.CMD_DCP_DELETE]:
            if op == 'get':
                return 0, couchbaseConstants.CMD_NOOP
            return 0, self.op_map['delete']

        if cmd == couchbaseConstants.CMD_GET:
            return 0, cmd

        return f'error: MCSink - unknown cmd: {cmd}, op: {op}', None

    def append_req(self, m: List[bytes], req: couchbaseConstants.REQUEST):
        hdr, ext, key, val, extra_meta = req
        m.append(hdr)
        if ext:
            m.append(ext)
        if key:
            m.append(key)
        if val:
            m.append(val)
        if extra_meta:
            m.append(extra_meta)

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        return (spec.startswith("memcached://") or
                spec.startswith("memcached-binary://"))

    @staticmethod
    def check(opts, spec: str, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[Dict[str, Any]]]:
        host, port, user, pswd, path = pump.parse_spec(opts, spec, int(getattr(opts, "port", 11211)))
        if opts.ssl:
            ports = couchbaseConstants.SSL_PORT
        rv, conn = MCSink.connect_mc(host, port, user, pswd, None, opts.ssl, opts.no_ssl_verify, opts.cacert)
        if rv != 0:
            return rv, None
        conn.close()  # type: ignore
        return 0, None

    def refresh_sink_map(self) -> couchbaseConstants.PUMP_ERROR:
        return 0

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design) -> couchbaseConstants.PUMP_ERROR:
        if source_design:
            logging.warn("warning: cannot restore bucket design"
                         " on a memached destination")
        return 0

    def consume_batch_async(self, batch: Optional[pump.Batch]) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                        Optional[pump.SinkBatchFuture]]:
        return self.push_next_batch(batch, pump.SinkBatchFuture(self, batch))

    def connect(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[cb_bin_client.MemcachedClient]]:
        host, port, user, pswd, path = \
            pump.parse_spec(self.opts, self.spec,
                            int(getattr(self.opts, "port", 11211)))
        if self.opts.ssl:
            port = couchbaseConstants.SSL_PORT
        return MCSink.connect_mc(host, port, user, pswd, self.sink_map["name"],
                                 self.opts.ssl, collections=self.opts.collection)

    @staticmethod
    def connect_mc(host: str, port: int, username: str, password: str, bucket: Optional[str], use_ssl: bool = False,
                   verify: bool = True, ca_cert: str = None, collections: bool = False)-> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[cb_bin_client.MemcachedClient]]:
        return pump.get_mcd_conn(host, port, username, password, bucket, use_ssl=use_ssl, verify=verify,
                                 ca_cert=ca_cert, collections=collections)

    def cmd_request(self, cmd: int, vbucket_id: int, key: bytes, val: bytes, flg: int, exp: int, cas: int,
                    meta: Optional[bytes],
                    opaque: int, dtype: int, nmeta: Any, conf_res: Optional[str]) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                                           Tuple[bytes, bytes, bytes,
                                                                                                 bytes, bytes]]:
        ext_meta = b''
        empty_tuple = (b'', b'', b'', b'', b'')
        if (cmd == couchbaseConstants.CMD_SET_WITH_META or
            cmd == couchbaseConstants.CMD_ADD_WITH_META or
            cmd == couchbaseConstants.CMD_DELETE_WITH_META):

            force = 0
            if int(self.conflict_resolve) == 0:
                force |= 1
            if int(self.lww_restore) == 1:
                force |= 2
            if meta is not None and len(meta) != 0:
                try:
                    ext = struct.pack(">IIQQI", flg, exp, int.from_bytes(meta, byteorder='big'), cas, force)
                except ValueError:
                    seq_no = str(meta)
                    if len(seq_no) > 8:
                        seq_no = seq_no[0:8]
                    if len(seq_no) < 8:
                        # The seq_no might be 32-bits from 2.0DP4, so pad with 0x00's.
                        seq_no = ('\x00\x00\x00\x00\x00\x00\x00\x00' + seq_no)[-8:]

                    seq_no_bytes: bytes = seq_no.encode()
                    check_seqno, = struct.unpack(">Q", seq_no_bytes)
                    if check_seqno:
                        ext = (struct.pack(">II", flg, exp) + seq_no_bytes +
                               struct.pack(">QI", cas, force))
                    else:
                        ext = struct.pack(">IIQQI", flg, exp, 1, cas, force)
            else:
                ext = struct.pack(">IIQQI", flg, exp, 1, cas, force)
            if conf_res:
                extra_meta = struct.pack(">BBHH",
                                couchbaseConstants.DCP_EXTRA_META_VERSION,
                                couchbaseConstants.DCP_EXTRA_META_CONFLICT_RESOLUTION,
                                len(conf_res),
                                conf_res)
                ext += struct.pack(">H", len(extra_meta))
        elif (cmd == couchbaseConstants.CMD_SET or
              cmd == couchbaseConstants.CMD_ADD):
            ext = struct.pack(couchbaseConstants.SET_PKT_FMT, flg, exp)
        elif (cmd == couchbaseConstants.CMD_DELETE or
              cmd == couchbaseConstants.CMD_GET or
              cmd == couchbaseConstants.CMD_NOOP):
            ext = b''
        else:
            return f'error: MCSink - unknown cmd for request: {cmd!s}', empty_tuple

        # Couchase currently allows only the xattr datatype to be set so we need
        # to strip out all of the other datatype flags
        dtype = dtype & couchbaseConstants.DATATYPE_HAS_XATTR

        hdr = self.cmd_header(cmd, vbucket_id, key, val, ext, 0, opaque, dtype)
        return 0, (hdr, ext, key, val, ext_meta)

    def cmd_header(self, cmd: int, vbucket_id: int, key: bytes, val: bytes, ext: bytes, cas: int, opaque: int,
                   dtype: int = 0,
                   fmt: str = couchbaseConstants.REQ_PKT_FMT,
                   magic: int = couchbaseConstants.REQ_MAGIC_BYTE) -> bytes:

        return struct.pack(fmt, magic, cmd,
                           len(key), len(ext), dtype, vbucket_id,
                           len(key) + len(ext) + len(val), opaque, cas)

    def read_conn(self, conn: cb_bin_client.MemcachedClient) -> Tuple[int, int, bytes, bytes, bytes, int, int]:
        ext = b''
        key = b''
        val = b''

        buf, cmd, errcode, extlen, keylen, data, cas, opaque = self.recv_msg(conn.s, getattr(conn, 'buf', b''))
        conn.buf = buf  # type: ignore

        if data:
            ext = data[0:extlen]
            key = data[extlen:extlen+keylen]
            val = data[extlen+keylen:]

        return cmd, errcode, ext, key, val, cas, opaque

    def recv_msg(self, sock: socket.socket, buf: bytes) -> Tuple[bytes, int, int, int, int, bytes, int, int]:
        pkt, buf = self.recv(sock, couchbaseConstants.MIN_RECV_PACKET, buf)
        if len(pkt) == 0:
            raise EOFError()
        magic, cmd, keylen, extlen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(couchbaseConstants.RES_PKT_FMT, pkt)  # type: int, int, int, int, int, int, int, int, int
        if magic != couchbaseConstants.RES_MAGIC_BYTE:
            raise Exception(f'unexpected recv_msg magic: {magic!s}')
        data, buf = self.recv(sock, datalen, buf)
        return buf, cmd, errcode, extlen, keylen, data, cas, opaque

    def recv(self, skt: socket.socket, nbytes: int, buf: bytes) -> Tuple[bytes, bytes]:
        while len(buf) < nbytes:
            data: bytes = b''
            try:
                data = skt.recv(max(nbytes - len(buf), 4096))
            except socket.timeout:
                logging.error("error: recv socket.timeout")
            except Exception as e:
                logging.error(f'error: recv exception: {e!s}')

            if data == b'':
                return b'', b''
            buf += data

        return buf[:nbytes], buf[nbytes:]
