#!/usr/bin/env python3
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import array
import socket
import random
import struct
import ssl
from typing import Optional, Tuple, Iterable, List, Union
from couchbaseConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from couchbaseConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from couchbaseConstants import SET_PKT_FMT, INCRDECR_RES_FMT
from couchbaseConstants import AUDIT_PKT_FMT
import couchbaseConstants


# Collections on the wire uses a varint encoding for the collection-ID
# A simple unsigned_leb128 encoded is used https://en.wikipedia.org/wiki/LEB128
# return a string with the binary encoding
def encodeCollectionId(cid: int) -> bytes:
    output = array.array('B', [0])
    while cid > 0:
        byte = cid & 0xFF
        cid >>= 7
        # CID has more bits
        if cid > 0:
            # Set the 'continue' bit of this byte
            byte |= 0x80
            output[-1] = byte
            output.append(0)
        else:
            output[-1] = byte
    return output.tobytes()


def decodeCollectionID(key: bytes) -> Tuple[int, bytes]:
    # A leb128 varint encodes the CID
    data = array.array('B')
    data.frombytes(key)
    cid = data[0] & 0x7f
    end = 1
    if (data[0] & 0x80) == 0x80:
        shift = 7
        for end in range(1, len(data)):
            cid |= ((data[end] & 0x7f) << shift)
            if (data[end] & 0x80) == 0:
                break
            shift = shift + 7

        end = end + 1
        if end == len(data):
            #  We should of stopped for a stop byte, not the end of the buffer
            raise ValueError("encoded key did not contain a stop byte")
    return cid, key[end:]


def skipCollectionID(key: bytes) -> bytes:
    _, k = decodeCollectionID(key)
    return k


class MemcachedError(Exception):
    """Error raised when a command fails."""

    def __init__(self, status: int, msg: str):
        supermsg = f'Memcached error #{repr(status)}'
        if msg:
            supermsg += f':  {msg}'
        Exception.__init__(self, supermsg)

        self.status=status
        self.msg=msg

    def __repr__(self):
        return f'<MemcachedError #{self.status} ``{self.msg}''>'


class MemcachedClient(object):
    """Simple memcached client."""

    vbucketId = 0

    def __init__(self, host='127.0.0.1', port=11211, family=socket.AF_UNSPEC, use_ssl=False, verify=True, cacert=None):
        self.host = host
        self.port = port
        for info in socket.getaddrinfo(self.host, self.port, family,
                                       socket.SOCK_STREAM):
            _family, socktype, proto, _, sockaddr = info
            try:
                sock = socket.socket(_family, socktype, proto)
                sock.settimeout(10)
                if use_ssl:
                    certReq = ssl.CERT_REQUIRED
                    if not verify:
                        certReq = ssl.CERT_NONE
                    sock = ssl.wrap_socket(sock, server_side=False, do_handshake_on_connect=True, cert_reqs=certReq,
                                           ca_certs=cacert)

                self.s = sock
                self.s.connect_ex(sockaddr)
                break
            except socket.error as sock_error:
                # If we get here socket objects will be close()d via
                # garbage collection.
                pass
        else:
            # Didn't break from the loop, re-raise the last error
            raise sock_error

        self.r=random.Random()

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd: int, key: bytes, val: bytes, opaque: int, extraHeader: bytes = b'', cas: int = 0,
                 extraMeta: bytes = b''):
        self._sendMsg(cmd, key, val, opaque, extraHeader=extraHeader, cas=cas,
                      vbucketId=self.vbucketId, extraMeta=extraMeta)

    def _sendMsg(self, cmd: int, key: bytes, val: bytes, opaque: int, extraHeader: bytes = b'', cas: int = 0,
                 dtype: int = 0, vbucketId: int =0, extraMeta: bytes = b'',
                 fmt: str = REQ_PKT_FMT, magic: int = REQ_MAGIC_BYTE):
        msg: bytes = struct.pack(fmt, magic, cmd, len(key), len(extraHeader), dtype, vbucketId,
                                 len(key) + len(extraHeader) + len(val) + len(extraMeta), opaque, cas)
        self.s.sendall(msg + extraHeader + key + val + extraMeta)

    def _recvMsg(self) -> Tuple[int, int, int, int, int, int, bytes]:
        response = b''
        while len(response) < MIN_RECV_PACKET:
            data = self.s.recv(MIN_RECV_PACKET - len(response))
            if data == b'':
                raise EOFError("Got empty data (remote died?).")
            response += data
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas=\
            struct.unpack(RES_PKT_FMT, response)

        rv = b''
        while remaining > 0:
            data = self.s.recv(remaining)
            if data == b'':
                raise EOFError("Got empty data (remote died?).")
            rv += data
            remaining -= len(data)

        assert (magic in (RES_MAGIC_BYTE, REQ_MAGIC_BYTE)), f'Got magic: {magic}'
        return cmd, errcode, opaque, cas, keylen, extralen, rv

    def _handleKeyedResponse(self, myopaque: Union[int, None]) -> Tuple[int, int, int, int, int, bytes]:
        cmd, errcode, opaque, cas, keylen, extralen, rv = self._recvMsg()
        assert myopaque is None or opaque == myopaque, f'expected opaque {myopaque:x}, got {opaque:x}'
        if errcode != 0:
            raise MemcachedError(errcode,  str(rv))
        return cmd, opaque, cas, keylen, extralen, rv

    def _handleSingleResponse(self, myopaque: Union[int, None]) -> Tuple[int, int, bytes]:
        cmd, opaque, cas, keylen, extralen, data = self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd: int, key: bytes, val: bytes, extraHeader: bytes = b'', cas: int = 0) -> Tuple[int, int, bytes]:
        """Send a command and await its response."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd: int, key: bytes, exp: int, flags: int, cas: int, val: bytes) -> Tuple[int, int, bytes]:
        return self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp),
            cas)

    def _cat(self, cmd: int, key: bytes, cas: int, val: bytes) -> Tuple[int, int, bytes]:
        return self._doCmd(cmd, key, val, b'', cas)

    def append(self, key: bytes, value: bytes, cas: int = 0) -> Tuple[int, int, bytes]:
        return self._cat(couchbaseConstants.CMD_APPEND, key, cas, value)

    def prepend(self, key: bytes, value: bytes, cas: int = 0) -> Tuple[int, int, bytes]:
        return self._cat(couchbaseConstants.CMD_PREPEND, key, cas, value)

    def __incrdecr(self, cmd, key, amt, init, exp):
        something, cas, val=self._doCmd(cmd, key, b'',
            struct.pack(couchbaseConstants.INCRDECR_PKT_FMT, amt, init, exp))
        return struct.unpack(INCRDECR_RES_FMT, val)[0], cas

    def incr(self, key, amt=1, init=0, exp=0):
        """Increment or create the named counter."""
        return self.__incrdecr(couchbaseConstants.CMD_INCR, key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0):
        """Decrement or create the named counter."""
        return self.__incrdecr(couchbaseConstants.CMD_DECR, key, amt, init, exp)

    def _doMetaCmd(self, cmd: int, key: bytes, value: bytes, cas: int, exp: int, flags: int, seqno: int,
                   remote_cas: int, options: Optional[int] = None, meta_len: Optional[int] = None) -> Tuple[int, int,
                                                                                                            bytes]:
        extra = b''
        if options is None and meta_len is None:
            extra = struct.pack('>IIQQ', flags, exp, seqno, remote_cas)
        if options is not None and meta_len is not None:
            extra = struct.pack('>IIQQIH', flags, exp, seqno, remote_cas, options, meta_len)
        if options is not None and meta_len is None:
            extra = struct.pack('>IIQQI', flags, exp, seqno, remote_cas, options)
        if options is None and meta_len is not None:
            extra = struct.pack('>IIQQH', flags, exp, seqno, remote_cas, meta_len)
        return self._doCmd(cmd, key, value, extra, cas)

    def _doRevCmd(self, cmd: int, key: bytes, exp: int, flags: int, value: bytes, rev: Tuple[int, bytes], cas: int = 0):
        seqno, revid = rev
        meta_data = struct.pack('>I', seqno) + revid
        meta_type = couchbaseConstants.META_REVID
        meta = (meta_type, meta_data)
        return self._doMetaCmd(cmd, key, value, cas, exp, flags, seqno, 0)  # pylint: disable=no-value-for-parameter

    def set(self, key: bytes, exp: int, flags: int, val: bytes) -> Tuple[int, int, bytes]:
        """Set a value in the memcached server."""
        return self._mutate(couchbaseConstants.CMD_SET, key, exp, flags, 0, val)

    def setWithMeta(self, key: bytes, value: bytes, exp: int, flags: int, seqno: int, remote_cas: int,
                    options: Optional[int] = None, meta_len: Optional[int] = None) -> Tuple[int, int, bytes]:
        """Set a value and its meta data in the memcached server."""
        return self._doMetaCmd(couchbaseConstants.CMD_SET_WITH_META,
                               key, value, 0, exp, flags, seqno, remote_cas, options, meta_len)

    def setWithRev(self, key: bytes, exp: int, flags: int, value: bytes, rev: Tuple[int, bytes]):
        """Set a value and its revision in the memcached server."""
        return self._doRevCmd(couchbaseConstants.CMD_SET_WITH_META,
                              key, exp, flags, value, rev)

    def add(self, key: bytes, exp: int, flags: int, val: bytes):
        """Add a value in the memcached server iff it doesn't already exist."""
        return self._mutate(couchbaseConstants.CMD_ADD, key, exp, flags, 0, val)

    def addWithMeta(self, key: bytes, value: bytes, exp: int, flags: int, seqno: int, remote_cas: int):
        return self._doMetaCmd(couchbaseConstants.CMD_ADD_WITH_META,
                               key, value, 0, exp, flags, seqno, remote_cas)

    def addWithRev(self, key: bytes, exp: int, flags: int, value: bytes, rev: Tuple[int, bytes]):
        return self._doRevCmd(couchbaseConstants.CMD_ADD_WITH_META,
                              key, exp, flags, value, rev)

    def replace(self, key: bytes, exp: int, flags: int, val: bytes):
        """Replace a value in the memcached server iff it already exists."""
        return self._mutate(couchbaseConstants.CMD_REPLACE, key, exp, flags, 0,
            val)

    def observe(self, key: bytes, vbucket: int) -> Tuple[int, int, int, bytes]:
        """Observe a key for persistence and replication."""
        value = struct.pack('>HH', vbucket, len(key)) + key
        opaque, cas, data = self._doCmd(couchbaseConstants.CMD_OBSERVE, b'', value)
        rep_time = (cas & 0xFFFFFFFF)
        persist_time = (cas >> 32) & 0xFFFFFFFF
        persisted = struct.unpack('>B', bytes(data[4+len(key)]))[0]
        return opaque, rep_time, persist_time, persisted

    def __parseGet(self, data: bytes, klen: int = 0) -> Tuple[int, int, bytes]:
        flags = struct.unpack(couchbaseConstants.GET_RES_FMT, data[:4])[0]
        return flags, data[1], data[4 + klen:]

    def get(self, key: bytes) -> Tuple[int, int, bytes]:
        """Get the value for a given key within the memcached server."""
        _, _, data = self._doCmd(couchbaseConstants.CMD_GET, key, b'')
        return self.__parseGet(data)

    def getMeta(self, key: bytes) -> Tuple[int, int, int, int, int]:
        """Get the metadata for a given key within the memcached server."""
        opaque, cas, data = self._doCmd(couchbaseConstants.CMD_GET_META, key, b'')
        deleted = struct.unpack('>I', data[0:4])[0]
        flags = struct.unpack('>I', data[4:8])[0]
        exp = struct.unpack('>I', data[8:12])[0]
        seqno = struct.unpack('>Q', data[12:20])[0]
        return (deleted, flags, exp, seqno, cas)

    def getl(self, key: bytes, exp: int = 15):
        """Get the value for a given key within the memcached server."""
        _, _, data = self._doCmd(couchbaseConstants.CMD_GET_LOCKED, key, b'',
                                 struct.pack(couchbaseConstants.GETL_PKT_FMT, exp))
        return self.__parseGet(data)

    def cas(self, key: bytes, exp: int, flags: int, oldVal: int, val: bytes):
        """CAS in a new value for the given key and comparison value."""
        self._mutate(couchbaseConstants.CMD_SET, key, exp, flags, oldVal, val)

    def touch(self, key: bytes, exp: int):
        """Touch a key in the memcached server."""
        return self._doCmd(couchbaseConstants.CMD_TOUCH, key, b'', struct.pack(couchbaseConstants.TOUCH_PKT_FMT, exp))

    def gat(self, key: bytes, exp: int):
        """Get the value for a given key and touch it within the memcached server."""
        _, _, data = self._doCmd(couchbaseConstants.CMD_GAT, key, b'',
                                 struct.pack(couchbaseConstants.GAT_PKT_FMT, exp))
        return self.__parseGet(data)

    def getr(self, key: bytes):
        """Get the value for a given key in a replica vbucket within the memcached server."""
        _, _, data = self._doCmd(couchbaseConstants.CMD_GET_REPLICA, key, b'')
        return self.__parseGet(data, len(key))

    def version(self):
        """Get the value for a given key within the memcached server."""
        return self._doCmd(couchbaseConstants.CMD_VERSION, b'', b'')

    def verbose(self, level: int):
        """Set the verbosity level."""
        return self._doCmd(couchbaseConstants.CMD_VERBOSE, b'', b'',
                           extraHeader=struct.pack(">I", level))

    def sasl_mechanisms(self):
        """Get the supported SASL methods."""
        return set(self._doCmd(couchbaseConstants.CMD_SASL_LIST_MECHS, b'', b'')[2].split(b' '))

    def sasl_auth_start(self, mech: bytes, data: bytes) -> Tuple[int, int, bytes]:
        """Start a sasl auth session."""
        return self._doCmd(couchbaseConstants.CMD_SASL_AUTH, mech, data)

    def sasl_auth_plain(self, user: str, password: str, foruser: str = '') -> Tuple[int, int, bytes]:
        """Perform plain auth."""
        return self.sasl_auth_start(b'PLAIN', b'\0'.join([foruser.encode(), user.encode(), password.encode()]))

    def stop_persistence(self) -> Tuple[int, int, bytes]:
        return self._doCmd(couchbaseConstants.CMD_STOP_PERSISTENCE, b'', b'')

    def start_persistence(self) -> Tuple[int, int, bytes]:
        return self._doCmd(couchbaseConstants.CMD_START_PERSISTENCE, b'', b'')

    def set_param(self, key: bytes, val: bytes, type: int) -> Tuple[int, int, bytes]:
        print("setting param:", key, val)
        type_bytes: bytes = struct.pack(couchbaseConstants.SET_PARAM_FMT, type)
        return self._doCmd(couchbaseConstants.CMD_SET_PARAM, key, val, type_bytes)

    def set_vbucket_state(self, vbucket: int, stateName: str):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        state = struct.pack(couchbaseConstants.VB_SET_PKT_FMT,
                            couchbaseConstants.VB_STATE_NAMES[stateName])
        return self._doCmd(couchbaseConstants.CMD_SET_VBUCKET_STATE, b'', b'', state)

    def get_vbucket_state(self, vbucket: int):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(couchbaseConstants.CMD_GET_VBUCKET_STATE, b'', b'')

    def delete_vbucket(self, vbucket: int):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(couchbaseConstants.CMD_DELETE_VBUCKET, b'', b'')

    def evict_key(self, key: bytes):
        return self._doCmd(couchbaseConstants.CMD_EVICT_KEY, key, b'')

    def getMulti(self, keys: Iterable[bytes]):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        opaqued=dict(enumerate(keys))
        terminal=len(opaqued)+10
        # Send all of the keys in quiet
        for k,v in opaqued.items():
            self._sendCmd(couchbaseConstants.CMD_GETQ, v, b'', k)

        self._sendCmd(couchbaseConstants.CMD_NOOP, b'', b'', terminal)

        # Handle the response
        rv={}
        done=False
        while not done:
            opaque, cas, data=self._handleSingleResponse(None)  # type: ignore
            if opaque != terminal:
                rv[opaqued[opaque]]=self.__parseGet((opaque, cas, data))  # type: ignore
            else:
                done=True

        return rv

    def setMulti(self, exp: int, flags: int, items):
        """Multi-set (using setq).

        Give me (key, value) pairs."""

        # If this is a dict, convert it to a pair generator
        if hasattr(items, 'iteritems'):
            items = items.items()

        opaqued=dict(enumerate(items))
        terminal=len(opaqued)+10
        extra=struct.pack(SET_PKT_FMT, flags, exp)

        # Send all of the keys in quiet
        for opaque,kv in opaqued.items():
            self._sendCmd(couchbaseConstants.CMD_SETQ, kv[0], kv[1], opaque, extra)

        self._sendCmd(couchbaseConstants.CMD_NOOP, b'', b'', terminal)

        # Handle the response
        failed = []
        done=False
        while not done:
            try:
                opaque, cas, data = self._handleSingleResponse(None)  # type: ignore
                done = opaque == terminal
            except MemcachedError as e:
                failed.append(e)

        return failed

    def delMulti(self, items):
        """Multi-delete (using delq).

        Give me a collection of keys."""

        opaqued = dict(enumerate(items))
        terminal = len(opaqued)+10
        extra = b''

        # Send all of the keys in quiet
        for opaque, k in opaqued.items():
            self._sendCmd(couchbaseConstants.CMD_DELETEQ, k, b'', opaque, extra)

        self._sendCmd(couchbaseConstants.CMD_NOOP, b'', b'', terminal)

        # Handle the response
        failed = []
        done=False
        while not done:
            try:
                opaque, cas, data = self._handleSingleResponse(None)
                done = opaque == terminal
            except MemcachedError as e:
                failed.append(e)

        return failed

    def stats(self, sub: bytes = b''):
        """Get stats."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(couchbaseConstants.CMD_STAT, sub, b'', opaque)
        done = False
        rv = {}
        while not done:
            cmd, opaque, cas, klen, extralen, data = self._handleKeyedResponse(None)
            if klen:
                rv[data[0:klen]] = data[klen:]
            else:
                done = True
        return rv

    def noop(self) -> Tuple[int, int, bytes]:
        """Send a noop command."""
        return self._doCmd(couchbaseConstants.CMD_NOOP, b'', b'')

    def helo(self, features: List[int]) -> Tuple[int, int, Tuple[int, ...]]:
        """Send a hello command for feature checking"""
        r1, r2, value = self._doCmd(couchbaseConstants.CMD_HELLO, b'', struct.pack('>' + ('H' * len(features)), *features))
        # divide length of value by 2 to find out how many 2-byte elements exist
        return r1, r2, struct.unpack('>' + ('H' * int(len(value)/2)) , value)

    def delete(self, key: bytes, cas: int = 0) -> Tuple[int, int, bytes]:
        """Delete the value for a given key within the memcached server."""
        return self._doCmd(couchbaseConstants.CMD_DELETE, key, b'', b'', cas)

    def flush(self, timebomb: int = 0) -> Tuple[int, int, bytes]:
        """Flush all storage in a memcached instance."""
        return self._doCmd(couchbaseConstants.CMD_FLUSH, b'', b'',
                           struct.pack(couchbaseConstants.FLUSH_PKT_FMT, timebomb))

    def bucket_select(self, name: bytes) -> Tuple[int, int, bytes]:
        return self._doCmd(couchbaseConstants.CMD_SELECT_BUCKET, name, b'')

    def restore_file(self, filename):
        """Initiate restore of a given file."""
        return self._doCmd(couchbaseConstants.CMD_RESTORE_FILE, filename, b'', b'', 0)

    def restore_complete(self):
        """Notify the server that we're done restoring."""
        return self._doCmd(couchbaseConstants.CMD_RESTORE_COMPLETE, b'', b'', b'', 0)

    def deregister_tap_client(self, tap_name):
        """Deregister the TAP client with a given name."""
        return self._doCmd(couchbaseConstants.CMD_DEREGISTER_TAP_CLIENT, tap_name, b'', b'', 0)

    def reset_replication_chain(self):
        """Reset the replication chain."""
        return self._doCmd(couchbaseConstants.CMD_RESET_REPLICATION_CHAIN, b'', b'', b'', 0)

    def audit(self, auditid, event):
        #print couchbaseConstants.CMD_AUDIT_PUT, auditid, event

        #return self._doCmd(couchbaseConstants.CMD_AUDIT_PUT, '', event, \
        #                   struct.pack(AUDIT_PKT_FMT, auditid))
        return 0
