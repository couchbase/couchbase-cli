import json
import random
import struct
import unittest
from enum import Enum, auto

import couchbaseConstants as cbcs
from pump_mc import MCSink


class DataSetType(Enum):
    NO_XATTRS = auto()
    TXN_XATTRS = auto()
    NO_TXN_XATTRS = auto()
    MULTIPLE_XATTRS = auto()
    ATR = auto()
    TXN_CLIENT_RECORD = auto()


class DataSetItem:
    def __init__(self, key, val=b'rawbytes', cas=0, expiry=0, data_type=0x00, revid=struct.pack('>Q', 0)):
        self.key = key
        self.val = val
        self.cas = cas
        self.expiry = expiry
        self.revid = revid
        self.data_type = data_type

        # expected value after the tes
        self.exp_val = val
        self.exp_cas = cas
        self.exp_expiry = expiry
        self.exp_skip = False
        self.exp_data_type = data_type
        self.exp_revid = revid

    def set_expected(self, val=None, cas=None, expiry=None, data_type=None, revid=None):
        if val:
            self.exp_val = val
        if cas:
            self.exp_cas = cas
        if expiry:
            self.exp_expiry = expiry
        if data_type:
            self.exp_data_type = data_type
        if revid:
            self.exp_revid = revid

    def match(self, skip, val, cas, expiry, data_type, revid):
        if self.exp_skip != skip:
            return False, f'Expected skip to be {self.exp_skip} got {skip} in element {self.key}'
        if skip:
            return True, ''
        if val != self.exp_val:
            return False, f'Expected val to be {self.exp_val} got {val} in element {self.key}'
        if cas != self.exp_cas:
            return False, f'Expected cas to be {self.exp_cas} got {cas} in element {self.key}'
        if expiry != self.exp_expiry:
            return False, f'Expected expiry to be {self.exp_expiry} got {expiry} in element {self.key}'
        if data_type != self.exp_data_type:
            return False, f'Expected datatype to be {self.exp_data_type} got {data_type} in element {self.key}'
        if revid != self.exp_revid:
            return False, f'Expected revid to be {self.exp_revid} got {revid} in element {self.key}'

        return True, ''


def get_XATTR_pair(key, i):
    xattr_body = b'"doc-' + str(i).encode() + b'"'
    if key == b'txn':
        xattr_body = json.dumps({'op': {'type': 'replace'}, 'restore': {'CAS': hex(i), 'exptime': i, 'id': str(i),
                                                                        'revid': str(i)}}).encode()

    xattr_pair = key + b'\00' + xattr_body + b'\00'
    xattr_pair_len = len(xattr_pair)
    return struct.pack('>I', xattr_pair_len) + xattr_pair


def get_value_with_XATTR(xattrs, val):
    xattr_len = len(xattrs)
    return struct.pack('>I', xattr_len) + xattrs + val


class TestFilterOutTXN(unittest.TestCase):
    @staticmethod
    def generate_test_set(data_set_type, size):
        data_set = []
        for i in range(size):
            item = DataSetItem(b'KEY:' + str(i).encode(), b'rawbytes', 0, 0, 0x00)
            if data_set_type == DataSetType.NO_XATTRS:
                pass
            elif data_set_type == DataSetType.NO_TXN_XATTRS:
                item.val = get_value_with_XATTR(get_XATTR_pair(b'xattrs:' + str(i).encode(), i), item.val)
                item.data_type = cbcs.DATATYPE_HAS_XATTR
                item.set_expected(item.val, data_type=cbcs.DATATYPE_HAS_XATTR)
            elif data_set_type == DataSetType.TXN_XATTRS:
                item.val = get_value_with_XATTR(get_XATTR_pair(b'txn', i), item.val)
                item.data_type = cbcs.DATATYPE_HAS_XATTR
                item.set_expected(cas=i, expiry=i, data_type=0x00, revid=struct.pack('>Q', i))
            elif data_set_type == DataSetType.MULTIPLE_XATTRS:
                xattrs_parts = [get_XATTR_pair(b'A:', i), get_XATTR_pair(b'B:', i), get_XATTR_pair(b'txn', i)]
                txn = get_XATTR_pair(b'txn', i)
                xattrs_parts.append(txn)

                random.shuffle(xattrs_parts)
                xattrs = b''.join(xattrs_parts)
                item.val = get_value_with_XATTR(xattrs, b'rawbytes')

                # get expected
                del xattrs_parts[xattrs_parts.index(txn)]
                xattrs = b''.join(xattrs_parts)
                item.set_expected(cas=i, expiry=i, val=get_value_with_XATTR(xattrs, b'rawbytes'),
                                  data_type=cbcs.DATATYPE_HAS_XATTR, revid=struct.pack('>Q', i))
                item.data_type = cbcs.DATATYPE_HAS_XATTR
            elif data_set_type == DataSetType.ATR:
                item.key = b'_txn:atr-' + str(i).encode() + b'-#ffff'
                xattr = get_XATTR_pair(item.key, i)
                item.val = get_value_with_XATTR(xattr, b'rawbytes')
                item.data_type = cbcs.DATATYPE_HAS_XATTR
                item.exp_skip = True
            elif data_set_type == DataSetType.TXN_CLIENT_RECORD:
                item.key = b'_txn:client-record'
                xattr = get_XATTR_pair(item.key, i)
                item.val = get_value_with_XATTR(xattr, b'rawbytes')
                item.data_type = cbcs.DATATYPE_HAS_XATTR
                item.exp_skip = True
            data_set.append(item)
        return data_set

    def run_data_set(self, data_set):
        for el in data_set:
            skip, val, cas, exp, revid, data_type = MCSink.filter_out_txn(el.key, el.val, el.cas, el.expiry, el.revid,
                                                                          el.data_type)
            match, err = el.match(skip, val, cas, exp, data_type, revid)
            self.assertTrue(match, err)

    def test_filer_out_txn_no_xattrs(self):
        data_set = self.generate_test_set(DataSetType.NO_XATTRS, 10)
        self.run_data_set(data_set)

    def test_filter_out_txn_no_txn_xattrs(self):
        data_set = self.generate_test_set(DataSetType.NO_TXN_XATTRS, 10)
        self.run_data_set(data_set)

    def test_filter_out_txn_txn_xattrs(self):
        data_set = self.generate_test_set(DataSetType.TXN_XATTRS, 10)
        self.run_data_set(data_set)

    def test_filter_out_txn_multiple_xattrs(self):
        data_set = self.generate_test_set(DataSetType.MULTIPLE_XATTRS, 10)
        self.run_data_set(data_set)

    def test_filter_out_txn_mixed_data_set(self):
        data_set = self.generate_test_set(DataSetType.NO_XATTRS, 10)
        data_set.extend(self.generate_test_set(DataSetType.NO_TXN_XATTRS, 10))
        data_set.extend(self.generate_test_set(DataSetType.TXN_XATTRS, 10))
        data_set.extend(self.generate_test_set(DataSetType.MULTIPLE_XATTRS, 10))
        data_set.extend(self.generate_test_set(DataSetType.ATR, 10))
        data_set.extend(self.generate_test_set(DataSetType.TXN_CLIENT_RECORD, 1))
        self.run_data_set(data_set)
