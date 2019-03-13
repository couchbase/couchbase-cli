#!/usr/bin/env python

import couchbaseConstants
import json
import pump
import random
import string
import struct
from typing import Tuple, Optional, Dict, Any
from cb_bin_client import encodeCollectionId

class GenSource(pump.Source):
    """Generates simple SET/GET workload, useful for basic testing.
       Examples:
         ./cbtransfer gen: http://10.3.121.192:8091
         ./cbtransfer gen:max-items=50000,min-value-size=10,exit-after-creates=1,\
                          prefix=steve1-,ratio-sets=1.0 \
                      http://10.3.121.192:8091 -B my-other-bucket --threads=10
    """
    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(GenSource, self).__init__(opts, spec, source_bucket, source_node,
                                        source_map, sink_map, ctl, cur)
        self.done = False
        self.body = None
        self.cur_ops = source_map['cfg']['cur-ops']
        self.cur_gets = source_map['cfg']['cur-gets']
        self.cur_sets = source_map['cfg']['cur-sets']
        self.cur_items = source_map['cfg']['cur-items']

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        """The gen spec follows gen:[key=value[,key=value]] format."""
        return spec.startswith("gen:")

    @staticmethod
    def check(opts, spec: str) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[Dict[str, Any]]]:
        rv, cfg = GenSource.parse_spec(opts, spec)
        if rv != 0:
            return rv, None
        return 0, {'cfg': cfg,
                   'spec': spec,
                   'buckets': [{'name': 'default',
                                'nodes': [{'hostname': f'N/A-{i!s}'}
                                          for i in range(opts.threads)]}]}

    @staticmethod
    def parse_spec(opts, spec: str) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[Dict[str, Any]]]:
        """Parse the comma-separated key=value configuration from the gen spec.
           Names and semantics were inspired from subset of mcsoda parameters."""
        cfg = {'cur-ops': 0,
               'cur-gets': 0,
               'cur-sets': 0,
               'cur-items': 0,
               'exit-after-creates': 0,
               'max-items': 10000,
               'min-value-size': 10,
               'prefix': "",
               'ratio-sets': 0.05,
               'json': 0,
               'low-compression': False,
               'xattr': False}

        for kv in spec[len("gen:"):].split(','):
            if kv:
                k = kv.split('=')[0].strip()
                v = kv.split('=')[1].strip()
                try:
                    if k in cfg:
                        # this type casting does not work for bools so this code has to be added
                        if v == 'True':
                            cfg[k] = True
                        elif v == 'False':
                            cfg[k] = False
                        else:
                            cfg[k] = type(cfg[k])(v)  # type: ignore
                    else:
                        return f'error: unknown workload gen parameter: {k}', None
                except ValueError:
                    return f'error: could not parse value from: {kv}', None
        return 0, cfg

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                              Optional[str]]:
        """No design from a GenSource."""
        return 0, None

    def provide_batch(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch]]:
        """Provides a batch of messages, with GET/SET ratios and keys
           controlled by a mcsoda-inspired approach, but simpler."""
        if self.done:
            return 0, None

        cfg: Dict[str, Any] = self.source_map['cfg']
        prefix: str = cfg['prefix']
        max_items: int = cfg['max-items']
        ratio_sets: float = cfg['ratio-sets']
        exit_after_creates: bool = cfg['exit-after-creates']
        low_compression: bool = cfg['low-compression']
        xattrs: bool = cfg['xattr']
        itr = None
        collections = self.opts.collection
        if collections:
            itr = iter(collections)

        json_body: bool = cfg['json']
        if not self.body:

            if low_compression:
                # Generate a document which snappy will struggle to compress.
                # Useful if your creating data-sets which utilise disk.
                random.seed(0)  # Seed to a fixed value so we always have the same document pattern.
                document = ''.join(random.choice(string.ascii_uppercase) for _ in range(cfg['min-value-size']))
            else:
                # else a string of 0 is fine, but will compress very well.
                document = "0" * cfg['min-value-size']

            self.body = document

        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        vbucket_id = 0x0000ffff
        cas, exp, flg = 0, 0, 0

        while (batch.size() < batch_max_size and
               batch.bytes < batch_max_bytes):
            if ratio_sets >= float(self.cur_sets) / float(self.cur_ops or 1):
                self.cur_sets = self.cur_sets + 1
                if xattrs:
                    cmd: int = couchbaseConstants.CMD_SUBDOC_MULTIPATH_MUTATION
                else:
                    cmd = couchbaseConstants.CMD_DCP_MUTATION
                if self.cur_items < max_items:
                    key = str(self.cur_items)
                    self.cur_items = self.cur_items + 1
                else:
                    key = str(self.cur_sets % self.cur_items)
            else:
                self.cur_gets = self.cur_gets + 1
                if xattrs:
                    cmd = couchbaseConstants.CMD_SUBDOC_MULTIPATH_LOOKUP
                else:
                    cmd = couchbaseConstants.CMD_GET
                key = str(self.cur_gets % self.cur_items)
            self.cur_ops = self.cur_ops + 1

            if json_body:
                value = f'{{"name": "{prefix}{key}", "age": {int(key) % 101}, "index": "{key}", "body":"{self.body}"}}'
            else:
                value = self.body

            if xattrs:
                value = json.dumps({"obj": value, "xattr_f": "field1", "xattr_v": "\"value1\""})

            value_bytes: bytes = value.encode()
            # generate a collection key
            if itr:
                try:
                    cid = int(next(itr), 16)
                except StopIteration:
                    itr = iter(collections)
                    cid = int(next(itr), 16)

                encodedCid = encodeCollectionId(cid)
                # Generate the pack format and pack the key
                docKey: bytes = struct.pack(
                    ("!" + str(len(encodedCid)) + "s"
                        + str(len(prefix)) + "s"
                        + str(len(key)) + "s").encode(),
                    encodedCid,
                    prefix.encode(),
                    key.encode());
            else:
                docKey = prefix.encode() + key.encode()

            datatype = 0x00
            if json_body:
                datatype = 0x01

            msg: couchbaseConstants.BATCH_MSG = (cmd, vbucket_id, docKey, flg, exp, cas, b'', value_bytes, 0, datatype,
                                                0, 0)
            batch.append(msg, len(value_bytes))

            if exit_after_creates and self.cur_items >= max_items:
                self.done = True
                return 0, batch

        if batch.size() <= 0:
            return 0, None
        return 0, batch

    @staticmethod
    def total_msgs(opts, source_bucket, source_node, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[int]]:
        """Returns max-items only if exit-after-creates was specified.
           Else, total msgs is unknown as GenSource does not stop generating."""
        if source_map['cfg']['exit-after-creates'] and source_map['cfg']['ratio-sets'] > 0:
            ratio = source_map['cfg']['ratio-sets']
            ops = 0
            sets = 0
            while sets != source_map['cfg']['max-items']:
                if ratio >= float(sets)/float(ops or 1):
                    sets += 1
                ops += 1
            return 0, ops
        return 0, None
