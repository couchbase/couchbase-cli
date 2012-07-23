#!/usr/bin/env python

import bson
import os
import simplejson as json
import struct

import memcacheConstants
import pump

BSON_SCHEME = "bson://"

class BSONSource(pump.Source):
    """Reads bson file."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(BSONSource, self).__init__(opts, spec, source_bucket, source_node,
                                         source_map, sink_map, ctl, cur)
        self.done = False
        self.f = None

    @staticmethod
    def can_handle(opts, spec):
        return spec.startswith(BSON_SCHEME) and \
            os.path.isfile(spec.replace(BSON_SCHEME, ""))

    @staticmethod
    def check(opts, spec):
        return 0, {'spec': spec,
                   'buckets': [{'name': os.path.basename(spec),
                                'nodes': [{'hostname': 'N/A'}]}]}

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        return 0, None

    def provide_batch(self):
        if self.done:
            return 0, None

        if not self.f:
            try:
                self.f = open(self.spec.replace(BSON_SCHEME, ""))
            except IOError, e:
                return "error: could not open bson: %s; exception: %s" % \
                    (self.spec, e), None

        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        cmd = memcacheConstants.CMD_TAP_MUTATION
        vbucket_id = 0x0000ffff
        cas, exp, flg = 0, 0, 0

        while (self.f and
               batch.size() < batch_max_size and
               batch.bytes < batch_max_bytes):
            doc_size_buf = self.f.read(4)
            if not doc_size_buf:
                self.done = True
                self.f.close()
                self.f = None
                break
            doc_size, = struct.unpack("<i", doc_size_buf)
            doc_buf = self.f.read(doc_size - 4)
            if not doc_buf:
                self.done = True
                self.f.close()
                self.f = None
                break
            doc = bson._elements_to_dict(doc_buf, dict, True)
            key = doc['_id']
            doc_json = json.dumps(doc)
            msg = (cmd, vbucket_id, key, flg, exp, cas, '', doc_json)
            batch.append(msg, len(doc))

        if batch.size() <= 0:
            return 0, None
        return 0, batch
