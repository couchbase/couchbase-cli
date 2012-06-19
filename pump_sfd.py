#!/usr/bin/env python

import ctypes
import logging
import os
import socket
import simplejson as json
import struct
import sys
import time

import couchstore
import memcacheConstants
import pump

SFD_SCHEME = "couchstore-files://"
SFD_VBUCKETS = 1024
SFD_REV_META = ">QII" # cas, exp, flg

class SFDSink(pump.Sink):
    """Sink for couchstore in couchbase server/file/directory layout."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        pump.Sink.__init__(self, opts, spec, source_bucket, source_node,
                           source_map, sink_map, ctl, cur)

        self.init_worker(SFDSink.run)

    @staticmethod
    def run(self):
        while not self.ctl['stop']:
            batch, future = self.pull_next_batch()
            if not batch:
                return self.future_done(future, 0)

            vbuckets = batch.group_by_vbucket_id(SFD_VBUCKETS)
            for vbucket_id, items in vbuckets.iteritems():
                rv, store = self.open_store(vbucket_id)
                if rv != 0:
                    return self.future_done(future, rv)

                bulk_keys = []
                bulk_vals = []

                for i, item in enumerate(items):
                    cmd, _vbucket_id, key, flg, exp, cas, val = item
                    if self.skip(key, vbucket_id):
                        continue

                    if cmd == memcacheConstants.CMD_TAP_MUTATION:
                        d = couchstore.DocumentInfo(str(key))
                        d.revMeta = str(struct.pack(SFD_REV_META, cas, exp, flg))
                        if self.sink_map:
                            # TODO: SFDSink - contentType / DocumentInfo.IS/INVALID/NON_JSON.
                            d.contentType = self.sink_map.get('contentType',
                                                              DocumentInfo.NON_JSON)
                        bulk_keys.append(str(key))
                        bulk_vals.append(str(val))
                    elif cmd == memcacheConstants.CMD_TAP_DELETE:
                        bulk_keys.append(str(key))
                        bulk_vals.append(None)
                    else:
                        self.future_done(future,
                                         "error: SFDSink bad cmd: " + str(cmd))
                        store.close()
                        return

                if bulk_keys and bulk_vals:
                    store.saveMultiple(bulk_keys, bulk_vals)

                store.commit()
                store.close()

            self.future_done(future, 0) # No return to keep looping.

    @staticmethod
    def can_handle(opts, spec):
        return spec.startswith(SFD_SCHEME)

    @staticmethod
    def check(opts, spec, source_map):
        # TODO: (2) SFDSink - check disk space.

        rv, dir = db_dir(spec)
        if rv != 0:
            return rv
        if not os.path.isdir(dir):
            return "error: not a directory: " + dir, None
        if not os.access(dir, os.W_OK):
            return "error: directory is not writable: " + dir, None
        return 0, None

    @staticmethod
    def consume_config(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_config):
        # TODO: (4) SFDSink - consume_config.
        return 0

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design):
        # TODO: (4) SFDSink - consume_design.
        return 0

    def consume_batch_async(self, batch):
        return self.push_next_batch(batch, pump.SinkBatchFuture(self, batch))

    def open_store(self, vbucket_id):
        if vbucket_id >= SFD_VBUCKETS:
            return "error: vbucket_id too large: " + str(vbucket_id), None

        rv, dir = db_dir(self.spec)
        if rv != 0:
            return rv

        # TODO: Open oldest *.couch.X file if it exists.
        db_file =  dir + "/" + str(vbucket_id) + ".couch.0"
        try:
            return 0, couchstore.CouchStore(db_file, 'c')
        except Exception as e:
            return "error: could not open couchstore: " + db_file + \
                "; exception: " + str(e), None


def db_dir(spec):
    if not spec.startswith(SFD_SCHEME):
        return "error: wrong scheme in spec: " + spec, None
    dir = spec[len(SFD_SCHEME):]
    if dir:
        return 0, os.path.normpath(dir)
    else:
        return "error: missing dir in spec: " + spec, None

