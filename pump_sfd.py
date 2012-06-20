#!/usr/bin/env python

import ctypes
import glob
import logging
import os
import Queue
import re
import socket
import simplejson as json
import struct
import sys
import threading
import time

import couchstore
import memcacheConstants
import pump

SFD_SCHEME = "couchstore-files://"
SFD_VBUCKETS = 1024
SFD_REV_META = ">QII" # cas, exp, flg
SFD_RE = '^([0-9]+)\.couch\.([0-9]+)$'

# TODO: (1) SFDSource - total_items.
# TODO: (1) SFDSink - ensure right user for bucket_dir.
# TODO: (1) SFDSink - ensure right user for couchstore file.

class SFDSource(pump.Source):
    """Reads couchstore files from a couchbase server data directory."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        pump.Source.__init__(self, opts, spec, source_bucket, source_node,
                             source_map, sink_map, ctl, cur)

        self.done = False
        self.queue = None

    @staticmethod
    def can_handle(opts, spec):
        return spec.startswith(SFD_SCHEME)

    @staticmethod
    def check(opts, spec):
        rv, d = data_dir(spec)
        if rv != 0:
            return rv

        buckets = []

        for bucket_dir in sorted(glob.glob(d + "/*/")):
            if not glob.glob(bucket_dir + "/*.couch.*"):
                continue
            bucket_name = os.path.basename(os.path.dirname(bucket_dir))
            if not bucket_name:
                return "error: bucket_name too short: " + bucket_dir, None
            buckets.append({'name': bucket_name,
                            'nodes': [{'hostname': 'N/A'}]})

        if not buckets:
            return "error: no bucket subdirectories at: " + d, None

        return 0, {'spec': spec, 'buckets': buckets}

    @staticmethod
    def provide_config(opts, source_spec, source_bucket, source_map):
        # TODO: (3) SFDSource - provide_config implementation.
        return 0, None

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        # TODO: (3) SFDSource - provide_design implementation.
        return 0, None

    def provide_batch(self):
        if self.done:
            return 0, None

        if not self.queue:
            name = "c" + threading.current_thread().name[1:]
            self.queue = Queue.Queue(2)
            self.thread = threading.Thread(target=self.loader, name=name)
            self.thread.daemon = True
            self.thread.start()

        rv, batch = self.queue.get()
        self.queue.task_done()
        if rv != 0 or batch is None:
            self.done = True
        return rv, batch

    def loader(self):
        rv, d = data_dir(self.spec)
        if rv != 0:
            self.queue.put((rv, None))
            return

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        store = None
        vbucket_id = None

        # Level of indirection since we can't use python 3 nonlocal statement.
        abatch = [ pump.Batch(self) ]

        def change_callback(doc_info):
            if doc_info:
                key = doc_info.id
                if self.skip(key, vbucket_id):
                    return

                # TODO: (1) SFDSource - handle contentType.

                if doc_info.deleted:
                    cmd = memcacheConstants.CMD_TAP_DELETE
                else:
                    cmd = memcacheConstants.CMD_TAP_MUTATION

                flg, exp, cas = struct.unpack(SFD_REV_META, doc_info.revMeta)
                val = doc_info.getContents()
                item = (cmd, vbucket_id, key, flg, exp, cas, val)
                abatch[0].append(item, len(val))

            if (abatch[0].size() >= batch_max_size or
                abatch[0].bytes >= batch_max_bytes):
                self.queue.put((0, abatch[0]))
                abatch[0] = pump.Batch(self)

        files = latest_couch_files(d + '/' + self.source_bucket['name'])

        for f in files:
            try:
                store = couchstore.CouchStore(f, 'r')
            except Exception as e:
                self.queue.put(("error: could not open couchstore file: " + f +
                                " exception: " + str(e), None))
                return

            vbucket_id = int(re.match(SFD_RE, os.path.basename(f)).group(1))
            store.forEachChange(0, change_callback)
            store.close()

        if abatch[0].size():
            self.queue.put((0, abatch[0]))
        self.queue.put((0, None))


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

                # TODO: (1) SFDSink - update _local/vbstate doc.

                bulk_keys = []
                bulk_vals = []

                for i, item in enumerate(items):
                    cmd, _vbucket_id, key, flg, exp, cas, val = item
                    if self.skip(key, vbucket_id):
                        continue

                    d = couchstore.DocumentInfo(str(key))
                    d.revMeta = str(struct.pack(SFD_REV_META, cas, exp, flg))

                    if cmd == memcacheConstants.CMD_TAP_MUTATION:
                        v = str(val)
                        try:
                            if (re.match('^\\s*{', v) and
                                json.loads(v) is not None):
                                d.contentType = couchstore.DocumentInfo.IS_JSON
                        except ValueError:
                            pass # NON_JSON is already the default contentType.
                    elif cmd == memcacheConstants.CMD_TAP_DELETE:
                        v = None
                    else:
                        self.future_done(future,
                                         "error: SFDSink bad cmd: " + str(cmd))
                        store.close()
                        return

                    bulk_keys.append(d)
                    bulk_vals.append(v)

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

        rv, dir = data_dir(spec)
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
        # data_dir   => /opt/couchbase/var/lib/couchbase/data/
        # bucket_dir =>   default/
        # store_path =>     VBUCKET_ID.couch.COMPACTION_NUM
        if vbucket_id >= SFD_VBUCKETS:
            return "error: vbucket_id too large: " + str(vbucket_id), None

        rv, d = data_dir(self.spec)
        if rv != 0:
            return rv, None

        bucket_dir = d + '/' + str(self.source_bucket['name'])
        if not os.path.isdir(bucket_dir):
            os.mkdir(bucket_dir)

        glob_pattern = "(%s).couch.*" % (vbucket_id)
        store_paths = latest_couch_files(bucket_dir, glob_pattern=glob_pattern)
        if not store_paths:
            store_paths = [bucket_dir + '/' + str(vbucket_id) + ".couch.0"]
        if len(store_paths) != 1:
            return ("error: no single, latest couch file for vbucket_id: %s" +
                    "; found %s") % (vbucket_id, store_paths), None
        try:
            return 0, couchstore.CouchStore(store_paths[0], 'c')
        except Exception as e:
            return "error: could not open couchstore: " + store_path + \
                "; exception: " + str(e), None


def latest_couch_files(bucket_dir, glob_pattern='*.couch.*', filter_re=SFD_RE):
    """Given directory of *.couch.VER files, returns files with largest VER suffixes."""
    files = glob.glob(bucket_dir + '/' + glob_pattern)
    files = [f for f in files if re.match(filter_re, os.path.basename(f))]
    matches = [(re.match(filter_re, os.path.basename(f)), f) for f in files]
    latest = {}
    for match, file in matches:
        top, _ = latest.get(match.group(1), (-1, None))
        cur = int(match.group(2))
        if cur > top:
            latest[match.group(1)] = (cur, file)
    return sorted([file for top, file in latest.values()])

def data_dir(spec):
    if not spec.startswith(SFD_SCHEME):
        return "error: wrong scheme in spec: " + spec, None
    dir = spec[len(SFD_SCHEME):]
    if dir:
        return 0, os.path.normpath(dir)
    else:
        return "error: missing dir in spec: " + spec, None

