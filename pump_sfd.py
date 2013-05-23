#!/usr/bin/env python

import glob
import logging
import os
import re
import simplejson as json
import struct
import threading

import couchstore
import couchbaseConstants
import pump
from cbcollections import defaultdict

from cbqueue import PumpQueue

SFD_SCHEME = "couchstore-files://"
SFD_VBUCKETS = 1024
SFD_REV_META = ">QII" # cas, exp, flg
SFD_REV_SEQ = ">Q"
SFD_RE = "^([0-9]+)\\.couch\\.([0-9]+)$"

# TODO: (1) SFDSource - total_msgs.
# TODO: (1) SFDSink - ensure right user for bucket_dir.
# TODO: (1) SFDSink - ensure right user for couchstore file.

class SFDSource(pump.Source):
    """Reads couchstore files from a couchbase server data directory."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(SFDSource, self).__init__(opts, spec, source_bucket, source_node,
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
            rv, v = SFDSource.vbucket_states(opts, spec, bucket_dir)
            if rv != 0:
                return rv, None
            buckets.append({'name': bucket_name,
                            'nodes': [{'hostname': 'N/A',
                                       'vbucket_states': v}]})

        if not buckets:
            return "error: no bucket subdirectories at: " + d, None

        return 0, {'spec': spec, 'buckets': buckets}

    @staticmethod
    def vbucket_states(opts, spec, bucket_dir):
        """Reads all the latest couchstore files in a directory, and returns
           map of state string (e.g., 'active') to map of vbucket_id to doc."""
        vbucket_states = defaultdict(dict)

        for f in latest_couch_files(bucket_dir):
            vbucket_id = int(re.match(SFD_RE, os.path.basename(f)).group(1))
            try:
                store = couchstore.CouchStore(f, 'r')
                try:
                    doc_str = store.localDocs['_local/vbstate']
                    if doc_str:
                        doc = json.loads(doc_str)
                        state = doc.get('state', None)
                        if state:
                            vbucket_states[state][vbucket_id] = doc
                        else:
                            return "error: missing vbucket_state from: %s" \
                                % (f), None
                except Exception, e:
                    return ("error: could not read _local/vbstate from: %s" +
                            "; exception: %s") % (f, e), None
                store.close()
            except Exception, e:
                return ("error: could not read couchstore file: %s" +
                        "; exception: %s") % (f, e), None

        if vbucket_states:
            return 0, vbucket_states
        return "error: no vbucket_states in files: %s" % (bucket_dir), None

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        rv, d = data_dir(source_spec)
        if rv != 0:
            return rv, None

        bucket_dir = d + '/' + source_bucket['name']
        if not os.path.isdir(bucket_dir):
            return 0, None

        rv, store, store_path = \
            open_latest_store(bucket_dir,
                              "master.couch.*",
                              "^(master)\\.couch\\.([0-9]+)$",
                              "master.couch.0",
                              mode='r')
        if rv != 0 or not store:
            return rv, None

        rows = []

        for doc_info in store.changesSince(0):
            if not doc_info.deleted:
                try:
                    doc_contents = doc_info.getContents(options=couchstore.CouchStore.DECOMPRESS)
                except Exception, e:
                    return ("error: could not read design doc: %s" +
                            "; source_spec: %s; exception: %s") % \
                            (doc_info.id, source_spec, e), None
                try:
                    doc = json.loads(doc_contents)
                except ValueError, e:
                    return ("error: could not parse design doc: %s" +
                            "; source_spec: %s; exception: %s") % \
                            (doc_info.id, source_spec, e), None

                doc['id'] = doc.get('id', doc_info.id)
                doc['_rev'] = doc.get('_rev', doc_info.revSequence)

                rows.append({'id': doc_info.id, 'doc': doc})

        store.close()

        return 0, json.dumps(rows)

    def provide_batch(self):
        if self.done:
            return 0, None

        if not self.queue:
            name = "c" + threading.currentThread().getName()[1:]
            self.queue = PumpQueue(2)
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

        source_vbucket_state = \
            getattr(self.opts, 'source_vbucket_state', 'active')

        source_nodes = self.source_bucket['nodes']
        if len(source_nodes) != 1:
            self.queue.put(("error: expected 1 node in source_bucket: %s"
                            % (self.source_bucket['name']), None))
            return

        vbucket_states = source_nodes[0].get('vbucket_states', None)
        if not vbucket_states:
            self.queue.put(("error: missing vbucket_states in source_bucket: %s"
                            % (self.source_bucket['name']), None))
            return

        vbuckets = vbucket_states.get(source_vbucket_state, None)
        if vbuckets is None: # Empty dict is valid.
            self.queue.put(("error: missing vbuckets in source_bucket: %s"
                            % (self.source_bucket['name']), None))
            return

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        store = None
        vbucket_id = None

        # Level of indirection since we can't use python 3 nonlocal statement.
        abatch = [pump.Batch(self)]

        def change_callback(doc_info):
            if doc_info:
                key = doc_info.id
                if self.skip(key, vbucket_id):
                    return

                if doc_info.deleted:
                    cmd = couchbaseConstants.CMD_TAP_DELETE
                    val = ''
                else:
                    cmd = couchbaseConstants.CMD_TAP_MUTATION
                    val = doc_info.getContents(options=couchstore.CouchStore.DECOMPRESS)

                cas, exp, flg = struct.unpack(SFD_REV_META, doc_info.revMeta)
                meta = struct.pack(SFD_REV_SEQ, doc_info.revSequence)
                msg = (cmd, vbucket_id, key, flg, exp, cas, meta, val)
                abatch[0].append(msg, len(val))

            if (abatch[0].size() >= batch_max_size or
                abatch[0].bytes >= batch_max_bytes):
                self.queue.put((0, abatch[0]))
                abatch[0] = pump.Batch(self)

        for f in latest_couch_files(d + '/' + self.source_bucket['name']):
            vbucket_id = int(re.match(SFD_RE, os.path.basename(f)).group(1))
            if not vbucket_id in vbuckets:
                continue

            try:
                store = couchstore.CouchStore(f, 'r')
            except Exception, e:
                self.queue.put(("error: could not open couchstore file: %s"
                                "; exception: %s" % (f, e), None))
                return

            store.forEachChange(0, change_callback)
            store.close()

        if abatch[0].size():
            self.queue.put((0, abatch[0]))
        self.queue.put((0, None))


class SFDSink(pump.Sink):
    """Sink for couchstore in couchbase server/file/directory layout."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(SFDSink, self).__init__(opts, spec, source_bucket, source_node,
                                      source_map, sink_map, ctl, cur)
        self.rehash = opts.extra.get("rehash", 0)
        self.init_worker(SFDSink.run)

    @staticmethod
    def run(self):
        destination_vbucket_state = \
            getattr(self.opts, 'destination_vbucket_state', 'active')

        vbucket_states = self.source_node.get('vbucket_states', {})

        while not self.ctl['stop']:
            batch, future = self.pull_next_batch()
            if not batch:
                return self.future_done(future, 0)

            vbuckets = batch.group_by_vbucket_id(SFD_VBUCKETS, self.rehash)
            for vbucket_id, msgs in vbuckets.iteritems():
                checkpoint_id = 0
                max_deleted_seqno = 0

                rv, store, store_path = self.open_store(vbucket_id)
                if rv != 0:
                    return self.future_done(future, rv)

                bulk_keys = []
                bulk_vals = []

                for i, msg in enumerate(msgs):
                    cmd, _vbucket_id, key, flg, exp, cas, meta, val = msg
                    if self.skip(key, vbucket_id):
                        continue

                    d = couchstore.DocumentInfo(str(key))
                    d.revMeta = str(struct.pack(SFD_REV_META, cas, exp, flg))
                    if meta:
                        if len(meta) > 8:
                            meta = meta[0:8]
                        if len(meta) < 8:
                            meta = ('\x00\x00\x00\x00\x00\x00\x00\x00' + meta)[-8:]
                        d.revSequence, = struct.unpack(SFD_REV_SEQ, meta)
                    else:
                        d.revSequence = 1

                    if cmd == couchbaseConstants.CMD_TAP_MUTATION:
                        v = str(val)
                        try:
                            if (re.match('^\\s*{', v) and
                                json.loads(v) is not None):
                                d.contentType = couchstore.DocumentInfo.IS_JSON
                        except ValueError:
                            pass # NON_JSON is already the default contentType.
                    elif cmd == couchbaseConstants.CMD_TAP_DELETE:
                        v = None
                    else:
                        self.future_done(future,
                                         "error: SFDSink bad cmd: " + str(cmd))
                        store.close()
                        return

                    bulk_keys.append(d)
                    bulk_vals.append(v)

                try:
                    if bulk_keys and bulk_vals:
                        vm = vbucket_states.get(destination_vbucket_state, None)
                        if vm:
                            vi = vm.get(vbucket_id, None)
                            if vi:
                                c = int(vi.get("checkpoint_id", checkpoint_id))
                                checkpoint_id = max(checkpoint_id, c)
                                m = int(vi.get("max_deleted_seqno", max_deleted_seqno))
                                max_deleted_seqno = max(max_deleted_seqno, m)

                        rv = self.save_vbucket_state(store, vbucket_id,
                                                     destination_vbucket_state,
                                                     checkpoint_id,
                                                     max_deleted_seqno)
                        if rv != 0:
                            self.future_done(future, rv)
                            store.close()
                            return

                        store.saveMultiple(bulk_keys, bulk_vals,
                                           options=couchstore.CouchStore.COMPRESS)

                    store.commit()
                    store.close()
                except Exception, e:
                    self.future_done(future,
                                     "error: could not save couchstore data"
                                     "; vbucket_id: %s; store_path: %s"
                                     "; exception: %s"
                                     % (vbucket_id, store_path, e))
                    return

            self.future_done(future, 0) # No return to keep looping.

    def save_vbucket_state(self, store, vbucket_id,
                           state, checkpoint_id, max_deleted_seqno):
        doc = json.dumps({'state': state,
                          'checkpoint_id': str(checkpoint_id),
                          'max_deleted_seqno': str(max_deleted_seqno)})
        try:
            store.localDocs['_local/vbstate'] = doc
        except Exception, e:
            return "error: save_vbucket_state() failed: " + str(e)
        return 0

    @staticmethod
    def can_handle(opts, spec):
        return spec.startswith(SFD_SCHEME)

    @staticmethod
    def check_base(opts, spec):
        if getattr(opts, "destination_operation", None) != None:
            return ("error: --destination-operation" +
                    " is not supported by this destination: %s") % (spec)

        # Skip immediate superclass Sink.check_base(),
        # since SFDSink can handle different vbucket states.
        return pump.EndPoint.check_base(opts, spec)

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
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design):
        if not source_design:
            return 0

        try:
            sd = json.loads(source_design)
        except ValueError, e:
            return "error: could not parse source_design: " + source_design

        rv, d = data_dir(sink_spec)
        if rv != 0:
            return rv

        bucket_dir = d + '/' + source_bucket['name']
        if not os.path.isdir(bucket_dir):
            os.mkdir(bucket_dir)

        rv, store, store_path = \
            open_latest_store(bucket_dir,
                              "master.couch.*",
                              "^(master)\\.couch\\.([0-9]+)$",
                              "master.couch.1")
        if rv != 0:
            return rv

        bulk_keys = []
        bulk_vals = []

        if sd:
            for row in sd['rows']:
                logging.debug("design_doc row: " + str(row))

                d = couchstore.DocumentInfo(str(row['id']))
                if '_rev' in row['doc']:
                    d.revMeta = str(row['doc']['_rev'])
                    del row['doc']['_rev']
                d.contentType = couchstore.DocumentInfo.IS_JSON

                bulk_keys.append(d)
                bulk_vals.append(json.dumps(row['doc']))

            if bulk_keys and bulk_vals:
                store.saveMultiple(bulk_keys, bulk_vals) # TODO: Compress ddocs?

        store.commit()
        store.close()
        return 0

    def consume_batch_async(self, batch):
        return self.push_next_batch(batch, pump.SinkBatchFuture(self, batch))

    def open_store(self, vbucket_id):
        # data_dir   => /opt/couchbase/var/lib/couchbase/data/
        # bucket_dir =>   default/
        # store_path =>     VBUCKET_ID.couch.COMPACTION_NUM
        if vbucket_id >= SFD_VBUCKETS:
            return "error: vbucket_id too large: %s" % (vbucket_id), None, None

        rv, bucket_dir = self.find_bucket_dir()
        if rv != 0:
            return rv, None, None

        return open_latest_store(bucket_dir, "%s.couch.*" % (vbucket_id), SFD_RE,
                                 str(vbucket_id) + ".couch.1", mode='c')

    def find_bucket_dir(self):
        rv, d = data_dir(self.spec)
        if rv != 0:
            return rv, None

        bucket_dir = d + '/' + self.source_bucket['name']
        if not os.path.isdir(bucket_dir):
            try:
                os.mkdir(bucket_dir)
            except OSError, e:
                return ("error: could not create bucket_dir: %s; exception: %s"
                        % (bucket_dir, e)), None

        return 0, bucket_dir

def open_latest_store(bucket_dir, glob_pattern, filter_re, default_name, mode='c'):
    store_paths = latest_couch_files(bucket_dir,
                                     glob_pattern=glob_pattern,
                                     filter_re=filter_re)
    if not store_paths:
        if mode == 'r':
            return 0, None, None
        store_paths = [bucket_dir + '/' + default_name]
    if len(store_paths) != 1:
        return ("error: no single, latest couchstore file: %s" +
                "; found: %s") % (glob_pattern, store_paths), None, None
    try:
        return 0, couchstore.CouchStore(str(store_paths[0]), mode), store_paths[0]
    except Exception, e:
        return ("error: could not open couchstore file: %s" +
                "; exception: %s") % (store_paths[0], e), None, None

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

