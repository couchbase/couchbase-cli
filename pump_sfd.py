#!/usr/bin/env python3

import glob
import logging
import os
import queue
import re
import json
import struct
import threading
from typing import Tuple, Any, List, Dict, Optional

import couchstore # pylint: disable=import-error
import couchbaseConstants
import pump
from cb_bin_client import decodeCollectionID, encodeCollectionId
from collections import defaultdict

SFD_SCHEME = "couchstore-files://"
SFD_VBUCKETS = 1024
SFD_REV_META = ">QIIBB" # cas, exp, flg, flex_meta, dtype
SFD_REV_META_PRE_4_6 = ">QIIBBB" # cas, exp, flg, flex_meta, dtype, conf_res
SFD_REV_SEQ = ">Q"
SFD_DB_SEQ = ">Q"
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
        print('Starting sfd source:  ', spec)

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        return spec.startswith(SFD_SCHEME)

    @staticmethod
    def check_base(opts, spec: str) -> couchbaseConstants.PUMP_ERROR:
        # Skip immediate superclass Source.check_base(),
        # since SFDSource can handle different vbucket states.
        return pump.EndPoint.check_base(opts, spec)

    @staticmethod
    def check(opts, spec: str) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[Dict[str, Any]]]:
        rv, d = data_dir(spec)
        if rv != 0:
            return rv, None

        buckets = []

        for bucket_dir in sorted(glob.glob(f'{d}/*/')):
            if not glob.glob(f'{bucket_dir}/*.couch.*'):
                continue
            bucket_name = os.path.basename(os.path.dirname(bucket_dir))
            if not bucket_name:
                return f'error: bucket_name too short: {bucket_dir}', None
            rv, v = SFDSource.vbucket_states(opts, spec, bucket_dir)
            if rv != 0:
                return rv, None
            buckets.append({'name': bucket_name,
                            'nodes': [{'hostname': 'N/A',
                                       'vbucket_states': v}]})

        if not buckets:
            return f'error: no bucket subdirectories at: {d}', None

        return 0, {'spec': spec, 'buckets': buckets}

    @staticmethod
    def vbucket_states(opts, spec, bucket_dir) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[Dict[str, Any]]]:
        """Reads all the latest couchstore files in a directory, and returns
           map of state string (e.g., 'active') to map of vbucket_id to doc."""
        vbucket_states: Dict[str, Any] = defaultdict(dict)

        for f in latest_couch_files(bucket_dir):
            vbucket_id = int(re.match(SFD_RE, os.path.basename(f)).group(1))  # type: ignore
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
                            return f'error: missing vbucket_state from: {f}', None
                except Exception as e:
                    return f'error: could not read _local/vbstate from: {f}; exception: {e}', None
                store.close()
            except Exception as e:
                return f'error: could not read couchstore file: {f}; exception: {e}', None

        if vbucket_states:
            return 0, vbucket_states
        return f'error: no vbucket_states in files: {bucket_dir}', None

    @staticmethod
    def provide_design(opts, source_spec: str, source_bucket, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                                   Optional[str]]:
        rv, d = data_dir(source_spec)
        if rv != 0:
            return rv, None

        bucket_dir = f'{d}/{source_bucket["name"]}'
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
                except Exception as e:
                    return f'error: could not read design doc: {doc_info.id}; source_spec: {source_spec};' \
                               f' exception: {e}', None
                try:
                    doc = json.loads(doc_contents)
                except ValueError as e:
                    return f'error: could not parse design doc: {doc_info.id}; source_spec: {source_spec};' \
                               f' exception: {e}', None

                doc['id'] = doc.get('id', doc_info.id)
                doc['_rev'] = doc.get('_rev', doc_info.revSequence)

                rows.append({'id': doc_info.id, 'doc': doc})

        store.close()

        return 0, json.dumps(rows)

    def provide_batch(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch]]:
        if self.done:
            return 0, None

        if not self.queue:
            name = "c" + threading.currentThread().getName()[1:]
            self.queue = queue.Queue(2)
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
            self.queue.put((f'error: expected 1 node in source_bucket: {self.source_bucket["name"]}', None))
            return

        vbucket_states = source_nodes[0].get('vbucket_states', None)
        if not vbucket_states:
            self.queue.put((f'error: missing vbucket_states in source_bucket: {self.source_bucket["name"]}', None))
            return

        vbuckets = vbucket_states.get(source_vbucket_state, None)
        if vbuckets is None: # Empty dict is valid.
            self.queue.put((f'error: missing vbuckets in source_bucket: {self.source_bucket["name"]}', None))
            return

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        store = None
        vbucket_id = None

        # Level of indirection since we can't use python 3 nonlocal statement.
        abatch: List[pump.Batch] = [pump.Batch(self)]

        def change_callback(doc_info):
            if doc_info:
                # Handle the new key name spacing for collections and co
                cid, key = decodeCollectionID(doc_info.id.encode())
                # Only support keys in the _default collection
                if cid != 0:
                    logging.debug('Skipping as not default collection')
                    return

                if self.skip(key, vbucket_id):
                    return

                if doc_info.deleted:
                    cmd = couchbaseConstants.CMD_DCP_DELETE
                    val = b''
                else:
                    cmd = couchbaseConstants.CMD_DCP_MUTATION
                    val = doc_info.getContents(options=couchstore.CouchStore.DECOMPRESS)
                try:
                    rev_meta_bytes = doc_info.revMeta.get_bytes()
                    if len(rev_meta_bytes) == 18:
                        conf_res = 0
                        cas, exp, flg, flex_meta, dtype = struct.unpack(SFD_REV_META, rev_meta_bytes)
                    elif len(rev_meta_bytes) == 19:
                        cas, exp, flg, flex_meta, dtype, conf_res = struct.unpack(SFD_REV_META_PRE_4_6, rev_meta_bytes)
                    else:
                        raise ValueError('Does not match pre- or post-4.6 format')
                    meta = int(doc_info.revSequence).to_bytes(8, 'big')
                    seqno = doc_info.sequence
                    nmeta = 0
                    msg = (cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, nmeta, conf_res)
                    abatch[0].append(msg, len(val))
                except Exception as e:
                    self.queue.put((f'error: could not read couchstore file due to unsupported file format version;'
                                    f' exception: {e}', None))
                    return

            if (abatch[0].size() >= batch_max_size or
                abatch[0].bytes >= batch_max_bytes):
                self.queue.put((0, abatch[0]))
                abatch[0] = pump.Batch(self)

        for f in latest_couch_files(f'{d}/{self.source_bucket["name"]}'):
            vbucket_id = int(re.match(SFD_RE, os.path.basename(f)).group(1))
            if not vbucket_id in vbuckets:
                continue

            try:
                store = couchstore.CouchStore(f, 'r')
                store.forEachChange(0, change_callback)
                store.close()
            except Exception as e:
                # MB-12270: Some files may be deleted due to compaction. We can
                # safely ignore them and move to next file.
                pass

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
        destination_vbucket_state = getattr(self.opts, 'destination_vbucket_state', 'active')

        vbucket_states = self.source_node.get('vbucket_states', {})

        while not self.ctl['stop']:
            batch, future = self.pull_next_batch()  # type: pump.Batch, pump.SinkBatchFuture
            if not batch:
                return self.future_done(future, 0)

            vbuckets = batch.group_by_vbucket_id(SFD_VBUCKETS, self.rehash)
            for vbucket_id, msgs in vbuckets.items():
                checkpoint_id = 0
                max_deleted_seqno = 0

                rv, store, store_path = self.open_store(vbucket_id)
                if rv != 0:
                    return self.future_done(future, rv)

                bulk_keys = []
                bulk_vals = []

                for i, msg in enumerate(msgs):
                    cmd, _vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, nmeta, conf_res = msg
                    if self.skip(key, vbucket_id):
                        continue

                    # TODO: add default collection to all keys in CC this should change to have the correct collection
                    key = encodeCollectionId(0) + key
                    d = couchstore.DocumentInfo(key.decode())
                    flex_meta = 1
                    d.revMeta = struct.pack(SFD_REV_META, cas, exp, flg, flex_meta, dtype)
                    if len(meta) != 0:
                        if len(meta) > 8:
                            meta = meta[0:8]
                        if len(meta) < 8:
                            meta = (b'\x00\x00\x00\x00\x00\x00\x00\x00' + meta)[-8:]
                        d.revSequence, = struct.unpack(SFD_REV_SEQ, meta)
                    else:
                        d.revSequence = 1

                    if seqno:
                        d.sequence = int(seqno)
                    if cmd == couchbaseConstants.CMD_TAP_MUTATION or cmd == couchbaseConstants.CMD_DCP_MUTATION:
                        try:
                            v = val
                            if dtype & 0x01:
                                d.contentType = couchstore.DocumentInfo.IS_JSON
                            # Why do this when we have a flag for it?
                            # if re.match('^\\s*{', v) and json.loads(v) is not None:
                            #     d.contentType = couchstore.DocumentInfo.IS_JSON
                        except ValueError:
                            pass # NON_JSON is already the default contentType.
                    elif cmd == couchbaseConstants.CMD_TAP_DELETE or cmd == couchbaseConstants.CMD_DCP_DELETE:
                        v = None
                    else:
                        self.future_done(future, f'error: SFDSink bad cmd: {cmd!s}')
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
                except Exception as e:
                    self.future_done(future, f'error: could not save couchstore data; vbucket_id: {vbucket_id}; '
                                             f'store_path: {store_path}; exception: {e}')
                    return

            self.future_done(future, 0) # No return to keep looping.

    def save_vbucket_state(self, store, vbucket_id,
                           state, checkpoint_id, max_deleted_seqno):
        doc = json.dumps({'state': state,
                          'checkpoint_id': str(checkpoint_id),
                          'max_deleted_seqno': str(max_deleted_seqno)})
        try:
            store.localDocs['_local/vbstate'] = doc
        except Exception as e:
            return f'error: save_vbucket_state() failed: {e!s}'
        return 0

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        return spec.startswith(SFD_SCHEME)

    @staticmethod
    def check_base(opts, spec: str) -> couchbaseConstants.PUMP_ERROR:
        if getattr(opts, "destination_operation", None) != None:
            return f'error: --destination-operation is not supported by this destination: {spec}'

        # Skip immediate superclass Sink.check_base(),
        # since SFDSink can handle different vbucket states.
        return pump.EndPoint.check_base(opts, spec)

    @staticmethod
    def check(opts, spec: str, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR, None]:
        # TODO: (2) SFDSink - check disk space.

        rv, dir = data_dir(spec)
        if rv != 0:
            return rv, None
        if not os.path.isdir(dir):
            return f'error: not a directory: {dir}', None
        if not os.access(dir, os.W_OK):
            return f'error: directory is not writable: {dir}', None
        return 0, None

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design) -> couchbaseConstants.PUMP_ERROR:
        if not source_design:
            return 0

        try:
            sd = json.loads(source_design)
        except ValueError as e:
            return f'error: could not parse source_design: {source_design}'

        rv, d = data_dir(sink_spec)
        if rv != 0:
            return rv

        bucket_dir = f'{d}/{source_bucket["name"]}'
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

                doc_info = couchstore.DocumentInfo(str(row['id']))
                if '_rev' in row['doc']:
                    doc_info.revMeta = str(row['doc']['_rev'])
                    del row['doc']['_rev']
                doc_info.contentType = couchstore.DocumentInfo.IS_JSON

                bulk_keys.append(doc_info)
                bulk_vals.append(json.dumps(row['doc']))

            if bulk_keys and bulk_vals:
                store.saveMultiple(bulk_keys, bulk_vals)  # type: ignore # TODO: Compress ddocs?

        store.commit()  # type: ignore
        store.close()  # type: ignore
        return 0

    def consume_batch_async(self, batch: Optional[pump.Batch]) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                        Optional[pump.SinkBatchFuture]]:
        return self.push_next_batch(batch, pump.SinkBatchFuture(self, batch))

    def open_store(self, vbucket_id: int):
        # data_dir   => /opt/couchbase/var/lib/couchbase/data/
        # bucket_dir =>   default/
        # store_path =>     VBUCKET_ID.couch.COMPACTION_NUM
        if vbucket_id >= SFD_VBUCKETS:
            return f'error: vbucket_id too large: {vbucket_id}', None, None

        rv, bucket_dir = self.find_bucket_dir()
        if rv != 0:
            return rv, None, None

        return open_latest_store(bucket_dir, f'{vbucket_id}.couch.*', SFD_RE, f'{vbucket_id!s}.couch.1', mode='c')

    def find_bucket_dir(self) -> Tuple[couchbaseConstants.PUMP_ERROR, str]:
        rv, d = data_dir(self.spec)
        if rv != 0:
            return rv, ''

        bucket_dir = d + '/' + self.source_bucket['name']
        if not os.path.isdir(bucket_dir):
            try:
                os.mkdir(bucket_dir)
            except OSError as e:
                return f'error: could not create bucket_dir: {bucket_dir}; exception: {e}', ''

        return 0, bucket_dir


def open_latest_store(bucket_dir: str, glob_pattern: str, filter_re: str, default_name: str, mode: str = 'c') \
        -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[couchstore.CouchStore], str]:
    store_paths = latest_couch_files(bucket_dir,
                                     glob_pattern=glob_pattern,
                                     filter_re=filter_re)
    if not store_paths:
        if mode == 'r':
            return 0, None, ''
        store_paths = [f'{bucket_dir}/{default_name}']
    if len(store_paths) != 1:
        return f'error: no single, latest couchstore file: {glob_pattern}; found: {store_paths}', None, ''
    try:
        return 0, couchstore.CouchStore(str(store_paths[0]), mode), store_paths[0]
    except Exception as e:
        return f'error: could not open couchstore file: {store_paths[0]}; exception: {e}', None, ''

def latest_couch_files(bucket_dir: str, glob_pattern: str = '*.couch.*', filter_re: str = SFD_RE) -> List[str]:
    """Given directory of *.couch.VER files, returns files with largest VER suffixes."""
    files = glob.glob(f'{bucket_dir}/{glob_pattern}')
    files = [f for f in files if re.match(filter_re, os.path.basename(f))]
    matches = [(re.match(filter_re, os.path.basename(f)), f) for f in files]
    latest: Dict[str, Tuple[int, str]] = {}
    for match, file in matches:
        top, _ = latest.get(match.group(1), (-1, None))  # type: ignore
        cur = int(match.group(2))  # type: ignore
        if cur > top:
            latest[match.group(1)] = (cur, file)  # type: ignore
    return sorted([file for top, file in latest.values()])


def data_dir(spec: str) -> Tuple[couchbaseConstants.PUMP_ERROR, str]:
    if not spec.startswith(SFD_SCHEME):
        return f'error: wrong scheme in spec: {spec}', ''
    dir = spec[len(SFD_SCHEME):]
    if dir:
        return 0, os.path.normpath(dir)
    else:
        return f'error: missing dir in spec: {spec}', ''
