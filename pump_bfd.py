#!/usr/bin/env python

import copy
import glob
import logging
import os
import simplejson as json
import string
import sys
import time
import urllib

import couchbaseConstants
import pump

import_stmts = (
    'from pysqlite2 import dbapi2 as sqlite3',
    'import sqlite3',
)
for status, stmt in enumerate(import_stmts):
    try:
        exec stmt
        break
    except ImportError:
        status = None
if status is None:
    sys.exit("Error: could not import sqlite3 module")

CBB_VERSION = 2004 # sqlite pragma user version.

class BFD:
    """Mixin for backup-file/directory EndPoint helper methods."""

    def bucket_name(self):
        return self.source_bucket['name']

    def node_name(self):
        return self.source_node['hostname']

    @staticmethod
    def design_path(spec, bucket_name):
        return os.path.normpath(spec) + \
            '/bucket-' + urllib.quote_plus(bucket_name) + \
            '/design.json'

    @staticmethod
    def db_dir(spec, bucket_name, node_name):
        return os.path.normpath(spec) + \
            '/bucket-' + urllib.quote_plus(bucket_name) + \
            '/node-' + urllib.quote_plus(node_name)

    @staticmethod
    def db_path(spec, bucket_name, node_name, num):
        return BFD.db_dir(spec, bucket_name, node_name) + \
            "/data-%s.cbb" % (string.rjust(str(num), 4, '0'))


# --------------------------------------------------

class BFDSource(BFD, pump.Source):
    """Can read from backup-file/directory layout."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(BFDSource, self).__init__(opts, spec, source_bucket, source_node,
                                        source_map, sink_map, ctl, cur)
        self.done = False
        self.files = None
        self.cursor_db = None

    @staticmethod
    def can_handle(opts, spec):
        return (os.path.isdir(spec) and
                glob.glob(spec + "/bucket-*/node-*/data-*.cbb"))

    @staticmethod
    def check(opts, spec):
        spec = os.path.normpath(spec)
        if not os.path.isdir(spec):
            return "error: backup_dir is not a directory: " + spec, None

        buckets = []

        bucket_dirs = glob.glob(spec + "/bucket-*")
        for bucket_dir in sorted(bucket_dirs):
            if not os.path.isdir(bucket_dir):
                return "error: not a bucket directory: " + bucket_dir, None

            bucket_name = os.path.basename(bucket_dir)[len("bucket-"):].strip()
            bucket_name = urllib.unquote_plus(bucket_name)
            if not bucket_name:
                return "error: bucket_name too short: " + bucket_dir, None

            bucket = { 'name': bucket_name, 'nodes': [] }
            buckets.append(bucket)

            node_dirs = glob.glob(bucket_dir + "/node-*")
            for node_dir in sorted(node_dirs):
                if not os.path.isdir(node_dir):
                    return "error: not a node directory: " + node_dir, None

                node_name = os.path.basename(node_dir)[len("node-"):].strip()
                node_name = urllib.unquote_plus(node_name)
                if not node_name:
                    return "error: node_name too short: " + node_dir, None

                bucket['nodes'].append({ 'hostname': node_name })

        return 0, { 'spec': spec,
                    'buckets': buckets }

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        fname = BFD.design_path(source_spec, source_bucket['name'])
        if os.path.exists(fname):
            try:
                f = open(fname, 'r')
                d = f.read()
                f.close()
                return 0, d
            except IOError, e:
                return ("error: could not read design: %s" +
                        "; exception: %s") % (fname, e), None
        return 0, None

    def provide_batch(self):
        if self.done:
            return 0, None

        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        s = "SELECT cmd, vbucket_id, key, flg, exp, cas, meta, val FROM cbb_msg"

        if self.files is None: # None != [], as self.files will shrink to [].
            g = glob.glob(BFD.db_dir(self.spec,
                                     self.bucket_name(),
                                     self.node_name()) + "/data-*.cbb")
            self.files = sorted(g)
        try:
            while (not self.done and
                   batch.size() < batch_max_size and
                   batch.bytes < batch_max_bytes):
                if self.cursor_db is None:
                    if not self.files:
                        self.done = True
                        return 0, batch

                    rv, db = connect_db(self.files[0], self.opts, CBB_VERSION)
                    if rv != 0:
                        return rv, None
                    self.files = self.files[1:]

                    cursor = db.cursor()
                    cursor.execute(s)

                    self.cursor_db = (cursor, db)

                cursor, db = self.cursor_db

                row = cursor.fetchone()
                if row:
                    vbucket_id = row[1]
                    key = row[2]
                    val = row[7]

                    if self.skip(key, vbucket_id):
                        continue

                    msg = (row[0], row[1], row[2], row[3], row[4],
                           int(row[5]), # CAS as 64-bit integer not string.
                           row[6], row[7])
                    batch.append(msg, len(val))
                else:
                    if self.cursor_db:
                        self.cursor_db[0].close()
                        self.cursor_db[1].close()
                    self.cursor_db = None

            return 0, batch

        except Exception, e:
            self.done = True
            if self.cursor_db:
                self.cursor_db[0].close()
                self.cursor_db[1].close()
            self.cursor_db = None

            return "error: exception reading backup file: " + str(e), None

    @staticmethod
    def total_msgs(opts, source_bucket, source_node, source_map):
        t = 0
        g = glob.glob(BFD.db_dir(source_map['spec'],
                                 source_bucket['name'],
                                 source_node['hostname']) + "/data-*.cbb")
        for x in sorted(g):
            rv, db = connect_db(x, opts, CBB_VERSION)
            if rv != 0:
                return rv, None

            cur = db.cursor()
            # TODO: (1) BFDSource - COUNT(*) is not indexed.
            cur.execute("SELECT COUNT(*) FROM cbb_msg;")
            t = t + cur.fetchone()[0]
            cur.close()
            db.close()

        return 0, t


# --------------------------------------------------

class BFDSink(BFD, pump.Sink):
    """Can write to backup-file/directory layout."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(BFDSink, self).__init__(opts, spec, source_bucket, source_node,
                                      source_map, sink_map, ctl, cur)
        self.init_worker(BFDSink.run)

    @staticmethod
    def run(self):
        """Worker thread to asynchronously store incoming batches into db."""
        s = "INSERT INTO cbb_msg (cmd, vbucket_id, key, flg, exp, cas, meta, val)" \
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        db = None
        cbb = 0       # Current cbb file NUM, like data-NUM.cbb.
        cbb_bytes = 0 # Current cbb msg value bytes total.
        cbb_max_bytes = \
            self.opts.extra.get("cbb_max_mb", 100000) * 1024 * 1024

        while not self.ctl['stop']:
            batch, future = self.pull_next_batch()
            if not batch:
                return self.future_done(future, 0)

            if db and cbb_bytes >= cbb_max_bytes:
                db.close()
                db = None
                cbb += 1
                cbb_bytes = 0

            if not db:
                rv, db = self.create_db(cbb)
                if rv != 0:
                    return self.future_done(future, rv)

            try:
                c = db.cursor()

                for i in range(0, batch.size()):
                    cmd, vbucket_id, key, flg, exp, cas, meta, val = \
                        batch.msg(i)

                    if self.skip(key, vbucket_id):
                        continue

                    if (cmd != couchbaseConstants.CMD_TAP_MUTATION and
                        cmd != couchbaseConstants.CMD_TAP_DELETE):
                        return self.future_done(future,
                                                "error: BFDSink bad cmd: " +
                                                str(cmd))

                    c.execute(s, (cmd, vbucket_id,
                                  sqlite3.Binary(key),
                                  flg, exp, str(cas),
                                  sqlite3.Binary(meta),
                                  sqlite3.Binary(val)))
                    cbb_bytes += len(val)

                db.commit()

                self.future_done(future, 0) # No return to keep looping.

            except sqlite3.Error, e:
                return self.future_done(future, "error: db error: " + str(e))
            except Exception, e:
                return self.future_done(future, "error: db exception: " + str(e))

    @staticmethod
    def can_handle(opts, spec):
        spec = os.path.normpath(spec)
        return (os.path.isdir(spec) or (not os.path.exists(spec) and
                                        os.path.isdir(os.path.dirname(spec))))

    @staticmethod
    def check(opts, spec, source_map):
        # TODO: (2) BFDSink - check disk space.
        # TODO: (2) BFDSink - check should report on pre-creatable directories.

        spec = os.path.normpath(spec)

        # Check that directory's empty.
        if os.path.exists(spec):
            if not os.path.isdir(spec):
                return "error: backup directory is not a directory: " + spec, None
            if not os.access(spec, os.W_OK):
                return "error: backup directory is not writable: " + spec, None
            if len(os.listdir(spec)):
                return "error: backup directory is not empty: " + spec, None
            return 0, None

        # Or, that the parent directory exists.
        parent_dir = os.path.dirname(spec)

        if not os.path.exists(parent_dir):
            return "error: missing parent directory: " + parent_dir, None
        if not os.path.isdir(parent_dir):
            return "error: parent directory is not a directory: " + parent_dir, None
        if not os.access(parent_dir, os.W_OK):
            return "error: parent directory is not writable: " + parent_dir, None
        return 0, None

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design):
        if source_design:
            fname = BFD.design_path(sink_spec,
                                    source_bucket['name'])
            try:
                rv = pump.mkdirs(fname)
                if rv:
                    return rv, None
                f = open(fname, 'w')
                f.write(source_design)
                f.close()
            except IOError, e:
                return ("error: could not write design: %s" +
                        "; exception: %s") % (fname, e), None
        return 0

    def consume_batch_async(self, batch):
        return self.push_next_batch(batch, pump.SinkBatchFuture(self, batch))

    def create_db(self, num):
        rv = self.mkdirs()
        if rv != 0:
            return rv, None

        rv, db = create_db(BFD.db_path(self.spec,
                                       self.bucket_name(),
                                       self.node_name(), num),
                           self.opts)
        if rv != 0:
            return rv, None

        try:
            cur = db.cursor()
            cur.execute("INSERT INTO cbb_meta (key, val) VALUES (?, ?)",
                        ("source_bucket.json",
                         json.dumps(cleanse(self.source_bucket))))
            cur.execute("INSERT INTO cbb_meta (key, val) VALUES (?, ?)",
                        ("source_node.json",
                         json.dumps(cleanse(self.source_node))))
            cur.execute("INSERT INTO cbb_meta (key, val) VALUES (?, ?)",
                        ("source_map.json",
                         json.dumps(cleanse(self.source_map))))
            cur.execute("INSERT INTO cbb_meta (key, val) VALUES (?, ?)",
                        ("start.datetime", time.strftime("%Y/%m/%d-%H:%M:%S")))
            db.commit()
        except sqlite3.Error, e:
            return "error: create_db error: " + str(e), None
        except Exception, e:
            return "error: create_db exception: " + str(e), None

        return 0, db

    def mkdirs(self):
        """Make directories, if not already, with structure like...
           <spec>/
             bucket-<BUCKETNAME>/
               design.json
               node-<NODE>/
                 data-<XXXX>.cbb"""
        spec = os.path.normpath(self.spec)
        if not os.path.isdir(spec):
            try:
                os.mkdir(spec)
            except OSError, e:
                return "error: could not mkdir: %s; exception: %s" % (spec, e)

        d = BFD.db_dir(self.spec, self.bucket_name(), self.node_name())
        if not os.path.isdir(d):
            try:
                os.makedirs(d)
            except OSError, e:
                return "error: could not mkdirs: %s; exception: %s" % (d, e)
        return 0


# --------------------------------------------------

def create_db(db_path, opts):
    try:
        logging.debug("  create_db: " + db_path)

        rv, db = connect_db(db_path, opts, 0)
        if rv != 0:
            return rv, None

        # The cas column is type text, not integer, because sqlite
        # integer is 63-bits instead of 64-bits.
        db.executescript("""
                  BEGIN;
                  CREATE TABLE cbb_msg
                     (cmd integer,
                      vbucket_id integer,
                      key blob, flg integer, exp integer, cas text,
                      meta blob, val blob);
                  CREATE TABLE cbb_meta
                     (key text,
                      val blob);
                  pragma user_version=%s;
                  COMMIT;
                """ % (CBB_VERSION))

        return 0, db

    except Exception, e:
        return "error: create_db exception: " + str(e), None

def connect_db(db_path, opts, version):
    try:
        # TODO: (1) BFD - connect_db - use pragma page_size.
        # TODO: (1) BFD - connect_db - use pragma max_page_count.
        # TODO: (1) BFD - connect_db - use pragma journal_mode.

        logging.debug("  connect_db: " + db_path)

        db = sqlite3.connect(db_path)
        db.text_factory = str

        cur = db.execute("pragma user_version").fetchall()[0][0]
        if cur != version:
            return "error: unexpected db user version: " + \
                str(cur) + " vs " + str(version), \
                None

        return 0, db

    except Exception, e:
        return "error: connect_db exception: " + str(e), None

def cleanse(d):
    """Elide passwords from hierarchy of dict/list's."""
    return cleanse_helper(copy.deepcopy(d))

def cleanse_helper(d):
    """Recursively, destructively elide passwords from hierarchy of dict/list's."""
    if type(d) is list:
        for x in d:
            cleanse_helper(x)
    elif type(d) is dict:
        for k, v in d.iteritems():
            if "assword" in k:
                d[k] = '<...ELIDED...>'
            else:
                d[k] = cleanse_helper(v)
    return d
