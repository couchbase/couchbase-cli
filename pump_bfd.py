#!/usr/bin/env python

import copy
import glob
import logging
import os
import simplejson as json
import string
import sys
import datetime
import time
import urllib
import fnmatch

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

CBB_VERSION = [2004, 2014] # sqlite pragma user version.

class BFD:
    """Mixin for backup-file/directory EndPoint helper methods."""
    NUM_VBUCKET = 1024

    def bucket_name(self):
        return self.source_bucket['name']

    def node_name(self):
        return self.source_node['hostname']

    @staticmethod
    def design_path(spec, bucket_name):
        bucket_path = os.path.normpath(spec) + "/bucket-" + urllib.quote_plus(bucket_name)
        if os.path.isdir(bucket_path):
            return bucket_path + '/design.json'
        else:
            path, dirs = BFD.find_latest_dir(spec, None)
            if path:
                path, dirs = BFD.find_latest_dir(path, None)
                if path:
                    return path + "/bucket-" + urllib.quote_plus(bucket_name) + '/design.json'
        return bucket_path + '/design.json'

    @staticmethod
    def construct_dir(parent, bucket_name, node_name):
        return os.path.join(parent,
                    "bucket-" + urllib.quote_plus(bucket_name),
                    "node-" + urllib.quote_plus(node_name))

    @staticmethod
    def check_full_dbfiles(parent_dir):
        return glob.glob(os.path.join(parent_dir, "data-*.cbb"))

    @staticmethod
    def get_precessors(parent_dir):
        json_file = open(os.path.join(parent_dir, "meta.json"), "r")
        json_data = json.load(json_file)
        json_file.close()
        return json_data["pred"]

    @staticmethod
    def db_dir(spec, bucket_name, node_name, mode=None):
        parent_dir = os.path.normpath(spec) + \
                        '/bucket-' + urllib.quote_plus(bucket_name) + \
                        '/node-' + urllib.quote_plus(node_name)
        if os.path.isdir(parent_dir):
            return parent_dir

        #check 3.0 directory structure
        tmstamp = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime())
        parent_dir = os.path.normpath(spec)
        timepath, dirs = BFD.find_latest_dir(parent_dir, None)
        if not timepath or not mode or mode == "full":
            # no any backup roots exists
            path = os.path.join(parent_dir, tmstamp, tmstamp+"-full")
            return BFD.construct_dir(path, bucket_name, node_name)

        #check if any full backup exists
        path, dirs = BFD.find_latest_dir(timepath, "full")
        if not path:
            path = os.path.join(timepath, tmstamp+"-full")
            return BFD.construct_dir(path, bucket_name, node_name)

        if mode.find("diff") >= 0:
            path = os.path.join(timepath, tmstamp+"-diff")
            return BFD.construct_dir(path, bucket_name, node_name)
        elif mode.find("accu") >= 0:
            path = os.path.join(timepath, tmstamp+"-accu")
            return BFD.construct_dir(path, bucket_name, node_name)
        else:
            return parent_dir

    @staticmethod
    def find_latest_dir(parent_dir, mode):
        all_subdirs = []
        latest_dir = None
        if not os.path.isdir(parent_dir):
            return latest_dir, all_subdirs
        for d in os.listdir(parent_dir):
            if not mode or d.find(mode) >= 0:
                bd = os.path.join(parent_dir, d)
                if os.path.isdir(bd):
                    all_subdirs.append(bd)
        if all_subdirs:
            latest_dir = max(all_subdirs, key=os.path.getmtime)
        return latest_dir, all_subdirs

    @staticmethod
    def find_seqno(opts, spec, bucket_name, node_name, mode):
        seqno = {}
        dep = {}
        dep_list = []
        for i in range(BFD.NUM_VBUCKET):
            seqno[i] = 0
            dep[i] = None
        file_list = []
        parent_dir = os.path.normpath(spec)

        if mode == "full":
            return seqno, dep_list
        timedir,latest_dirs = BFD.find_latest_dir(parent_dir, None)
        if not timedir:
            return seqno, dep_list
        fulldir, latest_dirs = BFD.find_latest_dir(timedir, "full")
        if not fulldir:
            return seqno, dep_list
        file_list.extend(recursive_glob(fulldir, 'data-*.cbb'))
        accudir, accu_dirs = BFD.find_latest_dir(timedir, "accu")
        if accudir:
            file_list.extend(recursive_glob(accudir, 'data-*.cbb'))
        if mode.find("diff") >= 0:
            diffdir, diff_dirs = BFD.find_latest_dir(timedir, "diff")
            if diff_dirs:
                for dir in diff_dirs:
                    file_list.extend(recursive_glob(dir, 'data-*.cbb'))

        for x in sorted(file_list):
            rv, db, ver = connect_db(x, opts, CBB_VERSION)
            if rv != 0:
                return seqno, dep_list

            for i in range(BFD.NUM_VBUCKET):
                cur = db.cursor()
                cur.execute("SELECT MAX(seqno) FROM cbb_msg where vbucket_id = %s;" % i)
                row = cur.fetchone()[0]
                if row:
                    if int(row) > seqno[i]:
                        seqno[i] = int(row)
                        dep[i] = x
                cur.close()
            db.close()

        for i in range(BFD.NUM_VBUCKET):
            if dep[i] and dep[i] not in dep_list:
                dep_list.append(dep[i])

        return seqno, dep_list

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
        if os.path.isdir(spec):
            dirs = glob.glob(spec + "/bucket-*/node-*/data-*.cbb")
            if dirs:
                return True
            path, dirs = BFD.find_latest_dir(spec, None)
            if path:
                path, dirs = BFD.find_latest_dir(path, "full")
                if path:
                    return glob.glob(path + "*/bucket-*/node-*/data-*.cbb")
        return False

    @staticmethod
    def check(opts, spec):
        spec = os.path.normpath(spec)
        if not os.path.isdir(spec):
            return "error: backup_dir is not a directory: " + spec, None

        buckets = []

        bucket_dirs = glob.glob(spec + "/bucket-*")
        if not bucket_dirs:
            #check 3.0 directory structure
            path, dirs = BFD.find_latest_dir(spec, None)
            if not path:
                return "error: no backup directory found: " + spec, None
            latest_dir, dir = BFD.find_latest_dir(path, "full")
            bucket_dirs = glob.glob(latest_dir + "/bucket-*")

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

        s = ["SELECT cmd, vbucket_id, key, flg, exp, cas, meta, val FROM cbb_msg",
             "SELECT cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, meta_size FROM cbb_msg"]

        if self.files is None: # None != [], as self.files will shrink to [].
            g =  glob.glob(BFD.db_dir(self.spec, self.bucket_name(), self.node_name()) + "/data-*.cbb")
            if not g:
                #check 3.0 file structure
                rv, file_list = BFDSource.list_files(self.opts,
                                                     self.spec,
                                                     self.bucket_name(),
                                                     self.node_name(),
                                                     "data-*.cbb")
                if rv != 0:
                    return rv, None
                from_date = getattr(self.opts, "from-date", None)
                if from_date:
                    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")

                to_date = getattr(self.opts, "to-date", None)
                if to_date:
                    to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")
                g = []
                for f in file_list:
                    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(f))
                    if (not from_date or mtime >= from_date) and (not to_date or mtime <= to_date):
                        g.append(f)
            self.files = sorted(g)
        try:
            ver = 0
            while (not self.done and
                   batch.size() < batch_max_size and
                   batch.bytes < batch_max_bytes):
                if self.cursor_db is None:
                    if not self.files:
                        self.done = True
                        return 0, batch

                    rv, db, ver = connect_db(self.files[0], self.opts, CBB_VERSION)
                    if rv != 0:
                        return rv, None
                    self.files = self.files[1:]

                    cursor = db.cursor()
                    cursor.execute(s[ver])

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
                    if ver == 1:
                        msg = msg + (row[8], row[9], row[10])
                    else:
                        msg = msg + (0, 0, 0)
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
        file_list = glob.glob(BFD.db_dir(
                                 source_map['spec'],
                                 source_bucket['name'],
                                 source_node['hostname']) + "/data-*.cbb")
        if not file_list:
            #check 3.0 directory structure
            rv, file_list = BFDSource.list_files(opts,
                                        source_map['spec'],
                                        source_bucket['name'],
                                        source_node['hostname'],
                                        "data-*.cbb")
            if rv != 0:
                return rv, None

        for x in sorted(file_list):
            rv, db, ver = connect_db(x, opts, CBB_VERSION)
            if rv != 0:
                return rv, None

            cur = db.cursor()
            # TODO: (1) BFDSource - COUNT(*) is not indexed.
            cur.execute("SELECT COUNT(*) FROM cbb_msg;")
            t = t + cur.fetchone()[0]
            cur.close()
            db.close()

        return 0, t

    @staticmethod
    def list_files(opts, spec, bucket, node, pattern):
        file_list = []
        prec_list = []
        path, dirs = BFD.find_latest_dir(spec, None)
        if not path:
            return "error: No valid data in path:" % spec, None
        path, dirs = BFD.find_latest_dir(path, None)
        if not path:
            return 0, file_list
        latest_dir = BFD.construct_dir(path, bucket, node)
        file_list.extend(glob.glob(os.path.join(latest_dir, pattern)))
        for p in BFD.get_precessors(latest_dir):
            prec_list.append(os.path.dirname(p))
        while len(prec_list) > 0:
            deps = glob.glob(os.path.join(prec_list[0], pattern))
            for d in glob.glob(os.path.join(prec_list[0], pattern)):
                if d not in file_list:
                    file_list.append(d)
            for p in BFD.get_precessors(prec_list[0]):
                dirname = os.path.dirname(p)
                if dirname not in prec_list:
                    prec_list.append(dirname)
            prec_list = prec_list[1:]

        return 0, file_list

# --------------------------------------------------

class BFDSink(BFD, pump.Sink):
    """Can write to backup-file/directory layout."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(BFDSink, self).__init__(opts, spec, source_bucket, source_node,
                                      source_map, sink_map, ctl, cur)
        self.mode = "full"
        self.init_worker(BFDSink.run)

    @staticmethod
    def run(self):
        """Worker thread to asynchronously store incoming batches into db."""
        s = "INSERT INTO cbb_msg (cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, \
            dtype, meta_size) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        db = None
        cbb = 0       # Current cbb file NUM, like data-NUM.cbb.
        cbb_bytes = 0 # Current cbb msg value bytes total.
        cbb_max_bytes = \
            self.opts.extra.get("cbb_max_mb", 100000) * 1024 * 1024
        seqno, dep = BFD.find_seqno(self.opts,
                                    self.spec,
                                    self.source_bucket,
                                    self.source_node,
                                    self.mode)
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
                rv, db, meta_file = self.create_db(cbb)
                if rv != 0:
                    return self.future_done(future, rv)
                json_file = open(meta_file, "w")
                json.dump({'pred': dep}, json_file, ensure_ascii=False)
                json_file.close()
            try:
                c = db.cursor()

                for msg in batch.msgs:
                    cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, nmeta = msg
                    if self.skip(key, vbucket_id):
                        continue
                    if cmd not in [couchbaseConstants.CMD_TAP_MUTATION,
                                   couchbaseConstants.CMD_TAP_DELETE,
                                   couchbaseConstants.CMD_UPR_MUTATION,
                                   couchbaseConstants.CMD_UPR_DELETE]:
                        return self.future_done(future,
                                                "error: BFDSink bad cmd: " +
                                                str(cmd))
                    c.execute(s, (cmd, vbucket_id,
                                  sqlite3.Binary(key),
                                  flg, exp, str(cas),
                                  sqlite3.Binary(str(meta)),
                                  sqlite3.Binary(val),
                                  seqno,
                                  dtype,
                                  nmeta))
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
        rv, dir = self.mkdirs()
        if rv != 0:
            return rv, None, None

        path = dir + "/data-%s.cbb" % (string.rjust(str(num), 4, '0'))
        rv, db = create_db(path, self.opts)
        if rv != 0:
            return rv, None, None

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
            return "error: create_db error: " + str(e), None, None
        except Exception, e:
            return "error: create_db exception: " + str(e), None, None

        return 0, db, dir + "/meta.json"

    def mkdirs(self):
        """Make directories, if not already, with structure like...
           <spec>/
             YYYY-MM-DDThhmmssZ/
                YYYY-MM-DDThhmmssZ-full /
                   bucket-<BUCKETNAME>/
                     design.json
                     node-<NODE>/
                       data-<XXXX>.cbb
                YYYY-MM-DDThhmmssZ-diff/
                   bucket-<BUCKETNAME>/
                     design.json
                     node-<NODE>/
                       data-<XXXX>.cbb
                   """
        """CBSE-1052: There appears to be a race condition in os.mkdir. Suppose
           more than two threads simultaneously try to create the same directory
           or different directories with a common non-existent ancestor. Both check
           the directory doesn't exists, then both invoke os.mkdir. One of these
           will throw OSError due to underlying EEXIST system error."""

        spec = os.path.normpath(self.spec)
        if not os.path.isdir(spec):
            try:
                os.mkdir(spec)
            except OSError, e:
                if not os.path.isdir(spec):
                    return "error: could not mkdir: %s; exception: %s" % (spec, e)

        d = BFD.db_dir(self.spec,
                       self.bucket_name(),
                       self.node_name(),
                       getattr(self.opts, "mode", "diff"))
        if not os.path.isdir(d):
            try:
                os.makedirs(d)
            except OSError, e:
                if not os.path.isdir(d):
                    return "error: could not mkdirs: %s; exception: %s" % (d, e), None
        return 0, d


# --------------------------------------------------

def create_db(db_path, opts):
    try:
        logging.debug("  create_db: " + db_path)

        rv, db, ver = connect_db(db_path, opts, [0])
        if rv != 0:
            logging.debug("fail to call connect_db:" + db_path)
            return rv, None

        # The cas column is type text, not integer, because sqlite
        # integer is 63-bits instead of 64-bits.
        db.executescript("""
                  BEGIN;
                  CREATE TABLE cbb_msg
                     (cmd integer,
                      vbucket_id integer,
                      key blob,
                      flg integer,
                      exp integer,
                      cas text,
                      meta blob,
                      val blob,
                      seqno integer,
                      dtype integer,
                      meta_size integer);
                  CREATE TABLE cbb_meta
                     (key text,
                      val blob);
                  pragma user_version=%s;
                  COMMIT;
                """ % (CBB_VERSION[1]))

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
        if cur not in version:
            logging.debug("dbpath is not empty: " + db_path)
            return "error: unexpected db user version: " + \
                str(cur) + " vs " + str(version), \
                None, None

        return 0, db, version.index(cur)

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

def recursive_glob(rootdir='.', pattern='*'):
    return [os.path.join(rootdir, filename)
            for rootdir, dirnames, filenames in os.walk(rootdir)
            for filename in filenames
            if fnmatch.fnmatch(filename, pattern)]

def local_to_utc(t):
    secs = time.mktime(t.timetuple())
    return datetime.datetime.utcfromtimestamp(secs)
