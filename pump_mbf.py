#!/usr/bin/env python

import collections
import glob
import logging
import os
import re
import sqlite3
import subprocess
import sys

import memcacheConstants

from pump import EndPoint, Source, Batch

MBF_VERSION = 2 # sqlite pragma user version for Couchbase 1.8.

class MBFSource(Source):
    """Can read 1.8 server master and *.mb data files."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(MBFSource, self).__init__(opts, spec, source_bucket, source_node,
                                        source_map, sink_map, ctl, cur)
        self.cursor_todo = None
        self.cursor_done = False

        self.s = """SELECT vbid, k, flags, exptime, cas, v
                      FROM `{{0}}`.`{{1}}` as kv,
                           `{0}`.vbucket_states as vb
                     WHERE kv.vbucket = vb.vbid
                       AND kv.vb_version = kv.vb_version
                       AND vb.state like '{1}'"""

    @staticmethod
    def can_handle(opts, spec):
        return os.path.isfile(spec) and MBFSource.version(spec) == 2

    @staticmethod
    def check_base(opts, spec):
        # Skip immediate superclass Source.check_base(),
        # since MBFSource can handle different vbucket states.
        return EndPoint.check_base(opts, spec)

    @staticmethod
    def check(opts, spec):
        spec = os.path.normpath(spec)
        if not os.path.isfile(spec):
            return "error: backup_dir is not a file: " + spec, None

        db_files = MBFSource.db_files(spec)
        versions = MBFSource.db_file_versions(db_files)
        logging.debug(" MBFSource check db file versions: %s" % (versions))
        if max(versions.values()) < 2:
            err = ("error: wrong backup/db file versions;\n" +
                   " either the metadata db file is not specified\n" +
                   " or the backup files need upgrading to version %s;\n" +
                   " please use cbdbupgrade or contact support.") \
                   % (MBF_VERSION)
            return err, None

        # Map of state string (e.g., 'active') to map of vbucket_id to info.
        vbucket_states = collections.defaultdict(dict)
        sql = """SELECT vbid, vb_version, state, checkpoint_id
                   FROM vbucket_states"""
        for db_file in [f for f in db_files if f.endswith(".mb")]:
            try:
                db = sqlite3.connect(db_file)
                cur = db.cursor()
                for row in cur.execute(sql):
                    vbucket_id = row[0]
                    state = row[2]
                    vbucket_states[state][vbucket_id] = {
                        'vbucket_id': vbucket_id,
                        'vb_version': row[1],
                        'state': state,
                        'checkpoint_id': row[3]
                        }
            except sqlite3.DatabaseError, e:
                pass # A missing vbucket_states table is expected.
            finally:
                cur.close()
                db.close()

        return 0, {'spec': spec,
                   'buckets':
                       [{'name': os.path.basename(spec),
                         'nodes': [{'hostname': 'N/A',
                                    'vbucket_states': vbucket_states
                                    }]}]}

    @staticmethod
    def db_file_versions(db_files):
        rv = {}
        for db_file in db_files:
            rv[db_file] = MBFSource.version(db_file)
        return rv

    @staticmethod
    def version(db_file):
        try:
            return int(MBFSource.run_sql(db_file, "PRAGMA user_version;")[0])
        except sqlite3.DatabaseError, e:
            logging.error("error: could not access user_version from: %s" +
                          "; exception: %s" +
                          "; perhaps it is being used by another program" +
                          " like couchbase-server", db_file, e)
            return 0

    @staticmethod
    def db_files(spec):
        return [spec] + glob.glob(spec + "-*.mb")

    @staticmethod
    def run_sql(db_file, sql):
        db = sqlite3.connect(db_file)
        cur = db.cursor()
        cur.execute(sql)
        rv = cur.fetchone()
        cur.close()
        db.close()
        return rv

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        return 0, None

    def provide_batch(self):
        if self.cursor_done:
            return 0, None

        batch = Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        source_vbucket_state = \
            getattr(self.opts, 'source_vbucket_state', 'active')

        try:
            if self.cursor_todo is None:
                rv, db, attached_dbs, table_dbs = self.connect_db()
                if rv != 0:
                    return rv, None

                # Determine which db the state table is in.
                try:
                    (state_db,) = table_dbs[u'vbucket_states']
                except ValueError:
                    db.close()
                    return "error: no unique vbucket_states table", None

                sql = self.s.format(state_db, source_vbucket_state)

                kv_names = []
                for kv_name, db_name in table_dbs.iteritems():
                    if (self.opts.id is None and
                        not kv_name.startswith('kv_')):
                        continue
                    if (self.opts.id is not None and
                        kv_name != "kv_{0}".format(self.opts.id)):
                        continue
                    kv_names.append(kv_name)

                db_kv_names = []
                for kv_name in sorted(kv_names,
                                      key=lambda x: int(x.split('_')[-1])):
                    for db_name in sorted(table_dbs[kv_name]):
                        db_kv_names.append((db_name, kv_name))

                self.cursor_todo = (db, sql, db_kv_names, None)

            db, sql, db_kv_names, cursor = self.cursor_todo
            if not db:
                self.cursor_done = True
                self.cursor_todo = None
                return 0, None

            while (not self.cursor_done and
                   batch.size() < batch_max_size and
                   batch.bytes < batch_max_bytes):
                if not cursor:
                    if not db_kv_names:
                        self.cursor_done = True
                        self.cursor_todo = None
                        db.close()
                        break

                    db_name, kv_name = db_kv_names.pop()
                    logging.debug("  MBFSource db/kv table: %s/%s" %
                                  (db_name, kv_name))

                    cursor = db.cursor()
                    cursor.execute(sql.format(db_name, kv_name))

                    self.cursor_todo = (db, sql, db_kv_names, cursor)

                row = cursor.fetchone()
                if row:
                    vbucket_id = row[0]
                    key = row[1]
                    flg = row[2]
                    exp = row[3]
                    cas = row[4]
                    val = row[5]

                    if self.skip(key, vbucket_id):
                        continue

                    meta = ''

                    batch.append((memcacheConstants.CMD_TAP_MUTATION,
                                  vbucket_id, key, flg, exp, cas, meta, val), len(val))
                else:
                    cursor.close()
                    self.cursor_todo = (db, sql, db_kv_names, None)
                    break # Close the batch; next pass hits new db_name/kv_name.

        except Exception, e:
            self.cursor_done = True
            self.cursor_todo = None
            return "error: MBFSource exception: " + str(e), None

        return 0, batch

    def connect_db(self):
        db = sqlite3.connect(':memory:')
        logging.debug("  MBFSource connect_db: %s" % self.spec)

        db_files = MBFSource.db_files(self.spec)
        logging.debug("  MBFSource db_files: %s" % db_files)

        attached_dbs = ["db{0}".format(i) for i in xrange(len(db_files))]
        db.executemany("attach ? as ?", zip(db_files, attached_dbs))

        # Find all tables, filling a table_name => db_name map.
        table_dbs = {}
        for db_name in attached_dbs:
            cursor = db.cursor()
            cursor.execute("SELECT name FROM %s.sqlite_master"
                           " WHERE type = 'table'" % db_name)
            for (table_name,) in cursor:
                table_dbs.setdefault(table_name, []).append(db_name)
            cursor.close()

        if not [True for table_name in table_dbs.keys()
                if 'kv_' in table_name]:
            db.close()
            return "error: no kv data was found;" + \
                " check if db files are correct", None, None, None

        logging.debug("  MBFSource total # tables: %s" % len(table_dbs))
        return 0, db, attached_dbs, table_dbs
