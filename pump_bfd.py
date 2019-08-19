#!/usr/bin/env python3

import copy
import errno
import glob
import logging
import os
import json
import string
import sys
import datetime
import time
import urllib.request, urllib.parse, urllib.error
import fnmatch
import sqlite3
from typing import List, Dict, Optional, Any, Tuple, Union

import couchbaseConstants
import pump

CBB_VERSION = [2004, 2014, 2015] # sqlite pragma user version.

DDOC_FILE_NAME = "design.json"
INDEX_FILE_NAME = "index.json"
FTS_FILE_NAME = "fts_index.json"
FTS_ALIAS_FILE_NAME = "fts_alias.json"


class BFD:
    """Mixin for backup-file/directory EndPoint helper methods."""
    NUM_VBUCKET = 1024

    def bucket_name(self) -> str:
        return self.source_bucket['name']  # type: ignore # pylint: disable=no-member

    def node_name(self) -> str:
        return self.source_node['hostname']  # type: ignore # pylint: disable=no-member

    @staticmethod
    def get_file_path(spec: str, bucket_name: str, file: str) -> str:
        bucket_name = urllib.parse.quote_plus(bucket_name)
        bucket_path = os.path.join(os.path.normpath(spec), "bucket-" + bucket_name)
        if os.path.isdir(bucket_path):
            return os.path.join(bucket_path, file)
        else:
            path, dirs = BFD.find_latest_dir(spec, None)
            if path:
                path, dirs = BFD.find_latest_dir(path, None)
                if path:
                    return os.path.join(path, "bucket-" + bucket_name, file)
        return os.path.join(bucket_path, file)

    @staticmethod
    def construct_dir(parent: str, bucket_name: str, node_name: str) -> str:
        return os.path.join(parent,
                            "bucket-" + urllib.parse.quote_plus(bucket_name),
                            "node-" + urllib.parse.quote_plus(node_name))

    @staticmethod
    def check_full_dbfiles(parent_dir: str) -> List[str]:
        return glob.glob(os.path.join(parent_dir, "data-*.cbb"))

    @staticmethod
    def get_predecessors(parent_dir: str):
        try:
            json_file = open(os.path.join(parent_dir, "meta.json"), "r")
            json_data = json.load(json_file)
            json_file.close()
            return json_data["pred"]
        except IOError:
            return []

    @staticmethod
    def get_server_version(parent_dir: str) -> str:
        try:
            json_file = open(os.path.join(parent_dir, "meta.json"), "r")
            json_data = json.load(json_file)
            json_file.close()
            if "version" in json_data:
                return json_data["version"]
            return "0.0.0"
        except IOError:
            return "0.0.0"

    @staticmethod
    def get_failover_log(parent_dir: str) -> Dict[str, Any]:
        try:
            filepath = os.path.join(parent_dir, "failover.json")
            if os.path.isfile(filepath):
                json_file = open(filepath, "r")
                json_data = json.load(json_file)
                json_file.close()
                return json_data
            else:
                return {}
        except IOError:
            return {}

    @staticmethod
    def list_files(opts, spec, bucket, node, pattern):
        file_list = []
        prec_list = []

    @staticmethod
    def write_json_file(parent_dir, filename, output_data):
        filepath = os.path.join(parent_dir, filename)
        json_data = {}
        if os.path.isfile(filepath):
            # load into existed meta data generated from previous batch run
            try:
                json_file = open(filepath, "r")
                json_data = json.load(json_file)
                json_file.close()
            except IOError:
                pass

        for i in range(BFD.NUM_VBUCKET):
            if output_data.get(i):
                str_index = str(i)
                historic_data_exist = json_data.get(str_index)
                if historic_data_exist:
                    # Historic data will share same data type as incoming ones.
                    # It will be sufficient to only check type for historic data
                    if isinstance(json_data[str_index], int):
                        # For seqno, we want to keep the highest seqno
                        if json_data[str_index] < output_data[i]:
                            json_data[str_index] = output_data[i]
                    elif isinstance(json_data[str_index], list):
                        # For each vbucket, we want to get the superset of its references
                        if len(json_data[str_index]) <= len(output_data[i]):
                            json_data[str_index] = output_data[i]
                else:
                    # Bookkeeping the incoming one.
                    json_data[str_index] = output_data[i]

        json_file = open(filepath, "w")
        json.dump(json_data, json_file, ensure_ascii=False)
        json_file.close()

    @staticmethod
    def db_dir(spec: str, bucket_name: str, node_name: str, tmstamp: Optional[str] = None, mode: Optional[str] = None,
               new_session: bool = False) -> str:
        parent_dir = f'{os.path.normpath(spec)}/bucket-{urllib.parse.quote_plus(bucket_name)}/node-' \
            f'{urllib.parse.quote_plus(node_name)}'
        if os.path.isdir(parent_dir):
            return parent_dir

        # check 3.0 directory structure
        if not tmstamp:
            tmstamp = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime())
        parent_dir = os.path.normpath(spec)
        rootpath, dirs = BFD.find_latest_dir(parent_dir, None)
        if not rootpath or not mode or mode == "full":
            # no any backup roots exists
            path = os.path.join(parent_dir, tmstamp, tmstamp+"-full")
            return BFD.construct_dir(path, bucket_name, node_name)

        # check if any full backup exists
        path, dirs = BFD.find_latest_dir(rootpath, "full")  # type: ignore
        if not path:
            path = os.path.join(rootpath, tmstamp+"-full")
            return BFD.construct_dir(path, bucket_name, node_name)
        else:
            # further check full backup for this bucket and node
            path = BFD.construct_dir(path, bucket_name, node_name)
            if not os.path.isdir(path):
                return path

        if mode.find("diff") >= 0:
            path, dirs = BFD.find_latest_dir(rootpath, "diff")  # type: ignore
            if not path or new_session:
                path = os.path.join(rootpath, tmstamp+"-diff")
                return BFD.construct_dir(path, bucket_name, node_name)
            else:
                path = BFD.construct_dir(path, bucket_name, node_name)
                if not os.path.isdir(path):
                    return path
                else:
                    path = os.path.join(rootpath, tmstamp+"-diff")
                    return BFD.construct_dir(path, bucket_name, node_name)
        elif mode.find("accu") >= 0:
            path, dirs = BFD.find_latest_dir(rootpath, "accu")  # type: ignore
            if not path or new_session:
                path = os.path.join(rootpath, tmstamp+"-accu")
                return BFD.construct_dir(path, bucket_name, node_name)
            else:
                path = BFD.construct_dir(path, bucket_name, node_name)
                if not os.path.isdir(path):
                    return path
                else:
                    path = os.path.join(rootpath, tmstamp+"-accu")
                    return BFD.construct_dir(path, bucket_name, node_name)
        else:
            return parent_dir

    @staticmethod
    def find_latest_dir(parent_dir: str, mode: Union[str, None]) -> Tuple[Optional[str], List[str]]:
        all_subdirs: List[str] = []
        latest_dir: Optional[str] = None
        if not os.path.isdir(parent_dir):
            return latest_dir, all_subdirs
        for d in os.listdir(parent_dir):
            if not mode or d.find(mode) >= 0:
                bd = os.path.join(parent_dir, d)
                if os.path.isdir(bd):
                    all_subdirs.append(bd)
        if all_subdirs:
            all_subdirs = sorted(all_subdirs,key=os.path.getmtime, reverse=True)
            latest_dir = all_subdirs[0]
        return latest_dir, all_subdirs

    @staticmethod
    def find_seqno(opts: Any, spec: str, bucket_name: str, node_name: str, mode: str) -> Tuple[Dict[Any, Any],
                                                                                               List[Any],
                                                                                               Dict[Any, Any],
                                                                                               Dict[Any, Any]]:
        seqno: Dict[Any, Any] = {}
        dep: Dict[Any, Any] = {}
        dep_list: List[Any] = []
        failover_log: Dict[Any, Any] = {}
        snapshot_markers: Dict[Any, Any] = {}
        for i in range(BFD.NUM_VBUCKET):
            seqno[str(i)] = 0
            dep[i] = None
            failover_log[i] = None
            snapshot_markers[i] = None

        file_list: List[Any] = []
        failoverlog_list: List[Any] = []
        snapshot_list: List[Any] = []
        seqno_list: List[Any] = []
        parent_dir: str = os.path.normpath(spec)

        if mode == "full":
            return seqno, dep_list, failover_log, snapshot_markers
        timedir,latest_dirs = BFD.find_latest_dir(parent_dir, None)
        if not timedir:
            return seqno, dep_list, failover_log, snapshot_markers
        fulldir, latest_dirs = BFD.find_latest_dir(timedir, "full")
        if not fulldir:
            return seqno, dep_list, failover_log, snapshot_markers

        path = BFD.construct_dir(fulldir, bucket_name, node_name)
        if not os.path.isdir(path):
            return seqno, dep_list, failover_log, snapshot_markers

        last_backup = BFD.construct_dir(fulldir, bucket_name, node_name)
        file_list.extend(recursive_glob(path, 'data-*.cbb'))
        failoverlog_list.extend(recursive_glob(path, 'failover.json'))
        snapshot_list.extend(recursive_glob(path, 'snapshot_markers.json'))
        seqno_list.extend(recursive_glob(path, 'seqno.json'))

        if mode.find("accu") < 0:
            accudir, accu_dirs = BFD.find_latest_dir(timedir, "accu")
            if accudir:
                path = BFD.construct_dir(accudir, bucket_name, node_name)
                last_backup = path
                if os.path.isdir(path):
                    file_list.extend(recursive_glob(path, 'data-*.cbb'))
                    failoverlog_list.extend(recursive_glob(path, 'failover.json'))
                    snapshot_list.extend(recursive_glob(path, 'snapshot_markers.json'))
                    seqno_list.extend(recursive_glob(path, 'seqno.json'))
        if mode.find("diff") >= 0:
            diffdir, diff_dirs = BFD.find_latest_dir(timedir, "diff")
            if diff_dirs:
                last_backup = BFD.construct_dir(diffdir, bucket_name, node_name) # type: ignore
                for dir in diff_dirs:
                    path = BFD.construct_dir(dir, bucket_name, node_name)
                    if os.path.isdir(path):
                        file_list.extend(recursive_glob(path, 'data-*.cbb'))
                        failoverlog_list.extend(recursive_glob(path, 'failover.json'))
                        snapshot_list.extend(recursive_glob(path, 'snapshot_markers.json'))
                        seqno_list.extend(recursive_glob(path, 'seqno.json'))

        if BFD.get_server_version(last_backup) < "5.0.0":
            return dict(), list(), dict(), dict()

        for x in sorted(seqno_list):
            try:
                json_file = open(x, "r")
                json_data = json.load(json_file)
                json_file.close()

                for vbid, seq in json_data.items():
                    if not seq:
                        continue
                    if seqno.get(vbid) < seq:
                        seqno[vbid] = seq
            except IOError:
                pass

        for log_file in sorted(failoverlog_list):
            try:
                json_file = open(log_file, "r")
                json_data = json.load(json_file)
                json_file.close()

                for vbid, flogs in json_data.items():
                    if not flogs:
                        continue
                    elif vbid not in failover_log:
                        failover_log[vbid] = flogs
                    else:
                        for logpair in flogs:
                            if not failover_log[vbid]:
                                failover_log[vbid] = [logpair]
                            elif logpair not in failover_log[vbid]:
                                failover_log[vbid].append(logpair)
            except IOError:
                pass

        for snapshot in sorted(snapshot_list):
            try:
                json_file = open(snapshot, "r")
                json_data = json.load(json_file)
                json_file.close()

                for vbid, markers in json_data.items():
                    snapshot_markers[vbid] = markers
            except IOError:
                pass

        for i in range(BFD.NUM_VBUCKET):
            if dep[i] and dep[i] not in dep_list:
                dep_list.append(dep[i])
        return seqno, dep_list, failover_log, snapshot_markers

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
    def can_handle(opts, spec: str) -> Union[bool, List[str]]:
        if os.path.isdir(spec):
            dirs = glob.glob(f'{spec}/bucket-*/node-*/data-*.cbb')
            if dirs:
                return True
            path, dirs = BFD.find_latest_dir(spec, None)
            if path:
                path, dirs = BFD.find_latest_dir(path, "full")
                if path:
                    return glob.glob(f'{path}*/bucket-*/node-*/data-*.cbb')
        return False

    @staticmethod
    def check(opts, spec: str) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[Dict[str, Any]]]:
        spec = os.path.normpath(spec)
        if not os.path.isdir(spec):
            return f'error: backup_dir is not a directory: {spec}', None

        buckets = []

        bucket_dirs = glob.glob(f'{spec}/bucket-*')
        if not bucket_dirs:
            # check 3.0 directory structure
            path, dirs = BFD.find_latest_dir(spec, None)
            if not path:
                return f'error: no backup directory found: {spec}', None
            latest_dir, dir = BFD.find_latest_dir(path, "full")
            if not latest_dir:
                return f'error: no valid backup directory found: {path}', None
            bucket_dirs = glob.glob(f'{latest_dir}/bucket-*')

        for bucket_dir in sorted(bucket_dirs):
            if not os.path.isdir(bucket_dir):
                return f'error: not a bucket directory: {bucket_dir}', None

            bucket_name = urllib.parse.unquote_plus(os.path.basename(bucket_dir)[len("bucket-"):].strip()).encode()
            if not bucket_name:
                return f'error: bucket_name too short: {bucket_dir}', None

            bucket = { 'name': bucket_name, 'nodes': [] }
            buckets.append(bucket)

            node_dirs = glob.glob(f'{bucket_dir}/node-*')
            for node_dir in sorted(node_dirs):
                if not os.path.isdir(node_dir):
                    return f'error: not a node directory: {node_dir}', None

                node_name = urllib.parse.unquote_plus(os.path.basename(node_dir)[len("node-"):].strip()).encode()
                if not node_name:
                    return f'error: node_name too short: {node_dir}', None

                bucket['nodes'].append({'hostname': node_name})  # type: ignore

        return 0, {'spec': spec, 'buckets': buckets}

    @staticmethod
    def provide_design(opts, source_spec: str, source_bucket: Dict[str, Any],
                       source_map: Any) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        return BFDSource.read_index_file(source_spec, source_bucket, DDOC_FILE_NAME)

    @staticmethod
    def provide_index(opts,  source_spec: str, source_bucket: Dict[str, Any],
                      source_map: Any) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        return BFDSource.read_index_file(source_spec, source_bucket, INDEX_FILE_NAME)

    @staticmethod
    def provide_fts_index(opts, source_spec: str, source_bucket: Dict[str, Any],
                          source_map: Any) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        return BFDSource.read_index_file(source_spec, source_bucket, FTS_FILE_NAME)

    @staticmethod
    def provide_fts_alias(opts, source_spec: str, source_bucket: Dict[str, Any],
                          source_map: Any) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        base_dir = source_spec
        path, dirs = BFD.find_latest_dir(source_spec, None)
        if path:
            path, dirs = BFD.find_latest_dir(path, None)
            if path:
                base_dir = path

        if os.path.exists(os.path.join(base_dir, FTS_ALIAS_FILE_NAME)):
            try:
                f = open(os.path.join(base_dir, FTS_ALIAS_FILE_NAME), 'r')
                d = f.read()
                f.close()
                return 0, d
            except IOError as e:
                return f'error: could not write {path}; exception: {e}', None

        return 0, None

    @staticmethod
    def read_index_file(source_spec: str, source_bucket: Dict[str, Any],
                        file_name: str) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[str]]:
        path = BFD.get_file_path(source_spec, source_bucket['name'], file_name)
        if os.path.exists(path):
            try:
                f = open(path, 'r')
                d = f.read()
                f.close()
                return 0, d
            except IOError as e:
                return f'error: could not read {path}; exception: {e}', None
        return 0, None

    def get_conflict_resolution_type(self) -> str:
        dir = BFD.construct_dir(self.source_map['spec'],
                                self.source_bucket['name'],
                                self.source_node['hostname'])
        metafile = os.path.join(dir, "meta.json")
        try:
            json_file = open(metafile, "r")
            json_data = json.load(json_file)
            json_file.close()
            if "conflict_resolution_type" in json_data:
                return json_data["conflict_resolution_type"]
            return "seqno"
        except IOError:
            return "seqno"

    def provide_batch(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch]]:
        if self.done:
            return 0, None

        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        s = ["SELECT cmd, vbucket_id, key, flg, exp, cas, meta, val FROM cbb_msg",
             "SELECT cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, meta_size FROM cbb_msg",
             "SELECT cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, meta_size, conf_res FROM cbb_msg"]

        if self.files is None:  # None != [], as self.files will shrink to [].
            g = glob.glob(BFD.db_dir(self.spec, self.bucket_name(), self.node_name()) + "/data-*.cbb")
            if not g:
                # check 3.0 file structure
                rv, file_list = BFDSource.list_files(self.opts,
                                                     self.spec,
                                                     self.bucket_name(),
                                                     self.node_name(),
                                                     "data-*.cbb")
                if rv != 0:
                    return rv, None
                from_date = getattr(self.opts, "from_date", None)
                if from_date:
                    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")

                to_date = getattr(self.opts, "to_date", None)
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

                    self.cursor_db = (cursor, db, ver)

                cursor, db, ver = self.cursor_db

                row = cursor.fetchone()
                if row:
                    vbucket_id = row[1]
                    key = row[2]
                    val = row[7]

                    if self.skip(key, vbucket_id):
                        continue

                    if ver == 2:
                        msg = (row[0], row[1], row[2], row[3], row[4],
                               int(row[5]),  # CAS as 64-bit integer not string.
                               row[6],  # revid as 64-bit integer too
                               row[7], row[8], row[9], row[10], row[11])
                    elif ver == 1:
                        msg = (row[0], row[1], row[2], row[3], row[4],
                               int(row[5]),  # CAS as 64-bit integer not string.
                               row[6],  # revid as 64-bit integer too
                               row[7], row[8], row[9], row[10], 0)
                    else:
                        msg = (row[0], row[1], row[2], row[3], row[4],
                               int(row[5]),  # CAS as 64-bit integer not string.
                               row[6],  # revid as 64-bit integer too
                               row[7], 0, 0, 0, 0)

                    batch.append(msg, len(val))
                else:
                    if self.cursor_db:
                        self.cursor_db[0].close()
                        self.cursor_db[1].close()
                    self.cursor_db = None

            return 0, batch

        except Exception as e:
            self.done = True
            if self.cursor_db:
                self.cursor_db[0].close()
                self.cursor_db[1].close()
            self.cursor_db = None

            return f'error: exception reading backup file: {e!s}', None

    @staticmethod
    def total_msgs(opts, source_bucket, source_node, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[int]]:
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
    def list_files(opts, spec: str, bucket: str, node: str, pattern: str) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                                   List[str]]:
        file_list: List[str] = []
        prec_list: List[str] = []
        path, dirs = BFD.find_latest_dir(spec, None)
        if not path:
            return f'error: No valid data in path: {spec}', file_list

        path, dirs = BFD.find_latest_dir(path, None)
        if not path:
            return 0, file_list

        for dir in dirs:
            latest_dir = BFD.construct_dir(dir, bucket, node)
            file_list.extend(glob.glob(os.path.join(latest_dir, pattern)))
            for p in BFD.get_predecessors(latest_dir):
                prec_list.append(os.path.dirname(p))
            if len(prec_list) > 0:
                break

        while len(prec_list) > 0:
            deps = glob.glob(os.path.join(prec_list[0], pattern))
            for d in glob.glob(os.path.join(prec_list[0], pattern)):
                if d not in file_list:
                    file_list.append(d)
            for p in BFD.get_predecessors(prec_list[0]):
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
            dtype, meta_size, conf_res) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        db = None
        cbb = 0        # Current cbb file NUM, like data-NUM.cbb.
        cbb_bytes = 0  # Current cbb msg value bytes total.
        db_dir = None
        cbb_max_bytes = self.opts.extra.get("cbb_max_mb", 100000) * 1024 * 1024
        _, dep, _, _ = BFD.find_seqno(self.opts, self.spec, self.bucket_name(), self.node_name(), self.mode)

        confResType = "seqno"
        if "conflictResolutionType" in self.source_bucket:
            confResType = self.source_bucket["conflictResolutionType"]

        version = "0.0.0"
        for bucket in self.source_map["buckets"]:
            if self.bucket_name() == bucket["name"]:
                for node in bucket["nodes"]:
                    if node["hostname"] == self.node_name():
                        if 'version' in node:
                            version = node["version"].split("-")[0]

        seqno_map = {}
        for i in range(BFD.NUM_VBUCKET):
            seqno_map[i] = 0
        while not self.ctl['stop']:
            if db and cbb_bytes >= cbb_max_bytes:
                db.close()
                db = None
                cbb += 1
                cbb_bytes = 0
                db_dir = None

            batch, future = self.pull_next_batch()  # type: pump.Batch, pump.SinkBatchFuture
            if not db:
                rv, db, db_dir = self.create_db(cbb)
                if rv != 0:
                    return self.future_done(future, rv)

                meta_file = os.path.join(db_dir, "meta.json")
                json_file = open(meta_file, "w")
                toWrite = {'pred': dep,
                           'conflict_resolution_type': confResType,
                           'version': version}
                json.dump(toWrite, json_file, ensure_ascii=False)
                json_file.close()

            if not batch:
                if db:
                    db.close()
                return self.future_done(future, 0)

            if (self.bucket_name(), self.node_name()) in self.cur['failoverlog']:
                BFD.write_json_file(db_dir,
                                    "failover.json",
                                    self.cur['failoverlog'][(self.bucket_name(), self.node_name())])
            if (self.bucket_name(), self.node_name()) in self.cur['snapshot']:
                BFD.write_json_file(db_dir,
                                    "snapshot_markers.json",
                                    self.cur['snapshot'][(self.bucket_name(), self.node_name())])
            try:
                c = db.cursor()

                for msg in batch.msgs:
                    cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, nmeta, conf_res = msg
                    if self.skip(key, vbucket_id):
                        continue
                    if cmd not in [couchbaseConstants.CMD_TAP_MUTATION,
                                   couchbaseConstants.CMD_TAP_DELETE,
                                   couchbaseConstants.CMD_DCP_MUTATION,
                                   couchbaseConstants.CMD_DCP_DELETE]:
                        if db:
                            db.close()
                        return self.future_done(future, f'error: BFDSink bad cmd: {cmd!s}')
                    keyb = key
                    metab = meta
                    valb = val
                    c.execute(s, (cmd, vbucket_id,
                                  sqlite3.Binary(keyb),
                                  flg, exp, str(cas),
                                  sqlite3.Binary(metab),
                                  sqlite3.Binary(valb),
                                  seqno,
                                  dtype,
                                  nmeta,
                                  conf_res))
                    cbb_bytes += len(val)
                    if seqno_map[vbucket_id] < seqno:
                        seqno_map[vbucket_id] = seqno
                db.commit()
                BFD.write_json_file(db_dir, "seqno.json", seqno_map)
                self.future_done(future, 0) # No return to keep looping.

            except sqlite3.Error as e:
                return self.future_done(future, f'error: db error: {e!s}')
            except Exception as e:
                return self.future_done(future, f'error: db exception: {e!s}')

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        spec = os.path.normpath(spec)
        return (os.path.isdir(spec) or (not os.path.exists(spec) and
                                        os.path.isdir(os.path.dirname(spec))))

    @staticmethod
    def check(opts, spec, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR, Any]:
        # TODO: (2) BFDSink - check disk space.
        # TODO: (2) BFDSink - check should report on pre-creatable directories.

        spec = os.path.normpath(spec)

        # Check that directory's empty.
        if os.path.exists(spec):
            if not os.path.isdir(spec):
                return f'error: backup directory is not a directory: {spec}', None
            if not os.access(spec, os.W_OK):
                return f'error: backup directory is not writable: {spec}', None
            return 0, None

        # Or, that the parent directory exists.
        parent_dir = os.path.dirname(spec)

        if not os.path.exists(parent_dir):
            return f'error: missing parent directory: {parent_dir}', None
        if not os.path.isdir(parent_dir):
            return f'error: parent directory is not a directory: {parent_dir}', None
        if not os.access(parent_dir, os.W_OK):
            return f'error: parent directory is not writable: {parent_dir}', None
        return 0, None

    @staticmethod
    def consume_design(opts, sink_spec, sink_map, source_bucket, source_map, index_defs):
        return BFDSink.write_index_file(sink_spec, source_bucket, index_defs, DDOC_FILE_NAME)

    @staticmethod
    def consume_index(opts, sink_spec, sink_map, source_bucket, source_map, index_defs):
        return BFDSink.write_index_file(sink_spec, source_bucket, index_defs, INDEX_FILE_NAME)

    @staticmethod
    def consume_fts_index(opts, sink_spec, sink_map, source_bucket, source_map, index_defs):
        return BFDSink.write_index_file(sink_spec, source_bucket, index_defs, FTS_FILE_NAME)

    @staticmethod
    def consume_fts_alias(opts, sink_spec, sink_map, source_bucket, source_map, alias):
        base_dir = sink_spec
        path, dirs = BFD.find_latest_dir(sink_spec, None)
        if path:
            path, dirs = BFD.find_latest_dir(path, None)
            if path:
                base_dir = path

        try:
            f = open(os.path.join(base_dir, FTS_ALIAS_FILE_NAME), 'w')
            f.write(alias)
            f.close()
        except IOError as e:
            return f'error: could not write {path}; exception: {e}', None

        return 0, None

    @staticmethod
    def write_index_file(sink_spec: str, source_bucket: Dict[str, Any], index_defs: str, file_name: str):
        if index_defs:
            path = BFD.get_file_path(sink_spec, source_bucket['name'], file_name)
            try:
                rv = pump.mkdirs(path)
                if rv:
                    return rv, None
                f = open(path, 'w')
                f.write(index_defs)
                f.close()
            except IOError as e:
                return f'error: could not write {path}; exception: {e}', None
        return 0

    def consume_batch_async(self, batch):
        return self.push_next_batch(batch, pump.SinkBatchFuture(self, batch))

    def create_db(self, num):
        rv, dir = self.mkdirs()
        if rv != 0:
            return rv, None, None

        path = f'{dir}/data-{num!s:0>4}.cbb'
        rv, db = create_db(path, self.opts)
        if rv != 0:
            return rv, None, None

        return 0, db, dir

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
            except OSError as error:
                if error.errno != errno.EEXIST:
                    return f'error: could not mkdir: {spec}; exception: {error.strerror}'

        new_session = self.ctl['new_session']
        self.ctl['new_session'] = False
        d = BFD.db_dir(self.spec,
                       self.bucket_name(),
                       self.node_name(),
                       self.ctl['new_timestamp'],
                       getattr(self.opts, "mode", "diff"),
                       new_session)
        if not os.path.isdir(d):
            try:
                os.makedirs(d)
            except OSError as e:
                if not os.path.isdir(d):
                    return f'error: could not mkdirs: {d}; exception: {e}', None
        return 0, d


# --------------------------------------------------

def create_db(db_path, opts):
    try:
        logging.debug(f'  create_db: {db_path}')

        rv, db, ver = connect_db(db_path, opts, [0])
        if rv != 0:
            logging.debug(f'fail to call connect_db: {db_path}')
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
                      meta_size integer,
                      conf_res integer);
                  pragma user_version=%s;
                  COMMIT;
                """ % (CBB_VERSION[2]))

        return 0, db

    except Exception as e:
        return f'error: create_db exception: {e!s}', None


def connect_db(db_path, opts, version):
    try:
        # TODO: (1) BFD - connect_db - use pragma page_size.
        # TODO: (1) BFD - connect_db - use pragma max_page_count.
        # TODO: (1) BFD - connect_db - use pragma journal_mode.

        logging.debug(f'  connect_db: {db_path}')

        db = sqlite3.connect(db_path)
        db.text_factory = str

        cur = db.execute("pragma user_version").fetchall()[0][0]
        if cur not in version:
            logging.debug(f'db_path is not empty: {db_path}')
            return f'error: unexpected db user version: {cur!s} vs {version!s}, maybe a backup directory created by' \
                       f' older releases is reused', None, None

        return 0, db, version.index(cur)

    except Exception as e:
        return f'error: connect_db exception: {e!s}', None, None


def cleanse(d):
    """Elide passwords from hierarchy of dict/list's."""
    return cleanse_helper(copy.deepcopy(d))


def cleanse_helper(d):
    """Recursively, destructively elide passwords from hierarchy of dict/list's."""
    if type(d) is list:
        for x in d:
            cleanse_helper(x)
    elif type(d) is dict:
        for k, v in d.items():
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
