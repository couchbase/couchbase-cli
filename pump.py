#!/usr/bin/env python3

import os
import base64
import copy
import http.client
import logging
import re
import queue
import json
import string
import sys
import threading
import time
import urllib.request, urllib.parse, urllib.error
import urllib.parse
import zlib
import platform
import subprocess
import socket
import ssl
from typing import List, Dict, Optional, Any, Tuple, Union, Sequence

import couchbaseConstants
import cb_bin_client
from cb_util import tag_user_data
from cluster_manager import ClusterManager
from collections import defaultdict
import snappy # pylint: disable=import-error

# TODO: (1) optionally log into backup directory

LOGGING_FORMAT = '%(asctime)s: %(threadName)s %(message)s'

NA = 'N/A'


class Batch(object):
    """Holds a batch of data being transfered from source to sink."""

    def __init__(self, source):
        self.source = source
        self.msgs: List[couchbaseConstants.BATCH_MSG] = []
        self.bytes: int = 0
        self.adjust_size: int = 0

    def append(self, msg: couchbaseConstants.BATCH_MSG, num_bytes: int):
        self.msgs.append(msg)
        self.bytes = self.bytes + num_bytes

    def size(self) -> int:
        return len(self.msgs)

    def msg(self, i: int) -> couchbaseConstants.BATCH_MSG:
        return self.msgs[i]

    def group_by_vbucket_id(self, vbuckets_num, rehash=0) -> Dict[int, List[couchbaseConstants.BATCH_MSG]]:
        """Returns dict of vbucket_id->[msgs] grouped by msg's vbucket_id."""
        g: Dict[int, List[couchbaseConstants.BATCH_MSG]] = defaultdict(list)
        for msg in self.msgs:
            cmd, vbucket_id, key = msg[:3]
            if vbucket_id == 0x0000ffff or rehash == 1:
                if self.source.opts.collection:
                    # Collections embeds the ID into the key field, but does not
                    # hash the ID as part of VB hashing
                    key = cb_bin_client.skipCollectionID(key)

                if isinstance(key, str):
                    key = key.encode()
                # Special case when the source did not supply a vbucket_id
                # (such as stdin source), so we calculate it.
                vbucket_id = ((zlib.crc32(key) >> 16) & 0x7FFF) % vbuckets_num
                msg = (cmd, vbucket_id) + msg[2:]  # type: ignore
            g[vbucket_id].append(msg)
        return g


class SinkBatchFuture(object):
    """Future completion of a sink consuming a batch."""

    def __init__(self, sink, batch):
        self.sink = sink
        self.batch = batch
        self.done = threading.Event()
        self.done_rv = None

    def wait_until_consumed(self):
        self.done.wait()
        return self.done_rv


# --------------------------------------------------

class ProgressReporter(object):
    """Mixin to report progress"""

    def report_init(self):
        self.beg_time = time.time()
        self.prev_time = self.beg_time
        self.prev = defaultdict(int)

    def report(self, prefix: str = "", emit=None):
        if not emit:
            emit = logging.info

        if getattr(self, "source", None):
            emit(f'{prefix}source : {self.source}')  # type: ignore # pylint: disable=no-member
        if getattr(self, "sink", None):
            emit(f'{prefix}sink   : {self.sink}')  # type: ignore # pylint: disable=no-member

        cur_time = time.time()
        delta = cur_time - self.prev_time
        c, p = self.cur, self.prev  # type: ignore # pylint: disable=no-member
        x = sorted([k for k in c.keys() if "_sink_" in k])

        width_k = max([5] + [len(k.replace("tot_sink_", "")) for k in x])
        width_v = max([20] + [len(str(c[k])) for k in x])
        width_d = max([10] + [len(str(c[k] - p[k])) for k in x])
        width_s = max([10] + [len("%0.1f" % ((c[k] - p[k]) / delta)) for k in x])
        emit(f'{prefix} {"":<{width_k}} : {"total":>{width_v}} | {"last":>{width_d}} | {"per sec":>{width_s}}')
        verbose_set = ["tot_sink_batch", "tot_sink_msg"]
        for k in x:
            if k not in verbose_set or self.opts.verbose > 0:  # type: ignore # pylint: disable=no-member
                per_sec = f'{(c[k] - p[k]) / delta:0.1f}'
                emit(f'{prefix} {k.replace("tot_sink_", ""):<{width_k}} : {c[k]!s:>{width_v}} | '
                     f'{(c[k]-p[k])!s:>{width_d}} | {per_sec:>{width_s}}')
        self.prev_time = cur_time
        self.prev = copy.copy(c)

    def bar(self, current, total):
        if not total:
            return '.'
        if sys.platform.lower().startswith('win'):
            cr = "\r"
        else:
            cr = chr(27) + "[A\n"
        pct = float(current) / total
        max_hash = 20
        num_hash = int(round(pct * max_hash))
        return (f'  [{"#" * num_hash}{" " * (max_hash - num_hash)}] {100.0*pct:0.1f}% ({current}/estimated {total} '
                f'msgs){cr}')


class PumpingStation(ProgressReporter):
    """Queues and watchdogs multiple pumps across concurrent workers."""

    def __init__(self, opts, source_class, source_spec, sink_class, sink_spec):
        self.opts = opts
        self.source_class = source_class
        self.source_spec = source_spec
        self.sink_class = sink_class
        self.sink_spec = sink_spec
        self.queue = None
        tmstamp = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime())
        self.ctl = { 'stop': False,
                     'rv': 0,
                     'new_session': True,
                     'new_timestamp': tmstamp}
        self.cur = defaultdict(int)

    def run(self):
        # TODO: (6) PumpingStation - monitor source for topology changes.
        # TODO: (4) PumpingStation - retry on err N times, M times / server.
        # TODO: (2) PumpingStation - track checksum in backup, for later restore.

        rv, source_map, sink_map = self.check_endpoints()
        if rv != 0:
            return rv

        if self.opts.dry_run:
            sys.stderr.write("done, but no data written due to dry-run\n")
            return 0

        source_buckets = self.filter_source_buckets(source_map)
        if not source_buckets:
            bucket_source = getattr(self.opts, "bucket_source", None)
            if bucket_source:
                return f'error: there is no bucket: {bucket_source} at source: {self.source_spec}'
            else:
                return f'error: no transferable buckets at source: {self.source_spec}'

        for source_bucket in sorted(source_buckets,
                                    key=lambda b: b['name']):
            logging.info(f'bucket: {source_bucket["name"]}')

            if not self.opts.extra.get("design_doc_only", 0):
                rv = self.transfer_bucket_msgs(source_bucket, source_map, sink_map)
                if rv != 0:
                    return rv
            else:
                sys.stderr.write("transfer design doc only. bucket msgs will be skipped.\n")

            if not self.opts.extra.get("data_only", 0):
                rv = self.transfer_bucket_design(source_bucket, source_map, sink_map)
                if rv:
                    logging.warn(rv)
                rv = self.transfer_bucket_index(source_bucket, source_map, sink_map)
                if rv:
                    logging.warn(rv)
                rv = self.transfer_bucket_fts_index(source_bucket, source_map, sink_map)
                if rv:
                    logging.warn(rv)
                rv = self.transfer_fts_alias(source_bucket, source_map, sink_map)
                if rv:
                    logging.warn(rv)

            else:
                sys.stderr.write("transfer data only. bucket design docs and index meta will be skipped.\n")

            # TODO: (5) PumpingStation - validate bucket transfers.

        # TODO: (4) PumpingStation - validate source/sink maps were stable.

        sys.stderr.write("done\n")
        return 0

    def check_endpoints(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Dict[str, Any], Dict[str, Any]]:
        logging.debug(f'source_class: {self.source_class}')
        rv = self.source_class.check_base(self.opts, self.source_spec)
        if rv != 0:
            return rv, {}, {}
        rv, source_map = self.source_class.check(self.opts, self.source_spec)
        if rv != 0:
            return rv, {}, {}

        logging.debug(f'sink_class: {self.sink_class}')
        rv = self.sink_class.check_base(self.opts, self.sink_spec)
        if rv != 0:
            return rv, {}, {}
        rv, sink_map = self.sink_class.check(self.opts, self.sink_spec, source_map)
        if rv != 0:
            return rv, {}, {}

        return rv, source_map, sink_map

    def filter_source_buckets(self, source_map: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter the source_buckets if a bucket_source was specified."""
        source_buckets = source_map['buckets']
        logging.debug(f'source_buckets: {",".join([returnString(n["name"]) for n in source_buckets])}')

        bucket_source = getattr(self.opts, "bucket_source", None)
        bucket_source = returnString(bucket_source)
        if bucket_source:
            logging.debug(f'bucket_source: {bucket_source}')
            source_buckets = [b for b in source_buckets
                              if returnString(b['name']) == bucket_source]
            logging.debug(f'source_buckets filtered: {",".join([returnString(n["name"]) for n in source_buckets])}')
        return source_buckets

    def filter_source_nodes(self, source_bucket: Dict[str, Any], source_map):
        """Filter the source_bucket's nodes if single_node was specified."""
        if getattr(self.opts, "single_node", None):
            if not source_map.get('spec_parts'):
                return (f'error: no single_node from source: {self.source_spec}; the source may not support the'
                        f' --single-node flag')
            source_nodes = filter_bucket_nodes(source_bucket,
                                               source_map.get('spec_parts'))
        else:
            source_nodes = source_bucket['nodes']
        logging.debug(f' source_nodes: {",".join([returnString(n.get("hostname", NA)) for n in source_nodes])}')
        return source_nodes

    def transfer_bucket_msgs(self, source_bucket: Dict[str, Any], source_map, sink_map) -> couchbaseConstants.PUMP_ERROR:
        source_nodes = self.filter_source_nodes(source_bucket, source_map)
        # Transfer bucket msgs with a Pump per source server.
        self.start_workers(len(source_nodes))
        self.report_init()

        self.ctl['run_msg'] = 0
        self.ctl['tot_msg'] = 0

        # Checks to be done:
        #    1. If source is a CB server check if hostname given is default or external:
        #       this is done by comparing self.source_spec to source_node[hostname] or to the alternate address
        #        1.1 Mark that all nodes in the server have to be connected using (default/external) depending on 1
        #    2. Check if sink is a CB server and if hostname given is default or external:
        #       This is done by comparing the sink_spec to the sink map
        #        2.2 ark that all nodes in the server have to be connected using (default/external) depending on 2
        #    3. Pass this information through the queue to the worker

        # step 1:
        alt_add = {'source': False, 'sink': False}
        if self.source_spec.startswith('http://') or self.source_spec.startswith('https://'):
            host = self.source_spec.lstrip("https://")
            host = host.lstrip("http://")
            ix = host.rfind(":")
            if ix != -1:
                try:
                    _ = int(host[ix+1:])
                    host = host[:ix]
                except ValueError:
                    pass
            for sn in source_nodes:
                if 'alternateAddresses' in sn and sn['alternateAddresses']['external']['hostname'] == host:
                    alt_add['source'] = True
                    break
        # step 2:
        if self.sink_spec.startswith('http://') or self.sink_spec.startswith('https://'):
            host = self.sink_spec.lstrip("https://")
            host = host.lstrip("http://")
            ix = host.rfind(":")
            if ix != -1:
                try:
                    _ = int(host[ix + 1:])
                    host = host[:ix]
                except ValueError:
                    pass
            for n in sink_map['buckets'][0]['nodes']:
                if 'alternateAddresses' in n and host == n['alternateAddresses']['external']['hostname']:
                    alt_add['sink'] = True
                    break

        for source_node in sorted(source_nodes,
                                  key=lambda n: returnString(n.get('hostname', NA))):
            logging.debug(f' enqueueing node: {source_node.get("hostname", NA)}')
            self.queue.put((source_bucket, source_node, source_map, sink_map, alt_add))

            rv, tot = self.source_class.total_msgs(self.opts,
                                                   source_bucket,
                                                   source_node,
                                                   source_map)
            if rv != 0:
                return rv
            if tot:
                self.ctl['tot_msg'] += tot

        # Don't use queue.join() as it eats Ctrl-C's.
        s = 0.05
        while self.queue.unfinished_tasks:
            time.sleep(s)
            s = min(1.0, s + 0.01)

        rv = self.ctl['rv']
        if rv != 0:
            return rv

        time.sleep(0.01)  # Allows threads to update counters.

        sys.stderr.write(self.bar(self.ctl['run_msg'],
                                  self.ctl['tot_msg']) + "\n")
        sys.stderr.write(f"bucket: {source_bucket['name']}, msgs transferred...\n")

        def emit(msg):
            sys.stderr.write(f'{msg}\n')
        self.report(emit=emit)

        return 0

    def transfer_bucket_design(self, source_bucket, source_map, sink_map) -> couchbaseConstants.PUMP_ERROR:
        """Transfer bucket design (e.g., design docs, views)."""
        rv, source_design = \
            self.source_class.provide_design(self.opts, self.source_spec,
                                             source_bucket, source_map)
        if rv == 0:
            if source_design:
                sources = source_design if isinstance(source_design, list) else [source_design]
                for source_design in sources:
                    rv = self.sink_class.consume_design(self.opts,
                                                self.sink_spec, sink_map,
                                                source_bucket, source_map,
                                                source_design)
        return rv

    def transfer_bucket_index(self, source_bucket, source_map, sink_map) -> couchbaseConstants.PUMP_ERROR:
        """Transfer bucket index meta."""
        rv, source_design = \
            self.source_class.provide_index(self.opts, self.source_spec,
                                             source_bucket, source_map)
        if rv == 0:
            if source_design:
                rv = self.sink_class.consume_index(self.opts,
                                                self.sink_spec, sink_map,
                                                source_bucket, source_map,
                                                source_design)
        return rv

    def transfer_bucket_fts_index(self, source_bucket, source_map, sink_map) -> couchbaseConstants.PUMP_ERROR:
        """Transfer bucket index meta."""
        rv, source_design = \
            self.source_class.provide_fts_index(self.opts, self.source_spec,
                                                source_bucket, source_map)
        if rv == 0:
            if source_design:
                rv = self.sink_class.consume_fts_index(self.opts,
                                                   self.sink_spec, sink_map,
                                                   source_bucket, source_map,
                                                   source_design)
        return rv

    def transfer_fts_alias(self, source_bucket, source_map, sink_map) -> couchbaseConstants.PUMP_ERROR:
        """Transfer fts alias meta."""
        rv, alias = self.source_class.provide_fts_alias(self.opts, self.source_spec, source_bucket, source_map)
        if rv == 0:
            if alias:
                rv = self.sink_class.consume_fts_alias(self.opts, self.sink_spec, sink_map, source_bucket, source_map,
                                                       alias)
        return rv

    @staticmethod
    def run_worker(self, thread_index):
        while not self.ctl['stop']:
            source_bucket, source_node, source_map, sink_map, alt_add = \
                self.queue.get()
            hostname = source_node.get('hostname', NA)
            logging.debug(f' node: {hostname}')
            logging.debug(f' Use alternate addresses: {alt_add}')

            curx = defaultdict(int)
            self.source_class.check_spec(source_bucket,
                                         source_node,
                                         self.opts,
                                         self.source_spec,
                                         curx)
            self.sink_class.check_spec(source_bucket,
                                       source_node,
                                       self.opts,
                                       self.sink_spec,
                                       curx)

            source = self.source_class(self.opts, self.source_spec, source_bucket,
                                       source_node, source_map, sink_map, self.ctl,
                                       curx)
            setattr(source, 'alt_add', alt_add['source'])
            sink = self.sink_class(self.opts, self.sink_spec, source_bucket,
                                   source_node, source_map, sink_map, self.ctl,
                                   curx)
            setattr(sink, 'alt_add', alt_add['sink'])

            src_conf_res = source.get_conflict_resolution_type()
            snk_conf_res = sink.get_conflict_resolution_type()
            _, snk_bucket = find_sink_bucket_name(self.opts, source_bucket["name"])

            forced = False
            if int(self.opts.extra.get("try_xwm", 1)) == 0:
                forced = True

            if int(self.opts.extra.get("conflict_resolve", 1)) == 0:
                forced = True

            if not forced and snk_conf_res != "any" and src_conf_res != "any" and src_conf_res != snk_conf_res:
                logging.error(f'Cannot transfer data, source bucket `{source_bucket["name"]}` uses {src_conf_res} '
                              f'conflict resolution but sink bucket `{snk_bucket}` uses {snk_conf_res} conflict '
                              f'resolution')
            else:
                rv = Pump(self.opts, source, sink, source_map, sink_map, self.ctl,
                          curx).run()

                for k, v in curx.items():
                    if isinstance(v, int):
                        self.cur[k] = self.cur.get(k, 0) + v

                logging.debug(f' node: {hostname}, done; rv: {rv}')
                if self.ctl['rv'] == 0 and rv != 0:
                    self.ctl['rv'] = rv

            self.queue.task_done()

    def start_workers(self, queue_size: int):
        if self.queue:
            return

        self.queue = queue.Queue(queue_size)

        threads = [threading.Thread(target=PumpingStation.run_worker,
                                    name="w" + str(i), args=(self, i))
                   for i in range(self.opts.threads)]
        for thread in threads:
            thread.daemon = True
            thread.start()

    @staticmethod
    def find_handler(opts, x, classes):
        for s in classes:
            if s.can_handle(opts, x):
                return s
        return None


class Pump(ProgressReporter):
    """Moves batches of data from one Source to one Sink."""

    def __init__(self, opts, source, sink, source_map, sink_map, ctl, cur):
        self.opts = opts
        self.source = source
        self.sink = sink
        self.source_map = source_map
        self.sink_map = sink_map
        self.ctl = ctl
        self.cur = cur  # Should be a defaultdict(int); 0 as default value.

    def run(self):
        future = None

        # TODO: (2) Pump - timeouts when providing/consuming/waiting.

        report = int(self.opts.extra.get("report", 5))
        report_full = int(self.opts.extra.get("report_full", 2000))

        self.report_init()

        n = 0

        while not self.ctl['stop']:
            rv_batch, batch = self.source.provide_batch()
            if rv_batch != 0:
                return self.done(rv_batch)

            if future:
                rv = future.wait_until_consumed()
                if rv != 0:
                    # TODO: (5) Pump - retry logic on consume error.
                    return self.done(rv)

                self.cur['tot_sink_batch'] += 1
                self.cur['tot_sink_msg'] += future.batch.size()
                self.cur['tot_sink_byte'] += future.batch.bytes

                self.ctl['run_msg'] += future.batch.size()
                self.ctl['tot_msg'] += future.batch.adjust_size

            if not batch:
                return self.done(0)

            self.cur['tot_source_batch'] += 1
            self.cur['tot_source_msg'] += batch.size()
            self.cur['tot_source_byte'] += batch.bytes

            rv_future, future = self.sink.consume_batch_async(batch)
            if rv_future != 0:
                return self.done(rv_future)

            n = n + 1
            if report_full > 0 and n % report_full == 0:
                if self.opts.verbose > 0:
                    sys.stderr.write("\n")
                logging.info("  progress...")
                self.report(prefix="  ")
            elif report > 0 and n % report == 0:
                sys.stderr.write(self.bar(self.ctl['run_msg'],
                                          self.ctl['tot_msg']))

        return self.done(0)

    def done(self, rv) -> couchbaseConstants.PUMP_ERROR:
        self.source.close()
        self.sink.close()

        logging.debug("  pump (%s->%s) done.", self.source, self.sink)
        self.report(prefix="  ")

        if (rv == 0 and
            (self.cur['tot_source_batch'] != self.cur['tot_sink_batch'] or
             self.cur['tot_source_msg'] != self.cur['tot_sink_msg'] or
             self.cur['tot_source_byte'] != self.cur['tot_sink_byte'])):
            return "error: sink missing some source msgs: " + str(self.cur)

        return rv


# --------------------------------------------------

class EndPoint(object):

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        self.opts = opts
        self.spec = spec
        self.source_bucket = source_bucket
        self.source_node = source_node
        self.source_map = source_map
        self.sink_map = sink_map
        self.ctl = ctl
        self.cur = cur

        self.only_key_re = None
        k = getattr(opts, "key", None)
        if k:
            self.only_key_re = re.compile(k)

        self.only_vbucket_id = getattr(opts, "id", None)

    @staticmethod
    def check_base(opts, spec) -> couchbaseConstants.PUMP_ERROR:
        k = getattr(opts, "key", None)
        if k:
            try:
                re.compile(k)
            except:
                return f'error: could not parse key regexp: {k}'
        return 0

    @staticmethod
    def check_spec(source_bucket, source_node, opts, spec, cur) -> couchbaseConstants.PUMP_ERROR:
        cur['seqno'] = {}
        cur['failoverlog'] = {}
        cur['snapshot'] = {}
        return 0

    def get_conflict_resolution_type(self) -> str:
        return "any"

    def __repr__(self):
        return f'{self.spec}({self.source_bucket.get("name", "")}@{self.source_node.get("hostname", "")})'

    def close(self):
        pass

    def skip(self, key: Union[str, bytes], vbucket_id: int) -> bool:
        if self.only_key_re and not re.search(self.only_key_re, returnString(key)):
            logging.warning(f'skipping msg with key: {tag_user_data(key)}')
            return True

        if self.only_vbucket_id is not None and self.only_vbucket_id != vbucket_id:
            logging.warning(f'skipping msg of vbucket_id: {vbucket_id!s}')
            return True

        return False

    def get_timestamp(self) -> str:
        # milliseconds with three digits
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def add_counter(self, key, val=1):
        self.cur[key] = self.cur.get(key, 0.0) + val

    def add_start_event(self, conn):
        return 0

    def add_stop_event(self, conn):
        return 0


class Source(EndPoint):
    """Base class for all data sources."""

    @staticmethod
    def can_handle(opts, spec):
        assert False, "unimplemented"

    @staticmethod
    def check_base(opts, spec) -> couchbaseConstants.PUMP_ERROR:
        rv = EndPoint.check_base(opts, spec)
        if rv != 0:
            return rv
        if getattr(opts, "source_vbucket_state", "active") != "active":
            return ("error: only --source-vbucket-state=active" +
                    " is supported by this source: %s") % (spec)
        return 0

    @staticmethod
    def check(opts, spec):
        """Subclasses can check preconditions before any pumping starts."""
        assert False, "unimplemented"

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        assert False, "unimplemented"

    @staticmethod
    def provide_index(opts, source_spec, source_bucket, source_map):
        return 0, None

    @staticmethod
    def provide_fts_index(opts, source_spec, source_bucket, source_map):
        return 0, None

    @staticmethod
    def provide_fts_alias(opts, source_spec, source_bucket, source_map):
        return 0, None

    def provide_batch(self):
        assert False, "unimplemented"

    @staticmethod
    def total_msgs(opts, source_bucket, source_node, source_map):
        return 0, None # Subclasses can return estimate # msgs.


class Sink(EndPoint):
    """Base class for all data sinks."""

    # TODO: (2) Sink handles filtered restore by data.

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(Sink, self).__init__(opts, spec, source_bucket, source_node,
                                   source_map, sink_map, ctl, cur)
        self.op = None

    @staticmethod
    def can_handle(opts, spec):
        assert False, "unimplemented"

    @staticmethod
    def check_base(opts, spec) -> couchbaseConstants.PUMP_ERROR:
        rv = EndPoint.check_base(opts, spec)
        if rv != 0:
            return rv
        if getattr(opts, "destination_vbucket_state", "active") != "active":
            return f'error: only --destination-vbucket-state=active is supported by this destination: {spec}'
        if getattr(opts, "destination_operation", None) != None:
            return f'error: --destination-operation is not supported by this destination: {spec}'
        return 0

    @staticmethod
    def check(opts, spec, source_map):
        """Subclasses can check preconditions before any pumping starts."""
        assert False, "unimplemented"

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design):
        assert False, "unimplemented"

    @staticmethod
    def consume_index(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design) -> couchbaseConstants.PUMP_ERROR:
        return 0

    @staticmethod
    def consume_fts_index(opts, sink_spec, sink_map,
                          source_bucket, source_map, source_design) -> couchbaseConstants.PUMP_ERROR:
        return 0

    @staticmethod
    def consume_fts_alias(opts, sink_spec, sink_map,
                          source_bucket, source_map, source_design) -> couchbaseConstants.PUMP_ERROR:
        return 0

    def consume_batch_async(self, batch):
        """Subclasses should return a SinkBatchFuture."""
        assert False, "unimplemented"

    @staticmethod
    def check_source(opts, source_class, source_spec: str, sink_class, sink_spec: str) -> couchbaseConstants.PUMP_ERROR:
        if source_spec == sink_spec:
            return f'error: source and sink must be different; source: {source_spec} sink: {sink_spec}'
        return 0

    def operation(self):
        if not self.op:
            self.op = getattr(self.opts, "destination_operation", None)
            if not self.op:
                self.op = "set"
                if getattr(self.opts, "add", False):
                    self.op = "add"
        return self.op

    def init_worker(self, target):
        self.worker_go = threading.Event()
        self.worker_work = None # May be None or (batch, future) tuple.
        self.worker = threading.Thread(target=target, args=(self,),
                                       name="s" + threading.currentThread().getName()[1:])
        self.worker.daemon = True
        self.worker.start()

    def push_next_batch(self, batch: Optional[Batch], future: Optional[SinkBatchFuture]) -> \
            Tuple[couchbaseConstants.PUMP_ERROR, Optional[SinkBatchFuture]]:
        """Push batch/future to worker."""
        if not self.worker.is_alive():
            return "error: cannot use a dead worker", None

        self.worker_work = (batch, future)
        self.worker_go.set()
        return 0, future

    def pull_next_batch(self) -> Tuple[Optional[Batch], SinkBatchFuture]:
        """Worker calls this method to get the next batch/future."""
        self.worker_go.wait()
        batch, future = self.worker_work
        self.worker_work = None
        self.worker_go.clear()
        return batch, future

    def future_done(self, future, rv):
        """Worker calls this method to finish a batch/future."""
        if rv != 0:
            logging.error(f'error: async operation: {rv} on sink: {self}')
        if future:
            future.done_rv = rv
            future.done.set()


# --------------------------------------------------

class StdInSource(Source):
    """Reads batches from stdin in memcached ascii protocol."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(StdInSource, self).__init__(opts, spec, source_bucket, source_node,
                                          source_map, sink_map, ctl, cur)
        self.f = sys.stdin

    @staticmethod
    def can_handle(opts, spec):
        return spec.startswith("stdin:") or spec == "-"

    @staticmethod
    def check(opts, spec):
        return 0, {'spec': spec,
                   'buckets': [{'name': 'stdin:',
                                'nodes': [{'hostname': 'N/A'}]}] }

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        return 0, None

    def provide_batch(self):
        batch = Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        vbucket_id = 0x0000ffff

        while (self.f and
               batch.size() < batch_max_size and
               batch.bytes < batch_max_bytes):
            line = self.f.readline()
            if not line:
                self.f = None
                return 0, batch

            parts = line.split(' ')
            if not parts:
                return "error: read empty line", None
            elif parts[0] == 'set' or parts[0] == 'add':
                if len(parts) != 5:
                    return f'error: length of set/add line: {line}', None
                cmd = couchbaseConstants.CMD_TAP_MUTATION
                key = parts[1]
                flg = int(parts[2])
                exp = int(parts[3])
                num = int(parts[4])
                if num > 0:
                    val = self.f.read(num)
                    if len(val) != num:
                        return f'error: value read failed at: {line}', None
                else:
                    val = ''
                end = self.f.read(2) # Read '\r\n'.
                if len(end) != 2:
                    return f'error: value end read failed at: {line}', None

                if not self.skip(key, vbucket_id):
                    msg = (cmd, vbucket_id, key, flg, exp, 0, b'', val, 0, 0, 0)
                    batch.append(msg, len(val))
            elif parts[0] == 'delete':
                if len(parts) != 2:
                    return f'error: length of delete line: {line}', None
                cmd = couchbaseConstants.CMD_TAP_DELETE
                key = parts[1]
                if not self.skip(key, vbucket_id):
                    msg = (cmd, vbucket_id, key, 0, 0, 0, b'', b'', 0, 0, 0)
                    batch.append(msg, 0)
            else:
                return f'error: expected set/add/delete but got: {line}', None

        if batch.size() <= 0:
            return 0, None

        return 0, batch


class StdOutSink(Sink):
    """Emits batches to stdout in memcached ascii protocol."""

    @staticmethod
    def can_handle(opts, spec):
        if spec.startswith("stdout:") or spec == "-":
            opts.threads = 1 # Force 1 thread to not overlap stdout.
            return True
        return False

    @staticmethod
    def check(opts, spec, source_map):
        return 0, None

    @staticmethod
    def check_base(opts, spec):
        if getattr(opts, "destination_vbucket_state", "active") != "active":
            return f'error: only --destination-vbucket-state=active is supported by this destination: {spec}'

        op = getattr(opts, "destination_operation", None)
        if op not in [None, 'set', 'add', 'get']:
            return f'error: --destination-operation unsupported value: {op}; use set, add, get'

        # Skip immediate superclass Sink.check_base(),
        # since StdOutSink can handle different destination operations.
        return EndPoint.check_base(opts, spec)

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design):
        if source_design:
            logging.warn("warning: cannot save bucket design"
                         " on a stdout destination")
        return 0

    def consume_batch_async(self, batch):
        op = self.operation()
        op_mutate = op in ['set', 'add']

        stdout = sys.stdout
        msg_visitor = None

        opts_etc = getattr(self.opts, "etc", None)
        if opts_etc:
            stdout = opts_etc.get("stdout", sys.stdout)
            msg_visitor = opts_etc.get("msg_visitor", None)

        mcd_compatible = self.opts.extra.get("mcd_compatible", 1)
        msg_tuple_format = 0
        for msg in batch.msgs:
            if msg_visitor:
                msg = msg_visitor(msg)
            if not msg_tuple_format:
                msg_tuple_format = len(msg)
            cmd, vbucket_id, key, flg, exp, cas, meta, val = msg[:8]
            seqno = dtype = nmeta = conf_res = 0
            if msg_tuple_format > 8:
                seqno, dtype, nmeta, conf_res = msg[8:]
            if self.skip(key, vbucket_id):
                continue
            if dtype > 2:
                try:
                    val = snappy.uncompress(val)
                except Exception as err:
                    pass
            try:
                if cmd == couchbaseConstants.CMD_TAP_MUTATION or \
                   cmd == couchbaseConstants.CMD_DCP_MUTATION:
                    if op_mutate:
                        # <op> <key> <flags> <exptime> <bytes> [noreply]\r\n
                        if mcd_compatible:
                            stdout.write(f'{op} {key} {flg} {exp} {len(val)!s}\r\n')
                        else:
                            stdout.write(f'{op} {key} {flg} {exp} {len(val)} {seqno} {dtype} {conf_res}\r\n')

                        try:
                            stdout.write(val.decode())
                        except TypeError:
                            stdout.write(f'{val}')

                        stdout.write("\r\n")
                    elif op == 'get':
                        stdout.write(f'get {key}\r\n')
                elif cmd == couchbaseConstants.CMD_TAP_DELETE or \
                     cmd == couchbaseConstants.CMD_DCP_DELETE:
                    if op_mutate:
                        stdout.write(f'delete {key}\r\n')
                elif cmd == couchbaseConstants.CMD_GET:
                    stdout.write(f'get {key}\r\n')
                else:
                    return f'error: StdOutSink - unknown cmd: {cmd!s}', None
            except IOError:
                return "error: could not write to stdout", None

        stdout.flush()
        future = SinkBatchFuture(self, batch)
        self.future_done(future, 0)
        return 0, future


# --------------------------------------------------

CMD_STR = {
    couchbaseConstants.CMD_TAP_CONNECT: "TAP_CONNECT",
    couchbaseConstants.CMD_TAP_MUTATION: "TAP_MUTATION",
    couchbaseConstants.CMD_TAP_DELETE: "TAP_DELETE",
    couchbaseConstants.CMD_TAP_FLUSH: "TAP_FLUSH",
    couchbaseConstants.CMD_TAP_OPAQUE: "TAP_OPAQUE",
    couchbaseConstants.CMD_TAP_VBUCKET_SET: "TAP_VBUCKET_SET",
    couchbaseConstants.CMD_TAP_CHECKPOINT_START: "TAP_CHECKPOINT_START",
    couchbaseConstants.CMD_TAP_CHECKPOINT_END: "TAP_CHECKPOINT_END",
    couchbaseConstants.CMD_NOOP: "NOOP"
}


def get_username(username: str) -> str:
    return username or os.environ.get('CB_REST_USERNAME', '')


def get_password(password: str) -> str:
    return password or os.environ.get('CB_REST_PASSWORD', '')


def parse_spec(opts, spec: str, port: int) -> Tuple[str, int, str, str, Any]:
    """Parse host, port, username, password, path from opts and spec."""

    # Example spec: http://Administrator:password@HOST:8091
    p = urllib.parse.urlparse(spec)

    # Example netloc: Administrator:password@HOST:8091
    # ParseResult tuple(scheme, netloc, path, params, query, fragment)
    netloc = p[1]

    if not netloc: # When urlparse() can't parse non-http URI's.
        netloc = spec.split('://')[-1].split('/')[0]

    pair = netloc.split('@') # [ "user:pwsd", "host:port" ].
    host = p.hostname
    port = p.port
    try:
       val = int(port)
    except ValueError:
       logging.warning(f'"{port}" is not int, reset it to default port number')
       port = 8091

    username = get_username(opts.username)
    password = get_password(opts.password)
    if len(pair) > 1:
        username = username or (pair[0] + ':').split(':')[0]
        password = password or (pair[0] + ':').split(':')[1]

    return host, port, username, password, p[2]


def rest_request(host: str, port: int, user: Optional[str], pswd: Optional[str], use_ssl: bool, path: str,
                 method: str = 'GET', body: str = '', reason: str = '', headers: Optional[Dict[str, Any]] = None,
                 verify: bool = True, ca_cert: Optional[str] = None) -> Tuple[Optional[str],
                                                                              Union[http.client.HTTPConnection,
                                                                              http.client.HTTPSConnection, None],
                                                                              bytes]:

    conn: Union[http.client.HTTPConnection, http.client.HTTPSConnection, None] = None
    if reason:
        reason = f'; reason: {reason}'
    logging.debug(f'rest_request: {tag_user_data(user)}@{host}:{port}{path}{reason}')
    if use_ssl:
        if port not in [couchbaseConstants.SSL_REST_PORT, couchbaseConstants.SSL_QUERY_PORT]:
            return f'error: invalid port {port} used when ssl option is specified', None, b''
        ctx = ssl.create_default_context(cafile=ca_cert)
        if not verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        conn = http.client.HTTPSConnection(host, port, context=ctx)
    else:
        conn = http.client.HTTPConnection(host, port)
    try:
        header = rest_headers(user, pswd, headers)
        conn.request(method, path, body, header)
        resp = conn.getresponse()
    except Exception as e:
        return f'error: could not access REST API: {host}:{port}{path}; please check source URL, server status,' \
                   f' username (-u) and password (-p); exception: {e}{reason}', None, b''

    if resp.status in [200, 201, 202, 204, 302]:
        return None, conn, resp.read()

    conn.close()
    if resp.status == 401:
        return f'error: unable to access REST API: {host}:{port}{path}; please check source URL, server status,' \
            f' username (-u) and password (-p) {reason}', None, b''

    return f'error: unable to access REST API: {host}:{port}{path}; please check source URL, server status,' \
        f' username (-u) and password (-p); response: {resp.status}{reason}', None, b''


def rest_headers(user: Optional[str], pswd: Optional[str], headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    if not headers:
        headers = {'Content-Type': 'application/json'}
    if user:
        auth = 'Basic ' + base64.encodebytes(f'{user}:{pswd or ""}'.strip().encode('utf-8')).decode().strip()
        headers['Authorization'] = auth
    return headers


def rest_request_json(host: str, port: int, user: Optional[str], pswd: Optional[str], ssl: bool, path: str,
                      reason: str = '', verify: bool = True, cacert: Optional[str] = None) -> Tuple[Optional[str],
                                                                                                    Optional[bytes],
                                                                                                    Optional[Dict[
                                                                                                        str, Any]]]:
    err, conn, rest_json = rest_request(host, port, user, pswd, ssl, path,
                                        reason=reason, verify=verify, ca_cert=cacert)
    if err:
        return err, None, None
    if conn:
        conn.close()
    try:
        return None, rest_json, json.loads(rest_json)
    except ValueError as e:
        return f'error: could not decode JSON from REST API: {host}:{port}{path}; exception: {e};' \
            f' please check URL, username (-u) and password (-p)', None, None


def rest_couchbase(opts, spec: str, check_sink_credential: bool =False) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                                 Optional[Dict[str, Any]]]:
    spec = spec.replace('couchbase://', 'http://')
    spec_parts = parse_spec(opts, spec, 8091)

    username = opts.username
    password = opts.password
    if check_sink_credential and opts.username_dest is not None and opts.password_dest is not None:
        username = opts.username_dest
        password = opts.password_dest

    rest = ClusterManager(spec, username, password, opts.ssl, False,
                          None, False)

    result, errors = rest.list_buckets(True)
    if errors:
        return errors[0], None

    buckets = []
    for bucket in result:
        if bucket["bucketType"] in ["membase", "couchbase", "ephemeral"]:
            buckets.append(bucket)

    return 0, {'spec': spec, 'buckets': buckets, 'spec_parts': parse_spec(opts, spec, 8091)}


def filter_bucket_nodes(bucket: Dict[str, Any], spec_parts: Sequence[Any]) -> List[Any]:
    host, port = spec_parts[:2]
    if host in ['localhost', '127.0.0.1']:
        host = get_ip()
    # Convert from raw IPv6
    if ':' in host:
        host_port = f'[{host}]:{port!s}'
    else:
        host_port = f'{host}:{port!s}'
    return [n for n in bucket['nodes'] if n.get('hostname') == host_port]


def get_ip() -> str:
    ip = None
    for fname in ['/opt/couchbase/var/lib/couchbase/ip_start',
                  '/opt/couchbase/var/lib/couchbase/ip',
                  '../var/lib/couchbase/ip_start',
                  '../var/lib/couchbase/ip']:
        try:
            f = open(fname, 'r')
            ip = f.read().strip()
            f.close()
            if ip and len(ip):
                if ip.find('@'):
                    ip = ip.split('@')[1]
                break
        except:
            pass
    if not ip or not len(ip):
        ip = '127.0.0.1'
    return ip


def find_source_bucket_name(opts, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR, str]:
    """If the caller didn't specify a bucket_source and
       there's only one bucket in the source_map, use that."""
    source_bucket = getattr(opts, "bucket_source", None)
    if (not source_bucket and
        source_map and
        source_map['buckets'] and
        len(source_map['buckets']) == 1):
        source_bucket = source_map['buckets'][0]['name']
    if not source_bucket:
        return "error: please specify a bucket_source", ''
    logging.debug(f'source_bucket: {source_bucket}')
    return 0, source_bucket


def find_sink_bucket_name(opts, source_bucket) -> Tuple[couchbaseConstants.PUMP_ERROR, str]:
    """Default bucket_destination to the same as bucket_source."""
    sink_bucket = getattr(opts, "bucket_destination", None) or source_bucket
    if not sink_bucket:
        return "error: please specify a bucket_destination", ''
    logging.debug(f'sink_bucket: {sink_bucket}')
    return 0, sink_bucket


def mkdirs(targetpath: str) -> couchbaseConstants.PUMP_ERROR:
    upperdirs = os.path.dirname(targetpath)
    if upperdirs and not os.path.exists(upperdirs):
        try:
            os.makedirs(upperdirs)
        except:
            return f'Cannot create upper directories for file: {targetpath}'
    return 0


def hostport(hoststring: str, port: int = 11210) -> Tuple[str, int]:
    if hoststring.startswith('['):
        matches = re.match(r'^\[([^\]]+)\](:(\d+))?$', hoststring)
    else:
        matches = re.match(r'^([^:]+)(:(\d+))?$', hoststring)
    if matches:
        # The host is the first group
        host = matches.group(1)
        # Optional port is the 3rd group
        if matches.group(3):
            port = int(matches.group(3))
    return host, port


def get_mcd_conn(host: str, port: int, username: str, password: str, bucket: Optional[str], use_ssl: bool = False,
                 verify: bool = True, ca_cert: Optional[str] = None, collections: bool = False) -> \
        Tuple[couchbaseConstants.PUMP_ERROR, Optional[cb_bin_client.MemcachedClient]]:
    conn = cb_bin_client.MemcachedClient(host, port, use_ssl=use_ssl, verify=verify, cacert=ca_cert)
    if not conn:
        return f'error: could not connect to memcached: {host}:{port!s}', None

    try:
        conn.sasl_auth_plain(username, password)
    except EOFError as e:
        return f'error: SASL auth error: {host}:{port}, {e}', None
    except cb_bin_client.MemcachedError as e:
        return f'error: SASL auth failed: {host}:{port}, {e}', None
    except socket.error as e:
        return f'error: SASL auth socket error: {host}:{port}, {e}', None

    features = [couchbaseConstants.HELO_XATTR, couchbaseConstants.HELO_XERROR]
    if collections:
        features.append(couchbaseConstants.HELO_COLLECTIONS)

    try:
        _, _, enabledFeatures = conn.helo(features)
        for feature in features:
            if feature not in enabledFeatures:
                return f'error: HELO denied feature:{feature} {host}:{port}', None
    except EOFError as e:
        return f'error: HELO error: {host}:{port}, {e}', None
    except cb_bin_client.MemcachedError as e:
        return f'error: HELO failed:{host}:{port}, {e}', None
    except socket.error as e:
        return f'error: HELO socket error: {host}:{port}, {e}', None

    if bucket:
        try:
            conn.bucket_select(bucket.encode())
        except EOFError as e:
            return f'error: Bucket select error: {host}:{port} {bucket}, {e}', None
        except cb_bin_client.MemcachedError as e:
            return f'error: Bucket select failed: {host}:{port} {bucket}, {e}', None
        except socket.error as e:
            return f'error: Bucket select socket error: {host}:{port} {bucket}, {e}', None

    return 0, conn


def returnString(byte_or_str: Union[str, bytes, int]) -> str:
    if byte_or_str is None:
        return None
    if isinstance(byte_or_str, bytes):
        return byte_or_str.decode()
    return str(byte_or_str)
