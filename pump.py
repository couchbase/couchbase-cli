#!/usr/bin/env python

import os
import base64
import copy
import httplib
import logging
import re
import simplejson as json
import string
import sys
import threading
import time
import urlparse
import zlib
import platform
import subprocess

import couchbaseConstants
from cbcollections import defaultdict
from cbqueue import PumpQueue

# TODO: (1) optionally log into backup directory

LOGGING_FORMAT = '%(asctime)s: %(threadName)s %(message)s'

NA = 'N/A'

class ProgressReporter(object):
    """Mixin to report progress"""

    def report_init(self):
        self.beg_time = time.time()
        self.prev_time = self.beg_time
        self.prev = defaultdict(int)

    def report(self, prefix="", emit=None):
        if not emit:
            emit = logging.info

        if getattr(self, "source", None):
            emit(prefix + "source : %s" % (self.source))
        if getattr(self, "sink", None):
            emit(prefix + "sink   : %s" % (self.sink))

        cur_time = time.time()
        delta = cur_time - self.prev_time
        c, p = self.cur, self.prev
        x = sorted([k for k in c.iterkeys() if "_sink_" in k])

        width_k = max([5] + [len(k.replace("tot_sink_", "")) for k in x])
        width_v = max([20] + [len(str(c[k])) for k in x])
        width_d = max([10] + [len(str(c[k] - p[k])) for k in x])
        width_s = max([10] + [len("%0.1f" % ((c[k] - p[k]) / delta)) for k in x])
        emit(prefix + " %s : %s | %s | %s"
             % (string.ljust("", width_k),
                string.rjust("total", width_v),
                string.rjust("last", width_d),
                string.rjust("per sec", width_s)))
        for k in x:
            emit(prefix + " %s : %s | %s | %s"
                 % (string.ljust(k.replace("tot_sink_", ""), width_k),
                    string.rjust(str(c[k]), width_v),
                    string.rjust(str(c[k] - p[k]), width_d),
                    string.rjust("%0.1f" % ((c[k] - p[k]) / delta), width_s)))
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
        return ("  [%s%s] %0.1f%% (%s/%s msgs)%s" %
                ('#' * num_hash, ' ' * (max_hash - num_hash),
                 100.0 * pct, current, total, cr))

class PumpingStation(ProgressReporter):
    """Queues and watchdogs multiple pumps across concurrent workers."""

    def __init__(self, opts, source_class, source_spec, sink_class, sink_spec):
        self.opts = opts
        self.source_class = source_class
        self.source_spec = source_spec
        self.sink_class = sink_class
        self.sink_spec = sink_spec
        self.queue = None
        self.ctl = { 'stop': False, 'rv': 0 }
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
                return ("error: there is no bucket: %s at source: %s" %
                        (bucket_source, self.source_spec))
            else:
                return ("error: no transferrable buckets at source: %s" %
                        (self.source_spec))

        for source_bucket in sorted(source_buckets,
                                    key=lambda b: b['name']):
            logging.info("bucket: " + source_bucket['name'])

            if not self.opts.extra.get("design_doc_only", 0):
                rv = self.transfer_bucket_msgs(source_bucket, source_map, sink_map)
                if rv != 0:
                    return rv
            else:
                sys.stderr.write("transfer design doc only. bucket msgs will be skipped.\n")

            if not self.opts.extra.get("data_only", 0):
                rv = self.transfer_bucket_design(source_bucket, source_map, sink_map)
                if rv != 0:
                    return rv
            else:
                sys.stderr.write("transfer data only. bucket design docs will be skipped.\n")

            # TODO: (5) PumpingStation - validate bucket transfers.

        # TODO: (4) PumpingStation - validate source/sink maps were stable.

        sys.stderr.write("done\n")
        return 0

    def check_endpoints(self):
        logging.debug("source_class: %s", self.source_class)
        rv = self.source_class.check_base(self.opts, self.source_spec)
        if rv != 0:
            return rv, None, None
        rv, source_map = self.source_class.check(self.opts, self.source_spec)
        if rv != 0:
            return rv, None, None

        logging.debug("sink_class: %s", self.sink_class)
        rv = self.sink_class.check_base(self.opts, self.sink_spec)
        if rv != 0:
            return rv, None, None
        rv, sink_map = self.sink_class.check(self.opts, self.sink_spec, source_map)
        if rv != 0:
            return rv, None, None

        return rv, source_map, sink_map

    def filter_source_buckets(self, source_map):
        """Filter the source_buckets if a bucket_source was specified."""
        source_buckets = source_map['buckets']
        logging.debug("source_buckets: " +
                      ",".join([n['name'] for n in source_buckets]))

        bucket_source = getattr(self.opts, "bucket_source", None)
        if bucket_source:
            logging.debug("bucket_source: " + bucket_source)
            source_buckets = [b for b in source_buckets
                              if b['name'] == bucket_source]
            logging.debug("source_buckets filtered: " +
                          ",".join([n['name'] for n in source_buckets]))
        return source_buckets

    def filter_source_nodes(self, source_bucket, source_map):
        """Filter the source_bucket's nodes if single_node was specified."""
        if getattr(self.opts, "single_node", None):
            if not source_map.get('spec_parts'):
                return ("error: no single_node from source: %s" +
                        "; the source may not support the --single-node flag") % \
                        (self.source_spec)
            source_nodes = filter_bucket_nodes(source_bucket,
                                               source_map.get('spec_parts'))
        else:
            source_nodes = source_bucket['nodes']

        logging.debug(" source_nodes: " + ",".join([n.get('hostname', NA)
                                                    for n in source_nodes]))
        return source_nodes

    def transfer_bucket_msgs(self, source_bucket, source_map, sink_map):
        source_nodes = self.filter_source_nodes(source_bucket, source_map)

        # Transfer bucket msgs with a Pump per source server.
        self.start_workers(len(source_nodes))
        self.report_init()

        self.ctl['run_msg'] = 0
        self.ctl['tot_msg'] = 0

        for source_node in sorted(source_nodes,
                                  key=lambda n: n.get('hostname', NA)):
            logging.debug(" enqueueing node: " +
                          source_node.get('hostname', NA))
            self.queue.put((source_bucket, source_node, source_map, sink_map))

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

        time.sleep(0.01) # Allows threads to update counters.

        sys.stderr.write(self.bar(self.ctl['run_msg'],
                                  self.ctl['tot_msg']) + "\n")
        sys.stderr.write("bucket: " + source_bucket['name'] +
                         ", msgs transferred...\n")
        def emit(msg):
            sys.stderr.write(msg + "\n")
        self.report(emit=emit)

        return 0

    def transfer_bucket_design(self, source_bucket, source_map, sink_map):
        """Transfer bucket design (e.g., design docs, views)."""
        rv, source_design = \
            self.source_class.provide_design(self.opts, self.source_spec,
                                             source_bucket, source_map)
        if rv == 0:
            rv = self.sink_class.consume_design(self.opts,
                                                self.sink_spec, sink_map,
                                                source_bucket, source_map,
                                                source_design)
        return rv

    @staticmethod
    def run_worker(self, thread_index):
        while not self.ctl['stop']:
            source_bucket, source_node, source_map, sink_map = \
                self.queue.get()

            hostname = source_node.get('hostname', NA)
            logging.debug(" node: %s" % (hostname))

            curx = defaultdict(int)

            rv = Pump(self.opts,
                      self.source_class(self.opts, self.source_spec,
                                        source_bucket, source_node,
                                        source_map, sink_map, self.ctl, curx),
                      self.sink_class(self.opts, self.sink_spec,
                                      source_bucket, source_node,
                                      source_map, sink_map, self.ctl, curx),
                      source_map, sink_map, self.ctl, curx).run()

            for k, v in curx.items():
                self.cur[k] = self.cur.get(k, 0) + v

            logging.debug(" node: %s, done; rv: %s" % (hostname, rv))
            if self.ctl['rv'] == 0 and rv != 0:
                self.ctl['rv'] = rv

            self.queue.task_done()

    def start_workers(self, queue_size):
        if self.queue:
            return

        self.queue = PumpQueue(queue_size)

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
        self.cur = cur # Should be a defaultdict(int); 0 as default value.

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

    def done(self, rv):
        self.source.close()
        self.sink.close()

        logging.debug("  pump (%s->%s) done.", self.source, self.sink)
        self.report(prefix="  ")

        if (rv == 0 and
            (self.cur['tot_source_batch'] != self.cur['tot_sink_batch'] or
             self.cur['tot_source_batch'] != self.cur['tot_sink_batch'] or
             self.cur['tot_source_batch'] != self.cur['tot_sink_batch'])):
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
    def check_base(opts, spec):
        k = getattr(opts, "key", None)
        if k:
            try:
                re.compile(k)
            except:
                return "error: could not parse key regexp: " + k
        return 0

    def __repr__(self):
        return "%s(%s@%s)" % \
            (self.spec,
             self.source_bucket.get('name', ''),
             self.source_node.get('hostname', ''))

    def close(self):
        pass

    def skip(self, key, vbucket_id):
        if (self.only_key_re and not re.search(self.only_key_re, key)):
            logging.warn("skipping msg with key: " + str(key))
            return True

        if (self.only_vbucket_id is not None and
            self.only_vbucket_id != vbucket_id):
            logging.warn("skipping msg of vbucket_id: " + str(vbucket_id))
            return True

        return False

    def add_counter(self, key, val=1):
        self.cur[key] = self.cur.get(key, 0.0) + val


class Source(EndPoint):
    """Base class for all data sources."""

    @staticmethod
    def can_handle(opts, spec):
        assert False, "unimplemented"

    @staticmethod
    def check_base(opts, spec):
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
    def check_base(opts, spec):
        rv = EndPoint.check_base(opts, spec)
        if rv != 0:
            return rv
        if getattr(opts, "destination_vbucket_state", "active") != "active":
            return ("error: only --destination-vbucket-state=active" +
                    " is supported by this destination: %s") % (spec)
        if getattr(opts, "destination_operation", None) != None:
            return ("error: --destination-operation" +
                    " is not supported by this destination: %s") % (spec)
        return 0

    @staticmethod
    def check(opts, spec, source_map):
        """Subclasses can check preconditions before any pumping starts."""
        assert False, "unimplemented"

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design):
        assert False, "unimplemented"

    def consume_batch_async(self, batch):
        """Subclasses should return a SinkBatchFuture."""
        assert False, "unimplemented"

    @staticmethod
    def check_source(opts, source_class, source_spec, sink_class, sink_spec):
        if source_spec == sink_spec:
            return "error: source and sink must be different;" \
                " source: " + source_spec + \
                " sink: " + sink_spec
        return None

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

    def push_next_batch(self, batch, future):
        """Push batch/future to worker."""
        if not self.worker.isAlive():
            return "error: cannot use a dead worker", None

        self.worker_work = (batch, future)
        self.worker_go.set()
        return 0, future

    def pull_next_batch(self):
        """Worker calls this method to get the next batch/future."""
        self.worker_go.wait()
        batch, future = self.worker_work
        self.worker_work = None
        self.worker_go.clear()
        return batch, future

    def future_done(self, future, rv):
        """Worker calls this method to finish a batch/future."""
        if rv != 0:
            logging.error("error: async operation: %s on sink: %s" %
                          (rv, self))
        if future:
            future.done_rv = rv
            future.done.set()


# --------------------------------------------------

class Batch(object):
    """Holds a batch of data being transfered from source to sink."""

    def __init__(self, source):
        self.source = source
        self.msgs = []
        self.bytes = 0

    def append(self, msg, num_bytes):
        self.msgs.append(msg)
        self.bytes = self.bytes + num_bytes

    def size(self):
        return len(self.msgs)

    def msg(self, i):
        return self.msgs[i]

    def group_by_vbucket_id(self, vbuckets_num, rehash=0):
        """Returns dict of vbucket_id->[msgs] grouped by msg's vbucket_id."""
        g = defaultdict(list)
        for msg in self.msgs:
            cmd, vbucket_id, key, flg, exp, cas, meta, val = msg
            if vbucket_id == 0x0000ffff or rehash == 1:
                # Special case when the source did not supply a vbucket_id
                # (such as stdin source), so we calculate it.
                vbucket_id = (zlib.crc32(key) >> 16) & (vbuckets_num - 1)
                msg = (cmd, vbucket_id, key, flg, exp, cas, meta, val)
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
                    return "error: length of set/add line: " + line, None
                cmd = couchbaseConstants.CMD_TAP_MUTATION
                key = parts[1]
                flg = int(parts[2])
                exp = int(parts[3])
                num = int(parts[4])
                if num > 0:
                    val = self.f.read(num)
                    if len(val) != num:
                        return "error: value read failed at: " + line, None
                else:
                    val = ''
                end = self.f.read(2) # Read '\r\n'.
                if len(end) != 2:
                    return "error: value end read failed at: " + line, None

                if not self.skip(key, vbucket_id):
                    msg = (cmd, vbucket_id, key, flg, exp, 0, '', val)
                    batch.append(msg, len(val))
            elif parts[0] == 'delete':
                if len(parts) != 2:
                    return "error: length of delete line: " + line, None
                cmd = couchbaseConstants.CMD_TAP_DELETE
                key = parts[1]
                if not self.skip(key, vbucket_id):
                    msg = (cmd, vbucket_id, key, 0, 0, 0, '', '')
                    batch.append(msg, 0)
            else:
                return "error: expected set/add/delete but got: " + line, None

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
            return ("error: only --destination-vbucket-state=active" +
                    " is supported by this destination: %s") % (spec)

        op = getattr(opts, "destination_operation", None)
        if not op in [None, 'set', 'add', 'get']:
            return ("error: --destination-operation unsupported value: %s" +
                    "; use set, add, get") % (op)

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

        for msg in batch.msgs:
            if msg_visitor:
                msg = msg_visitor(msg)

            cmd, vbucket_id, key, flg, exp, cas, meta, val = msg
            if self.skip(key, vbucket_id):
                continue

            try:
                if cmd == couchbaseConstants.CMD_TAP_MUTATION:
                    if op_mutate:
                        # <op> <key> <flags> <exptime> <bytes> [noreply]\r\n
                        stdout.write("%s %s %s %s %s\r\n" %
                                     (op, key, flg, exp, len(val)))
                        stdout.write(val)
                        stdout.write("\r\n")
                    elif op == 'get':
                        stdout.write("get %s\r\n" % (key))
                elif cmd == couchbaseConstants.CMD_TAP_DELETE:
                    if op_mutate:
                        stdout.write("delete %s\r\n" % (key))
                elif cmd == couchbaseConstants.CMD_GET:
                    stdout.write("get %s\r\n" % (key))
                else:
                    return "error: StdOutSink - unknown cmd: " + str(cmd), None
            except IOError:
                return "error: could not write to stdout", None

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

def parse_spec(opts, spec, port):
    """Parse host, port, username, password, path from opts and spec."""

    # Example spec: http://Administrator:password@HOST:8091
    p = urlparse.urlparse(spec)

    # Example netloc: Administrator:password@HOST:8091
    #ParseResult tuple(scheme, netloc, path, params, query, fragment)
    netloc = p[1]

    if not netloc: # When urlparse() can't parse non-http URI's.
        netloc = spec.split('://')[-1].split('/')[0]

    pair = netloc.split('@') # [ "user:pwsd", "host:port" ].
    host = (pair[-1] + ":" + str(port)).split(':')[0]
    port = (pair[-1] + ":" + str(port)).split(':')[1]

    username = opts.username
    password = opts.password
    if len(pair) > 1:
        username = username or (pair[0] + ':').split(':')[0] \
            or os.environ['CB_REST_USERNAME'] or ''
        password = password or (pair[0] + ':').split(':')[1] \
            or os.environ['CB_REST_PASSWORD'] or ''

    return host, port, username, password, p[2]

def rest_request(host, port, user, pswd, path, method='GET', body='', reason='', headers=None):
    if reason:
        reason = "; reason: %s" % (reason)
    logging.debug("rest_request: %s@%s:%s%s%s" % (user, host, port, path, reason))
    conn = httplib.HTTPConnection(host, port)
    try:
        header = rest_headers(user, pswd, headers)
        conn.request(method, path, body, header)
        resp = conn.getresponse()
    except Exception, e:
        return ("error: could not access REST API: %s:%s%s" +
                "; please check source URL, username (-u) and password (-p)" +
                "; exception: %s%s") % \
                (host, port, path, e, reason), None, None

    if resp.status in [200, 201, 202, 204, 302]:
        return None, conn, resp.read()

    conn.close()
    if resp.status == 401:
        return ("error: unable to access REST API: %s:%s%s" +
                "; please check source URL, username (-u) and password (-p)%s") % \
                (host, port, path, reason), None, None

    return ("error: unable to access REST API: %s:%s%s" +
            "; please check source URL, username (-u) and password (-p)" +
            "; response: %s%s") % \
            (host, port, path, resp.status, reason), None, None

def rest_headers(user, pswd, headers=None):
    if not headers:
        headers = {'Content-Type': 'application/json'}
    if user:
        auth = 'Basic ' + \
            string.strip(base64.encodestring(user + ':' + (pswd or '')))
        headers['Authorization'] = auth
    return headers

def rest_request_json(host, port, user, pswd, path, reason=''):
    err, conn, rest_json = rest_request(host, port, user, pswd, path,
                                        reason=reason)
    if err:
        return err, None, None
    if conn:
        conn.close()
    try:
        return None, rest_json, json.loads(rest_json)
    except ValueError, e:
        return ("error: could not decode JSON from REST API: %s:%s%s" +
                "; exception: %s" +
                "; please check URL, username (-u) and password (-p)") % \
                (host, port, path, e), None, None

def rest_couchbase(opts, spec):
    spec = spec.replace('couchbase://', 'http://')

    spec_parts = parse_spec(opts, spec, 8091)
    host, port, user, pswd, path = spec_parts

    if not path or path == '/':
        path = '/pools/default/buckets'

    if int(port) in [11210, 11211]:
        return "error: invalid port number %s, which is reserved for moxi service" % port, None

    err, rest_json, rest_data = \
        rest_request_json(host, int(port), user, pswd, path)
    if err:
        return err, None

    if type(rest_data) == type([]):
        rest_buckets = rest_data
    else:
        rest_buckets = [ rest_data ] # Wrap single bucket in a list.

    buckets = []

    for bucket in rest_buckets:
        if not (bucket and
                bucket.get('name', None) and
                bucket.get('bucketType', None) and
                bucket.get('nodes', None) and
                bucket.get('nodeLocator', None)):
            return "error: unexpected JSON value from: " + spec + \
                " - did you provide the right source URL?", None

        if bucket['nodeLocator'] == 'vbucket' and \
                (bucket['bucketType'] == 'membase' or
                 bucket['bucketType'] == 'couchbase'):
            buckets.append(bucket)
        else:
            logging.warn("skipping bucket that is not a couchbase-bucket: " +
                         bucket['name'])

    if user is None or pswd is None:
        # Check if we have buckets other than the default one
        if len(rest_buckets) > 0:
            if len(rest_buckets) > 1 or rest_buckets[0].get('name', None) != "default":
                return "error: REST username (-u) and password (-p) are required " + \
                       "to access all bucket(s)", None
    return 0, {'spec': spec, 'buckets': buckets, 'spec_parts': spec_parts}

def filter_bucket_nodes(bucket, spec_parts):
    host, port = spec_parts[:2]
    if host in ['localhost', '127.0.0.1']:
        host = get_ip()
    host_port = host + ':' + str(port)
    return filter(lambda n: n.get('hostname') == host_port,
                  bucket['nodes'])

def get_ip():
    ip = None
    for fname in ['/opt/couchbase/var/lib/couchbase/ip_start',
                  '/opt/couchbase/var/lib/couchbase/ip',
                  '../var/lib/couchbase/ip_start',
                  '../var/lib/couchbase/ip']:
        try:
            f = open(fname, 'r')
            ip = string.strip(f.read())
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

def find_source_bucket_name(opts, source_map):
    """If the caller didn't specify a bucket_source and
       there's only one bucket in the source_map, use that."""
    source_bucket = getattr(opts, "bucket_source", None)
    if (not source_bucket and
        source_map and
        source_map['buckets'] and
        len(source_map['buckets']) == 1):
        source_bucket = source_map['buckets'][0]['name']
    if not source_bucket:
        return "error: please specify a bucket_source", None
    logging.debug("source_bucket: " + source_bucket)
    return 0, source_bucket

def find_sink_bucket_name(opts, source_bucket):
    """Default bucket_destination to the same as bucket_source."""
    sink_bucket = getattr(opts, "bucket_destination", None) or source_bucket
    if not sink_bucket:
        return "error: please specify a bucket_destination", None
    logging.debug("sink_bucket: " + sink_bucket)
    return 0, sink_bucket

def mkdirs(targetpath):
    upperdirs = os.path.dirname(targetpath)
    if upperdirs and not os.path.exists(upperdirs):
        try:
            os.makedirs(upperdirs)
        except:
            return "Cannot create upper directories for file:%s" % targetpath
    return 0
