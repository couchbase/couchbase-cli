#!/usr/bin/env python

import base64
import copy
import collections
import httplib
import logging
import Queue
import re
import simplejson as json
import string
import sys
import threading
import time
import urlparse

import mc_bin_client
import memcacheConstants

LOGGING_FORMAT = '%(asctime)s: %(threadName)s %(message)s'

NA = 'N/A'

class ProgressReporter:
    """Mixin to report progress"""

    def report_init(self):
        self.beg_time = time.time()
        self.prev_time = self.beg_time
        self.prev = collections.defaultdict(int)

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

        width_k = max([10] + [len(k) for k in x])
        width_v = max([10] + [len(str(c[k])) for k in x])
        width_d = max([10] + [len(str(c[k] - p[k])) for k in x])
        width_s = max([10] + [len("%0.1f" % ((c[k] - p[k]) / delta)) for k in x])
        emit(prefix + " %s : %s | %s | %s"
             % (string.ljust("counter", width_k),
                string.rjust("total", width_v),
                string.rjust("last", width_d),
                string.rjust("per sec", width_s)))
        for k in x:
            emit(prefix + " %s : %s | %s | %s"
                 % (string.ljust(k, width_k),
                    string.rjust(str(c[k]), width_v),
                    string.rjust(str(c[k] - p[k]), width_d),
                    string.rjust("%0.1f" % ((c[k] - p[k]) / delta), width_s)))
        self.prev_time = cur_time
        self.prev = copy.copy(c)


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
        self.cur = collections.defaultdict(int)

    def run(self):
        logging.debug("source_class: %s", self.source_class)
        rv, source_map = self.source_class.check(self.opts, self.source_spec)
        if rv != 0:
            return rv
        logging.debug("sink_class: %s", self.sink_class)
        rv, sink_map = self.sink_class.check(self.opts, self.sink_spec, source_map)
        if rv != 0:
            return rv

        # TODO: (6) PumpingStation - monitor source for topology changes.
        # TODO: (4) PumpingStation - retry/reconnect on err N times, M times / server.
        # TODO: (2) PumpingStation - track checksum in backup, used later at restore.

        # Filter the source_buckets if a bucket_source was specified.
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

        for source_bucket in sorted(source_buckets, key=lambda b: b['name']):
            logging.info("bucket: " + source_bucket['name'])

            # Transfer bucket configuration (e.g., memory quota).
            rv, source_config = \
                self.source_class.provide_config(self.opts, self.source_spec,
                                                 source_bucket)
            if rv != 0:
                return rv
            rv = self.sink_class.consume_config(self.opts, self.sink_spec,
                                                source_bucket, source_config)
            if rv != 0:
                return rv

            # Filter the source_nodes if single_node was specified.
            source_nodes = source_bucket['nodes']
            logging.debug(" source_nodes: " + ",".join([n.get('hostname', NA)
                                                        for n in source_nodes]))

            single_node = getattr(self.opts, "single_node", None)
            if single_node:
                if not source_map.get('spec_parts'):
                    return "error: cannot support single_node from source: " + \
                        self.source_spec
                single_host, single_port, _, _, _ = source_map.get('spec_parts')
                single_host_port = single_host + ':' + single_port
                logging.debug(" single_host_port: " + single_host_port)
                source_nodes = [n for n in source_nodes
                                if n.get('hostname', NA) == single_host_port]
                logging.debug(" source_nodes filtered: " +
                              ",".join([n.get('hostname', NA)
                                        for n in source_nodes]))

            # Transfer bucket items with a Pump per source server.
            self.start_workers(len(source_nodes))
            self.report_init()

            for source_node in sorted(source_nodes,
                                      key=lambda n: n.get('hostname', NA)):
                logging.debug(" enqueueing node: " +
                              source_node.get('hostname', NA))
                self.queue.put((source_bucket, source_node, source_map, sink_map))

            # Don't use queue.join() as it eats Ctrl-C's.
            while self.queue.unfinished_tasks:
                time.sleep(0.2)

            rv = self.ctl['rv']
            if rv != 0:
                return rv

            sys.stdout.write("\n")
            sys.stdout.write("bucket: " + source_bucket['name'] +
                             ", items transferred...\n")
            def emit(msg):
                sys.stdout.write(msg + "\n")
            self.report(emit=emit)

            # Transfer bucket design (e.g., design docs, views).
            rv, design = \
                self.source_class.provide_design(self.opts, self.source_spec,
                                                 source_bucket)
            if rv != 0:
                return rv
            rv = self.sink_class.consume_design(self.opts, self.sink_spec,
                                                source_bucket, design)
            if rv != 0:
                return rv

            print "done"

            # TODO: (5) PumpingStation - validate bucket transfers.

        # TODO: (4) PumpingStation - validate source/sink maps were stable.

        return 0

    @staticmethod
    def run_worker(self, thread_index):
        while not self.ctl['stop']:
            source_bucket, source_node, source_map, sink_map = \
                self.queue.get()

            hostname = source_node.get('hostname', NA)
            logging.debug(" node: %s" % (hostname))

            curx = collections.defaultdict(int)

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

        self.queue = Queue.Queue(queue_size)
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

        report_dot = int(self.opts.extra.get("report_dot", 1000))
        report_full = int(self.opts.extra.get("report_full", 50000))

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
                self.cur['tot_sink_item'] += future.batch.size()
                self.cur['tot_sink_byte'] += future.batch.bytes

            if not batch:
                return self.done(0)

            self.cur['tot_source_batch'] += 1
            self.cur['tot_source_item'] += batch.size()
            self.cur['tot_source_byte'] += batch.bytes

            rv_future, future = self.sink.consume_batch_async(batch)
            if rv_future != 0:
                return self.done(rv_future)

            n = n + 1
            if report_full > 0 and n % report_full == 0:
                sys.stderr.write("\n")
                logging.info("  progress...")
                self.report(prefix="  ")
            elif report_dot > 0 and n % report_dot == 0:
                sys.stderr.write('.')

    def done(self, rv):
        logging.debug("  pump (%s->%s) done.", self.source, self.sink)
        self.report(prefix="  ")

        if (self.cur['tot_source_batch'] != self.cur['tot_sink_batch'] or
            self.cur['tot_source_batch'] != self.cur['tot_sink_batch'] or
            self.cur['tot_source_batch'] != self.cur['tot_sink_batch']):
            return "error: sink missing some source items: " + str(self.cur)

        return rv


# --------------------------------------------------

class EndPoint():

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
            # TODO: (1) EndPoint - handle bad key regexp.
            self.only_key_re = re.compile(k)

        self.only_vbucket_id = getattr(opts, "id", None)

    def __repr__(self):
        return "%s(%s@%s)" % \
            (self.spec,
             self.source_bucket.get('name', ''),
             self.source_node.get('hostname', ''))

    def skip(self, key, vbucket_id):
        if (self.only_key_re and not re.search(self.only_key_re, key)):
            logging.warn("skip item with key: " + str(key))
            return True

        if (self.only_vbucket_id is not None and
            self.only_vbucket_id != vbucket_id):
            logging.warn("skip item of vbucket_id: " + str(vbucket_id))
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
    def check(opts, spec):
        """Subclasses can check preconditions before any pumping starts."""
        assert False, "unimplemented"

    @staticmethod
    def provide_config(opts, spec, bucket):
        assert False, "unimplemented"

    @staticmethod
    def provide_design(opts, spec, bucket):
        assert False, "unimplemented"

    def provide_batch(self):
        assert False, "unimplemented"


class Sink(EndPoint):
    """Base class for all data sinks."""

    # TODO: (2) Sink handles filtered restore by data.

    @staticmethod
    def can_handle(opts, spec):
        assert False, "unimplemented"

    @staticmethod
    def check(opts, spec, source_map):
        """Subclasses can check preconditions before any pumping starts."""
        assert False, "unimplemented"

    @staticmethod
    def consume_config(opts, spec, bucket, config):
        assert False, "unimplemented"

    @staticmethod
    def consume_design(opts, spec, bucket, design):
        assert False, "unimplemented"

    def consume_batch_async(self, batch):
        """Subclasses should return a SinkBatchFuture."""
        assert False, "unimplemented"

    def init_worker(self, target):
        self.worker_go = threading.Event()
        self.worker_work = None # May be None or (batch, future) tuple.
        self.worker = threading.Thread(target=target, args=(self,),
                                       name="s" + threading.current_thread().name[1:])
        self.worker.daemon = True
        self.worker.start()

    def push_next_batch(self, batch, future):
        """Push batch/future to worker."""
        if not self.worker.is_alive():
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
            logging.error("error: async/future operation: %s on sink: %s" %
                          (rv, self))
        future.done_rv = rv
        future.done.set()


# --------------------------------------------------

class Batch:
    """Holds a batch of data being transfered from source to sink."""

    def __init__(self, source):
        self.source = source
        self.items = []
        self.bytes = 0
        self.stats = {} # TODO: (1) Batch - record stats.

    def append(self, item, num_bytes):
        self.items.append(item)
        self.bytes = self.bytes + num_bytes

    def size(self):
        return len(self.items)

    def item(self, i):
        return self.items[i]

    def group_by(self, index):
        """Returns dict of value->[items] grouped by each item's value."""
        g = collections.defaultdict(list)
        for item in self.items:
            g[item[index]].append(item)
        return g


class SinkBatchFuture:
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

class StdOutSink(Sink):
    """Emits batches to stdout in memcached ascii protocol."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        Sink.__init__(self, opts, spec, source_bucket, source_node,
                      source_map, sink_map, ctl, cur)

    @staticmethod
    def can_handle(opts, spec):
        if spec.startswith("stdout:"):
            opts.threads = 1 # Force 1 thread to not overlap stdout.
            return True
        return False

    @staticmethod
    def check(opts, spec, source_map):
        return 0, None

    @staticmethod
    def consume_config(opts, spec, bucket, config):
        if config:
            logging.warn("warning: cannot restore bucket configuration"
                         " on a stdout destination")
        return 0

    @staticmethod
    def consume_design(opts, spec, bucket, design):
        if design:
            logging.warn("warning: cannot restore bucket design"
                         " on a stdout destination")
        return 0

    def consume_batch_async(self, batch):
        use_add = bool(getattr(self.opts, "add", False))

        stdout = sys.stdout
        item_visitor = None

        opts_etc = getattr(self.opts, "etc", None)
        if opts_etc:
            stdout = opts_etc.get("stdout", sys.stdout)
            item_visitor = opts_etc.get("item_visitor", None)

        for item in batch.items:
            if item_visitor:
                item = item_visitor(item)

            cmd, vbucket_id, key, flg, exp, cas, val = item

            if cmd == memcacheConstants.CMD_TAP_MUTATION:
                # <op> <key> <flags> <exptime> <bytes> [noreply]\r\n
                op = 'set'
                if use_add:
                    op = 'add'
                stdout.write("%s %s %s %s %s\r\n" %
                             (op, key, flg, exp, len(val)))
                stdout.write(val)
                stdout.write("\r\n")
            elif cmd == memcacheConstants.CMD_TAP_DELETE:
                stdout.write("delete %s\r\n" % (key))
            else:
                "error: StdOutSink - unknown cmd: " + str(cmd), None

        future = SinkBatchFuture(self, batch)
        self.future_done(future, 0)
        return 0, future


# --------------------------------------------------

CMD_STR = {
    memcacheConstants.CMD_TAP_CONNECT: "TAP_CONNECT",
    memcacheConstants.CMD_TAP_MUTATION: "TAP_MUTATION",
    memcacheConstants.CMD_TAP_DELETE: "TAP_DELETE",
    memcacheConstants.CMD_TAP_FLUSH: "TAP_FLUSH",
    memcacheConstants.CMD_TAP_OPAQUE: "TAP_OPAQUE",
    memcacheConstants.CMD_TAP_VBUCKET_SET: "TAP_VBUCKET_SET",
    memcacheConstants.CMD_TAP_CHECKPOINT_START: "TAP_CHECKPOINT_START",
    memcacheConstants.CMD_TAP_CHECKPOINT_END: "TAP_CHECKPOINT_END",
    memcacheConstants.CMD_NOOP: "NOOP"
}

def parse_spec(opts, spec, port):
    """Parse host, port, username, password, path from opts and spec."""

    # Example spec: http://Administrator:password@HOST:8091
    p = urlparse.urlparse(spec)

    # Example netloc: Administrator:password@HOST:8091
    netloc = p.netloc

    if not netloc: # When urlparse() can't parse non-http URI's.
        netloc = spec.split('://')[-1].split('/')[0]

    list = netloc.split('@')
    host = (list[-1] + ":" + str(port)).split(':')[0]
    port = (list[-1] + ":" + str(port)).split(':')[1]

    username = opts.username
    password = opts.password
    if len(list) > 1:
        username = username or (list[0] + ':').split(':')[0]
        password = password or (list[0] + ':').split(':')[1]

    return host, port, username, password, p.path

def rest_connect(host, port, user, pswd, path):
    logging.debug("rest_connect: %s@%s:%s%s" % (user, host, port, path))

    conn = httplib.HTTPConnection(host, port)
    try:
        conn.request('GET', path, '', rest_headers(user, pswd))
    except:
        return "error: could not access REST API: " + host + ":" + str(port) + \
            " - please check source URL, username (-u) and password (-p)", \
            None, None

    resp = conn.getresponse()
    if resp.status in [200, 201, 202, 204, 302]:
        return None, conn, resp.read()

    conn.close()

    if resp.status == 401:
        return "error: unable to access REST API" \
            " - please check the username (-u) and password (-p)", \
            None, None

    return "error: unable to access REST API" \
        " - please check your URL and server", \
        None, None

def rest_headers(user, pswd, headers={}):
    if user:
        auth = 'Basic ' + \
            string.strip(base64.encodestring(user + ':' + (pswd or '')))
        headers['Authorization'] = auth
    return headers

def rest_couchbase(opts, spec):
    spec = spec.replace('couchbase://', 'http://')

    spec_parts = parse_spec(opts, spec, 8091)
    host, port, user, pswd, path = spec_parts

    if not path or path == '/':
        path = '/pools/default/buckets'

    err, conn, rest_json = rest_connect(host, port, user, pswd, path)
    if err:
        return err, None
    if conn:
        conn.close()
    try:
        rest_data = json.loads(rest_json)
    except:
        return "error: could not decode JSON from REST API: " + spec + \
            " - please check URL, username (-u) and password (-p)", None

    if type(rest_data) == type([]):
        rest_buckets = rest_data
    else:
        rest_buckets = [ rest_data ] # Wrap single bucket in a list.

    buckets = []

    for bucket in rest_buckets:
        if not (bucket and
                bucket['name'] and bucket['bucketType'] and
                bucket['nodes'] and bucket['nodeLocator'] and
                bucket['vBucketServerMap']):
            return "error: unexpected JSON value from: " + spec + \
                " - did you provide the right source URL?", None

        if bucket['nodeLocator'] == 'vbucket' and \
                (bucket['bucketType'] == 'membase' or
                 bucket['bucketType'] == 'couchbase'):
            buckets.append(bucket)
        else:
            logging.warn("skipping bucket that is not a couchbase-bucket: " +
                         bucket['name'])

    return 0, { 'spec': spec,
                'buckets': buckets,
                'rest_json': rest_json,
                'rest_data': rest_data,
                'spec_parts' : spec_parts }

