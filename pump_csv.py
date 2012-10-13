#!/usr/bin/env python

import csv
import logging
import os
import simplejson as json
import sys

import memcacheConstants
import pump

def number_try_parse(str):
    for func in (int, float):
        try:
            return func(str)
        except ValueError:
            pass

    return str

class CSVSource(pump.Source):
    """Reads csv file, where first line is field names and one field
       should be 'id'."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(CSVSource, self).__init__(opts, spec, source_bucket, source_node,
                                        source_map, sink_map, ctl, cur)
        self.done = False
        self.r = None # An iterator of csv.reader()

    @staticmethod
    def can_handle(opts, spec):
        return spec.endswith(".csv") and os.path.isfile(spec)

    @staticmethod
    def check(opts, spec):
        return 0, {'spec': spec,
                   'buckets': [{'name': os.path.basename(spec),
                                'nodes': [{'hostname': 'N/A'}]}]}

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        return 0, None

    def provide_batch(self):
        if self.done:
            return 0, None

        if not self.r:
            try:
                self.r = csv.reader(open(self.spec, 'rU'))
                self.fields = self.r.next()
                if not 'id' in self.fields:
                    return ("error: no 'id' field in 1st line of csv: %s" %
                            (self.spec)), None
            except StopIteration:
                return ("error: could not read 1st line of csv: %s" %
                        (self.spec)), None
            except IOError, e:
                return ("error: could not open csv: %s; exception: %s" %
                        (self.spec, e)), None

        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        cmd = memcacheConstants.CMD_TAP_MUTATION
        vbucket_id = 0x0000ffff
        cas, exp, flg = 0, 0, 0

        while (self.r and
               batch.size() < batch_max_size and
               batch.bytes < batch_max_bytes):
            try:
                vals = self.r.next()
                doc = {}
                for i, field in enumerate(self.fields):
                    if field == 'id':
                        doc[field] = vals[i]
                    else:
                        doc[field] = number_try_parse(vals[i])
                doc_json = json.dumps(doc)
                msg = (cmd, vbucket_id, doc['id'], flg, exp, cas, '', doc_json)
                batch.append(msg, len(doc))
            except StopIteration:
                self.done = True
                self.r = None

        if batch.size() <= 0:
            return 0, None
        return 0, batch


class CSVSink(pump.Sink):
    """Emits batches to stdout in CSV format."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(CSVSink, self).__init__(opts, spec, source_bucket, source_node,
                                      source_map, sink_map, ctl, cur)
        self.writer = None

    @staticmethod
    def can_handle(opts, spec):
        if spec == "csv:":
            opts.threads = 1 # Force 1 thread to not overlap stdout.
            return True
        return False

    @staticmethod
    def check(opts, spec, source_map):
        return 0, None

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design):
        if source_design:
            logging.warn("warning: cannot save bucket design"
                         " on a CSV destination")
        return 0

    def consume_batch_async(self, batch):
        if not self.writer:
            self.writer = csv.writer(sys.stdout)
            self.writer.writerow(['id', 'flags', 'expiration', 'cas', 'value'])

        for msg in batch.msgs:
            cmd, vbucket_id, key, flg, exp, cas, meta, val = msg
            if self.skip(key, vbucket_id):
                continue

            try:
                if cmd == memcacheConstants.CMD_TAP_MUTATION:
                    self.writer.writerow([key, flg, exp, cas, val])
                elif cmd == memcacheConstants.CMD_TAP_DELETE:
                    pass
                elif cmd == memcacheConstants.CMD_GET:
                    pass
                else:
                    return "error: CSVSink - unknown cmd: " + str(cmd), None
            except IOError:
                return "error: could not write csv to stdout", None

        future = pump.SinkBatchFuture(self, batch)
        self.future_done(future, 0)
        return 0, future
