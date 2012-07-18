#!/usr/bin/env python

import csv
import os
import simplejson as json

import memcacheConstants
import pump

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
    def provide_config(opts, source_spec, source_bucket, source_map):
        return 0, None

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        return 0, None

    def provide_batch(self):
        if self.done:
            return 0, None

        if not self.r:
            try:
                self.r = csv.reader(open(self.spec, 'r'))
                self.fields = self.r.next()
                if not 'id' in self.fields:
                    return ("error: no 'id' field in 1st line of csv: %s" %
                            (self.spec)), None
            except StopIteration:
                return ("error: could not read 1st line of csv: %s" %
                        (self.spec)), None
            except IOError as e:
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
                    doc[field] = vals[i]
                doc_json = json.dumps(doc)
                msg = (cmd, vbucket_id, doc['id'], flg, exp, cas, '', doc_json)
                batch.append(msg, len(doc))
            except StopIteration:
                self.done = True
                self.r = None

        if batch.size() <= 0:
            return 0, None
        return 0, batch

