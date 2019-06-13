#!/usr/bin/env python

import csv
import logging
import os
import json
import sys
import struct
import urllib.request, urllib.parse, urllib.error

from typing import Tuple, Union, Any, Dict, List, Optional
from ast import literal_eval

import couchbaseConstants
import pump
import snappy # pylint: disable=import-error

# Our max document size is 20MB, but let's make this extra large in case the
# are spaces and such that can be removed before sending to Couchbase.
csv.field_size_limit(100 * 1024 * 1024)


def number_try_parse(s):
    for func in (int, float):
        try:
            v = func(s)
            if s == str(v):
                return v
        except ValueError:
            pass
    return s


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
    def can_handle(opts, spec: str) -> bool:
        return spec.endswith(".csv") and os.path.isfile(spec)

    @staticmethod
    def check(opts, spec: str) -> Tuple[couchbaseConstants.PUMP_ERROR, Dict[str, Any]]:
        return 0, {'spec': spec,
                   'buckets': [{'name': os.path.basename(spec),
                                'nodes': [{'hostname': 'N/A'}]}]}

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                              Optional[str]]:
        return 0, None

    def provide_batch(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch]]:
        if self.done:
            return 0, None

        if not self.r:
            try:
                self.r = csv.reader(open(self.spec, 'r', encoding='utf-8'))
                self.fields = next(self.r)
                if not 'id' in self.fields:
                    return f'error: no \'id\' field in 1st line of csv: {self.spec}', None
            except StopIteration:
                return f'error: could not read 1st line of csv: {self.spec}', None
            except IOError as e:
                return f'error: could not open csv: {self.spec}; exception: {e!s}', None

        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        cmd = couchbaseConstants.CMD_TAP_MUTATION
        vbucket_id = 0x0000ffff

        while (self.r and
               batch.size() < batch_max_size and
               batch.bytes < batch_max_bytes):
            try:
                vals = next(self.r)
                doc = {}
                for i, field in enumerate(self.fields):
                    if i >= len(vals):
                        continue
                    if field == 'id':
                        doc[field] = vals[i]
                    else:
                        doc[field] = number_try_parse(vals[i])
                if doc['id']:
                    msg: couchbaseConstants.BATCH_MSG = (cmd, vbucket_id, doc['id'].encode(), 0, 0, 0, b'',
                                                         literal_eval(doc['value']), 0, 0, 0, 0)
                    batch.append(msg, len(doc))
            except StopIteration:
                self.done = True
                self.r = None
            except Exception as e:
                logging.error(f'error: fails to read from csv file {e}')
                continue

        if batch.size() <= 0:
            return 0, None
        return 0, batch


class CSVSink(pump.Sink):
    """Emits batches to stdout in CSV format."""
    CSV_SCHEME = "csv:"
    CSV_JSON_SCHEME = "csv-json:"

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(CSVSink, self).__init__(opts, spec, source_bucket, source_node,
                                      source_map, sink_map, ctl, cur)
        self.csvfile = None
        self.writer = None
        self.fields = None

    def bucket_name(self) -> str:
        if 'name' in self.source_bucket:
            return self.source_bucket['name']
        else:
            return ""

    def node_name(self) -> str:
        if 'hostname' in self.source_node:
            return self.source_node['hostname']
        else:
            return ""

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        if spec.startswith(CSVSink.CSV_SCHEME) or spec.startswith(CSVSink.CSV_JSON_SCHEME):
            opts.threads = 1  # Force 1 thread to not overlap stdout.
            return True
        return False

    @staticmethod
    def check(opts, spec, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR, Any]:
        rv: couchbaseConstants.PUMP_ERROR = 0
        if spec.endswith(".csv"):
            if spec.startswith(CSVSink.CSV_JSON_SCHEME):
                targetpath = spec[len(CSVSink.CSV_JSON_SCHEME):]
            else:
                targetpath = spec[len(CSVSink.CSV_SCHEME):]
            targetpath = os.path.normpath(targetpath)
            rv = pump.mkdirs(targetpath)

        return rv, None

    @staticmethod
    def consume_design(opts, sink_spec, sink_map,
                       source_bucket, source_map, source_design) -> couchbaseConstants.PUMP_ERROR:
        if source_design:
            logging.warn("warning: cannot save bucket design"
                         " on a CSV destination")
        return 0

    def consume_batch_async(self, batch: pump.Batch) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                              Optional[pump.SinkBatchFuture]]:
        if not self.writer:
            self.csvfile = sys.stdout
            if self.spec.startswith(CSVSink.CSV_JSON_SCHEME):
                if len(batch.msgs) <= 0:
                    future = pump.SinkBatchFuture(self, batch)
                    self.future_done(future, 0)
                    return 0, future

                cmd, vbucket_id, key, flg, exp, cas, meta, val_bytes = batch.msgs[0][:8]
                doc = json.loads(val_bytes)
                self.fields = sorted(doc.keys())
                if 'id' not in self.fields:
                    self.fields = ['id'] + self.fields
                if self.spec.endswith(".csv"):
                    filename = self.get_csvfile(self.spec[len(CSVSink.CSV_JSON_SCHEME):])
                    try:
                        self.csvfile = open(filename, "w", encoding='utf-8')
                    except IOError as e:
                        return f'error: could not write csv to file: {filename}', None
                self.writer = csv.writer(self.csvfile)
                self.writer.writerow(self.fields)
            else:
                if self.spec.endswith(".csv"):
                    filename = self.get_csvfile(self.spec[len(CSVSink.CSV_SCHEME):])
                    try:
                        self.csvfile = open(filename, "w",  encoding='utf-8')
                    except IOError as e:
                        return f'error: could not write csv to file: {filename}', None
                self.writer = csv.writer(self.csvfile)
                self.writer.writerow(['id', 'flags', 'expiration', 'cas', 'value', 'rev', 'vbid', 'dtype'])
        msg_tuple_format = 0
        for msg in batch.msgs:
            cmd, vbucket_id, key, flg, exp, cas, meta, val_bytes = msg[:8]
            if self.skip(key, vbucket_id):
                continue
            if not msg_tuple_format:
                msg_tuple_format = len(msg)
            seqno = dtype = nmeta = 0
            if msg_tuple_format > 8:
                seqno, dtype, nmeta, conf_res = msg[8:12]
            if dtype > 2:
                try:
                    val_bytes = snappy.uncompress(val_bytes)
                except Exception as err:
                    pass
            try:
                if cmd in [couchbaseConstants.CMD_TAP_MUTATION,
                           couchbaseConstants.CMD_DCP_MUTATION]:
                    if self.fields:
                        if val_bytes and len(val_bytes) > 0:
                            try:
                                row = []
                                doc = json.loads(val_bytes)
                                if type(doc) == dict:
                                    for field in self.fields:
                                        if field == 'id':
                                            row.append(pump.returnString(key))
                                        else:
                                            row.append(doc[field])
                                    self.writer.writerow(row)
                            except ValueError:
                                pass
                    else:
                        #rev = self.convert_meta(meta)
                        self.writer.writerow([pump.returnString(key), flg, exp, cas, val_bytes, meta, vbucket_id, dtype])
                elif cmd in [couchbaseConstants.CMD_TAP_DELETE, couchbaseConstants.CMD_DCP_DELETE]:
                    pass
                elif cmd == couchbaseConstants.CMD_GET:
                    pass
                else:
                    return f'error: CSVSink - unknown cmd: {cmd!s}', None
            except IOError:
                return "error: could not write csv to stdout", None

        future = pump.SinkBatchFuture(self, batch)
        self.future_done(future, 0)
        return 0, future

    def close(self):
        if self.csvfile is not None and self.csvfile != sys.stdout:
            self.csvfile.close()
            self.csvfile = None

    def get_csvfile(self, base: str) -> str:
        extension = os.path.splitext(base)
        filename = extension[0]
        if self.bucket_name():
            filename = f'{filename}_{urllib.parse.quote_plus(self.bucket_name())}'
        if self.node_name():
            filename = f'{filename}_{urllib.parse.quote_plus(self.node_name())}'
        return filename + extension[1]

    def convert_meta(self, meta) -> int:
        seq_no = str(meta)
        if len(seq_no) > 8:
            seq_no = seq_no[0:8]
        if len(seq_no) < 8:
            seq_no = ('\x00\x00\x00\x00\x00\x00\x00\x00' + seq_no)[-8:]
        check_seqno, = struct.unpack(">Q", seq_no.encode())
        if not check_seqno:
            check_seqno = 1

        return check_seqno
