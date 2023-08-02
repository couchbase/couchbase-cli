import ast
import csv
import json
import os
import sqlite3
import struct
import tempfile
import time
import unittest
import zipfile
from collections import defaultdict

import snappy
from mock_binary_server import MockMemcachedServer
from mock_server import MockRESTServer

import couchbaseConstants as cbcs
from cb_bin_client import MemcachedClient
from pump import Batch, filter_bucket_nodes
from pump_bfd import BFD, CBB_VERSION, DDOC_FILE_NAME, FTS_FILE_NAME, INDEX_FILE_NAME, BFDSource
from pump_bfd2 import BFDSinkEx
from pump_csv import CSVSink, CSVSource
from pump_dcp import DCPStreamSource
from pump_gen import GenSource
from pump_json import JSONSource
from pump_mc import MCSink


# ----------------- support classes -----------------
class Ditto:
    def __init__(self, vals):
        for k, v in vals.items():
            setattr(self, k, v)

    def transform(self, vals):
        for k, v in vals.items():
            setattr(self, k, v)

# -----------------  Source tests -------------------
# TODO(cg14823): workout couchstore import before doing SFD store tes


class TestCSVSource(unittest.TestCase):
    def setUp(self):
        self.source = CSVSource(None, None, None, None, None, None, None, None)
        self.opts = Ditto({'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000}})

    def test_can_handle(self):
        # Test valid name but non-existent file
        self.assertFalse(self.source.can_handle(None, "some.csv"))
        # Test existent non csv file
        with tempfile.NamedTemporaryFile() as f:
            self.assertFalse(self.source.can_handle(None, f.name))
        # Test existent csv file
        with tempfile.NamedTemporaryFile(suffix=".csv") as f:
            self.assertTrue(self.source.can_handle(None, f.name))

    def test_check(self):
        # Test check with no speck used general exception as depending on python version it will raise attribute error
        # or type error
        with self.assertRaises(Exception):
            self.source.check(None, None)

        with tempfile.NamedTemporaryFile(suffix=".csv") as f:
            rv, v = self.source.check(None, f.name)
            self.assertEqual(0, rv)
            self.assertEqual(v, {'spec': f.name, 'buckets': [{'name': os.path.basename(f.name),
                                                              'nodes': [{'hostname': 'N/A'}]}]})

    def test_provide_batch_nonexistent_file(self):
        # non-existent file name
        file_name = '/tmp/this_file_does_not_exist.csv'
        self.source = CSVSource(self.opts, file_name, None, None, None, None, None, None)
        rv, _ = self.source.provide_batch()
        self.assertEqual(
            "error: could not open csv: {}; exception: [Errno 2] No such file or directory: '{}'".format(file_name,
                                                                                                         file_name), rv)

    def test_provide_batch_empty_file(self):
        with tempfile.NamedTemporaryFile(suffix=".csv") as f:
            # empty file
            self.source = CSVSource(self.opts, f.name, None, None, None, None, None, None)
            rv, _ = self.source.provide_batch()
            self.assertEqual('error: could not read 1st line of csv: {}'.format(f.name), rv)

    def test_provide_batch_no_id(self):
        with tempfile.NamedTemporaryFile(prefix='no-id', suffix=".csv", mode='w', encoding='utf-8', newline='',
                                         delete=False) as f:
            # No id column
            writer = csv.DictWriter(f, fieldnames=['f1', 'f2'])
            writer.writeheader()
            writer.writerow({'f1': 'v1', 'f2': 'v2'})
            name = f.name

        self.source = CSVSource(self.opts, name, None, None, None, None, None, None)
        rv, _ = self.source.provide_batch()
        os.remove(name)
        self.assertEqual('error: no \'id\' field in 1st line of csv: {}'.format(name), rv)

    def test_provide_batch_only_headers(self):
        with tempfile.NamedTemporaryFile(prefix='only-headers-', suffix=".csv", mode='w', encoding='utf-8', newline='',
                                         delete=False) as f:
            # No id column
            writer = csv.DictWriter(f, fieldnames=['id', 'f1', 'f2'])
            writer.writeheader()
            name = f.name

        self.source = CSVSource(self.opts, name, None, None, None, None, None, None)
        rv, batch = self.source.provide_batch()
        os.remove(name)
        self.assertEqual(0, rv)
        self.assertEqual(batch, None)

    def test_provide_batch(self):
        with tempfile.NamedTemporaryFile(prefix='good-batch', suffix=".csv", mode='w', encoding='utf-8', newline='',
                                         delete=False) as f:
            # No id column
            writer = csv.DictWriter(f, fieldnames=['id', 'value'])
            writer.writeheader()
            writer.writerow({'id': '0', 'value': b'v1.0'})
            writer.writerow({'id': '1', 'value': b"{'a': 1}"})
            name = f.name

        msgs = [(cbcs.CMD_TAP_MUTATION, 0x0000ffff, b'0', 0, 0, 0, b'', b'v1.0', 0, 0, 0, 0),
                (cbcs.CMD_TAP_MUTATION, 0x0000ffff, b'1', 0, 0, 0, b'', b'{\'a\': 1}', 0, 0, 0, 0)]

        self.source = CSVSource(self.opts, name, None, None, None, None, None, None)
        rv, batch = self.source.provide_batch()
        self.assertEqual(0, rv)
        self.assertEqual(batch.size(), 2)
        self.assertListEqual(batch.msgs, msgs)
        os.remove(name)


class TestJSONSource(unittest.TestCase):
    def setUp(self):
        self.source = JSONSource(None, None, None, None, None, None, None, None)
        self.opts = Ditto({'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000}})

    def test_can_handle(self):
        # Test invalid scheme
        self.assertFalse(self.source.can_handle(None, 'https://localhost:9000'))

        # Test valid scheme but does not exist directory/file
        self.assertFalse(self.source.can_handle(None, 'json:///made/up/dir'))
        self.assertFalse(self.source.can_handle(None, 'json:///maked_up_file.json'))

        # Test valid
        with tempfile.NamedTemporaryFile(suffix=".json") as f:
            self.assertTrue(self.source.can_handle(None, 'json://{0}'.format(f.name)))

        with tempfile.TemporaryDirectory() as tmpdirname:
            self.assertTrue(self.source.can_handle(None, 'json://{0}'.format(tmpdirname)))

    def test_save_doc(self):
        batch = Batch(self.source)
        self.source.save_doc(batch, 'KEY:0', json.dumps({"field": "value"}))
        msgs = [
            (cbcs.CMD_TAP_MUTATION, 0x0000ffff, 'KEY:0', 0x02000006, 0, 0, b'',
             json.dumps({"field": "value"}), 0, 0, 0, 0)
        ]
        self.assertEqual(batch.size(), 1)
        self.assertListEqual(msgs, batch.msgs)

    def test_gen_dockey(self):
        self.assertEqual('filename', self.source.gen_dockey('/some/filename.json'))

    def test_enumerate_files(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            f1 = open(os.path.join(tmpdirname, 'design_docs'), 'w')
            f1.close()
            f_can = []
            self.source.enumerate_files(tmpdirname, f_can, False, False)
            self.assertEqual(f_can, [os.path.join(tmpdirname, 'design_docs')])
            os.remove(os.path.join(tmpdirname, 'design_docs'))

    def test_provide_design_dir(self):
        # Test scenario where design is in a directory
        with tempfile.TemporaryDirectory() as tmpdirname:
            f1 = open(os.path.join(tmpdirname, 'design_docs'), 'w')
            f1.write('design doc')
            f1.close()
            rv, des = self.source.provide_design(None, 'json://' + tmpdirname, None, None)
            self.assertEqual(rv, 0)
            self.assertListEqual(des, [b'design doc'])
            os.remove(os.path.join(tmpdirname, 'design_docs'))

        # Test scenario where no design doc in the dir
        with tempfile.TemporaryDirectory() as tmpdirname:
            rv, des = self.source.provide_design(None, 'json://' + tmpdirname, None, None)
            self.assertEqual(rv, 0)
            self.assertListEqual(des, [])

    def test_provide_design_zip(self):
        # Test scenario where design is in a zip structure should be :
        with tempfile.TemporaryDirectory() as tmpdirname:
            f1 = open(os.path.join(tmpdirname, 'design_docs'), 'w')
            f1.write('design doc')
            fname = f1.name
            f1.close()

            zipF = zipfile.ZipFile(os.path.join(tmpdirname, 'test_zip.zip'), 'w')
            zipF.write(fname, 'design_docs' + os.path.sep + 'design_doc', zipfile.ZIP_DEFLATED)
            zipF.close()

            rv, des = self.source.provide_design(None, 'json://' + os.path.join(tmpdirname, 'test_zip.zip'), None, None)
            self.assertEqual(rv, 0)
            self.assertListEqual(des, [b'design doc'])
            os.remove(fname)
            os.remove(os.path.join(tmpdirname, 'test_zip.zip'))

    def test_provide_batch_dir(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            # create directory
            os.mkdir(os.path.join(tmpdirname, 'docs'))
            # create json data
            f1 = open(os.path.join(tmpdirname, 'docs', 'doc0.json'), 'w')
            json.dump({'string': 'value', 'number': 1}, f1)
            f1.close()

            f2 = open(os.path.join(tmpdirname, 'docs', 'doc1'), 'w')
            json.dump({'bool': True, 'list': [0, 1, 2]}, f2)
            f2.close()

            self.source = JSONSource(self.opts, 'json://' + tmpdirname, None, None, None, None, None, None)
            rv, batch = self.source.provide_batch()
            self.assertEqual(rv, 0)
            self.assertEqual(batch.size(), 2)
            expected_msgs = [
                (cbcs.CMD_TAP_MUTATION, 0x0000ffff, 'doc0', 0x02000006, 0, 0, '',
                 json.dumps({'string': 'value', 'number': 1}), 0, 0, 0, 0),
                (cbcs.CMD_TAP_MUTATION, 0x0000ffff, 'doc1', 0x02000006, 0, 0, '',
                 json.dumps({'bool': True, 'list': [0, 1, 2]}), 0, 0, 0, 0)
            ]
            self.assertEqual(batch.msgs.sort(key=lambda tup: tup[2]), expected_msgs.sort(key=lambda tup: tup[2]))

    def test_provide_batch_zip(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            # create json data
            f1 = open(os.path.join(tmpdirname, 'doc0.json'), 'w')
            json.dump({'string': 'value', 'number': 1}, f1)
            f1.close()

            f2 = open(os.path.join(tmpdirname, 'doc1'), 'w')
            json.dump({'bool': True, 'list': [0, 1, 2]}, f2)
            f2.close()

            # create zip
            zipF = zipfile.ZipFile(os.path.join(tmpdirname, 'test_zip.zip'), 'w')
            zipF.write(os.path.join(tmpdirname, 'doc0.json'), 'docs' + os.path.sep + 'doc0.json', zipfile.ZIP_DEFLATED)
            zipF.write(os.path.join(tmpdirname, 'doc1'), 'docs' + os.path.sep + 'doc1', zipfile.ZIP_DEFLATED)
            zipF.close()

            self.source = JSONSource(self.opts, 'json://' + os.path.join(tmpdirname, 'test_zip.zip'), None, None, None,
                                     None, None, None)
            rv, batch = self.source.provide_batch()
            self.assertEqual(rv, 0)
            self.assertEqual(batch.size(), 2)
            expected_msgs = [
                (cbcs.CMD_TAP_MUTATION, 0x0000ffff, 'doc0', 0x02000006, 0, 0, '',
                 json.dumps({'string': 'value', 'number': 1}), 0, 0, 0, 0),
                (cbcs.CMD_TAP_MUTATION, 0x0000ffff, 'doc1', 0x02000006, 0, 0, '',
                 json.dumps({'bool': True, 'list': [0, 1, 2]}), 0, 0, 0, 0)
            ]
            self.assertEqual(batch.msgs.sort(key=lambda tup: tup[2]), expected_msgs.sort(key=lambda tup: tup[2]))


class TestGenSource(unittest.TestCase):
    def setUp(self):
        self.source_map = {'cfg':
                           {'cur-ops': 0,
                            'cur-gets': 0,
                            'cur-sets': 0,
                            'cur-items': 0,
                            'exit-after-creates': True,
                            'max-items': 5,
                            'min-value-size': 10,
                            'prefix': "",
                            'ratio-sets': 1,
                            'json': True,
                            'low-compression': False,
                            'xattr': False}
                           }
        self.opts = Ditto({'collection': False, 'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000}})
        self.source = GenSource(self.opts, None, None, None, self.source_map, None, None, None)

    def test_can_handle(self):
        self.assertFalse(self.source.can_handle(None, 'http://localhost:9000'))
        self.assertTrue(self.source.can_handle(None, 'gen:'))

    def test_provide_batch_json_all_sets_stop_after_creation(self):
        rv, batch = self.source.provide_batch()
        self.assertEqual(rv, 0)
        self.assertEqual(batch.size(), 5)
        for v in batch.msgs:
            self.assertEqual(v[0], cbcs.CMD_DCP_MUTATION)

    def test_provide_batch_json_all_sets_stop_after_creation_xattr(self):
        self.source_map['cfg']['xattr'] = True
        self.source = GenSource(self.opts, None, None, None, self.source_map, None, None, None)

        rv, batch = self.source.provide_batch()
        self.assertEqual(rv, 0)
        self.assertEqual(batch.size(), 5)
        for v in batch.msgs:
            self.assertEqual(v[0], cbcs.CMD_SUBDOC_MULTIPATH_MUTATION)
            self.assertTrue(b'obj' in v[7])
            self.assertTrue(b'xattr_f' in v[7])
            self.assertTrue(b'xattr_v' in v[7])

    def test_provide_batch_invalid_collection_id(self):
        self.opts.collection = '_default'
        self.source = GenSource(self.opts, None, None, None, self.source_map, None, None, None)

        rv, batch = self.source.provide_batch()
        self.assertNotEqual(0, rv, 'expected an error to be returned')
        self.assertEqual(batch, None, 'expected batch to be none')
        self.assertIn('Invalid collection id', rv, 'expected invalid collection id error message')

    def test_provide_batch_some_gets(self):
        """This test ensures that the gets generated by cbworkloadgen have the correct datatype"""
        self.source_map['cfg']['ratio-sets'] = 0.5
        self.source_map['cfg']['max-items'] = 10
        self.source = GenSource(self.opts, None, None, None, self.source_map, None, None, None)

        rv, batch = self.source.provide_batch()
        self.assertEqual(rv, 0)
        self.assertGreater(batch.size(), 10)

        gets = 0
        for v in batch.msgs:
            self.assertIn(v[0], [cbcs.CMD_GET, cbcs.CMD_DCP_MUTATION])
            if v[0] == cbcs.CMD_GET:
                self.assertEqual(v[9], 0x00)
                gets += 1
            else:
                self.assertEqual(v[9], cbcs.DATATYPE_JSON)

        self.assertGreater(gets, 0)


class TestBFDSource(unittest.TestCase):
    def setUp(self):
        self.source = BFDSource(None, None, None, None, None, None, None, None)
        self.opts = Ditto({'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000}})

    @staticmethod
    def create_folder_struct(base, mode, base_time='1996-10-07T070000Z', time='1996-10-07T070000Z', bucket='default'):
        path = os.path.join(base, base_time, time + '-' + mode, 'bucket-' + bucket, 'node-1')
        os.makedirs(path, exist_ok=True)
        f = open(os.path.join(path, 'data-1.cbb'), 'w')
        f.close()
        return path

    @staticmethod
    def _create_table_and_return(file):
        conn = sqlite3.connect(file)
        cur = conn.cursor()
        cur.execute(''' CREATE TABLE cbb_msg
        (cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, meta_size, conf_res)''')
        cur.execute('pragma user_version={}'.format(CBB_VERSION[2]))
        conn.commit()
        return cur, conn

    @staticmethod
    def _insert_data(curr, conn, test_data=[]):
        s = "INSERT INTO cbb_msg (cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, \
                               dtype, meta_size, conf_res) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        for d in test_data:
            curr.execute(s, (d[0], d[1],
                             sqlite3.Binary(d[2]),
                             d[3], d[4], str(d[5]),
                             sqlite3.Binary(d[6]),
                             sqlite3.Binary(d[7]),
                             d[8], d[9], d[10],
                             d[11]))
        conn.commit()

    def test_can_handle(self):
        self.assertFalse(self.source.can_handle(None, 'http://localhost:9000'))
        self.assertFalse(self.source.can_handle(None, 'couchstore-files://data/'))
        self.assertFalse(self.source.can_handle(None, '/made/up/dir'))
        with tempfile.TemporaryDirectory() as tmpdirname:
            self.assertFalse(self.source.can_handle(None, tmpdirname))
            self.create_folder_struct(tmpdirname, 'full')
            self.assertTrue(self.source.can_handle(None, tmpdirname))

    def test_check(self):
        # valid cbbackup dir structure
        with tempfile.TemporaryDirectory() as tmpdirname, self.subTest(i='valid dir structure'):
            self.create_folder_struct(tmpdirname, 'full')
            rv, v = self.source.check(None, tmpdirname)
            self.assertEqual(0, rv)
            self.assertEqual(v['buckets'], [{'name': b'default', 'nodes': [{'hostname': b'1'}]}])

        # not existent dir
        with self.subTest(i='no dir structure'):
            rv, _ = self.source.check(None, '/made/up/dir')
            self.assertEqual(rv, "error: backup_dir is not a directory: /made/up/dir")

        # empty dir
        with tempfile.TemporaryDirectory() as tmpdirname, self.subTest(i='empty dir structure'):
            rv, _ = self.source.check(None, tmpdirname)
            self.assertEqual(rv, "error: no backup directory found: " + tmpdirname)

        # Valid structure but with no full backup
        with tempfile.TemporaryDirectory() as tmpdirname, self.subTest(i='valid dir structure, no full backup'):
            self.create_folder_struct(tmpdirname, 'accu')
            rv, _ = self.source.check(None, tmpdirname)
            self.assertEqual(rv, "error: no valid backup directory found: " + tmpdirname + '/1996-10-07T070000Z')

    def test_provide_json_files(self):
        for (file, fn) in [(DDOC_FILE_NAME, self.source.provide_design),
                           (INDEX_FILE_NAME, self.source.provide_index),
                           (FTS_FILE_NAME, self.source.provide_fts_index)]:
            with tempfile.TemporaryDirectory() as tmpdirname, self.subTest(i=file):
                path = self.create_folder_struct(tmpdirname, 'full')
                bucket_level, _ = os.path.split(path)
                ddoc = open(os.path.join(bucket_level, file), 'w')
                ddoc.write('the file')
                ddoc.close()

                rv, returned_ddoc = fn(None, tmpdirname, {'name': b'default', 'nodes': [{'hostname': b'1'}]}, None)
                self.assertEqual(rv, 0)
                self.assertEqual(returned_ddoc, 'the file')

    def test_provide_batch_one_full(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            # setup the data for the test
            path = self.create_folder_struct(tmpdirname, 'full')
            curr, conn = self._create_table_and_return(os.path.join(path, 'data-1.cbb'))
            test_data = [
                (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 0, 0, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 1, 'KEY:1'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 10, 0, 0, 0),
            ]

            self._insert_data(curr, conn, test_data)
            curr.close()
            conn.close()

            self.source = BFDSource(self.opts, tmpdirname, {'name': b'default', 'nodes': [{'hostname': b'1'}]},
                                    {'hostname': b'1'}, None, None, None, None)
            rv, batch = self.source.provide_batch()
            self.assertEqual(rv, 0, 'Unexpected error: {}'.format(rv))
            self.assertEqual(batch.size(), len(test_data))
            self.assertListEqual(batch.msgs, test_data)

    def test_provide_batch_one_full_one_incr(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            # setup the data for the full
            path = self.create_folder_struct(tmpdirname, 'full')
            curr, conn = self._create_table_and_return(os.path.join(path, 'data-1.cbb'))
            test_data = [
                (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 0, 0, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 1, 'KEY:1'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 10, 0, 0, 0),
            ]

            self._insert_data(curr, conn, test_data)
            curr.close()
            conn.close()

            # setup data for the incremental
            path = self.create_folder_struct(tmpdirname, 'diff', time='1996-11-21T130000Z')
            curr, conn = self._create_table_and_return(os.path.join(path, 'data-1.cbb'))
            test_data2 = [
                (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'new_value'}).encode(), 25, 0, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 1, 'KEY:8'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 27, 0, 0, 0),
            ]

            self._insert_data(curr, conn, test_data2)
            curr.close()
            conn.close()
            expected_out = [
                (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 0, 0, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 1, 'KEY:1'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 10, 0, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'new_value'}).encode(), 25, 0, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 1, 'KEY:8'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 27, 0, 0, 0)
            ]

            self.source = BFDSource(self.opts, tmpdirname, {'name': b'default', 'nodes': [{'hostname': b'1'}]},
                                    {'hostname': b'1'}, None, None, None, None)
            rv, batch = self.source.provide_batch()
            self.assertEqual(rv, 0, 'Unexpected error: {}'.format(rv))
            self.assertEqual(batch.size(), len(expected_out))
            self.assertListEqual(batch.msgs, expected_out)


class DCPHelperClass:
    def __init__(self, responses=[], vbucket=0):
        self.vbucket = vbucket
        self.msgs = []
        self.responses = responses

    def _send_cmd(self, cmd, key, val, opaque, extra_header=b'', cas=0, extra_meta=b''):
        self._send_msg(cmd, key, val, opaque, extra_header=extra_header, cas=cas, vbucket_id=self.vbucket,
                       extra_meta=extra_meta)

    def _send_msg(self, cmd, key, val, opaque, extra_header=b'', cas=0, dtype=0, vbucket_id=0, extra_meta=b'',
                  fmt=cbcs.REQ_PKT_FMT, magic=cbcs.REQ_MAGIC_BYTE):
        self.msgs.append((cmd, key, val, opaque, extra_header, cas, dtype, vbucket_id, extra_meta, fmt, magic))

    def get(self):
        if len(self.responses) == 0:
            return None
        val = self.responses[0]
        self.responses = self.responses[1:]
        return val

    def empty(self):
        return len(self.responses) == 0

    def _set_response(self, response):
        self.responses = response


class TestDCPSource(unittest.TestCase):
    def setUp(self):
        self.opts = Ditto({'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000}, 'process_name': 'test'})
        self.source = DCPStreamSource(self.opts, '', None, {'version': '0.0.0-0000-enterprise'}, None, None, None, None)

    def test_can_handle(self):
        self.assertFalse(DCPStreamSource.can_handle(None, ''))
        self.assertFalse(DCPStreamSource.can_handle(None, 'localhost'))
        self.assertTrue(DCPStreamSource.can_handle(None, 'http://localhost'))
        self.assertTrue(DCPStreamSource.can_handle(None, 'https://localhost'))
        self.assertTrue(DCPStreamSource.can_handle(None, 'couchbase://localhost'))

    def test_set_ssl_automatically(self):
        self.source = DCPStreamSource(self.opts, 'https://localhsot:8091', None, {'version': '0.0.0-0000-enterprise'},
                                      None, None, None, None)
        self.assertTrue(self.source.opts.ssl)

    def test_provide_index(self):
        server = MockRESTServer('127.0.0.1', 9211)
        server_args = {'/pools/default/nodeServices': {'nodesExt': [{'services': {'indexHttp': 9211}}]},
                       '/getIndexMetadata': {'result': 'INDEX'}}
        server.set_args(server_args)
        server.run()
        opts = Ditto({'username': 'Administrator', 'password': 'asdasd',
                     'ssl': False, 'no_ssl_verify': True, 'cacert': None})
        error, index = DCPStreamSource.provide_index(opts, 'http://127.0.0.1:9211', {'name': 'default'}, None)
        server.shutdown()
        self.assertEqual(0, error, 'Got unexpected error: {}'.format(error))
        self.assertEqual(index, '"INDEX"')
        self.assertEqual(server_args['query'], 'bucket=default')

    def test_provide_fts_index(self):
        server = MockRESTServer('127.0.0.1', 9211)
        server_args = {'/pools/default/nodeServices': {'nodesExt': [{'services': {'fts': 9211}}]},
                       '/api/index': {'indexDefs': {'indexDefs': {
                           'a': {'type': 'fulltext-index', 'sourceName': 'default'},
                           'b': {'type': 'INVALID', 'sourceName': 'INVALID'}
                       }}}}
        server.set_args(server_args)
        server.run()
        opts = Ditto({'username': 'Administrator', 'password': 'asdasd',
                     'ssl': False, 'no_ssl_verify': True, 'cacert': None})
        error, index = DCPStreamSource.provide_fts_index(opts, 'http://127.0.0.1:9211', {'name': 'default'}, None)
        server.shutdown()
        self.assertEqual(0, error, 'Got unexpected error: {}'.format(error))
        self.assertListEqual(json.loads(index), [{'type': 'fulltext-index', 'sourceName': 'default'}])

    def test_request_dcp_streams(self):
        helper_class = DCPHelperClass()
        self.source = DCPStreamSource(self.opts, 'http://localhost:8091', None, {'version': '0.0.0-0000-enterprise'},
                                      None, None, None, None)
        self.source.dcp_conn = helper_class
        vbid = 0
        flags = 0
        start_seqno = 0
        end_seqno = 0
        vb_uuid = 1
        self.source.request_dcp_stream(vbid, flags, start_seqno, end_seqno, vb_uuid, start_seqno, end_seqno)
        self.assertEqual(len(self.source.stream_list), 1)
        self.assertEqual(self.source.stream_list[vbid], (vbid, flags, start_seqno, end_seqno, vb_uuid,
                                                         start_seqno, end_seqno))
        self.assertEqual(len(helper_class.msgs), 1)
        cmd = helper_class.msgs[0][0]
        self.assertEqual(cmd, cbcs.CMD_DCP_REQUEST_STREAM)

    def test_provide_dcp_batch_actual_no_data_transfer(self):
        helper_class = DCPHelperClass()
        self.source = DCPStreamSource(self.opts, 'http://localhost:9112', None, {'version': '0.0.0-0000-enterprise'},
                                      None, None, None, None)
        self.source.stream_list = ['stream_1']
        self.source.response = helper_class
        self.source.dcp_conn = helper_class
        error, batch = self.source.provide_dcp_batch_actual()
        self.assertEqual(error, 0)
        self.assertEqual(batch, None)
        self.assertTrue(self.source.dcp_done)

    def test_provide_batch_uncompress(self):
        helper_class = DCPHelperClass()
        self.opts = Ditto({'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000, 'uncompress': 1.0},
                           'process_name': 'test'})
        self.source = DCPStreamSource(self.opts, 'http://localhost:9112', None, {'version': '0.0.0-0000-enterprise'},
                                      None, None, None, None)

        # extras is formed by seqno, rev_seqno, flg, exp, loctime, metalen, nru
        extra1 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 1, 1, 0, 0, 0, 0, 0)
        extra2 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 2, 1, 0, 0, 0, 0, 0)
        # Test MB-33810: Setting a large Rev
        extra3 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 3, 258, 0, 0, 0, 0, 0)
        data1 = extra1 + b'KEY:1' + snappy.compress(b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
        data2 = extra2 + b'KEY:2' + snappy.compress(b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')
        data3 = extra3 + b'KEY:3' + b'{"field3":"value3"}'
        # cmd, errcode, opaque, cas, keylen, extlen, data, datalen, dtype, bytes_read
        data = [
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:1'), len(extra1), data1, len(data1), cbcs.DATATYPE_COMPRESSED,
             1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:2'), len(extra2), data2, len(data2), cbcs.DATATYPE_COMPRESSED,
             1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:3'), len(extra3), data3, len(data3), 0, 1000),
            (cbcs.CMD_DCP_END_STREAM, 0, 0, 0, 0, 0, b'', 0, 0, 100)
        ]

        helper_class._set_response(data)

        self.source.stream_list = ['stream_1']
        self.source.response = helper_class
        self.source.dcp_conn = helper_class
        error, batch = self.source.provide_dcp_batch_actual()
        self.assertEqual(error, 0)
        self.assertTrue(self.source.dcp_done)
        self.assertNotEqual(batch, None)
        # should receive a buffer ack message when finish consuming the data
        self.assertEqual(len(helper_class.msgs), 1)
        self.assertEqual(helper_class.msgs[0][0], cbcs.CMD_DCP_BUFFER_ACK)
        # Batch should contain 3 mutations
        self.assertEqual(batch.size(), 3)

        expected_out = [
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:1', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]),
             b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 0, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:2', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]),
             b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 2, 0, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:3', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 1, 2]), b'{"field3":"value3"}', 3, 0,
             0, 0)
        ]

        for m in batch.msgs:
            self.assertIn(m, expected_out)

    def test_provide_dcp_batch_actual_mutations(self):
        helper_class = DCPHelperClass()
        self.source = DCPStreamSource(self.opts, 'http://localhost:9112', None, {'version': '0.0.0-0000-enterprise'},
                                      None, None, None, None)
        # Data expected by response: cmd, errcode, opaque, cas, keylen, extlen, data, datalen, dtype, bytes_read

        # extras is formed by seqno, rev_seqno, flg, exp, loctime, metalen, nru
        extra1 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 1, 1, 0, 0, 0, 0, 0)
        extra2 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 2, 1, 0, 0, 0, 0, 0)
        # Test MB-38683: MAX rev minus one
        extra3 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 3, (2**64 - 1), 0, 0, 0, 0, 0)
        data1 = extra1 + b'KEY:1' + b'{"field1":"value1"}'
        data2 = extra2 + b'KEY:2' + b'{"field2":"value2"}'
        data3 = extra3 + b'KEY:3' + b'{"field3":"value3"}'
        data = [
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:1'), len(extra1), data1, len(data1), 0, 1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:2'), len(extra2), data2, len(data2), 0, 1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:3'), len(extra3), data2, len(data3), 0, 1000),
            (cbcs.CMD_DCP_END_STREAM, 0, 0, 0, 0, 0, b'', 0, 0, 100)
        ]

        helper_class._set_response(data)

        self.source.stream_list = ['stream_1']
        self.source.response = helper_class
        self.source.dcp_conn = helper_class
        error, batch = self.source.provide_dcp_batch_actual()
        self.assertEqual(error, 0)
        self.assertTrue(self.source.dcp_done)
        self.assertNotEqual(batch, None)
        # should receive a buffer ack message when finish consuming the data
        self.assertEqual(len(helper_class.msgs), 1)
        self.assertEqual(helper_class.msgs[0][0], cbcs.CMD_DCP_BUFFER_ACK)
        # Batch should contain 3 mutations
        self.assertEqual(batch.size(), 3)

        expected_out = [(cbcs.CMD_DCP_MUTATION, 0, b'KEY:1', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]),
                         b'{"field1":"value1"}', 1, 0, 0, 0),
                        (cbcs.CMD_DCP_MUTATION, 0, b'KEY:2', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]),
                         b'{"field2":"value2"}', 2, 0, 0, 0),
                        (cbcs.CMD_DCP_MUTATION, 0, b'KEY:3', 0, 0, 0, bytes(
                            [255, 255, 255, 255, 255, 255, 255, 254]),
                         b'{"field3":"value3"}', 3, 0, 0, 0)]

        for m in batch.msgs:
            self.assertIn(m, expected_out)

    def test_provide_dcp_mutations_compression(self):
        """Test transferring out with compression see MB-44580"""
        helper_class = DCPHelperClass()
        self.source = DCPStreamSource(self.opts, 'http://localhost:9112', None, {'version': '0.0.0-0000-enterprise'},
                                      None, None, None, None)
        # Data expected by response: cmd, errcode, opaque, cas, keylen, extlen, data, datalen, dtype, bytes_read

        # extras is formed by seqno, rev_seqno, flg, exp, loctime, metalen, nru
        extra1 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 1, 1, 0, 0, 0, 0, 0)
        extra2 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 2, 1, 0, 0, 0, 0, 0)
        # Test MB-38683: MAX rev minus one
        extra3 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 3, (2 ** 64 - 1), 0, 0, 0, 0, 0)

        compressed_values = [
            snappy.compress(b'{"field1":"00000000000000000000000000000000000000000000000000000000000000000000000001"}'),
            snappy.compress(b'{"field2":"00000000000000000000000000000000000000000000000000000000000000000000000002"}'),
            snappy.compress(b'{"field3":"00000000000000000000000000000000000000000000000000000000000000000000000003"}'),
        ]
        data1 = extra1 + b'KEY:1' + compressed_values[0]
        data2 = extra2 + b'KEY:2' + compressed_values[1]
        data3 = extra3 + b'KEY:3' + compressed_values[2]
        data = [
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:1'), len(extra1), data1, len(data1), 3, 1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:2'), len(extra2), data2, len(data2), 3, 1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:3'), len(extra3), data2, len(data3), 3, 1000),
            (cbcs.CMD_DCP_END_STREAM, 0, 0, 0, 0, 0, b'', 0, 0, 100)
        ]

        helper_class._set_response(data)

        self.source.stream_list = ['stream_1']
        self.source.response = helper_class
        self.source.dcp_conn = helper_class
        error, batch = self.source.provide_dcp_batch_actual()
        self.assertEqual(error, 0)
        self.assertTrue(self.source.dcp_done)
        self.assertNotEqual(batch, None)
        # should receive a buffer ack message when finish consuming the data
        self.assertEqual(len(helper_class.msgs), 1)
        self.assertEqual(helper_class.msgs[0][0], cbcs.CMD_DCP_BUFFER_ACK)
        # Batch should contain 3 mutations
        self.assertEqual(batch.size(), 3)

        expected_out = [
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:1', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]), compressed_values[0], 1, 3,
             0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:2', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]), compressed_values[1], 2, 3,
             0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:3', 0, 0, 0, bytes([255, 255, 255, 255, 255, 255, 255, 254]),
             compressed_values[2], 3, 3, 0, 0)
        ]

        for m in batch.msgs:
            self.assertIn(m, expected_out)

    def test_provide_dcp_mutations_uncompressed(self):
        """Test transferring out can uncompress see MB-44580"""
        helper_class = DCPHelperClass()
        opts = self.opts
        opts.transform({'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000, "uncompress": 1}})
        self.source = DCPStreamSource(opts, 'http://localhost:9112', None, {'version': '0.0.0-0000-enterprise'},
                                      None, None, None, None)
        # Data expected by response: cmd, errcode, opaque, cas, keylen, extlen, data, datalen, dtype, bytes_read

        # extras is formed by seqno, rev_seqno, flg, exp, loctime, metalen, nru
        extra1 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 1, 1, 0, 0, 0, 0, 0)
        extra2 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 2, 1, 0, 0, 0, 0, 0)
        # Test MB-38683: MAX rev minus one
        extra3 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 3, (2 ** 64 - 1), 0, 0, 0, 0, 0)

        compressed_values = [
            snappy.compress(b'{"field1":"00000000000000000000000000000000000000000000000000000000000000000000000001"}'),
            snappy.compress(b'{"field2":"00000000000000000000000000000000000000000000000000000000000000000000000002"}'),
            snappy.compress(b'{"field3":"00000000000000000000000000000000000000000000000000000000000000000000000003"}'),
        ]
        data1 = extra1 + b'KEY:1' + compressed_values[0]
        data2 = extra2 + b'KEY:2' + compressed_values[1]
        data3 = extra3 + b'KEY:3' + compressed_values[2]
        data = [
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:1'), len(extra1), data1, len(data1), 3, 1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:2'), len(extra2), data2, len(data2), 3, 1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:3'), len(extra3), data2, len(data3), 3, 1000),
            (cbcs.CMD_DCP_END_STREAM, 0, 0, 0, 0, 0, b'', 0, 0, 100)
        ]

        helper_class._set_response(data)

        self.source.stream_list = ['stream_1']
        self.source.response = helper_class
        self.source.dcp_conn = helper_class
        error, batch = self.source.provide_dcp_batch_actual()
        self.assertEqual(error, 0)
        self.assertTrue(self.source.dcp_done)
        self.assertNotEqual(batch, None)
        # should receive a buffer ack message when finish consuming the data
        self.assertEqual(len(helper_class.msgs), 1)
        self.assertEqual(helper_class.msgs[0][0], cbcs.CMD_DCP_BUFFER_ACK)
        # Batch should contain 3 mutations
        self.assertEqual(batch.size(), 3)

        expected_out = [
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:1', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]),
             snappy.uncompress(compressed_values[0]), 1, 1, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:2', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]),
             snappy.uncompress(compressed_values[1]), 2, 1, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:3', 0, 0, 0, bytes([255, 255, 255, 255, 255, 255, 255, 254]),
             snappy.uncompress(compressed_values[2]), 3, 1, 0, 0)
        ]

        for m in batch.msgs:
            self.assertIn(m, expected_out)

    def test_provide_dcp_mutations_and_delitions_multiple_streams(self):
        helper_class = DCPHelperClass()
        self.source = DCPStreamSource(self.opts, 'http://localhost:9112', None, {'version': '0.0.0-0000-enterprise'},
                                      None, None, None, None)
        # Data expected by response: cmd, errcode, opaque, cas, keylen, extlen, data, datalen, dtype, bytes_read
        # the errcode for mutation/deletion is the vbid

        # extras is formed by seqno, rev_seqno, flg, exp, loctime, metalen, nru
        extra = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 1, 1, 0, 0, 0, 0, 0)
        extra2 = struct.pack(cbcs.DCP_MUTATION_PKT_FMT, 2, 1, 0, 0, 0, 0, 0)
        # extra format seqno, revs_seqno, metalen
        extra3 = struct.pack(cbcs.DCP_DELETE_PKT_FMT, 2, 1, 0)
        data1 = extra + b'KEY:0' + b'{"field":"value"}'
        data2 = extra2 + b'KEY:1' + b'{"field1":"value1"}'
        data3 = extra + b'KEY:2' + b'{"field":"value"}'
        data4 = extra3 + b'KEY:2'
        data = [
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:0'), len(extra), data1, len(data1), 0, 1000),
            (cbcs.CMD_DCP_MUTATION, 0, 0, 0, len(b'KEY:1'), len(extra2), data2, len(data2), 0, 1000),
            (cbcs.CMD_DCP_MUTATION, 1, 1, 0, len(b'KEY:2'), len(extra), data3, len(data3), 0, 1000),
            (cbcs.CMD_DCP_DELETE, 1, 1, 0, len(b'KEY:2'), len(extra3), data4, len(data4), 0, 500),
            (cbcs.CMD_DCP_END_STREAM, 0, 0, 0, 0, 0, b'', 0, 0, 100),
            (cbcs.CMD_DCP_END_STREAM, 0, 1, 0, 0, 0, b'', 0, 0, 100),
        ]

        helper_class._set_response(data)

        self.source.stream_list = {0: 'stream_1', 1: 'stream2'}
        self.source.response = helper_class
        self.source.dcp_conn = helper_class
        error, batch = self.source.provide_dcp_batch_actual()
        self.assertEqual(error, 0)
        self.assertTrue(self.source.dcp_done)
        self.assertNotEqual(batch, None)
        # should receive a buffer ack message when finish consuming the data
        self.assertEqual(len(helper_class.msgs), 1)
        self.assertEqual(helper_class.msgs[0][0], cbcs.CMD_DCP_BUFFER_ACK)
        # should contain 3 mutations and 1 deletion
        self.assertEqual(batch.size(), 4)

        expected_out = [
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:0', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]), b'{"field":"value"}', 1, 0, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:1', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]), b'{"field1":"value1"}', 2, 0, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 1, b'KEY:2', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]), b'{"field":"value"}', 1, 0, 0, 0),
            (cbcs.CMD_DCP_DELETE, 1, b'KEY:2', 0, 0, 0, bytes([0, 0, 0, 0, 0, 0, 0, 1]), b'', 2, 0, 0, 0)
        ]

        for m in batch.msgs:
            self.assertIn(m, expected_out)


class TestCSVSink(unittest.TestCase):
    def setUp(self):
        self.opts = Ditto({'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000, 'threads': 1}})
        self.sink = CSVSink(None, None, None, None, None, None, None, None)

    def test_bucket_name(self):
        with self.assertRaises(TypeError):
            self.sink.bucket_name()

        self.sink.source_bucket = {'name': 'default'}
        self.assertEqual(self.sink.bucket_name(), 'default')

    def test_node_name(self):
        with self.assertRaises(TypeError):
            self.sink.node_name()

        self.sink.source_node = {'hostname': 'default'}
        self.assertEqual(self.sink.node_name(), 'default')

    def test_can_handle(self):
        opts = Ditto({'threads': 10})
        self.assertFalse(self.sink.can_handle(None, 'http://localhost:8091'))
        self.assertFalse(self.sink.can_handle(None, '/some/file.csv'))
        self.assertTrue(self.sink.can_handle(opts, 'csv:/some/file.csv'))
        self.assertEqual(opts.threads, 1)

    def test_consume_batch(self):
        # setup sink
        with tempfile.TemporaryDirectory() as tmpdirname, self.subTest(i='basic test'):
            self.sink = CSVSink(self.opts, 'csv:' + os.path.join(tmpdirname, 'test-out.csv'),
                                {'name': 'default'}, {'hostname': 'node1'}, None, None, None, None)
            # test data
            msgs = [
                (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'new_value'}).encode(), 0, 0, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 1, 'KEY:8'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 1, 0, 0, 0),
            ]
            batch = Batch(None)
            batch.msgs = msgs

            rv, _ = self.sink.consume_batch_async(batch)
            self.assertEqual(0, rv)
            self.sink.close()

            with open(os.path.join(tmpdirname, 'test-out_default_node1.csv'), 'r') as rf:
                reader = csv.reader(rf)
                count = 0
                next(reader)  # skip headers
                for row in reader:
                    self.assertEqual(len(row), 8)
                    _, vbid, key, flg, exp, cas, meta, val, seqno, dtype = msgs[count][:10]
                    self.assertEqual(row[0], key.decode())
                    self.assertEqual(int(row[1]), flg)
                    self.assertEqual(int(row[2]), exp)
                    self.assertEqual(int(row[3]), cas)
                    self.assertEqual(ast.literal_eval(row[4]), val)
                    # self.assertEqual(row[5].encode(), meta) meta is not being stored properly at the moment
                    self.assertEqual(int(row[6]), vbid)
                    self.assertEqual(int(row[7]), dtype)
                    count += 1

                self.assertEqual(count, len(msgs))

        with tempfile.TemporaryDirectory() as tmpdirname, self.subTest(i='batch with gets'):
            self.sink = CSVSink(self.opts, 'csv:' + os.path.join(tmpdirname, 'test-out.csv'),
                                {'name': 'default'}, {'hostname': 'node1'}, None, None, None, None)
            # test data
            msgs = [
                (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'new_value'}).encode(), 0, 0, 0, 0),
                (cbcs.CMD_GET, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'new_value'}).encode(), 0, 0, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 1, 'KEY:8'.encode(), 0x02000006, 0, 0, ''.encode(),
                 json.dumps({'field': 'value'}).encode(), 1, 0, 0, 0),
            ]
            batch = Batch(None)
            batch.msgs = msgs

            skip_ix = [1]

            rv, _ = self.sink.consume_batch_async(batch)
            self.assertEqual(0, rv)
            self.sink.close()

            with open(os.path.join(tmpdirname, 'test-out_default_node1.csv'), 'r') as rf:
                reader = csv.reader(rf)
                count = 0
                next(reader)  # skip headers
                for row in reader:
                    while count in skip_ix:
                        count += 1
                    self.assertEqual(len(row), 8)
                    _, vbid, key, flg, exp, cas, meta, val, seqno, dtype = msgs[count][:10]
                    self.assertEqual(row[0], key.decode())
                    self.assertEqual(int(row[1]), flg)
                    self.assertEqual(int(row[2]), exp)
                    self.assertEqual(int(row[3]), cas)
                    self.assertEqual(ast.literal_eval(row[4]), val)
                    # meta is not being stored properly at the moment see MB-32975
                    # self.assertEqual(row[5].encode(), meta)
                    self.assertEqual(int(row[6]), vbid)
                    self.assertEqual(int(row[7]), dtype)
                    count += 1

                self.assertEqual(count, len(msgs))

    def test_consume_compressed_batch(self):
        with tempfile.TemporaryDirectory() as tmpdirname, self.subTest(i='compressed batch'):
            self.sink = CSVSink(self.opts, 'csv:' + os.path.join(tmpdirname, 'test-out.csv'),
                                {'name': 'default'}, {'hostname': 'node1'}, None, None, None, None)
            # test data
            values = [json.dumps({'field': 'new_value'}).encode(), json.dumps({'field': 'value'}).encode()]
            msgs = [
                (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, ''.encode(),
                 snappy.compress(values[0]), 0, 0x02, 0, 0),
                (cbcs.CMD_DCP_MUTATION, 1, 'KEY:8'.encode(), 0x02000006, 0, 0, ''.encode(),
                 snappy.compress(values[1]), 1, 0x02, 0, 0),
            ]
            batch = Batch(None)
            batch.msgs = msgs

            rv, _ = self.sink.consume_batch_async(batch)
            self.assertEqual(0, rv)
            self.sink.close()

            with open(os.path.join(tmpdirname, 'test-out_default_node1.csv'), 'r') as rf:
                reader = csv.reader(rf)
                count = 0
                next(reader)  # skip headers
                for i, row in enumerate(reader):
                    self.assertEqual(len(row), 8)
                    _, vbid, key, flg, exp, cas, meta, val, seqno, dtype = msgs[count][:10]
                    self.assertEqual(row[0], key.decode())
                    self.assertEqual(int(row[1]), flg)
                    self.assertEqual(int(row[2]), exp)
                    self.assertEqual(int(row[3]), cas)
                    self.assertEqual(ast.literal_eval(row[4]), values[i])
                    # meta is not being stored properly at the moment see MB-32975
                    # self.assertEqual(row[5].encode(), meta)
                    self.assertEqual(int(row[6]), vbid)
                    self.assertEqual(int(row[7]), dtype)
                    count += 1

                self.assertEqual(count, len(msgs))


class TestBFDSinkEx(unittest.TestCase):
    def setUp(self):
        self.opts = Ditto({'extra': {'batch_max_size': 100, 'batch_max_bytes': 40000}})

    def test_can_handle(self):
        self.assertFalse(BFDSinkEx.can_handle(None, 'http://localhost:8091'))
        self.assertFalse(BFDSinkEx.can_handle(None, '/some/made/up/dir'))
        with tempfile.TemporaryDirectory() as tempdirname:
            self.assertTrue(BFDSinkEx.can_handle(None, tempdirname))

    def test_check(self):
        with self.subTest(i='made up dir'):
            rv, _ = BFDSinkEx.check(None, '/made/up/dir', None)
            self.assertEqual(rv, "error: missing parent directory: /made/up")

        with tempfile.NamedTemporaryFile() as f, self.subTest(i='file given as backup dir'):
            rv, _ = BFDSinkEx.check(None, f.name, None)
            self.assertEqual(rv, "error: backup directory is not a directory: " + f.name)

        with tempfile.TemporaryDirectory() as tempdirname, self.subTest(i='given a golder'):
            rv, _ = BFDSinkEx.check(None, tempdirname, None)
            self.assertEqual(rv, 0)

    def test_consume_services_metadata(self):
        for (defs, fn, fname) in [('ddoc', BFDSinkEx.consume_design, DDOC_FILE_NAME),
                                  ('index', BFDSinkEx.consume_index, INDEX_FILE_NAME),
                                  ('fts', BFDSinkEx.consume_fts_index, FTS_FILE_NAME)]:
            with tempfile.TemporaryDirectory() as tmpdirname, self.subTest(i=defs):
                rv = fn(None, tmpdirname, None, {'name': b'default', 'nodes': [{'hostname': b'1'}]}, None, defs)
                self.assertEqual(rv, 0)
                file_path = BFD.get_file_path(tmpdirname, b'default', fname)
                f = open(file_path, 'r')
                data = f.read()
                f.close()
                self.assertEqual(defs, data)

    def test_consume_batch(self):
        msgs = [
            (cbcs.CMD_DCP_MUTATION, 0, 'KEY:0'.encode(), 0x02000006, 0, 0, json.dumps({'f': 'v'}).encode(),
             json.dumps({'field': 'new_value'}).encode(), 0, 0, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 1, 'KEY:8'.encode(), 0x02000006, 0, 0, ''.encode(),
             json.dumps({'field': 'value'}).encode(), 1, 0, 0, 0),
        ]
        test_batch = Batch(None)
        test_batch.msgs = msgs

        with tempfile.TemporaryDirectory() as tmpdirname:
            # give it a batch it should create the backup dir and populate it
            self.sink = BFDSinkEx(self.opts, tmpdirname, {'name': 'default'}, {'hostname': 'node1'},
                                  {'buckets': [{'name': 'default',
                                                'nodes': [{'hostname': 'node1', 'version': '0.0.0-0000-enterprise'}]}]},
                                  None, {'stop': False, 'new_session': True, 'new_timestamp': '1996-10-07T070000Z'},
                                  {'seqno': {}, 'failoverlog': {}, 'snapshot': {}})
            BFDSinkEx.check_spec(self.sink.source_bucket, self.sink.source_node, self.sink.opts, tmpdirname,
                                 self.sink.cur)

            rv, future = self.sink.consume_batch_async(test_batch)
            self.assertEqual(rv, 0)
            future.wait_until_consumed()
            self.assertEqual(future.done_rv, 0)
            self.sink.close()

            # check the data file was created
            expected_path = os.path.join(tmpdirname, '1996-10-07T070000Z', '1996-10-07T070000Z-full', 'bucket-default',
                                         'node-node1', 'data-0000.cbb')
            self.assertTrue(os.path.exists(expected_path))
            self.assertTrue(os.path.isfile(expected_path))

            conn = sqlite3.connect(expected_path)
            cur = conn.cursor()
            count = 0
            for row in cur.execute('SELECT cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, \
                               dtype, meta_size, conf_res FROM cbb_msg ORDER BY seqno'):
                cmd, vbid, key, flg, exp, cas, meta, val, seqno, dtype, meta_size, conf_res = msgs[count][:]
                cmd_o, vbid_o, key_o, flg_o, exp_o, cas_o, meta_o, val_o, seqno_o, dtype_o, meta_size_o, \
                    conf_res_o = row[:]
                self.assertEqual(cmd, cmd_o)
                self.assertEqual(vbid, vbid_o)
                self.assertEqual(key, key_o)
                self.assertEqual(flg, flg_o)
                self.assertEqual(exp, exp_o)
                self.assertEqual(cas, int(cas_o))
                # meta is broken as the bytes value is stored as b"b''" instead of b'' see MB-32975
                # self.assertEqual(meta, meta_o)
                self.assertEqual(val, val_o)
                self.assertEqual(seqno, seqno_o)
                self.assertEqual(dtype, dtype_o)
                self.assertEqual(meta_size, meta_size_o)
                self.assertEqual(conf_res, conf_res_o)

                count += 1

            self.assertEqual(count, len(msgs))


class FakeSocket:
    def __init__(self):
        self.sent = []
        self.received = []

    def sendall(self, msgs):
        self.sent.append(msgs)


class TestMCSink(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    def test_check_base(self):
        self.assertEqual(MCSink.check_base(Ditto({}), ''), 0)
        self.assertEqual(MCSink.check_base(Ditto({'destination_vbucket_state': 'replica'}), ''),
                         'error: only --destination-vbucket-state=active is supported by this destination: ')

    def test_append_req(self):
        opts = Ditto({'extra': {}})
        sink = MCSink(opts, 'localhost:9878', None, None, None, {'buckets': ['default']}, {'stop': False},
                      defaultdict(int))
        m = []
        requests = [
            (b'header-0', b'', b'key-0', b'val-0', b''),
            (b'header-1', b'extra-1', b'key-1', b'val-1', b''),
            (b'header-2', b'', b'key-1', b'val-1', b'extra-meta-2')
        ]

        for r in requests:
            sink.append_req(m, r)

        expected_out = []
        for (hdr, ext, key, val, extra_meta) in requests:
            expected_out.append(hdr)
            if ext:
                expected_out.append(ext)
            expected_out.append(key)
            expected_out.append(val)
            if extra_meta:
                expected_out.append(extra_meta)

        self.assertListEqual(expected_out, m)

    def test_snd_msg_sets(self):
        opts = Ditto({'extra': {}})
        fake_connection = Ditto({'s': FakeSocket()})
        sink = MCSink(opts, 'localhost:9878', None, None, None, {'buckets': ['default']}, {'stop': False},
                      defaultdict(int))

        msgs = [
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:0', 0, 0, 0, b'', b'VAL:0', 0, 0, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:1', 0, 0, 0, b'', b'VAL:1', 0, 0, 0, 0),
            (cbcs.CMD_DCP_MUTATION, 0, b'KEY:3', 0, 0, 0, b'', b'VAL:1', 0, cbcs.DATATYPE_COMPRESSED, 0, 0)
        ]
        err, skipped = sink.send_msgs(fake_connection, msgs, 'set', 0)
        self.assertEqual(err, 0)
        self.assertEqual(skipped, [])
        self.assertNotEqual(len(fake_connection.s.sent), 0)

        expected_out = b''
        i = 0
        for (_, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, nmeta, conf_res) in msgs:
            ext = struct.pack(cbcs.SET_PKT_FMT, flg, exp)
            hdr = struct.pack(cbcs.REQ_PKT_FMT, cbcs.REQ_MAGIC_BYTE, cbcs.CMD_SET, len(key), len(ext), dtype,
                              vbucket_id, len(key) + len(ext) + len(val), i, cas)
            i += 1
            expected_out += hdr + ext + key + val

        self.assertEqual(fake_connection.s.sent, [expected_out])


# ------ Memcached client tests ------

class MCHelper:
    def __init__(self, test_data={}):
        self.test_data = test_data
        self.failed = False
        self.reason = ""

    def set_test_data(self, key, val):
        self.test_data[key] = val

    @staticmethod
    def res(req, cmd, key=b'', extra_header=b'', dtype=0, errcode=0, body=b'', opaque=0, cas=0):
        res = struct.pack(cbcs.RES_PKT_FMT, cbcs.RES_MAGIC_BYTE, cmd, len(key), len(extra_header),
                          dtype, errcode, len(key) + len(body) + len(extra_header), opaque, cas)
        res = res + key + extra_header + body
        req.sendall(res)

    def correct_opaque_response_no_body(self, data, req, log=False):
        if len(data[0]) < cbcs.MIN_RECV_PACKET:
            if log:
                print('Not enough data received')
            return

        magic, cmd, keylen, extraheaden, dtype, vbid, bodylen, opaque, cas = \
            struct.unpack(cbcs.REQ_PKT_FMT, data[0][:cbcs.MIN_RECV_PACKET])

        res_data = b''
        if 'response' in self.test_data:
            res_data = self.test_data['response']

        if len(data[0]) < cbcs.MIN_RECV_PACKET + bodylen:
            if log:
                print('Body not received but we don\'t really care about it')

        self.res(req, cmd, dtype=dtype, body=res_data, opaque=opaque, cas=cas)

    def handle_sasl_auth_plain(self, data, req, log=False):
        if len(data[0]) < cbcs.MIN_RECV_PACKET:
            if log:
                print('Not enough data received')
            return

        magic, cmd, keylen, extraheaderlen, dtype, vbid, bodylen, opaque, cas = \
            struct.unpack(cbcs.REQ_PKT_FMT, data[0][:cbcs.MIN_RECV_PACKET])

        if cmd != cbcs.CMD_SASL_AUTH:
            self.failed = True
            self.reason = "Received {} expected {}".format(cmd, cbcs.CMD_SASL_AUTH)
            self.res(req, cbcs.CMD_SASL_AUTH, errcode=cbcs.ERR_EINTERNAL, body=self.reason.encode(), opaque=opaque,
                     cas=cas)
            return
        if len(data[0][cbcs.MIN_RECV_PACKET:]) < bodylen:
            self.failed = True
            self.reason = "Did not receive body"
            self.res(req, cbcs.CMD_SASL_AUTH, errcode=cbcs.ERR_EINTERNAL, body=self.reason.encode(), opaque=opaque,
                     cas=cas)
            return
        body = data[0][cbcs.MIN_RECV_PACKET:]
        key = body[:keylen]
        if key != 'PLAIN'.encode():
            self.failed = True
            self.reason = "Expected mechanism PLAIN got {}".format(key)
            self.res(req, cbcs.CMD_SASL_AUTH, errcode=cbcs.ERR_EINTERNAL, body=self.reason.encode(), opaque=opaque,
                     cas=cas)
            return

        if extraheaderlen != 0:
            self.failed = True
            self.reason = "Did not expect extra headers"
            self.res(req, cbcs.CMD_SASL_AUTH, errcode=cbcs.ERR_EINTERNAL, body=self.reason.encode(), opaque=opaque,
                     cas=cas)
            return
        user_pass = body[keylen:]
        if user_pass != b'\x00Administrator\x00asdasd':
            self.failed = True
            self.reason = "Received {} expected {}".format(user_pass, b'\x00Administrator\x00asdasd')
            self.res(req, cbcs.CMD_SASL_AUTH, errcode=cbcs.ERR_EINTERNAL, body=self.reason.encode(), opaque=opaque,
                     cas=cas)
            return

        self.res(req, cbcs.CMD_SASL_AUTH, errcode=0, body=b'SUCCESS', opaque=opaque, cas=0)

    def test_noop(self, data, req, log=False):
        if len(data[0]) < cbcs.MIN_RECV_PACKET:
            if log:
                print('Not enough data received')
            return

        magic, cmd, keylen, extraheaderlen, dtype, vbid, bodylen, opaque, cas = \
            struct.unpack(cbcs.REQ_PKT_FMT, data[0][:cbcs.MIN_RECV_PACKET])

        errcode = 0
        if cmd != cbcs.CMD_NOOP:
            self.failed = True
            self.reason = "Got command {} expected {}".format(cmd, cbcs.CMD_NOOP)
            errcode = cbcs.ERR_EINVAL
        elif bodylen != 0:
            self.failed = True
            self.reason = "Expected no body instead have body of length {}".format(bodylen)
            errcode = cbcs.ERR_EINVAL

        self.res(req, cbcs.CMD_NOOP, errcode=errcode, body=self.reason.encode(), opaque=opaque, cas=cas)

    def test_bucket_select(self, data, req, log=False):
        if len(data[0]) < cbcs.MIN_RECV_PACKET:
            if log:
                print('Not enough data received')
            return

        magic, cmd, keylen, extraheaderlen, dtype, vbid, bodylen, opaque, cas = \
            struct.unpack(cbcs.REQ_PKT_FMT, data[0][:cbcs.MIN_RECV_PACKET])

        errcode = 0
        if cmd != cbcs.CMD_SELECT_BUCKET:
            self.failed = True
            self.reason = "Got command {} expected {}".format(cmd, cbcs.CMD_SELECT_BUCKET)
            errcode = cbcs.ERR_EINVAL
        elif keylen != len(self.test_data['bucket']):
            self.failed = True
            self.reason = "Expected keylen to be {} got {}".format(len(self.test_data['bucket']), keylen)
            errcode = cbcs.ERR_EINVAL

        key = data[0][cbcs.MIN_RECV_PACKET:cbcs.MIN_RECV_PACKET + keylen]
        if not self.failed and key != self.test_data['bucket']:
            self.failed = True
            self.reason = "Expected key to be {} got {}".format(self.test_data['bucket'], key)
            errcode = cbcs.ERR_EINVAL

        self.res(req, cbcs.CMD_SELECT_BUCKET, errcode=errcode, body=self.reason.encode(), opaque=opaque, cas=cas)

    def test_helo(self, data, req, log=False):
        if len(data[0]) < cbcs.MIN_RECV_PACKET:
            if log:
                print('Not enough data received')
            return

        magic, cmd, keylen, extraheaderlen, dtype, vbid, bodylen, opaque, cas = \
            struct.unpack(cbcs.REQ_PKT_FMT, data[0][:cbcs.MIN_RECV_PACKET])

        errcode = 0
        if cmd != cbcs.CMD_HELLO:
            self.failed = True
            self.reason = "Got command {} expected {}".format(cmd, cbcs.CMD_HELLO)
            errcode = cbcs.ERR_EINVAL

        key = data[0][cbcs.MIN_RECV_PACKET:cbcs.MIN_RECV_PACKET + keylen]
        features = data[0][cbcs.MIN_RECV_PACKET + keylen:cbcs.MIN_RECV_PACKET + bodylen]
        if features != self.test_data['packed_features']:
            self.failed = True
            self.reason = "Got features {} expected {}".format(features, self.test_data['packed_features'])
            errcode = cbcs.ERR_EINVAL

        if b'couchbase-cli/' not in key:
            self.failed = True
            self.reason = f'Expected the agent to be of the format couchbase-cli/0.0.0-0000-enterprise got {key}'
            errcode = cbcs.ERR_EINVAL

        self.res(req, cbcs.CMD_SELECT_BUCKET, errcode=errcode, body=self.reason.encode(), opaque=opaque, cas=cas)


class TestMemcachedClient(unittest.TestCase):
    def setUp(self):
        self.server = MockMemcachedServer(debug=False)
        self.host, self.port = self.server.get_host_address()
        self.helper_class = MCHelper({'response': b'res_data', 'user': 'Administrator', 'pass': 'asdasd'})

    def tearDown(self):
        self.server.stop()

    def test_send_msg(self):
        self.server.start()
        client = MemcachedClient(self.host, self.port)
        client._send_msg(cbcs.CMD_DCP_MUTATION, b'KEY', b'', 1, b'', 0, 0, 0, b'')
        time.sleep(1)
        client.close()
        self.server.stop()
        expected_data = struct.pack(cbcs.REQ_PKT_FMT, cbcs.REQ_MAGIC_BYTE,
                                    cbcs.CMD_DCP_MUTATION, 3, 0, 0, 0, 3 + 0 + 0 + 0, 1, 0) + b'' + b'KEY' + b'' + b''

        data_received = self.server.server.data
        self.assertEqual(data_received[0], expected_data)

    def test_do_cmd(self):
        self.server.set_handler(self.helper_class.correct_opaque_response_no_body)
        self.server.start()

        try:
            client = MemcachedClient(self.host, self.port)
            opaque_o, cas_o, data = client._do_cmd(cbcs.CMD_DCP_MUTATION, b'KEY', b'', b'', 0)
        except Exception as e:
            client.close()
            self.server.stop()
            self.fail(e)

        client.close()
        self.server.stop()
        self.assertEqual(cas_o, 0)
        self.assertEqual(data, b'res_data')

    def test_mutate(self):
        self.server.set_handler(self.helper_class.correct_opaque_response_no_body)
        self.server.start()

        try:
            client = MemcachedClient(self.host, self.port)
            opaque_o, cas_o, data = client._mutate(cbcs.CMD_DCP_MUTATION, b'KEY', 0, 0, 0, b'blob')
        except Exception as e:
            client.close()
            self.server.stop()
            self.fail(e)

        client.close()
        self.server.stop()
        self.assertEqual(cas_o, 0)
        self.assertEqual(data, b'res_data')

    def test_sasl_auth_plain(self):
        self.server.set_handler(self.helper_class.handle_sasl_auth_plain)
        self.server.start()

        try:
            client = MemcachedClient(self.host, self.port)
            opaque_o, cas_o, data = client.sasl_auth_plain('Administrator', 'asdasd')
        except Exception as e:
            client.close()
            self.server.stop()
            self.fail(e)

        client.close()
        self.server.stop()
        self.assertEqual(cas_o, 0)
        self.assertEqual(data, b'SUCCESS')
        self.assertFalse(self.helper_class.failed, self.helper_class.reason)

    def test_noop(self):
        self.server.set_handler(self.helper_class.test_noop)
        self.server.start()

        try:
            client = MemcachedClient(self.host, self.port)
            opaque_o, cas_o, data = client.noop()
        except Exception as e:
            client.close()
            self.server.stop()
            self.fail(e)

        client.close()
        self.server.stop()
        self.assertFalse(self.helper_class.failed, self.helper_class.reason)

    def test_bucket_select(self):
        self.helper_class.set_test_data('bucket', b'default')
        self.server.set_handler(self.helper_class.test_bucket_select)
        self.server.start()

        try:
            client = MemcachedClient(self.host, self.port)
            opaque_o, cas_o, data = client.bucket_select(b'default')
        except Exception as e:
            client.close()
            self.server.stop()
            self.fail(e)

        client.close()
        self.server.stop()
        self.assertFalse(self.helper_class.failed, self.helper_class.reason)

    def test_helo(self):
        features = [cbcs.HELO_COLLECTIONS, cbcs.HELO_XATTR, cbcs.HELO_XERROR]
        self.helper_class.set_test_data('packed_features', struct.pack('>' + ('H' * len(features)), *features))
        self.server.set_handler(self.helper_class.test_helo)
        self.server.start()

        try:
            client = MemcachedClient(self.host, self.port)
            opaque_o, cas_o, data = client.helo(features)
        except Exception as e:
            client.close()
            self.server.stop()
            self.fail(e)

        client.close()
        self.server.stop()
        self.assertFalse(self.helper_class.failed, self.helper_class.reason)


class TestPumpFunctions(unittest.TestCase):
    def test_filter_bucket_nodes(self):
        test_cases = [
            [{'nodes': [{'hostname': 'SUPERHOST.com:9000'}]}, ['superhost.com', 9000],
             [{'hostname': 'SUPERHOST.com:9000'}]],
            [{'nodes': [{'hostname': 'SUPERHOST1.com:9000'}]}, ['superhost.com', 9000], []],
            [{'nodes': [{'hostname': 'superhost.com:9000'}]}, ['superhost.com', 9000],
             [{'hostname': 'superhost.com:9000'}]],
            [{'nodes': [{'hostname': '10.15.11.0:9000'}]}, ['10.15.11.0', 9000], [{'hostname': '10.15.11.0:9000'}]]
        ]

        for (i, test_case) in enumerate(test_cases):
            with self.subTest(i=i):
                self.assertListEqual(filter_bucket_nodes(test_case[0], test_case[1]), test_case[2])
