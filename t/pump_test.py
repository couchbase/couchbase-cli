#!/usr/bin/env python

"""
Unit tests for backup/restore/transfer/pump.
"""

import glob
import os
import Queue
import select
import simplejson as json
import shutil
import socket
import struct
import tempfile
import threading
import time
import unittest
import BaseHTTPServer

import pump
import pump_transfer
import pump_bfd
import pump_cb
import pump_mc
import pump_tap

import mc_bin_client
import memcacheConstants

from memcacheConstants import CMD_TAP_MUTATION
from memcacheConstants import CMD_TAP_DELETE

# TODO: (1) Sink - run() test that key & val remains intact.
# TODO: (1) Sink - run() test that flg remains intact.
# TODO: (1) Sink - run() test that exp remains intact.
# TODO: (1) test - multiple buckets.

class MockHTTPServer(BaseHTTPServer.HTTPServer):
    """Subclass that remembers the rest_server; and, SO_REUSEADDR."""

    def __init__(self, host_port, handler, rest_server):
        self.rest_server = rest_server # Instance of MockRESTServer.
        BaseHTTPServer.HTTPServer.__init__(self, host_port, handler)

    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        BaseHTTPServer.HTTPServer.server_bind(self)


class MockRESTHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """Checks that requests match the expected requests."""

    def do_GET(self):
        test = self.server.rest_server.test
        assert test, \
            "missing a test for incoming REST request: " + \
            self.command + " " + self.path

        expects = self.server.rest_server.expects
        assert len(expects) > 0, \
            "expected no more REST requests but received: " + \
            self.command + " " + self.path

        # Unshift the expected request and pre-canned response.
        request, response = expects[0]
        self.server.rest_server.expects = expects[1:]

        # Test the expected request.
        assert self.command == request['command']
        assert self.path == request['path'], self.path + " != " + request['path']

        # Send the pre-canned response.
        if response['code'] != 200:
            self.send_error(response['code'], response['message'])
        else:
            self.send_response(200)
            self.send_header("Content-Type", 'text/html')
            self.end_headers()
            self.wfile.write(response['message'])


class MockRESTServer(threading.Thread):

    def __init__(self, port):
        threading.Thread.__init__(self)

        self.daemon = True
        self.stop = False
        self.host = "127.0.0.1"
        self.port = port
        self.reset()

    def reset(self, test=None, expects=[]):
        self.test = test
        self.expects = expects

    def host_port(self):
        return self.host + ":" + str(self.port)

    def url(self):
        return "http://" + self.host_port()

    def run(self):
        host_port = ('', self.port)
        httpd = MockHTTPServer(host_port, MockRESTHandler, self)

        while not self.stop:
            try:
                httpd.handle_request()
            except:
                print "  MockRESTServer: exception"
                self.stop = True

        if httpd.socket:
            httpd.socket.close()


mrs = MockRESTServer(18091)
mrs.start()

# ------------------------------------------------

class MockMemcachedServer(threading.Thread):

    def __init__(self, port):
        threading.Thread.__init__(self)

        self.daemon = True
        self.stop = False
        self.host = "127.0.0.1"
        self.port = port
        self.backlog = 5
        self.reset()

    def reset(self, test=None):
        self.test = test
        self.sessions = {}
        self.queue = Queue.Queue(1000)

    def host_port(self):
        return self.host + ":" + str(self.port)

    def url(self):
        return "http://" + self.host_port()

    def run(self):
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind(('', self.port))
            self.server.listen(self.backlog)

            while not self.stop:
                client, address = self.server.accept()
                c = MockMemcachedSession(client, address, self)
                self.sessions[len(self.sessions)] = c
                c.start()

        except socket.error, (value, message):
            self.close()
            print "MockServer socket error: ", message
            sys.exit(1)

        self.close()


class MockMemcachedSession(threading.Thread):
    def __init__(self, client, address, server, recv_len=1024):
        threading.Thread.__init__(self)
        self.daemon = True
        self.server = server
        self.client = client
        self.address = address
        self.recv_len = recv_len
        self.loops = 0 # Number of loops without progress.
        self.loops_max = 10
        self.go = threading.Event()

    def log(self, msg):
        print self, self.server.port, msg

    def run(self):
        input = [self.client]

        self.loops = 0

        while (self.client and self.loops < self.loops_max):
            self.log("loops (" + str(self.loops) + ")")
            self.loops = self.loops + 1

            iready, oready, eready = select.select(input, [], [], 1)
            if len(eready) > 0:
                return self.close("select eready")

            if len(iready) > 0:
                self.log("recv...")
                data = self.client.recv(self.recv_len)
                if data and len(data) > 0:
                    self.loops = 0
                    self.log("recv done: " + data)
                    self.server.queue.put((self, data))
                    self.go.wait()
                    self.go.clear()
                else:
                    return self.close("recv no data")

        if self.loops >= self.loops_max:
            return self.close("loops too long")

        return self.close()

    def close(self, msg=None):
        self.log("close: " + (msg or ''))
        if self.client:
            self.client.close()
        self.client = None

        self.server.queue.put((None, None))


mms0 = MockMemcachedServer(18092)
mms0.start()

mms1 = MockMemcachedServer(18093)
mms1.start()

# ------------------------------------------------

class Worker(threading.Thread):

    def __init__(self, target, args=[]):
        threading.Thread.__init__(self, target=target, args=args, group=None)
        self.daemon = True


# ------------------------------------------------

class TestPumpingStationFind(unittest.TestCase):

    def setUp(self):
        self.find = pump.PumpingStation.find_handler

    def test_find_handlers(self):
        self.assertEqual(3, len(pump_transfer.SOURCES))
        self.assertEqual(4, len(pump_transfer.SINKS))
        self.assertEqual(pump_tap.TAPDumpSource,
                         self.find(None,
                                   "http://HOST:8091/pools/default",
                                   pump_transfer.SOURCES))
        self.assertEqual(pump_tap.TAPDumpSource,
                         self.find(None,
                                   "http://HOST:8091/",
                                   pump_transfer.SOURCES))
        self.assertEqual(pump_tap.TAPDumpSource,
                         self.find(None,
                                   "http://HOST",
                                   pump_transfer.SOURCES))

    def test_find_couchbase_handlers(self):
        self.assertEqual(pump_tap.TAPDumpSource,
                         self.find(None,
                                   "couchbase://HOST:8091",
                                   pump_transfer.SOURCES))
        self.assertEqual(pump_cb.CBSink,
                         self.find(None,
                                   "couchbase://HOST:8091",
                                   pump_transfer.SINKS))

    def test_find_bfd_handlers(self):
        d = tempfile.mkdtemp()
        self.assertEqual(pump_bfd.BFDSource,
                         self.find(None, d,
                                   pump_transfer.SOURCES))
        self.assertEqual(pump_bfd.BFDSink,
                         self.find(None, d,
                                   pump_transfer.SINKS))
        os.removedirs(d)

    def test_find_more_handlers(self):
        self.assertEqual(None,
                         self.find(None,
                                   "not-a-real-source",
                                   pump_transfer.SOURCES))
        self.assertEqual(pump_mc.MCSink,
                         self.find(None,
                                   "memcached://HOST:8091",
                                   pump_transfer.SINKS))
        self.assertEqual(pump.StdOutSink,
                         self.find(self,
                                   "stdout:",
                                   pump_transfer.SINKS))


class TestBackupParseSpec(unittest.TestCase):

    def setUp(self):
        mrs.reset()

    def tearDown(self):
        mrs.reset()

    def test_argv_missing(self):
        backup = pump_transfer.Backup()
        self.assertNotEqual(0, backup.main([]))
        self.assertNotEqual(0, backup.main(["cbbackup"]))

    def test_parse_spec(self):
        b = pump_transfer.Backup()

        err, opts, source, backup_dir = \
            b.opt_parse(["cbbackup", "not-a-real-thing://HOST:1234", "2"])
        self.assertEqual(None, err)

        err, opts, source, backup_dir = \
            b.opt_parse(["cbbackup", "http://HOST:1234", "2"])
        self.assertEqual(None, err)
        self.assertEqual("http://HOST:1234", source)
        self.assertEqual("2", backup_dir)

        host, port, user, pswd, path = \
            pump.parse_spec(opts, source, 1313)
        self.assertEqual("HOST", host)
        self.assertEqual("1234", port)
        self.assertEqual(None, user)
        self.assertEqual(None, pswd)
        self.assertEqual("", path)

        err, opts, source, backup_dir = \
            b.opt_parse(["cbbackup", "http://HOST:1234/pools", "2"])
        self.assertEqual(None, err)
        host, port, user, pswd, path = \
            pump.parse_spec(opts, source, 1313)
        self.assertEqual("/pools", path)

        err, opts, source, backup_dir = \
            b.opt_parse(["cbbackup", "http://HOST:1234/pools/default", "2"])
        self.assertEqual(None, err)
        host, port, user, pswd, path = \
            pump.parse_spec(opts, source, 1313)
        self.assertEqual("/pools/default", path)

        err, opts, source, backup_dir = \
            b.opt_parse(["cbbackup", "http://HOST:1234", "2",
                         "--username=Uabc", "--password=P123"])
        self.assertEqual(None, err)
        self.assertEqual("http://HOST:1234", source)
        self.assertEqual("2", backup_dir)

        host, port, user, pswd, path = \
            pump.parse_spec(opts, source, 1313)
        self.assertEqual("HOST", host)
        self.assertEqual("1234", port)
        self.assertEqual("Uabc", user)
        self.assertEqual("P123", pswd)
        self.assertEqual("", path)

        err, opts, source, backup_dir = \
            b.opt_parse(["cbbackup", "http://User:Pswd@HOST:1234", "2"])
        self.assertEqual(None, err)
        self.assertEqual("http://User:Pswd@HOST:1234", source)
        self.assertEqual("2", backup_dir)

        host, port, user, pswd, path = \
            pump.parse_spec(opts, source, 1313)
        self.assertEqual("HOST", host)
        self.assertEqual("1234", port)
        self.assertEqual("User", user)
        self.assertEqual("Pswd", pswd)
        self.assertEqual("", path)

        err, opts, source, backup_dir = \
            b.opt_parse(["cbbackup", "http://User:Pswd@HOST:1234", "2",
                         "--username=Uabc", "--password=P123"])
        self.assertEqual(None, err)
        self.assertEqual("http://User:Pswd@HOST:1234", source)
        self.assertEqual("2", backup_dir)

        host, port, user, pswd, path = \
            pump.parse_spec(opts, source, 1313)
        self.assertEqual("HOST", host)
        self.assertEqual("1234", port)
        self.assertEqual("Uabc", user)
        self.assertEqual("P123", pswd)
        self.assertEqual("", path)


class TestKeyFilter(unittest.TestCase):

    def setUp(self):
        mrs.reset()

    def tearDown(self):
        mrs.reset()

    def test_bad_key_filter(self):
        d = tempfile.mkdtemp()
        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d,
                                          "-k", "((**"])
        self.assertNotEqual(0, rv)
        shutil.rmtree(d)


class TestTAPDumpSourceCheck(unittest.TestCase):

    def setUp(self):
        mrs.reset()

    def tearDown(self):
        mrs.reset()

    def test_check(self):
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200,
                            'message': SAMPLE_JSON_pools_default_buckets })])

        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup", mrs.url(), "2"])
        self.assertEqual(mrs.url(), source)
        self.assertEqual("2", backup_dir)
        rv, map = pump_tap.TAPDumpSource.check(opts, source)
        self.assertEqual(0, rv)
        self.assertTrue(map is not None)

    def test_check_no_server(self):
        mrs.reset()

        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup",
                                              "http://localhost:6666666",
                                              "2"])
        rv, map = pump_tap.TAPDumpSource.check(opts, source)
        self.assertNotEqual(0, rv)
        self.assertTrue(map is None)

    def test_check_not_json(self):
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200,
                            'message': "this is not JSON" })])

        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup", mrs.url(), "2"])
        self.assertEqual(mrs.url(), source)
        self.assertEqual("2", backup_dir)
        rv, map = pump_tap.TAPDumpSource.check(opts, source)
        self.assertNotEqual(0, rv)
        self.assertTrue(map is None)

    def test_check_bad_json(self):
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200,
                            'message': '["this":"is JSON but unexpected"]' })])

        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup", mrs.url(), "2"])
        self.assertEqual(mrs.url(), source)
        self.assertEqual("2", backup_dir)
        rv, map = pump_tap.TAPDumpSource.check(opts, source)
        self.assertNotEqual(0, rv)
        self.assertTrue(map is None)

    def test_check_multiple_buckets(self):
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200,
                            'message': """[{"name":"a",
                                            "bucketType":"membase",
                                            "nodes":["fake-nodes-data"],
                                            "nodeLocator":"vbucket",
                                            "vBucketServerMap":{"fake":"map"}},
                                           {"name":"b",
                                            "bucketType":"membase",
                                            "nodes":["fake-nodes-data"],
                                            "nodeLocator":"vbucket",
                                            "vBucketServerMap":{"fake":"map"}}]""" })])

        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup", mrs.url(), "2"])
        self.assertEqual(mrs.url(), source)
        self.assertEqual("2", backup_dir)
        rv, map = pump_tap.TAPDumpSource.check(opts, source)
        self.assertEqual(0, rv)
        self.assertTrue(map is not None)
        self.assertEqual(2, len(map['buckets']))
        self.assertEqual('a', map['buckets'][0]['name'])
        self.assertEqual('b', map['buckets'][1]['name'])

    def test_check_non_membase_bucket_type(self):
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200,
                            'message': """[{"name":"a",
                                            "bucketType":"not-membase-bucket-type",
                                            "nodes":["fake-nodes-data"],
                                            "nodeLocator":"vbucket",
                                            "vBucketServerMap":{"fake":"map"}},
                                           {"name":"b",
                                            "bucketType":"membase",
                                            "nodes":["fake-nodes-data"],
                                            "nodeLocator":"vbucket",
                                            "vBucketServerMap":{"fake":"map"}}]""" })])

        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup", mrs.url(), "2"])
        self.assertEqual(mrs.url(), source)
        self.assertEqual("2", backup_dir)
        rv, map = pump_tap.TAPDumpSource.check(opts, source)
        self.assertEqual(0, rv)
        self.assertTrue(map is not None)
        self.assertEqual(1, len(map['buckets']))
        self.assertEqual('b', map['buckets'][0]['name'])


class TestBFDSinkCheck(unittest.TestCase):

    def test_check(self):
        d = tempfile.mkdtemp()
        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup", "1", d])
        self.assertEqual(d, backup_dir)
        rv, map = pump_bfd.BFDSink.check(opts, backup_dir, None)
        self.assertEqual(0, rv)
        os.removedirs(d)

    def test_check_parent_exists(self):
        d = tempfile.mkdtemp()
        dchild = d + "/child"
        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup", "1", dchild])
        self.assertEqual(dchild, backup_dir)
        rv, map = pump_bfd.BFDSink.check(opts, backup_dir, None)
        self.assertEqual(0, rv)
        os.removedirs(d)

    def test_check_missing(self):
        d = "/dir/no/exist"
        err, opts, source, backup_dir = \
            pump_transfer.Backup().opt_parse(["cbbackup", "1", d])
        self.assertEqual(d, backup_dir)
        rv, map = pump_bfd.BFDSink.check(opts, backup_dir, None)
        self.assertNotEqual(0, rv)


# ------------------------------------------------

class BackupTestHelper(unittest.TestCase):
    """Provides helper methods to check backup files."""

    def expect_backup_contents(self, backup_dir,
                               expected_memcached_stream=None,
                               expected_items=None):
        mock_stdout = MockStdOut()

        t = pump_transfer.Transfer()
        rv = t.main(["cbtransfer", backup_dir, "stdout:", "-t", "1"],
                    opts_etc={"stdout": mock_stdout,
                              "item_visitor": mock_stdout.item_visitor})
        self.assertEqual(0, rv)

        if expected_memcached_stream:
            self.assertEqual(expected_memcached_stream,
                             ''.join(mock_stdout.msgs))

        if expected_items:
            for idx, actual_item in enumerate(mock_stdout.items):
                expected_item = expected_items[idx]
                self.assertTrue(expected_item)

                ecmd, evbucket_id, ekey, eflg, eexp, ecas, eval = \
                    expected_item
                acmd, avbucket_id, akey, aflg, aexp, acas, aval = \
                    actual_item

                self.assertEqual(ecmd, acmd)
                self.assertEqual(evbucket_id, avbucket_id)
                self.assertEqual(ekey, akey)
                self.assertEqual(eflg, aflg)
                self.assertEqual(eexp, aexp)
                self.assertEqual(ecas, acas)
                self.assertEqual(str(eval), str(aval))

            self.assertEqual(len(expected_items), len(mock_stdout.items))

    def check_cbb_file_exists(self, dir, num=1):
        self.assertEqual(1, len(glob.glob(dir + "/bucket-*")))
        self.assertEqual(num, len(glob.glob(dir + "/bucket-*/node-*")))
        self.assertEqual(num, len(glob.glob(dir + "/bucket-*/node-*/data-0000.cbb")))


class MCTestHelper(unittest.TestCase):
    """Provides memcached binary protocol helper methods."""

    def setUp(self):
        mrs.reset()
        mms0.reset()
        mms1.reset()

    def tearDown(self):
        mrs.reset()
        mms0.reset()
        mms1.reset()

    def json_2_nodes(self):
        j = SAMPLE_JSON_pools_default_buckets
        j = j.replace("HOST0:8091", mrs.host_port())
        j = j.replace("HOST1:8091", mrs.host + ":8091") # Assuming test won't contact 2nd REST server.
        j = j.replace("HOST0:11210", mms0.host_port())
        j = j.replace("HOST1:11210", mms1.host_port())
        j = j.replace("HOST0", mms0.host)
        j = j.replace("HOST1", mms1.host)
        m = json.loads(j)
        m[0]['nodes'][0]['ports']['direct'] = mms0.port
        m[0]['nodes'][1]['ports']['direct'] = mms1.port
        j = json.dumps(m)
        return j

    def parse_msg(self, buf, magic_expected):
        head = buf[:memcacheConstants.MIN_RECV_PACKET]
        data = buf[memcacheConstants.MIN_RECV_PACKET:]
        magic, cmd, keylen, extlen, dtype, vbucket_id, datalen, opaque, cas = \
            struct.unpack(memcacheConstants.REQ_PKT_FMT, head)
        self.assertEqual(magic, magic_expected)

        ext = ''
        key = ''
        val = ''
        if data:
            ext = data[0:extlen]
            key = data[extlen:extlen+keylen]
            val = data[extlen+keylen:]
        return cmd, vbucket_id, ext, key, val, opaque, cas

    def parse_req(self, buf):
        return self.parse_msg(buf, memcacheConstants.REQ_MAGIC_BYTE)

    def parse_res(self, buf):
        return self.parse_msg(buf, memcacheConstants.RES_MAGIC_BYTE)

    def check_auth(self, req, user, pswd):
        self.assertTrue(req)
        cmd, vbucket_id, ext, key, val, opaque, cas = \
            self.parse_req(req)
        self.assertEqual(memcacheConstants.CMD_SASL_AUTH, cmd)
        self.assertEqual(0, vbucket_id)
        self.assertEqual('', ext)
        self.assertEqual('PLAIN', key)
        self.assertEqual('\x00' + user + '\x00' + pswd, val)
        self.assertEqual(0, cas)
        return cmd, vbucket_id, ext, key, val, opaque, cas

    def check_tap_connect(self, req):
        self.assertTrue(req)
        cmd, vbucket_id, ext, key, val, opaque, cas = \
            self.parse_req(req)
        self.assertEqual(memcacheConstants.CMD_TAP_CONNECT, cmd)
        self.assertEqual(0, vbucket_id)

        expect_ext, expect_val = \
            pump_tap.TAPDumpSource.encode_tap_connect_opts({
                memcacheConstants.TAP_FLAG_DUMP: '',
                memcacheConstants.TAP_FLAG_SUPPORT_ACK: '',
                })

        self.assertEqual(expect_ext, ext)
        self.assertTrue(key) # Expecting non-empty TAP name.
        self.assertEqual(expect_val, val)
        self.assertEqual(0, cas)

        return cmd, vbucket_id, ext, key, val, opaque, cas

    def header(self, cmd, vbucket_id, key, val, ext, opaque, cas,
               dtype=0,
               fmt=memcacheConstants.REQ_PKT_FMT,
               magic=memcacheConstants.REQ_MAGIC_BYTE):
        return struct.pack(fmt, magic, cmd,
                           len(key), len(ext), dtype, vbucket_id,
                           len(key) + len(ext) + len(val), opaque, cas)

    def req_header(self, cmd, vbucket_id, key, val, ext, opaque, cas,
                   dtype=0):
        return self.header(cmd, vbucket_id, key, val, ext, opaque, cas,
                           dtype=dtype,
                           fmt=memcacheConstants.REQ_PKT_FMT,
                           magic=memcacheConstants.REQ_MAGIC_BYTE)

    def res_header(self, cmd, vbucket_id, key, val, ext, opaque, cas,
                   dtype=0):
        return self.header(cmd, vbucket_id, key, val, ext, opaque, cas,
                           dtype=dtype,
                           fmt=memcacheConstants.RES_PKT_FMT,
                           magic=memcacheConstants.RES_MAGIC_BYTE)

    def req(self, cmd, vbucket_id, key, val, ext, opaque, cas,
            dtype=0):
        return self.req_header(cmd, vbucket_id, key, val, ext, opaque, cas,
                               dtype=dtype) + ext + key + val

    def res(self, cmd, vbucket_id, key, val, ext, opaque, cas,
            dtype=0):
        return self.res_header(cmd, vbucket_id, key, val, ext, opaque, cas,
                               dtype=dtype) + ext + key + val


# ------------------------------------------------

class TestTAPDumpSource(MCTestHelper, BackupTestHelper):

    def test_failed_auth(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_failed_auth)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertNotEqual(0, rv)

        w.join()
        shutil.rmtree(d)

    def worker_failed_auth(self):
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            self.assertTrue(req)
            client.close("simulate auth fail by closing conn")
            client.go.set()

    def test_close_after_auth(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_close_after_auth)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)

        # Some empty BFD files should be created after good SASL auth,
        # even though this test closes TAP connections right away.
        self.check_cbb_file_exists(d, num=2)

        w.join()
        shutil.rmtree(d)

    def worker_close_after_auth(self):
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.close("simulate failure right after auth")
            client.go.set()

    def test_close_after_TAP_connect(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_close_after_TAP_connect)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)

        # Some empty BFD files should be created,
        # even though this test closes after TAP connect attempt.
        self.check_cbb_file_exists(d, num=2)

        w.join()
        shutil.rmtree(d)

    def worker_close_after_TAP_connect(self):
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)
            client.close("simulate failure right after TAP connect")
            client.go.set()


class TestTAPDumpSourceMutations(MCTestHelper, BackupTestHelper):

    def test_1_mutation(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_1_mutation)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)

        # Two BFD files should be created, with 1 item each.
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d,
                                    "set a 0 0 1\r\nA\r\n"
                                    "set a 0 0 1\r\nA\r\n")
        w.join()
        shutil.rmtree(d)

    def worker_1_mutation(self):
        # Sends one TAP_MUTATION with an ACK.
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0, 0, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        123, 'a', 'A', ext, 789, 321))
            client.go.set()

            client, res = mms.queue.get()
            cmd, vbucket_id, ext, key, val, opaque, cas = \
                self.parse_res(res)
            self.assertEqual(CMD_TAP_MUTATION, cmd)
            self.assertEqual(0, vbucket_id)
            self.assertEqual('', ext)
            self.assertEqual('', key)
            self.assertEqual(789, opaque)
            self.assertEqual(0, cas)
            self.assertEqual('', val)

            client.close("close after ack received")
            client.go.set()

    def test_2_mutation(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_2_mutation)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)
        self.check_cbb_file_exists(d, num=2)
        # 0xfedcba01 == 4275878401, using high numbers to check endianess.
        # 0xffeedd00 == 4293844224
        self.expect_backup_contents(d,
                                    "set a 4275878401 0 1\r\nA\r\n"
                                    "set b 0 4293844224 1\r\nB\r\n"
                                    "set a 4275878401 0 1\r\nA\r\n"
                                    "set b 0 4293844224 1\r\nB\r\n",
                                    [(CMD_TAP_MUTATION, 123, 'a', 0xfedcba01, 0, 321, 'A'),
                                     (CMD_TAP_MUTATION, 1234, 'b', 0, 0xffeedd00, 4321, 'B'),
                                     (CMD_TAP_MUTATION, 123, 'a', 0xfedcba01, 0, 321, 'A'),
                                     (CMD_TAP_MUTATION, 1234, 'b', 0, 0xffeedd00, 4321, 'B')])
        w.join()
        shutil.rmtree(d)

    def worker_2_mutation(self):
        # Sends two TAP_MUTATION's with an ACK on the last.
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, 0, 0, 0xfedcba01, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        123, 'a', 'A', ext, 789, 321))

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0, 0, 0xffeedd00)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        1234, 'b', 'B', ext, 987, 4321))
            client.go.set()

            client, res = mms.queue.get()
            cmd, vbucket_id, ext, key, val, opaque, cas = \
                self.parse_res(res)
            self.assertEqual(CMD_TAP_MUTATION, cmd)
            self.assertEqual(0, vbucket_id)
            self.assertEqual('', ext)
            self.assertEqual('', key)
            self.assertEqual(987, opaque)
            self.assertEqual(0, cas)
            self.assertEqual('', val)

            client.close("close after ack received")
            client.go.set()

    def test_key_filter_some(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_2_mutation)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d, "-k", "a"])
        self.assertEqual(0, rv)
        self.check_cbb_file_exists(d, num=2)
        # 0xfedcba01 == 4275878401
        self.expect_backup_contents(d,
                                    "set a 4275878401 0 1\r\nA\r\n"
                                    "set a 4275878401 0 1\r\nA\r\n",
                                    [(CMD_TAP_MUTATION, 123, 'a', 0xfedcba01, 0, 321, 'A'),
                                     (CMD_TAP_MUTATION, 123, 'a', 0xfedcba01, 0, 321, 'A')])
        w.join()
        shutil.rmtree(d)

    def test_key_filter_everything(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_2_mutation)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d, "-k", "aaa"])
        self.assertEqual(0, rv)
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d, "", [])
        w.join()
        shutil.rmtree(d)

    def test_2_mutation_chopped_header(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        self.chop_at = 16 # Header length is 24 bytes.
        w = Worker(target=self.worker_2_chopped)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)

        # Two BFD files should be created, with 1 item each.
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d,
                                    "set a 0 0 1\r\nA\r\n"
                                    "set a 0 0 1\r\nA\r\n")
        w.join()
        shutil.rmtree(d)

    def test_2_mutation_chopped_body(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        self.chop_at = 26 # Header length is 24 bytes.
        w = Worker(target=self.worker_2_chopped)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)

        # Two BFD files should be created, with 1 item each.
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d,
                                    "set a 0 0 1\r\nA\r\n"
                                    "set a 0 0 1\r\nA\r\n",
                                    [(CMD_TAP_MUTATION, 123, 'a', 0, 0, 321, 'A'),
                                     (CMD_TAP_MUTATION, 123, 'a', 0, 0, 321, 'A')])
        w.join()
        shutil.rmtree(d)

    def worker_2_chopped(self):
        # Sends two TAP_MUTATION's, but second message is chopped.
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, 0, 0, 0, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        123, 'a', 'A', ext, 987, 321))

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0, 0, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        1234, 'b', 'B', ext, 789, 4321)[0:self.chop_at])
            client.close("close after sending chopped message")
            client.go.set()

    def test_delete(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_delete)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d,
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "delete a\r\n"
                                    "set b 0 12345 1\r\nB\r\n"
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "delete a\r\n"
                                    "set b 0 12345 1\r\nB\r\n",
                                    [(CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_DELETE, 111, 'a', 0, 0, 333, ''),
                                     (CMD_TAP_MUTATION, 1234, 'b', 0, 12345, 4321, 'B'),
                                     (CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_DELETE, 111, 'a', 0, 0, 333, ''),
                                     (CMD_TAP_MUTATION, 1234, 'b', 0, 12345, 4321, 'B')])
        w.join()
        shutil.rmtree(d)

    def worker_delete(self):
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, 0, 0, 40302010, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        123, 'a', 'A', ext, 789, 321))

            ext = struct.pack(memcacheConstants.TAP_GENERAL_PKT_FMT,
                              0, 0, 0)
            client.client.send(self.req(CMD_TAP_DELETE,
                                        111, 'a', '', ext, 777, 333))

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0, 0, 12345)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        1234, 'b', 'B', ext, 987, 4321))
            client.go.set()

            client, res = mms.queue.get()
            cmd, vbucket_id, ext, key, val, opaque, cas = \
                self.parse_res(res)
            self.assertEqual(CMD_TAP_MUTATION, cmd)
            self.assertEqual(0, vbucket_id)
            self.assertEqual('', ext)
            self.assertEqual('', key)
            self.assertEqual(987, opaque)
            self.assertEqual(0, cas)
            self.assertEqual('', val)

            client.close("close after ack received")
            client.go.set()

    def test_delete_ack(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_delete_ack)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d,
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "delete a\r\n"
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "delete a\r\n",
                                    [(CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_DELETE, 111, 'a', 0, 0, 333, ''),
                                     (CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_DELETE, 111, 'a', 0, 0, 333, '')])
        w.join()
        shutil.rmtree(d)

    def worker_delete_ack(self):
        # The last sent message is a TAP_DELETE with TAP_FLAG_ACK.
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, 0, 0, 40302010, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        123, 'a', 'A', ext, 789, 321))

            ext = struct.pack(memcacheConstants.TAP_GENERAL_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0)
            client.client.send(self.req(CMD_TAP_DELETE,
                                        111, 'a', '', ext, 777, 333))
            client.go.set()

            client, res = mms.queue.get()
            cmd, vbucket_id, ext, key, val, opaque, cas = \
                self.parse_res(res)
            self.assertEqual(CMD_TAP_DELETE, cmd)
            self.assertEqual(0, vbucket_id)
            self.assertEqual('', ext)
            self.assertEqual('', key)
            self.assertEqual(777, opaque)
            self.assertEqual(0, cas)
            self.assertEqual('', val)

            client.close("close after ack received")
            client.go.set()

    def test_noop(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_noop)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d,
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "delete a\r\n"
                                    "set b 0 12345 1\r\nB\r\n"
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "delete a\r\n"
                                    "set b 0 12345 1\r\nB\r\n",
                                    [(CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_DELETE, 111, 'a', 0, 0, 333, ''),
                                     (CMD_TAP_MUTATION, 1234, 'b', 0, 12345, 4321, 'B'),
                                     (CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_DELETE, 111, 'a', 0, 0, 333, ''),
                                     (CMD_TAP_MUTATION, 1234, 'b', 0, 12345, 4321, 'B')])
        w.join()
        shutil.rmtree(d)

    def worker_noop(self):
        # Has CMD_NOOP's sprinkled amongst the stream.
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)

            client.client.send(self.req(memcacheConstants.CMD_NOOP,
                                        111, 'a', '', '', 777, 333))

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, 0, 0, 40302010, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        123, 'a', 'A', ext, 789, 321))

            client.client.send(self.req(memcacheConstants.CMD_NOOP,
                                        111, 'a', '', '', 777, 333))

            ext = struct.pack(memcacheConstants.TAP_GENERAL_PKT_FMT,
                              0, 0, 0)
            client.client.send(self.req(CMD_TAP_DELETE,
                                        111, 'a', '', ext, 777, 333))

            client.client.send(self.req(memcacheConstants.CMD_NOOP,
                                        111, 'a', '', '', 777, 333))

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0, 0, 12345)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        1234, 'b', 'B', ext, 987, 4321))
            client.go.set()

            client, res = mms.queue.get()
            cmd, vbucket_id, ext, key, val, opaque, cas = \
                self.parse_res(res)
            self.assertEqual(CMD_TAP_MUTATION, cmd)
            self.assertEqual(0, vbucket_id)
            self.assertEqual('', ext)
            self.assertEqual('', key)
            self.assertEqual(987, opaque)
            self.assertEqual(0, cas)
            self.assertEqual('', val)

            client.close("close after ack received")
            client.go.set()

    def test_tap_cmd_opaque(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_tap_cmd_opaque)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d,
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "delete a\r\n"
                                    "set b 0 12345 0\r\n\r\n"
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "delete a\r\n"
                                    "set b 0 12345 0\r\n\r\n",
                                    [(CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_DELETE, 111, 'a', 0, 0, 333, ''),
                                     (CMD_TAP_MUTATION, 1234, 'b', 0, 12345, 4321, ''),
                                     (CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_DELETE, 111, 'a', 0, 0, 333, ''),
                                     (CMD_TAP_MUTATION, 1234, 'b', 0, 12345, 4321, '')])
        w.join()
        shutil.rmtree(d)

    def worker_tap_cmd_opaque(self):
        # Has CMD_TAP_OPAQUE's sprinkled amongst the stream.
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)

            client.client.send(self.req(memcacheConstants.CMD_TAP_OPAQUE,
                                        111, 'o0', '', '', 777, 333))

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, 0, 0, 40302010, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        123, 'a', 'A', ext, 789, 321))

            ext = struct.pack(memcacheConstants.TAP_GENERAL_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0)
            client.client.send(self.req(memcacheConstants.CMD_TAP_OPAQUE,
                                        111, 'o1', '', ext, 888, 444))
            client.go.set()

            client, res = mms.queue.get()
            cmd, vbucket_id, ext, key, val, opaque, cas = \
                self.parse_res(res)
            self.assertEqual(memcacheConstants.CMD_TAP_OPAQUE, cmd)
            self.assertEqual(0, vbucket_id)
            self.assertEqual('', ext)
            self.assertEqual('', key)
            self.assertEqual(888, opaque)
            self.assertEqual(0, cas)
            self.assertEqual('', val)

            ext = struct.pack(memcacheConstants.TAP_GENERAL_PKT_FMT,
                              0, 0, 0)
            client.client.send(self.req(CMD_TAP_DELETE,
                                        111, 'a', '', ext, 777, 333))

            client.client.send(self.req(memcacheConstants.CMD_TAP_OPAQUE,
                                        111, 'o2', '', '', 999, 555))

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0, 0, 12345)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        1234, 'b', '', ext, 987, 4321))
            client.go.set()

            client, res = mms.queue.get()
            cmd, vbucket_id, ext, key, val, opaque, cas = \
                self.parse_res(res)
            self.assertEqual(CMD_TAP_MUTATION, cmd)
            self.assertEqual(0, vbucket_id)
            self.assertEqual('', ext)
            self.assertEqual('', key)
            self.assertEqual(987, opaque)
            self.assertEqual(0, cas)
            self.assertEqual('', val)

            client.close("close after ack received")
            client.go.set()

    def test_flush_all(self):
        d = tempfile.mkdtemp()
        mrs.reset(self, [({ 'command': 'GET',
                            'path': '/pools/default/buckets'},
                          { 'code': 200, 'message': self.json_2_nodes() })])

        w = Worker(target=self.worker_flush_all)
        w.start()

        rv = pump_transfer.Backup().main(["cbbackup", mrs.url(), d])
        self.assertEqual(0, rv)
        self.check_cbb_file_exists(d, num=2)
        self.expect_backup_contents(d,
                                    "set a 40302010 0 1\r\nA\r\n"
                                    "set a 40302010 0 1\r\nA\r\n",
                                    [(CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A'),
                                     (CMD_TAP_MUTATION, 123, 'a', 40302010, 0, 321, 'A')])
        w.join()
        shutil.rmtree(d)

    def worker_flush_all(self):
        for mms in [mms0, mms1]:
            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_auth(req, 'default', '')
            client.client.send(self.res(cmd, 0, '', '', '', opaque, 0))
            client.go.set()

            client, req = mms.queue.get()
            cmd, _, _, _, _, opaque, _ = \
                self.check_tap_connect(req)

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, 0, 0, 40302010, 0)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        123, 'a', 'A', ext, 789, 321))

            # After we send a flush-all, backup ignores the rest of the stream.

            ext = struct.pack(memcacheConstants.TAP_GENERAL_PKT_FMT,
                              0, 0, 0)
            client.client.send(self.req(memcacheConstants.CMD_TAP_FLUSH,
                                        111, 'a', '', ext, 777, 333))

            ext = struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                              0, memcacheConstants.TAP_FLAG_ACK, 0, 0, 12345)
            client.client.send(self.req(CMD_TAP_MUTATION,
                                        1234, 'b', 'B', ext, 987, 4321))


# ------------------------------------------------------

SAMPLE_JSON_pools = """
{"pools":[{"name":"default",
           "uri":"/pools/default",
           "streamingUri":"/poolsStreaming/default"}],
 "isAdminCreds":false,"uuid":"7f48e8e8-5e89-4220-b064-fa62cbd0ff2f",
 "implementationVersion":"1.8.0r-55-g80f24f2-enterprise",
 "componentsVersion":{"os_mon":"2.2.6","mnesia":"4.4.19","inets":"5.6","kernel":"2.14.4",
                      "sasl":"2.1.9.4","ns_server":"1.8.0r-55-g80f24f2-enterprise",
                      "stdlib":"1.17.4"}}
"""

SAMPLE_JSON_pools_default = """
{"storageTotals":{
  "ram":{"quotaUsed":629145600,"usedByData":54117632,"total":8312143872.0,
         "quotaTotal":6647971840.0,"used":4401639424.0},
  "hdd":{"usedByData":5117960,"total":26966704128.0,
         "quotaTotal":26966704128.0,"used":5258507304.0,"free":21573363304.0}},
  "name":"default","alerts":[],
  "nodes":[
    {"systemStats":{
       "cpu_utilization_rate":0.2631578947368421,
       "swap_total":1073737728,"swap_used":0},
     "interestingStats":{"curr_items":0,"curr_items_tot":0,"vb_replica_curr_items":0},
     "uptime":"745","memoryTotal":4156071936.0,"memoryFree":1760247808,
     "mcdMemoryReserved":3170,"mcdMemoryAllocated":3170,
     "clusterMembership":"active",
     "status":"healthy",
     "hostname":"HOST0:8091",
     "clusterCompatibility":1,
     "version":"1.8.0r-55-g80f24f2-enterprise",
     "os":"x86_64-unknown-linux-gnu",
     "ports":{"proxy":11211,"direct":11210}},
    {"systemStats":{
       "cpu_utilization_rate":0.7389162561576355,
       "swap_total":1073737728,"swap_used":0},
     "interestingStats":{"curr_items":0,"curr_items_tot":0,"vb_replica_curr_items":0},
     "uptime":"735","memoryTotal":4156071936.0,"memoryFree":2150256640.0,
     "mcdMemoryReserved":3170,"mcdMemoryAllocated":3170,
     "clusterMembership":"active",
     "status":"healthy",
     "hostname":"HOST1:8091",
     "clusterCompatibility":1,
     "version":"1.8.0r-55-g80f24f2-enterprise",
     "os":"x86_64-unknown-linux-gnu",
     "ports":{"proxy":11211,"direct":11210}}
  ],
  "buckets":{
    "uri":"/pools/default/buckets?v=2979176"},
    "controllers":{
      "addNode":{"uri":"/controller/addNode"},
      "rebalance":{"uri":"/controller/rebalance"},
      "failOver":{"uri":"/controller/failOver"},
      "reAddNode":{"uri":"/controller/reAddNode"},
      "ejectNode":{"uri":"/controller/ejectNode"},
      "testWorkload":{"uri":"/pools/default/controller/testWorkload"}},
    "balanced":true,
    "failoverWarnings":[],
    "rebalanceStatus":"none",
    "rebalanceProgressUri":"/pools/default/rebalanceProgress",
    "stopRebalanceUri":"/controller/stopRebalance",
    "nodeStatusesUri":"/nodeStatuses",
    "stats":{"uri":"/pools/default/stats"},
    "counters":{"rebalance_success":1,"rebalance_start":1}}
"""

SAMPLE_JSON_pools_default_buckets = """
[{"name":"default","bucketType":"membase",
  "authType":"sasl","saslPassword":"",
  "proxyPort":0,
  "uri":"/pools/default/buckets/default",
  "streamingUri":"/pools/default/bucketsStreaming/default",
  "flushCacheUri":"/pools/default/buckets/default/controller/doFlush",
  "nodes":[
    {"systemStats":{"cpu_utilization_rate":0.25,"swap_total":1073737728,"swap_used":0},
     "interestingStats":{"curr_items":0,"curr_items_tot":0,"vb_replica_curr_items":0},
     "uptime":"1210","memoryTotal":4156071936.0,"memoryFree":1757093888,
     "mcdMemoryReserved":3170,"mcdMemoryAllocated":3170,
     "replication":1.0,
     "clusterMembership":"active",
     "status":"healthy",
     "hostname":"HOST0:8091",
     "clusterCompatibility":1,
     "version":"1.8.0r-55-g80f24f2-enterprise",
     "os":"x86_64-unknown-linux-gnu",
     "ports":{"proxy":11211,"direct":11210}},
    {"systemStats":{"cpu_utilization_rate":0.49875311720698257,"swap_total":1073737728,"swap_used":0},
     "interestingStats":{"curr_items":0,"curr_items_tot":0,"vb_replica_curr_items":0},
     "uptime":"1205","memoryTotal":4156071936.0,"memoryFree":2142023680,
     "mcdMemoryReserved":3170,"mcdMemoryAllocated":3170,
     "replication":1.0,
     "clusterMembership":"active",
     "status":"healthy",
     "hostname":"HOST1:8091",
     "clusterCompatibility":1,
     "version":"1.8.0r-55-g80f24f2-enterprise",
     "os":"x86_64-unknown-linux-gnu",
     "ports":{"proxy":11211,"direct":11210}}],
   "stats":{"uri":"/pools/default/buckets/default/stats",
            "directoryURI":"/pools/default/buckets/default/statsDirectory",
            "nodeStatsListURI":"/pools/default/buckets/default/nodes"},
   "nodeLocator":"vbucket",
   "vBucketServerMap":{
     "hashAlgorithm":"CRC",
     "numReplicas":1,
     "serverList":["HOST0:11210","HOST1:11210"],
     "vBucketMap":[[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0]]},
   "replicaNumber":1,
   "quota":{"ram":629145600,"rawRAM":314572800},
   "basicStats":{"quotaPercentUsed":8.601765950520834,"opsPerSec":0,"diskFetches":0,
                 "itemCount":0,"diskUsed":5117960,"memUsed":54117632}}]
"""

class MockStdOut:
    def __init__(self):
        self.msgs = []
        self.items = []

    def write(self, m):
        self.msgs.append(str(m))

    def item_visitor(self, item):
        self.items.append(item)
        return item


if __name__ == '__main__':
    unittest.main()
