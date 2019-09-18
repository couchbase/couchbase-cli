"""This test do not ensure correct server side behaviour they only check that the cli makes the correct requests and
that validates input correctly"""
import unittest
import sys
import json
import itertools
from io import StringIO
import time
import threading
from cbmgr import CouchbaseCLI
from mock_server import MockRESTServer


host = '127.0.0.1'
port = 6789
cluster_connect_args = ['-c', host+":"+str(port), '-u', 'Administrator', '-p', 'asdasd']


class CommandTest(unittest.TestCase):
    def setUp(self):
        self.cli = CouchbaseCLI()
        self.stdout = sys.stdout
        self.sink = StringIO()
        self.str_output = ''
        self.server = MockRESTServer(host, port)

    def tearDown(self):
        self.stop_capture()
        if self.server is not None:
            self.server.shutdown()

    def capture(self):
        sys.stdout = self.sink

    def stop_capture(self):
        sys.stdout = self.stdout
        if self.str_output == '':
            self.str_output = str(self.sink.getvalue())

    def no_error_run(self, cmd_args, server_args):
        self.server.set_args(server_args)
        self.server.run()
        self.capture()
        try:
            self.cli.execute(self.cli.parse(cmd_args))
        except Exception as e:
            self.stop_capture()
            self.fail(f'Error: "{self.str_output}" occurred. Exception: {e}')
        self.stop_capture()

    def system_exit_run(self, cmd_args, server_args):
        self.server.set_args(server_args)
        self.server.run()

        self.capture()
        with self.assertRaises(SystemExit):
            self.cli.execute(self.cli.parse(cmd_args))
        self.stop_capture()

    def rest_parameter_match(self, expected_params, length_match=True):
        if length_match:
            self.assertEqual(len(expected_params), len(self.server.rest_params))
        for parameter in expected_params:
            self.assertIn(parameter, self.server.rest_params)


class TestClusterInit(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'cluster-init']
        self.command_args = [
            '--cluster-username', 'Administrator',
            '--cluster-password', 'asdasd',
            '--cluster-port', str(port),
        ]
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestClusterInit, self).setUp()

    def test_init_cluster_basic(self):
        self.server_args['init'] = False
        self.no_error_run(self.command + cluster_connect_args[:2] + self.command_args, self.server_args)
        self.assertIn('SUCCESS', self.str_output)

    def test_init_cluster_all_options(self):
        self.server_args['init'] = False
        full_options = cluster_connect_args[:2] + self.command_args + [
            '--cluster-ramsize', '512', '--services', 'data,query,fts,eventing,analytics',
            '--cluster-index-ramsize', '512', '--cluster-fts-ramsize', '512',
            '--cluster-eventing-ramsize', '512', '--cluster-name', 'name',
            '--index-storage-setting', 'memopt'
        ]

        self.no_error_run(self.command + full_options, self.server_args)
        self.assertIn('SUCCESS', self.str_output)
        expected_params = ['memoryQuota=512', 'eventingMemoryQuota=512', 'ftsMemoryQuota=512', 'clusterName=name',
                          'indexMemoryQuota=512', 'storageMode=memory_optimized',
                          'username=Administrator', 'password=asdasd', 'port=6789']
        self.rest_parameter_match(expected_params, False)

    def test_error_when_cluster_already_init(self):
        self.system_exit_run(self.command + cluster_connect_args[:2] + self.command_args, self.server_args)
        self.assertIn('Cluster is already initialized', self.str_output)

    def test_error_no_data_service(self):
        self.server_args['init'] = False
        self.system_exit_run(self.command + cluster_connect_args[:2] + self.command_args + ['--services', 'fts,query'],
                             self.server_args)
        self.assertIn('Cannot set up first cluster node without the data service', self.str_output)

    def test_error_analytics_CE(self):
        self.server_args['init'] = False
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + cluster_connect_args[:2] + self.command_args +
                             ['--services', 'data,cbas,eventing'], self.server_args)

    def test_error_no_cluster_username(self):
        self.server_args['init'] = False
        self.system_exit_run(self.command + cluster_connect_args[:2], self.server_args)


class TestBucketCompact(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-compact'] + cluster_connect_args
        self.command_args = ['--bucket', 'name']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True, 'buckets':[]}

        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        self.bucket_memcahed = {'name': 'name', 'bucketType': 'memcached'}
        super(TestBucketCompact, self).setUp()

    def test_basic_bucket_compact(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn('POST:/pools/default/buckets/'+self.bucket_membase['name']+'/controller/compactBucket',
                      self.server.trace)

    def test_bucket_view_compact(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + ['--view-only'], self.server_args)
        self.assertNotIn('POST:/pools/default/buckets/' + self.bucket_membase['name'] + '/controller/compactBucket',
                        self.server.trace)
        self.assertNotIn('POST:/pools/default/buckets/' + self.bucket_membase['name'] + '/controller/compactDatabases',
                         self.server.trace)
        self.assertIn('GET:/pools/default/buckets/' + self.bucket_membase['name'] + '/ddocs', self.server.trace)

    def test_bucket_data_compact(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + ['--data-only'], self.server_args)
        self.assertNotIn('POST:/pools/default/buckets/' + self.bucket_membase['name'] + '/controller/compactBucket',
                         self.server.trace)
        self.assertIn('POST:/pools/default/buckets/' + self.bucket_membase['name'] + '/controller/compactDatabases',
                      self.server.trace)
        self.assertNotIn('GET:/pools/default/buckets/' + self.bucket_membase['name'] + '/ddocs', self.server.trace)

    def test_error_view_only_and_data_only(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.system_exit_run(self.command + self.command_args + ['--data-only', '--view-only'], self.server_args)
        self.assertIn('Cannot compact data only and view only', self.str_output)

    def test_error_compact_memcahed_bucket(self):
        self.server_args['buckets'].append(self.bucket_memcahed)
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn('Cannot compact memcached buckets', self.str_output)

    def test_error_compact_non_existent_bucket(self):
        self.system_exit_run(self.command + self.command_args, self.server_args)


class TestBucketCreate(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-create'] + cluster_connect_args
        self.command_args = ['--bucket', 'name']
        self.command_couch_args = ['--bucket-type', 'couchbase', '--bucket-ramsize', '100', '--bucket-replica', '0',
                                   '--bucket-eviction-policy', 'fullEviction']
        self.command_EE_args = ['--compression-mode', 'active', '--max-ttl', '20']
        self.command_auto_compaction_args = ['--database-fragmentation-threshold-percentage', '25',
                                             '--view-fragmentation-threshold-percentage', '20',
                                             '--parallel-db-view-compaction', '1', '--purge-interval', '2']

        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets':[]}

        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        self.bucket_memcahed = {'name': 'name', 'bucketType': 'memcached'}
        super(TestBucketCreate, self).setUp()

    def test_basic_bucket_create(self):
        self.no_error_run(self.command + self.command_args + self.command_couch_args + self.command_EE_args,
                          self.server_args)
        expected_params = [
            'bucketType=couchbase', 'name=name', 'evictionPolicy=fullEviction', 'replicaNumber=0', 'maxTTL=20',
            'compressionMode=active', 'ramQuotaMB=100'
        ]
        self.rest_parameter_match(expected_params)

    def test_bucket_create_with_auto_compaction_settings(self):
        self.no_error_run(self.command + self.command_args + self.command_couch_args +
                          self.command_auto_compaction_args, self.server_args)

        expected_params = [
            'bucketType=couchbase', 'databaseFragmentationThreshold%5Bpercentage%5D=25', 'name=name',
            'evictionPolicy=fullEviction', 'autoCompactionDefined=true', 'parallelDBAndViewCompaction=1',
            'replicaNumber=0', 'purgeInterval=2', 'viewFragmentationThreshold%5Bpercentage%5D=20', 'ramQuotaMB=100'
        ]

        self.rest_parameter_match(expected_params)

    def test_bucket_create_ephemeral(self):
        args = [
            '--bucket-type', 'ephemeral', '--bucket-ramsize', '100', '--bucket-replica', '0',
            '--bucket-eviction-policy', 'noEviction', '--bucket-replica', '0'
        ]
        self.no_error_run(self.command + self.command_args + args, self.server_args)
        expected_params = [
            'bucketType=ephemeral', 'replicaNumber=0', 'evictionPolicy=noEviction', 'name=name', 'ramQuotaMB=100'
        ]
        self.rest_parameter_match(expected_params)

    def test_bucket_create_memcahced(self):
        args = [
            '--bucket-type', 'memcached', '--bucket-ramsize', '100'
        ]
        self.no_error_run(self.command + self.command_args + args, self.server_args)
        expected_params = [
            'bucketType=memcached', 'ramQuotaMB=100', 'name=name'
        ]
        self.rest_parameter_match(expected_params)

    def test_bucket_create_EE_options_in_CE(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + self.command_args + self.command_couch_args + self.command_EE_args,
                             self.server_args)
        self.assertIn('can only be configured on enterprise edition', self.str_output)

    def test_bucket_create_ephemeral_with_couch_options(self):
        args = [
            '--bucket-type', 'ephemeral', '--bucket-ramsize', '100', '--bucket-replica', '0',
            '--bucket-eviction-policy', 'fullEviction', '--bucket-replica', '0'
        ]
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn('--bucket-eviction-policy must either be noEviction or nruEviction', self.str_output)

    def test_bucket_create_memcached_with_couch_options(self):
        args = [
            '--bucket-type', 'memcached', '--bucket-ramsize', '100', '--bucket-replica', '0',
        ]
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn('--bucket-replica cannot be specified for a memcached bucket', self.str_output)


class TestBucketDelete(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-delete'] + cluster_connect_args
        self.command_args = ['--bucket', 'name']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets':[]}

        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        self.bucket_memcahed = {'name': 'name', 'bucketType': 'memcached'}
        super(TestBucketDelete, self).setUp()

    def test_bucket_delete(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertEqual('name', self.server_args['delete-bucket'])
        self.assertIn('DELETE:/pools/default/buckets/name', self.server.trace)

    def test_delete_bucket_not_exist(self):
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn('Bucket not found', self.str_output)


class TestBucketEdit(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-edit'] + cluster_connect_args
        self.command_args = ['--bucket', 'name']
        self.command_couch_args = ['--bucket-ramsize', '100', '--bucket-replica', '0', '--bucket-priority', 'high',
                                   '--bucket-eviction-policy', 'fullEviction', '--enable-flush', '1']
        self.command_EE_args = ['--compression-mode', 'active', '--max-ttl', '20']
        self.command_auto_compaction_args = ['--database-fragmentation-threshold-percentage', '25',
                                             '--view-fragmentation-threshold-percentage', '20',
                                             '--parallel-db-view-compaction', '1', '--purge-interval', '2']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets':[]}
        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        self.bucket_memcahed = {'name': 'name', 'bucketType': 'memcached'}
        super(TestBucketEdit, self).setUp()

    def test_bucket_edit(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + self.command_couch_args + self.command_EE_args,
                          self.server_args)
        expected_params = [
            'evictionPolicy=fullEviction', 'flushEnabled=1', 'threadsNumber=8', 'replicaNumber=0', 'maxTTL=20',
            'compressionMode=active', 'ramQuotaMB=100'
        ]
        self.rest_parameter_match(expected_params)

    def test_error_bucket_does_not_exist(self):
        self.system_exit_run(self.command + self.command_args + self.command_couch_args + self.command_EE_args,
                             self.server_args)


class TestBucketFlush(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-flush'] + cluster_connect_args
        self.command_args = ['--bucket', 'name', '--force']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets': []}
        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        super(TestBucketFlush, self).setUp()

    def test_flush_bucket(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn('POST:/pools/default/buckets/name/controller/doFlush', self.server.trace)

    def test_no_bucket_flush(self):
        self.system_exit_run(self.command + self.command_args, self.server_args)


class TestBucketList(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-list'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets': []}
        self.bucket_membase = {'name': 'name', 'bucketType': 'membase', 'replicaNumber': '0',
                               'quota': {'ram': '100'}, 'basicStats': {'memUsed': '100'}}
        self.bucket_memcahed = {'name': 'name1', 'bucketType': 'memcached', 'replicaNumber': '0',
                               'quota': {'ram': '100'}, 'basicStats': {'memUsed': '100'}}
        super(TestBucketList, self).setUp()

    def test_bucket_list(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.server_args['buckets'].append(self.bucket_memcahed)
        self.no_error_run(self.command, self.server_args)
        expected_out = ['name\n bucketType: membase\n numReplicas: 0\n ramQuota: 100\n ramUsed: 100',
                        'name1\n bucketType: memcached\n numReplicas: 0\n ramQuota: 100\n ramUsed: 100']
        self.assertIn(expected_out[0], self.str_output)
        self.assertIn(expected_out[1], self.str_output)

    def test_bucket_list_json(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.server_args['buckets'].append(self.bucket_memcahed)
        self.no_error_run(self.command + ['-o', 'json'], self.server_args)
        expected_out = ['"bucketType": "membase"', '"quota": {"ram": "100"}', '"replicaNumber": "0"',
                        '"basicStats": {"memUsed": "100"}', '"name": "name"', '"name": "name1"',
                        '"bucketType": "memcached"']

        for p in expected_out:
            self.assertIn(p, self.str_output)

    def test_bucket_list_empty_json(self):
        self.no_error_run(self.command + ['-o', 'json'], self.server_args)
        self.assertIn('[]', self.str_output)

    def test_bucket_list_empty(self):
        self.no_error_run(self.command, self.server_args)


class TestCollectLogsStart(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'collect-logs-start'] + cluster_connect_args
        self.basic_args = ['--all-nodes', '--redaction-level', 'none']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True, 'buckets': []}
        super(TestCollectLogsStart, self).setUp()

    def test_collect_logs_start_basic(self):
        self.no_error_run(self.command + self.basic_args, self.server_args)
        self.assertIn('Log collection started', self.str_output)
        self.assertIn('POST:/controller/startLogsCollection', self.server.trace)
        expected_params = ['logRedactionLevel=none', 'nodes=%2A']
        self.rest_parameter_match(expected_params)

    def test_collect_logs_start_redacted(self):
        self.no_error_run(self.command + self.basic_args[:-1] + ['partial'], self.server_args)
        self.assertIn('Log collection started', self.str_output)
        self.assertIn('POST:/controller/startLogsCollection', self.server.trace)
        expected_params = ['logRedactionLevel=partial', 'nodes=%2A']
        self.rest_parameter_match(expected_params)


class TestCollectLogsStatus(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'collect-logs-status'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets': [], 'tasks': [{'type': 'clusterLogsCollection', 'status': 'none'}]}
        super(TestCollectLogsStatus, self).setUp()

    def test_collect_log_status(self):
        self.no_error_run(self.command, self.server_args)
        self.assertIn('Status: none', self.str_output)

    def test_collect_log_no_tas(self):
        self.server_args['tasks'] = []
        self.no_error_run(self.command, self.server_args)
        self.assertIn('No log collection tasks were found', self.str_output)


class TestCollectLogsStop(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'collect-logs-stop'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestCollectLogsStop, self).setUp()

    def test_collect_log_stop(self):
        self.no_error_run(self.command, self.server_args)
        self.assertIn('POST:/controller/cancelLogsCollection', self.server.trace)


class TestFailover(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'failover'] + cluster_connect_args
        self.basic_args = ['--server-failover', 'localhost:6789', '--no-progress-bar']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'healthy',
             'clusterMembership': 'active'},
            {'otpNode': 'ns1@some-host','hostname': 'some-host:6789', 'status': 'healthy',
             'clusterMembership': 'active'}
        ]}
        self.server_args['tasks'] = [{'type': 'rebalance', 'status': 'notRunning'}]
        super(TestFailover, self).setUp()

    def test_failover_simple(self):
        self.no_error_run(self.command + self.basic_args, self.server_args)
        self.assertIn('POST:/controller/startGracefulFailover', self.server.trace)

    def test_failover_force(self):
        self.no_error_run(self.command + self.basic_args + ['--force'], self.server_args)
        self.assertIn('POST:/controller/failOver', self.server.trace)
        self.assertIn('allowUnsafe=true', self.server.rest_params)

    def test_failover_force_non_existent_node(self):
        self.basic_args[1] = 'random-node:6789'
        self.system_exit_run(self.command + self.basic_args + ['--force'], self.server_args)
        self.assertIn('Server can\'t be failed over because it\'s not part of the cluster', self.str_output)

    def test_failover_force_unhealthy_node(self):
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'unhealthy',
             'clusterMembership': 'active'}]}
        self.system_exit_run(self.command + self.basic_args, self.server_args)
        self.assertIn('can\'t be gracefully failed over because it is not healthy', self.str_output)


class TestGroupManage(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'group-manage'] + cluster_connect_args
        self.group_name = ['--group-name', 'name']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['server-group'] = {'groups': [
            {'name': 'name', 'uri': '/pools/default/serverGroups/0',
             'nodes': [{'hostname': 'n1:8091'}, {'hostname': 'n2:8091'}]},
            {'name': 'name1', 'uri': '/pools/default/serverGroups/1', 'nodes': []}
        ], 'uri': '/pools/default/serverGroups/rev=1'}
        super(TestGroupManage, self).setUp()

    def test_group_create(self):
        self.no_error_run(self.command + self.group_name + ['--create'], self.server_args)
        self.assertIn('name=name', self.server.rest_params)
        self.assertIn('POST:/pools/default/serverGroups', self.server.trace)

    def test_group_create_no_name(self):
        self.system_exit_run(self.command + ['--create'], self.server_args)
        self.assertIn('--group-name is required with --create flag', self.str_output)

    def test_group_delete(self):
        self.no_error_run(self.command + self.group_name + ['--delete'], self.server_args)
        self.assertIn('name=name', self.server.rest_params)
        self.assertIn('DELETE:/pools/default/serverGroups/0', self.server.trace)

    def test_group_delete_group_not_exist(self):
        self.system_exit_run(self.command + ['--delete', '--group-name', 'g1'], self.server_args)
        self.assertIn('not found', self.str_output)

    def test_group_list_no_name(self):
        self.no_error_run(self.command + ['--list'], self.server_args)
        expected_out = ['name', 'name1']
        self.assertIn(expected_out[0], self.str_output)
        self.assertIn(expected_out[1], self.str_output)

    def test_group_list_with_name(self):
        self.server_args['server-group'] = {'groups': [
            {'name': 'name', 'uri': '/pools/default/serverGroups/0', 'nodes': []},
            {'name': 'name1', 'uri': '/pools/default/serverGroups/1', 'nodes': []}
        ]}
        self.no_error_run(self.command + ['--list'] + self.group_name, self.server_args)
        expected_out = ['name', 'name1']
        self.assertIn(expected_out[0], self.str_output)
        self.assertNotIn(expected_out[1], self.str_output)

    def test_group_rename(self):
        self.no_error_run(self.command + ['--rename', 'nombre'] + self.group_name, self.server_args)
        self.assertIn('name=nombre', self.server.rest_params)
        self.assertIn('PUT:/pools/default/serverGroups/0', self.server.trace)

    def test_group_move(self):
        self.no_error_run(self.command + ['--move-servers', 'n1,n2', '--from-group', 'name', '--to-group', 'name1'],
                          self.server_args)
        self.assertEqual(1, len(self.server.rest_params))
        json_arr = json.loads(self.server.rest_params[0])
        expected_out = [
            {"nodes": [], "name": "name", "uri": "/pools/default/serverGroups/0"},
            {"nodes": [{"hostname": "n1:8091"}, {"hostname": "n2:8091"}], "name": "name1", "uri": "/pools/default/serverGroups/1"}
        ]
        self.assertEqual(len(json_arr['groups']), len(expected_out))
        for e in expected_out:
            self.assertTrue(e == json_arr['groups'][0] or e == json_arr['groups'][1])

    def test_error_group_move_non_existent_node(self):
        self.system_exit_run(self.command + ['--move-servers', 'n3', '--from-group', 'name', '--to-group', 'name1'],
                             self.server_args)
        self.assertIn('Can\'t move n3:8091 because it doesn\'t exist in', self.str_output)

    def test_error_move_to_not_exist(self):
        self.system_exit_run(self.command + ['--move-servers', 'n1', '--from-group', 'name', '--to-group', 'name2'],
                             self.server_args)
        self.assertIn('Group to move servers to `name2` not found', self.str_output)

    def test_error_move_from_not_exist(self):
        self.system_exit_run(self.command + ['--move-servers', 'n1', '--from-group', 'name2', '--to-group', 'name1'],
                             self.server_args)
        self.assertIn('Group to move servers from `name2` not found', self.str_output)


class TestHostList(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'host-list'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'unhealthy',
             'clusterMembership': 'active', 'configuredHostname': '127.0.0.1:6789'}]}
        super(TestHostList, self).setUp()

    def test_host_list(self):
        self.no_error_run(self.command, self.server_args)
        for n in self.server_args['pools_default']['nodes']:
            self.assertIn(n['configuredHostname'], self.str_output)


class TestNodeInit(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'node-init'] + cluster_connect_args
        self.name_args = ['--node-init-hostname', 'foo']
        self.path_args = ['--node-init-data-path', '/foo/bar/data', '--node-init-index-path', '/foo/bar/index',
                          '--node-init-java-home', '/foo/bar/java', '--node-init-analytics-path', '/foo/bar/analytics']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestNodeInit, self).setUp()

    def test_node_init(self):
        self.no_error_run(self.command + self.name_args + self.path_args, self.server_args)
        expected_params = [
            'path=%2Ffoo%2Fbar%2Fdata', 'index_path=%2Ffoo%2Fbar%2Findex', 'cbas_path=%2Ffoo%2Fbar%2Fanalytics',
            'java_home=%2Ffoo%2Fbar%2Fjava', 'hostname=foo'
        ]
        self.rest_parameter_match(expected_params)

    def test_node_init_ipv6_and_ipv4(self):
        self.system_exit_run(self.command + self.name_args + ['--ipv4', '--ipv6'], self.server_args)
        self.assertIn('Use either --ipv4 or --ipv6', self.str_output)

    def test_node_init_ipv6(self):
        self.no_error_run(self.command + self.name_args + ['--ipv6'], self.server_args)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableExternalListener', self.server.trace)
        expected_params = [
            'hostname=foo', 'afamily=ipv4', 'afamily=ipv6', 'afamily=ipv6'
        ]
        self.rest_parameter_match(expected_params)

    def test_node_init_ipv6(self):
        self.no_error_run(self.command + self.name_args + ['--ipv4'], self.server_args)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableExternalListener', self.server.trace)
        expected_params = [
            'hostname=foo', 'afamily=ipv4', 'afamily=ipv4', 'afamily=ipv6'
        ]
        self.rest_parameter_match(expected_params)


class TestRebalance(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'rebalance'] + cluster_connect_args
        self.cmd_args = ['--server-remove', 'localhost:6789', '--no-progress-bar']

        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'healthy',
             'clusterMembership': 'active'},
            {'otpNode': 'ns1@some-host','hostname': 'some-host:6789', 'status': 'healthy',
             'clusterMembership': 'active'}
        ]}
        self.server_args['tasks'] = [{'type': 'rebalance', 'status': 'notRunning'}]
        super(TestRebalance, self).setUp()

    def test_rebalance(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        self.assertIn('POST:/controller/rebalance', self.server.trace)
        expected_params = ['ejectedNodes=ns1%40localhost', 'knownNodes=ns1%40localhost%2Cns1%40some-host']
        self.rest_parameter_match(expected_params)

# TODO: test rebalance status
# TODO: Test rebalance stop
# TODO: test recovery
# TODO: test reset admin password


class TestServerAdd(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'server-add'] + cluster_connect_args
        self.cmd_args = ['--server-add', 'some-host:6789', '--server-add-username', 'Administrator',
                         '--server-add-password', 'asdasd']

        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['server-group'] = {'groups': [
            {'name': 'name', 'uri': '/pools/default/serverGroups/0',
             'addNodeURI': '/pools/default/serverGroups/0/addNode',
             'nodes': [{'hostname': 'n1:8091'}, {'hostname': 'n2:8091'}]},
            {'name': 'name1', 'uri': '/pools/default/serverGroups/1',
             'addNodeURI': '/pools/default/serverGroups/1/addNode','nodes': []}
        ], 'uri': '/pools/default/serverGroups/rev=1'}
        self.server_args['indexes-settings'] = {'logLevel': 'info', 'indexerThreads': 0, 'storageMode': 'plasma',
                                'stableSnapshotInterval': 5000, 'maxRollbackPoints': 2, 'memorySnapshotInterval': 200}

        super(TestServerAdd, self).setUp()

    def test_server_add_basic(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        self.assertIn('POST:/pools/default/serverGroups/0/addNode', self.server.trace)
        expected_params = ['hostname=some-host%3A6789', 'user=Administrator', 'password=asdasd', 'services=kv']
        self.rest_parameter_match(expected_params)

    def test_server_add_group_name(self):
        self.no_error_run(self.command + self.cmd_args + ['--group-name', 'name1'], self.server_args)
        self.assertIn('POST:/pools/default/serverGroups/1/addNode', self.server.trace)
        expected_params = ['hostname=some-host%3A6789', 'user=Administrator', 'password=asdasd', 'services=kv']
        self.rest_parameter_match(expected_params)

    def test_server_add_services(self):
        self.no_error_run(self.command + self.cmd_args + ['--services', 'data,analytics,eventing,query,fts'],
                          self.server_args)
        self.assertIn('POST:/pools/default/serverGroups/0/addNode', self.server.trace)
        expected_params = ['hostname=some-host%3A6789', 'user=Administrator', 'password=asdasd',
                           ['services=kv%2Cfts%2Ccbas%2Cn1ql%2Ceventing']]
        self.assertEquals(len(expected_params), len(self.server.rest_params))
        for p in expected_params:
            if len(p) == 1:
                a = ''.join(sorted(p[0]))
                found = False
                for x in self.server.rest_params:
                    b = ''.join(sorted(x))
                    if a == b:
                        found = True
                        break
                self.assertTrue(found)
            else:
                self.assertIn(p, self.server.rest_params)


class TestServerInfo(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'server-info'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestServerInfo, self).setUp()

    def test_get_info(self):
        self.no_error_run(self.command, self.server_args)
        self.assertIn('GET:/nodes/self', self.server.trace)


class TestServerList(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'server-list'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'healthy',
             'clusterMembership': 'active'},
            {'otpNode': 'ns1@some-host', 'hostname': 'some-host:6789', 'status': 'healthy',
             'clusterMembership': 'active'}
        ]}
        super(TestServerList, self).setUp()

    def test_server_list(self):
        self.no_error_run(self.command, self.server_args)
        expected_out = ['ns1@localhost localhost:6789 healthy active', 'ns1@some-host some-host:6789 healthy active']
        for p in expected_out:
            self.assertIn(p, self.str_output)


class TestSettingAlert(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-alert'] + cluster_connect_args
        self.email_args = ['--enable-email-alert', '1', '--email-recipients', 'email1', '--email-sender', 'email2',
                           '--email-host', 'emailhost', '--email-port', '3000', '--enable-email-encrypt', '1',
                           '--email-user', 'emailuser', '--email-password', 'emailpwd']
        self.all_allerts = ['--alert-auto-failover-node', '--alert-auto-failover-max-reached',
                            '--alert-auto-failover-node-down', '--alert-auto-failover-cluster-small',
                            '--alert-auto-failover-disable', '--alert-ip-changed', '--alert-disk-space',
                            '--alert-meta-overhead', '--alert-meta-oom', '--alert-write-failed',
                            '--alert-audit-msg-dropped', '--alert-indexer-max-ram', '--alert-timestamp-drift-exceeded',
                            '--alert-communication-issue']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingAlert, self).setUp()

    def test_set_all_alerts(self):
        self.no_error_run(self.command + self.all_allerts + ['--enable-email-alert', '0'], self.server_args)
        expected_params = ['alerts=auto_failover_node%2Cauto_failover_maximum_reached%2Cauto_failover_other' +
                           '_nodes_down%2Cauto_failover_cluster_too_small%2Cauto_failover_disabled%2Cip%2Cdisk%2' +
                           'Coverhead%2Cep_oom_errors%2Cep_item_commit_failed%2Caudit_dropped_events%2Cindexer_ram_' +
                           'max_usage%2Cep_clock_cas_drift_threshold_exceeded%2Ccommunication_issue', 'enabled=false',
                           'emailEncrypt=false']

        self.assertIn('POST:/settings/alerts', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_set_all_alerts_and_email(self):
        self.no_error_run(self.command + self.all_allerts + self.email_args, self.server_args)
        expected_params = ['alerts=auto_failover_node%2Cauto_failover_maximum_reached%2Cauto_failover_other' +
                           '_nodes_down%2Cauto_failover_cluster_too_small%2Cauto_failover_disabled%2Cip%2Cdisk%2' +
                           'Coverhead%2Cep_oom_errors%2Cep_item_commit_failed%2Caudit_dropped_events%2Cindexer_ram_' +
                           'max_usage%2Cep_clock_cas_drift_threshold_exceeded%2Ccommunication_issue', 'enabled=true',
                           'emailEncrypt=true', 'sender=email2', 'recipients=email1', 'emailUser=emailuser',
                           'emailPass=emailpwd', 'emailHost=emailhost', 'emailPort=3000']

        self.assertIn('POST:/settings/alerts', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_set_email(self):
        self.no_error_run(self.command + self.email_args, self.server_args)
        expected_params = ['alerts=', 'emailEncrypt=true', 'sender=email2', 'recipients=email1', 'emailUser=emailuser',
                           'emailPass=emailpwd', 'emailHost=emailhost', 'emailPort=3000', 'enabled=true']
        self.assertIn('POST:/settings/alerts', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_error_when_email_enabled_but_no_info(self):
        self.system_exit_run(self.command + ['--enable-email-alert', '1'], self.server_args)
        self.assertIn('must be set when email alerts are enabled', self.str_output)


class TestSettingAudit(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-audit'] + cluster_connect_args
        self.cmd_args = ['--audit-enabled', '1', '--audit-log-path', 'path', '--audit-log-rotate-interval', '1',
                         '--audit-log-rotate-size', '2']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingAudit, self).setUp()

    def test_setting_audit(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        expected_params = ['auditdEnabled=true', 'logPath=path', 'rotateInterval=1', 'rotateSize=2']
        self.assertIn('POST:/settings/audit', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_setting_audit_enabled_no_options(self):
        self.system_exit_run(self.command + ['--audit-enabled', '1'], self.server_args)
        self.assertIn('The audit log path must be specified when auditing is first set up', self.str_output)


class TestSettingAutofailover(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-autofailover'] + cluster_connect_args
        self.cmd_args = ['--enable-auto-failover', '1', '--auto-failover-timeout', '10']
        self.EE_args = ['--enable-failover-of-server-groups', '1', '--max-failovers', '3',
                        '--enable-failover-on-data-disk-issues', '1', '--failover-data-disk-period', '20']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingAutofailover, self).setUp()

    def test_setting_auto_failover_basic(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        expected_params = ['enabled=true', 'timeout=10']
        self.assertIn('POST:/settings/autoFailover', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_setting_auto_failover_EE(self):
        self.no_error_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        expected_params = ['enabled=true', 'timeout=10', 'failoverServerGroup=true',
                           'failoverOnDataDiskIssues%5Benabled%5D=true', 'failoverOnDataDiskIssues%5BtimePeriod%5D=20',
                           'maxCount=3']
        self.assertIn('POST:/settings/autoFailover', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_setting_audit_CE(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        expected_params = ['enabled=true', 'timeout=10']
        self.assertIn('POST:/settings/autoFailover', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_setting_audit_CE_EE_flags(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        self.assertIn('can only be configured on enterprise edition', self.str_output)


class TestSettingAutoreporovision(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-autoreprovision'] + cluster_connect_args
        self.cmd_args = ['--enabled', '1', '--max-nodes', '3']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingAutoreporovision, self).setUp()

    def test_basic_autoreporvision_setting(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        expected_params = ['enabled=true', 'maxNodes=3']
        self.assertIn('POST:/settings/autoReprovision', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_error_autoreporvision_setting_enabled_and_no_max_nodes(self):
        self.system_exit_run(self.command + ['--enabled', '1'], self.server_args)
        self.assertIn('must be specified if auto-reprovision is enabled', self.str_output)


class TestSettingCluster(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-cluster'] + cluster_connect_args
        self.admin_args = ['--cluster-username', 'admin', '--cluster-password', 'password', '--cluster-port', '9000']
        self.cluster_settings = ['--cluster-ramsize', '100', '--cluster-index-ramsize', '200', '--cluster-fts-ramsize',
                                 '300', '--cluster-eventing-ramsize', '400', '--cluster-analytics-ramsize', '500',
                                 '--cluster-name', 'name']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingCluster, self).setUp()

    def test_setting_cluster_admin(self):
        self.no_error_run(self.command + self.admin_args, self.server_args)
        self.assertIn('POST:/settings/web', self.server.trace)
        expected_params = ['username=admin', 'password=password', 'port=9000']
        self.rest_parameter_match(expected_params)

    def test_setting_cluster_memory(self):
        self.no_error_run(self.command + self.cluster_settings, self.server_args)
        self.assertIn('POST:/pools/default', self.server.trace)
        expected_params = ['ftsMemoryQuota=300', 'memoryQuota=100', 'clusterName=name', 'eventingMemoryQuota=400',
                           'cbasMemoryQuota=500', 'indexMemoryQuota=200']
        self.rest_parameter_match(expected_params)


class TestSettingCompaction(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-compaction'] + cluster_connect_args
        self.basic_args = [
            '--compaction-db-percentage', '10', '--compaction-db-size', '20', '--compaction-view-percentage', '25',
            '--compaction-view-size', '30', '--enable-compaction-parallel', '0', '--enable-compaction-abort', '1',
            '--compaction-period-from', '12:00', '--compaction-period-to', '15:00', '--metadata-purge-interval', '3'
        ]
        self.circular_args = ['--gsi-compaction-mode', 'circular', '--compaction-gsi-interval', 'Monday,Tuesday,Sunday',
                              '--compaction-gsi-period-from', '00:00', '--compaction-gsi-period-to', '12:00',
                              '--enable-gsi-compaction-abort', '1']
        self.append_args = ['--gsi-compaction-mode', 'append', '--compaction-gsi-percentage', '20']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingCompaction, self).setUp()

    def test_setting_compaction_basic(self):
        self.no_error_run(self.command + self.basic_args, self.server_args)
        expected_params = ['allowedTimePeriod%5BtoMinute%5D=0', 'databaseFragmentationThreshold%5Bpercentage%5D=10',
                           'allowedTimePeriod%5BtoHour%5D=15', 'allowedTimePeriod%5BfromMinute%5D=0',
                           'viewFragmentationThreshold%5Bsize%5D=31457280', 'parallelDBAndViewCompaction=false',
                           'databaseFragmentationThreshold%5Bsize%5D=20971520',
                           'allowedTimePeriod%5BabortOutside%5D=true', 'allowedTimePeriod%5BfromHour%5D=12',
                           'purgeInterval=3.0', 'viewFragmentationThreshold%5Bpercentage%5D=25']
        self.assertIn('POST:/controller/setAutoCompaction', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_all_settings_circular(self):
        self.no_error_run(self.command + self.basic_args+ self.circular_args, self.server_args)
        expected_params =['allowedTimePeriod%5BtoMinute%5D=0', 'databaseFragmentationThreshold%5Bpercentage%5D=10',
         'allowedTimePeriod%5BtoHour%5D=15', 'indexCircularCompaction%5Binterval%5D%5BabortOutside%5D=true',
         'indexCompactionMode=circular', 'allowedTimePeriod%5BfromMinute%5D=0',
         'viewFragmentationThreshold%5Bsize%5D=31457280', 'indexCircularCompaction%5Binterval%5D%5BfromMinute%5D=0',
         'parallelDBAndViewCompaction=false', 'databaseFragmentationThreshold%5Bsize%5D=20971520',
         'allowedTimePeriod%5BabortOutside%5D=true', 'allowedTimePeriod%5BfromHour%5D=12',
         'indexCircularCompaction%5Binterval%5D%5BtoMinute%5D=0', 'purgeInterval=3.0',
         'viewFragmentationThreshold%5Bpercentage%5D=25', 'indexCircularCompaction%5Binterval%5D%5BtoHour%5D=12',
         'indexCircularCompaction%5Binterval%5D%5BfromHour%5D=0',
         'indexCircularCompaction%5BdaysOfWeek%5D=Monday%2CTuesday%2CSunday']
        self.assertIn('POST:/controller/setAutoCompaction', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_compaction_setting_abort_outside_with_no_time(self):
        self.system_exit_run(self.command + self.basic_args[:12], self.server_args)
        self.assertIn('is required when using --enable-compaction-abort', self.str_output)


class TestSettingIndex(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-index'] + cluster_connect_args
        self.basic_args = [
            '--index-max-rollback-points', '2', '--index-stable-snapshot-interval', '30',
            '--index-memory-snapshot-interval', '40', '--index-threads', '1', '--index-log-level', 'warn',
            '--index-storage-setting', 'memopt'
        ]
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingIndex, self).setUp()

    def test_setting_index(self):
        self.no_error_run(self.command + self.basic_args, self.server_args)
        expected_params = ['storageMode=memory_optimized', 'maxRollbackPoints=2', 'stableSnapshotInterval=30',
                           'memorySnapshotInterval=40', 'indexerThreads=1', 'logLevel=warn']
        self.assertIn('POST:/settings/indexes', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_no_settings(self):
        self.system_exit_run(self.command, self.server_args)
        self.assertIn('No settings specified to be changed', self.str_output)


class TestSASL(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-saslauthd'] + cluster_connect_args
        self.basic_args = [
            '--enabled', '1', '--admins', 'admin1,admin2', '--roadmins', 'admin3,admin4',
        ]
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSASL, self).setUp()

    def test_ldap_enabled_default_none(self):
        self.no_error_run(self.command + self.basic_args, self.server_args)
        expected_params = ['roAdmins=admin3%0Aadmin4', 'admins=admin1%0Aadmin2', 'enabled=true']
        self.assertIn('POST:/settings/saslauthdAuth', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_ldap_enabled_default_admins(self):
        self.no_error_run(self.command + self.basic_args + ['--default', 'admins'], self.server_args)
        expected_params = ['roAdmins=admin3%0Aadmin4', 'enabled=true']
        self.assertIn('POST:/settings/saslauthdAuth', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_ldap_enabled_default_roadmins(self):
        self.no_error_run(self.command + self.basic_args + ['--default', 'roadmins'], self.server_args)
        expected_params = ['admins=admin1%0Aadmin2', 'enabled=true']
        self.assertIn('POST:/settings/saslauthdAuth', self.server.trace)
        self.rest_parameter_match(expected_params)


class TestSettingNotification(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-notification'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingNotification, self).setUp()

    def test_enable_notification(self):
        self.no_error_run(self.command + ['--enable-notifications', '1'], self.server_args)
        expected_params = ['sendStats=true']
        self.assertIn('POST:/settings/stats', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_disable_notification(self):
        self.no_error_run(self.command + ['--enable-notifications', '0'], self.server_args)
        expected_params = ['sendStats=false']
        self.assertIn('POST:/settings/stats', self.server.trace)
        self.rest_parameter_match(expected_params)


class TestSettingPasswordPolicy(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-password-policy'] + cluster_connect_args
        self.get_args = ['--get']
        self.set_args = ['--set', '--min-length', '8', '--uppercase', '1', '--lowercase', '1', '--digit', '1',
                         '--special-char', '1']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['password-policy'] = {
            'enforceDigits': False,
            'enforceLowercase': False,
            'enforceSpecialChars': False,
            'enforceUppercase': False,
            'minLength': 6
        }
        super(TestSettingPasswordPolicy, self).setUp()

    def test_get_password_policy(self):
        self.no_error_run(self.command + self.get_args, self.server_args)
        expected_out = ['"enforceDigits": false', '"enforceLowercase": false', '"enforceSpecialChars": false',
                        '"enforceUppercase": false', '"minLength": 6']

        self.assertIn('GET:/settings/passwordPolicy', self.server.trace)
        for p in expected_out:
            self.assertIn(p, self.str_output)

    def test_set_password_policy(self):
        self.no_error_run(self.command + self.set_args, self.server_args)
        expected_params = ['enforceDigits=true', 'enforceLowercase=true', 'enforceSpecialChars=true',
                           'enforceUppercase=true', 'minLength=8']

        self.assertIn('POST:/settings/passwordPolicy', self.server.trace)
        self.rest_parameter_match(expected_params)


class TestSettingSecurity(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-security'] + cluster_connect_args
        self.set_args = ['--set', '--disable-http-ui']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingSecurity, self).setUp()

    def test_set_settings(self):
        self.no_error_run(self.command + ['--set', '--disable-http-ui', '1', '--cluster-encryption-level', 'all',
                                          '--tls-honor-cipher-order', '1', '--tls-min-version', 'tlsv1.2'],
                          self.server_args)
        expected_params = ['disableUIOverHttp=true', 'clusterEncryptionLevel=all', 'tlsMinVersion=tlsv1.2',
                           'honorCipherOrder=true']
        self.assertIn('POST:/settings/security', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_invalid_choice(self):
        self.system_exit_run(self.command + ['--set', '--tls-min-version', 'tlsv2'], self.server_args)

    def test_cipher_suites_empty(self):
        self.no_error_run(self.command + ['--set', '--cipher-suites', ''],
                          self.server_args)
        expected_params = ['cipherSuites=%5B%5D']
        self.assertIn('POST:/settings/security', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_cipher_suites_list(self):
        self.no_error_run(self.command + ['--set', '--cipher-suites', 'suite1,suite2'],
                          self.server_args)
        expected_params = ['cipherSuites=%5B%22suite1%22%2C+%22suite2%22%5D']
        self.assertIn('POST:/settings/security', self.server.trace)
        self.rest_parameter_match(expected_params)


class TestSettingXdcr(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-xdcr'] + cluster_connect_args
        self.cmd_args = ['--checkpoint-interval', '60', '--worker-batch-size', '50', '--doc-batch-size', '40',
                         '--failure-restart-interval', '30', '--optimistic-replication-threshold', '20',
                         '--source-nozzle-per-node', '10', '--target-nozzle-per-node', '5', '--bandwidth-usage-limit',
                         '4', '--log-level', 'Trace', '--stats-interval', '3', '--max-processes', '8']
        self.EE_args = ['--enable-compression', '1']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingXdcr, self).setUp()

    def test_set_xdcr_settings(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        self.assertIn('POST:/settings/replications', self.server.trace)
        expected_params = ['targetNozzlePerNode=5', 'docBatchSizeKb=40', 'logLevel=Trace', 'failureRestartInterval=30',
                           'networkUsageLimit=4', 'optimisticReplicationThreshold=20', 'statsInterval=3',
                           'checkpointInterval=60', 'workerBatchSize=50', 'sourceNozzlePerNode=10', 'goMaxProcs=8']
        self.rest_parameter_match(expected_params)

    def test_set_xdcr_settings_EE(self):
        self.no_error_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        self.assertIn('POST:/settings/replications', self.server.trace)
        expected_params = ['targetNozzlePerNode=5', 'docBatchSizeKb=40', 'logLevel=Trace', 'failureRestartInterval=30',
                           'networkUsageLimit=4', 'optimisticReplicationThreshold=20', 'statsInterval=3',
                           'checkpointInterval=60', 'workerBatchSize=50', 'sourceNozzlePerNode=10',
                           'compressionType=Auto', 'goMaxProcs=8']
        self.rest_parameter_match(expected_params)

    def test_set_xdcr_settings_EE_in_CE(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        self.assertIn('can only be configured on enterprise edition', self.str_output)

# TODO: TestSettingMasterPassword
# TODO: TestSslManage
# TODO: TestRestCipherSuites
# TODO: TestResetAdminPassword


class TestUserManage(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'user-manage'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['rbac-users'] = [
          {
            "id": "read",
            "domain": "local",
            "roles": [
              {
                "role": "ro_admin",
                "origins": [{"type": "group"}, {"type": "user"}],
              },
              {
                "role": "admin",
                "origins": [{"type": "group"}],
              }
            ],
            "name": "name",
            "password_change_date": "2018-11-15T15:01:16.000Z",
            "groups": ["group1", "group2"]
          },
          {
            "id": "write",
            "domain": "local",
            "roles": [],
            "name": "name",
            "password_change_date": "2018-11-15T15:19:55.000Z"
          }
        ]
        self.server_args['rbac-groups'] = [
          {
            "id": "group1",
            "roles": [
              {
                "role": "bucket_full_access",
                "bucket_name": "*"
              },
              {
                "role": "replication_admin"
              }
            ],
            "ldap_group_ref": "test=ldap",
            "description": "descr"
          }
        ]
        super(TestUserManage, self).setUp()

    def test_list_users(self):
        self.no_error_run(self.command + ['--list'], self.server_args)
        self.assertIn('GET:/settings/rbac/users', self.server.trace)
        expected_out = ['"id": "write"', '"id": "read"']
        for p in expected_out:
            self.assertIn(p, self.str_output)

    def test_get_user(self):
        self.no_error_run(self.command + ['--get', '--rbac-username', 'write'], self.server_args)
        self.assertIn('GET:/settings/rbac/users', self.server.trace)
        expected_out = ['"id": "write"', '"domain": "local"', '"roles"', '"name": "name"']
        for p in expected_out:
            self.assertIn(p, self.str_output)

    def test_delete_local_user(self):
        self.no_error_run(self.command + ['--delete', '--rbac-username', 'write', '--auth-domain', 'local'],
                          self.server_args)
        self.assertIn('DELETE:/settings/rbac/users/local/write', self.server.trace)

    def test_delete_external_user(self):
        self.no_error_run(self.command + ['--delete', '--rbac-username', 'write', '--auth-domain', 'external'],
                          self.server_args)
        self.assertIn('DELETE:/settings/rbac/users/external/write', self.server.trace)

    def test_get_my_roles(self):
        self.server_args['whoami'] = {"id": "Administrator", "domain": "admin", "roles": [{"role": "admin"}]}
        self.no_error_run(self.command + ['--my-roles'], self.server_args)
        self.assertIn('GET:/whoami', self.server.trace)
        self.assertIn('"id": "Administrator', self.str_output)

    def test_set_local_user(self):
        self.no_error_run(self.command + ['--set', '--rbac-username', 'username', '--rbac-password', 'pwd',
                                          '--auth-domain', 'local', '--roles', 'admin', '--rbac-name', 'name', '--user-groups', ''],
                          self.server_args)
        self.assertIn('PUT:/settings/rbac/users/local/username', self.server.trace)
        expected_params = ['name=name', 'password=pwd', 'roles=admin', 'groups=']
        self.rest_parameter_match(expected_params)

    def test_set_existing_user(self):
        self.no_error_run(self.command + ['--set', '--rbac-username', 'read',
                                          '--auth-domain', 'local', '--rbac-name', 'name2'],
                          self.server_args)
        self.assertIn('PUT:/settings/rbac/users/local/read', self.server.trace)
        expected_params = ['name=name2', 'roles=ro_admin', 'groups=group1%2Cgroup2']
        self.rest_parameter_match(expected_params)

    def test_set_external_user(self):
        self.no_error_run(self.command + ['--set', '--rbac-username', 'username', '--auth-domain',
                                         'external', '--roles', 'admin', '--rbac-name', 'name', '--user-groups', ''], self.server_args)
        self.assertIn('PUT:/settings/rbac/users/external/username', self.server.trace)
        expected_params = ['name=name', 'roles=admin', 'groups=']
        self.rest_parameter_match(expected_params)

    def test_create_group_basic(self):
        self.no_error_run(self.command + ['--set-group', '--group-name', 'user-group', '--roles', 'admin',
                                          '--group-description', 'Lorem ipsum dolor', '--ldap-ref', 'some-ref'],
                          self.server_args)
        self.assertIn('PUT:/settings/rbac/groups/user-group', self.server.trace)
        expected_params = ['description=Lorem+ipsum+dolor', 'roles=admin', 'ldap_group_ref=some-ref']
        self.rest_parameter_match(expected_params)

    def test_change_existing_group(self):
        self.no_error_run(self.command + ['--set-group', '--group-name', 'group1',
                                          '--group-description', 'Lorem ipsum dolor'],
                          self.server_args)
        self.assertIn('PUT:/settings/rbac/groups/group1', self.server.trace)
        expected_params = ['description=Lorem+ipsum+dolor',
                           'roles=bucket_full_access%5B%2A%5D%2Creplication_admin',
                           'ldap_group_ref=test%3Dldap']
        self.rest_parameter_match(expected_params)

    def test_create_group_no_name(self):
        self.system_exit_run(self.command + ['--set-group', '--roles', 'ro_admin',
                                          '--group-description', 'Lorem ipsum dolor', '--ldap-ref', 'some-ref'],
                          self.server_args)
        self.assertIn('--group-name is required with --set-group', self.str_output)

    def test_create_group_non_ldap(self):
        self.no_error_run(self.command + ['--set-group', '--group-name', 'user-group', '--roles', 'admin',
                                          '--group-description', 'Lorem ipsum dolor'],
                          self.server_args)
        self.assertIn('PUT:/settings/rbac/groups/user-group', self.server.trace)
        expected_params = ['description=Lorem+ipsum+dolor', 'roles=admin']
        self.rest_parameter_match(expected_params)

    def test_delete_group(self):
        self.no_error_run(self.command + ['--delete-group', '--group-name', 'name'], self.server_args)
        self.assertIn('DELETE:/settings/rbac/groups/name', self.server.trace)

    def test_delete_group_no_name(self):
        self.system_exit_run(self.command + ['--delete-group'], self.server_args)
        self.assertIn('--group-name is required with the --delete-group option', self.str_output)

    def test_list_group(self):
        self.no_error_run(self.command + ['--list-groups'], self.server_args)
        self.assertIn('GET:/settings/rbac/groups', self.server.trace)

    def test_get_group(self):
        self.no_error_run(self.command + ['--get-group', '--group-name', 'group1'], self.server_args)
        self.assertIn('GET:/settings/rbac/groups/group1', self.server.trace)

    def test_get_group_no_group_name(self):
        self.system_exit_run(self.command + ['--get-group'], self.server_args)
        self.assertIn('--group-name is required with the --get-group option', self.str_output)


class TestXdcrReplicate(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'xdcr-replicate'] + cluster_connect_args
        self.cmd_args = ['--xdcr-cluster-name', 'name', '--xdcr-hostname', 'hostname', '--xdcr-username', 'username',
                         '--xdcr-password', 'pwd']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestXdcrReplicate, self).setUp()

    def test_create_EE(self):
        self.no_error_run(self.command + ['--create', '--xdcr-cluster-name', 'cluster1', '--xdcr-to-bucket', 'bucket2',
                                          '--xdcr-from-bucket', 'bucket1', '--filter-expression', 'key:[a-zA-z]+',
                                          '--xdcr-replication-mode', 'capi', '--enable-compression', '1'],
                          self.server_args)
        self.assertIn('POST:/controller/createReplication', self.server.trace)
        expectedParams = ['toBucket=bucket2', 'fromBucket=bucket1', 'toCluster=cluster1', 'compressionType=Auto',
                          'type=capi', 'filterExpression=key%3A%5Ba-zA-z%5D%2B', 'replicationType=continuous']

        self.rest_parameter_match(expectedParams)

    def test_create_CE_with_EE(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + ['--create', '--xdcr-cluster-name', 'cluster1', '--xdcr-to-bucket', 'bucket2',
                                          '--xdcr-from-bucket', 'bucket1', '--filter-expression', 'key:[a-zA-z]+',
                                          '--xdcr-replication-mode', 'capi', '--enable-compression', '1'],
                          self.server_args)
        self.assertIn('can only be configured on enterprise edition', self.str_output)

    def test_delete_replicate(self):
        self.no_error_run(self.command + ['--delete', '--xdcr-replicator', '1'], self.server_args)
        self.assertIn('DELETE:/controller/cancelXDCR/1', self.server.trace)

    def test_settings(self):
        self.no_error_run(self.command + ['--settings', '--xdcr-replicator', '1', '--filter-expression', 'key:',
                                          '--filter-skip-restream', '--enable-compression', '1',
                                          '--checkpoint-interval', '60', '--worker-batch-size', '5000',
                                          '--doc-batch-size', '5000', '--failure-restart-interval', '100',
                                          '--optimistic-replication-threshold', '100', '--stats-interval', '200',
                                          '--log-level', 'Info', '--bandwidth-usage-limit', '5', '--priority', 'Medium'
                                          ], self.server_args)
        self.assertIn('POST:/settings/replications/1', self.server.trace)
        expected_params = ['checkpointInterval=60', 'workerBatchSize=5000', 'docBatchSizeKb=5000',
                           'failureRestartInterval=100', 'optimisticReplicationThreshold=100', 'statsInterval=200',
                           'compressionType=Auto', 'filterExpression=key%3A', 'filterSkipRestream=1', 'logLevel=Info',
                           'networkUsageLimit=5', 'priority=Medium']
        self.rest_parameter_match(expected_params)

    def test_setting_filter_args(self):
        self.no_error_run(self.command + ['--settings', '--xdcr-replicator', '1', '--filter-expression', 'key:',
                                          '--filter-skip-restream', '--reset-expiry', '1',
                                          '--filter-deletion', '0', '--filter-expiration', '1'], self.server_args)
        self.assertIn('POST:/settings/replications/1', self.server.trace)
        expected_params = ['filterExpression=key%3A', 'filterSkipRestream=1', 'filterBypassExpiry=true',
                           'filterDeletion=false', 'filterExpiration=true']
        self.rest_parameter_match(expected_params)

    def test_list_replicate(self):
        self.no_error_run(self.command + ['--list'], self.server_args)
        self.assertIn('GET:/pools/default/tasks', self.server.trace)

    def test_pause_resume(self):
        self.no_error_run(self.command + ['--resume', '--xdcr-replicator', '1'], self.server_args)
        self.assertIn('GET:/pools/default/tasks', self.server.trace)


class TestXdcrSetup(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'xdcr-setup'] + cluster_connect_args
        self.cmd_args = ['--xdcr-cluster-name', 'name', '--xdcr-hostname', 'hostname', '--xdcr-username', 'username',
                         '--xdcr-password', 'pwd']
        # TODO: encryption setting test
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestXdcrSetup, self).setUp()

    def test_create_xdcr(self):
        self.no_error_run(self.command + ['--create'] + self.cmd_args, self.server_args)
        self.assertIn('POST:/pools/default/remoteClusters', self.server.trace)
        expected_params = ['name=name', 'hostname=hostname', 'username=username', 'password=pwd', 'demandEncryption=0']
        self.rest_parameter_match(expected_params)

    def test_delete_xdcr(self):
        self.no_error_run(self.command + ['--delete'] + self.cmd_args, self.server_args)
        self.assertIn('DELETE:/pools/default/remoteClusters/name', self.server.trace)

    def test_edit_xdcr(self):
        self.no_error_run(self.command + ['--edit'] + self.cmd_args, self.server_args)
        self.assertIn('POST:/pools/default/remoteClusters/name', self.server.trace)
        expected_params = ['name=name', 'hostname=hostname', 'username=username', 'password=pwd', 'demandEncryption=0']
        self.rest_parameter_match(expected_params)

    def test_list_xdcr(self):
        self.server_args['remote-clusters'] = [{'name': 'name', 'uuid': '1', 'hostname': 'host', 'username': 'user',
                                                'uri': 'uri', 'deleted': False}]
        self.no_error_run(self.command + ['--list'] + self.cmd_args, self.server_args)
        self.assertIn('GET:/pools/default/remoteClusters/', self.server.trace)
        expected_out = ['cluster name: name', 'uuid: 1', 'host name: host', 'user name: user', 'uri: uri']
        for p in expected_out:
            self.assertIn(p, self.str_output)

# TODO: TestEventingFunctionSetup


class TestUserChangePassword(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'user-change-password', '--new-password', 'pwd'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestUserChangePassword, self).setUp()

    def test_user_change_password(self):
        self.no_error_run(self.command, self.server_args)
        self.assertIn('POST:/controller/changePassword', self.server.trace)
        expected_params = ['password=pwd']
        self.rest_parameter_match(expected_params)


class TestCollectionManage(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'collection-manage'] + cluster_connect_args + ['--bucket', 'name']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestCollectionManage, self).setUp()

    def test_create_scope(self):
        self.no_error_run(self.command + ['--create-scope', 'scope_1'], self.server_args)
        self.assertIn('POST:/pools/default/buckets/name/collections', self.server.trace)
        expected_params = ['name=scope_1']
        self.rest_parameter_match(expected_params)

    def test_delete_scope(self):
        self.no_error_run(self.command + ['--drop-scope', 'scope_1'], self.server_args)
        self.assertIn('DELETE:/pools/default/buckets/name/collections/scope_1', self.server.trace)

    def list_scopes(self):
        self.server_args['collection_manifest'] = {"scope_1":{"collection_1":{}}, "scope_2":{"collection_2":{}}}
        self.no_error_run(self.command + ['--list-scopes'], self.server_args)
        self.assertIn('GET:/pools/default/buckets/name/collections', self.server.trace)
        expected_out = ['scope_1', 'scope_2']
        for p in expected_out:
            self.assertIn(p, self.str_output)

    def test_create_collection(self):
        self.no_error_run(self.command + ['--create-collection', 'scope_1.collection_1', '--max-ttl', '100'],
                          self.server_args)
        self.assertIn('POST:/pools/default/buckets/name/collections/scope_1', self.server.trace)
        expected_params = ['name=collection_1', 'maxTTL=100']
        self.rest_parameter_match(expected_params)

    def test_delete_collection(self):
        self.no_error_run(self.command + ['--drop-collection', 'scope_1.collection_1'], self.server_args)
        self.assertIn('DELETE:/pools/default/buckets/name/collections/scope_1/collection_1', self.server.trace)

    def list_collections(self):
        self.server_args['collection_manifest'] = {"scope_1":{"collection_1":{}, "collection_2":{}}}
        self.no_error_run(self.command + ['--list-collections', 'scope_1'], self.server_args)
        self.assertIn('GET:/pools/default/buckets/name/collections', self.server.trace)
        expected_out = ['collection_1', 'collection_2']
        for p in expected_out:
            self.assertIn(p, self.str_output)


class TestSettingLdap(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-ldap'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.authentication_args = ['--authentication-enabled', '1', '--hosts', '0.0.0.0', '--port', '369',
                                    '--encryption', 'none', '--request-timeout', '2000', '--max-parallel', '20',
                                    '--max-cache-size', '20', '--cache-value-lifetime', '2000000',
                                    '--disable-cert-validation']
        self.authorization_args = ['--authorization-enabled', '1', '--query-dn', 'admin', '--query-pass', 'pass',
                                   '--enable-nested-groups', '1', '--nested-group-max-depth', '10', '--group-query',
                                   '%D?memberOf?base']
        super(TestSettingLdap, self).setUp()

    def test_set_ldap_authentication_only(self):
        self.no_error_run(self.command + self.authentication_args + ['--authorization-enabled', '0'], self.server_args)
        self.assertIn('POST:/settings/ldap', self.server.trace)
        expected_params = ['authenticationEnabled=true', 'authorizationEnabled=false', 'hosts=0.0.0.0', 'port=369',
                           'encryption=None', 'requestTimeout=2000', 'maxParallelConnections=20',
                           'maxCacheSize=20', 'cacheValueLifetime=2000000', 'serverCertValidation=false']
        self.rest_parameter_match(expected_params)

    def test_set_ldap_all(self):
        self.no_error_run(self.command + self.authentication_args + self.authorization_args, self.server_args)
        self.assertIn('POST:/settings/ldap', self.server.trace)
        expected_params = ['authenticationEnabled=true', 'authorizationEnabled=true', 'hosts=0.0.0.0', 'port=369',
                           'encryption=None', 'requestTimeout=2000', 'maxParallelConnections=20',
                           'maxCacheSize=20', 'cacheValueLifetime=2000000', 'bindDN=admin', 'bindPass=pass',
                           'nestedGroupsEnabled=true', 'nestedGroupsMaxDepth=10',
                           'groupsQuery=%25D%3FmemberOf%3Fbase', 'serverCertValidation=false']
        self.rest_parameter_match(expected_params)


class TestSettingOnDemand(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-on-demand'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True, '/settings/license': {
            "reporting_enabled": False, "contract_id": "0", "customer_token": "0",
            "reporting_interval": 3600000, "reporting_endpoint": "test",
            "reporting_timeout": 5000}}
        self.set_args = ['--reporting-enabled', '1', '--contract-id', '1', '--customer-token', '2']
        super(TestSettingOnDemand, self).setUp()

    def test_licensing_ce(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command+['--set'], self.server_args)
        self.assertIn('This command is ony available in enterprise edition', self.str_output)

    def test_licensing_list(self):
        self.no_error_run(self.command+['--get'], self.server_args)
        self.assertIn('GET:/settings/license', self.server.trace)
        self.assertIn("Reporting enabled: False\nContract id: 0\nReporting interval: 3600000 ms\n" +
                      "Reporting time out: 5000 ms\nReporting endpoint: test", self.str_output)

    def test_licensing_json(self):
        self.no_error_run(self.command + ['--get', '-o', 'json'], self.server_args)
        self.assertIn('GET:/settings/license', self.server.trace)
        json_str = json.dumps(self.server_args['/settings/license'])
        json_str = json_str.replace("{", "")
        json_str = json_str.replace("}", "")
        fields = json_str.split(",")
        for f in fields:
            self.assertIn(f.strip(), self.str_output)

    def test_licensing_no_options(self):
        self.system_exit_run(self.command, self.server_args)
        self.assertIn('Please provide one of --set, --get or --generate-report', self.str_output)

    def test_licensing_2_options(self):
        self.system_exit_run(self.command + ['--set', '--get'], self.server_args)
        self.assertIn('Please provide only one of --set, --get or --generate-report', self.str_output)

    def test_licensing_configure(self):
        self.no_error_run(self.command + ['--set'] + self.set_args, self.server_args)
        self.assertIn('POST:/settings/license', self.server.trace)
        expected_params = ['reporting_enabled=true', 'contract_id=1', 'customer_token=2']
        self.rest_parameter_match(expected_params, False)


class TestIpFamily(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'ip-family'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestIpFamily, self).setUp()

    def testGetSingleNode(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv4', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def testGetMultipleNode(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet6'}, {'addressFamily': 'inet6'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv6', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def testGetMultipleNodeTLS(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet6_tls'}, {'addressFamily': 'inet6_tls'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv6', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def testGetMultipleNodeMixed(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet'}, {'addressFamily': 'inet6_tls'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster is in mixed mode', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def testSetIPv4(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--set', '--ipv4'], self.server_args)
        self.assertIn('Switched IP family of the cluster', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableExternalListener', self.server.trace)
        expected_params = ['afamily=ipv4', 'afamily=ipv4', 'afamily=ipv6']
        self.rest_parameter_match(expected_params, True)

    def testSetIPv6(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--set', '--ipv6'], self.server_args)
        self.assertIn('Switched IP family of the cluster', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableExternalListener', self.server.trace)
        expected_params = ['afamily=ipv6', 'afamily=ipv6', 'afamily=ipv4']
        self.rest_parameter_match(expected_params, True)


class TestClusterEncryption(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'node-to-node-encryption'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestClusterEncryption, self).setUp()

    def testErrorDisableAndEnable(self):
        self.system_exit_run(self.command + ['--enable', '--disable'], self.server_args)

    def testGetEncryptionFalse(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'nodeEncryption': False, 'hostname': 'host1'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Node-to-node encryption is disabled', self.str_output)

    def testGetEncryptionTrue(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'nodeEncryption': True, 'hostname': 'host1'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Node-to-node encryption is enabled', self.str_output)

    def testGetEncryptionMixedMode(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'nodeEncryption': True, 'hostname': 'host1'},
                                                      {'nodeEncryption': False, 'hostname': 'host2'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster is in mixed mode', self.str_output)

    def testEnableClusterEncryptionIPv4(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--enable'], self.server_args)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableExternalListener', self.server.trace)
        self.rest_parameter_match(['nodeEncryption=on', 'nodeEncryption=on', 'nodeEncryption=off'])

    def testDisableClusterEncryptionIPv4(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--disable'], self.server_args)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableExternalListener', self.server.trace)
        self.rest_parameter_match(['nodeEncryption=off', 'nodeEncryption=off', 'nodeEncryption=on'])


class TestSettingRebalance(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-rebalance'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingRebalance, self).setUp()

    def testCEInvalid(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + ['--get'], self.server_args)
        self.assertIn('Automatic rebalance retry configuration is an Enterprise Edition only feature', self.str_output)

    def testMoreThanOneAction(self):
        self.system_exit_run(self.command + ['--get', '--set'], self.server_args)
        self.assertIn('Provide either --set, --get, --cancel or --pending-info', self.str_output)

    def testGetHumanFriendly(self):
        self.server_args['/settings/retryRebalance'] = {"enabled": False, "afterTimePeriod": 300, "maxAttempts": 1}
        self.no_error_run(self.command + ['--get'], self.server_args)
        expected_output = ['Automatic rebalance retry disabled', 'Retry wait time: 300', 'Maximum number of retries: 1']
        for e in expected_output:
            self.assertIn(e, self.str_output)

    def testGetJson(self):
        self.server_args['/settings/retryRebalance'] = {"enabled": False, "afterTimePeriod": 300, "maxAttempts": 1}
        self.no_error_run(self.command + ['--get', '--output', 'json'], self.server_args)
        self.assertIn('{"enabled": false, "afterTimePeriod": 300, "maxAttempts": 1}', self.str_output)

    def testSet(self):
        self.no_error_run(self.command + ['--set', '--enable', '1', '--wait-for', '5', '--max-attempts', '3'],
                          self.server_args)
        expected_params = ['enabled=true', 'afterTimePeriod=5', 'maxAttempts=3']
        self.rest_parameter_match(expected_params)

    def testSetWaitForOutOfRange(self):
        self.system_exit_run(self.command + ['--set', '--enable', '1', '--wait-for', '1', '--max-attempts', '3'],
                          self.server_args)
        self.assertIn('--wait-for must be a value between 5 and 3600', self.str_output)


if __name__ == '__main__':
    unittest.main()
