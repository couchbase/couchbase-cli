"""This test do not ensure correct server side behaviour they only check that the cli makes the correct requests and
that validates input correctly"""
import json
import os
import socket
import sys
import tempfile
import threading
import unittest
import urllib
from argparse import Namespace
from io import StringIO
from unittest.mock import MagicMock, patch

from mock_server import MockRESTServer, generate_self_signed_cert

from cbmgr import AnalyticsLinkSetup, CollectionManage, CouchbaseCLI, ResetAdminPassword, validate_credential_flags
from couchbaseConstants import parse_host_port

host = '127.0.0.1'
port = 6789
cluster_connect_args = ['-c', host + ":" + str(port), '-u', 'Administrator', '-p', 'asdasd']


# setUpModule will generate new certificates for the mock HTTPS server used during unit testing.
#
# NOTE: We don't remove the key/cert once generated since they're included in the '.gitignore' file.
def setUpModule():
    generate_self_signed_cert(os.path.dirname(os.path.abspath(__file__)))


class ValidateCredentialFlags(unittest.TestCase):
    def test_validate_credential_flags(self):
        tests = {
            "ValidUsernameAndPassword": {
                "username": "username",
                "password": "password",
            },
            "ValidCertAuthWithHTTPS": {
                "host": "https://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
            },
            "ValidCertAuthWithCouchbases": {
                "host": "couchbases://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
            },
            "OnlyUsername": {
                "username": "username",
                "errors": ["the --username/--password flags must be supplied together"],
            },
            "OnlyPassword": {
                "password": "password",
                "errors": ["the --username/--password flags must be supplied together"],
            },
            "OnlyClientCert": {
                "host": "https://localhost:8091",
                "client_ca": "/path/to/cert",
                "errors": ["when no cert/key password is provided, the --client-cert/--client-key flags must be supplied together"],
            },
            "OnlyClientKey": {
                "host": "https://localhost:8091",
                "client_pk": "/path/to/key",
                "errors": ["certificate authentication requires a certificate to be supplied with the --client-cert flag"],
            },
            "CertAuthWithUsername": {
                "host": "https://localhost:8091",
                "username": "username",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
                "errors": ["expected either --username and --password or --client-cert and --client-key but not both"],
            },
            "CertAuthWithPassword": {
                "host": "https://localhost:8091",
                "password": "password",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
                "errors": ["expected either --username and --password or --client-cert and --client-key but not both"],
            },
            "InsecureConnectionEmpty": {
                "host": "",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
                "errors": ["certificate authentication requires a secure connection, use https:// or couchbases://"],
            },
            "InsecureConnectionEmpty": {
                "host": "hostname",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
                "errors": ["certificate authentication requires a secure connection, use https:// or couchbases://"],
            },
            "InsecureConnectionHTTP": {
                "host": "http://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
                "errors": ["certificate authentication requires a secure connection, use https:// or couchbases://"],
            },
            "InsecureConnectionCouchbase": {
                "host": "couchbase://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
                "errors": ["certificate authentication requires a secure connection, use https:// or couchbases://"],
            },
            "DoNotWantBothPasswords": {
                "host": "https://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_ca_password": "password",
                "client_pk": "/path/to/key",
                "client_pk_password": "password",
                "errors": ["--client-cert-password and  --client-key-password can't be supplied together"],
            },
            "BothUnencrypted": {
                "host": "https://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
            },
            "EncryptedPKCS#8": {
                "host": "https://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_pk": "/path/to/key",
                "client_pk_password": "password",
            },
            "EncryptedPKCS#12": {
                "host": "https://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_ca_password": "password",
            },
            "CertPasswordWithoutCert": {
                "host": "https://localhost:8091",
                "client_ca_password": "password",
                "errors": ["certificate authentication requires a certificate to be supplied with the --client-cert flag"],
            },
            "KeyPasswordWithoutKey": {
                "host": "https://localhost:8091",
                "client_ca": "/path/to/cert",
                "client_pk_password": "password",
                "errors": ["--client-key-password provided without --client-key"],
            },
            "NoCredentials": {
                "host": "https://localhost:8091",
                "errors": ["cluster credentials required, expected --username/--password or --client-cert/--client-key"],
            },
            "NoCredentialsAndCredentialsNotRequired": {
                "host": "https://localhost:8091",
                "credentials_required": False,
            },
        }

        def value(test, key):
            return test[key] if key in test else None

        for name, test in tests.items():
            with self.subTest(name):
                self.assertEqual(value(test, "errors"), validate_credential_flags(
                    value(test, "host"),
                    value(test, "username"),
                    value(test, "password"),
                    value(test, "client_ca"),
                    value(test, "client_ca_password"),
                    value(test, "client_pk"),
                    value(test, "client_pk_password"),
                    True if "credentials_required" not in test else test["credentials_required"],
                ))


class ExpandCollectionTest(unittest.TestCase):
    def test_expand_collection(self):
        test_and_result = {
            '.': ['_default', '_default'],
            'beer.': ['beer', '_default'],
            '.beer': ['_default', 'beer'],
            'scope.collection': ['scope', 'collection'],
        }

        for test_val, expected in test_and_result.items():
            scope, collection, err = CollectionManage.expand_collection_shortcut(test_val)
            self.assertEqual(err, None, 'Unexpected error')
            self.assertEqual(expected[0], scope, 'incorrect scope')
            self.assertEqual(expected[1], collection, 'incorrect collection')

    def test_expand_collection_invalid_paths(self):
        test = [
            '..', 'beer.sampl.e', '', ' ',
        ]

        for test_val in test:
            _, _, err = CollectionManage.expand_collection_shortcut(test_val)
            self.assertNotEqual(err, None, 'Expected an error')


class HostPortSplitTest(unittest.TestCase):
    def test_parse_host_port(self):
        testcase = {
            'localhost:9000': ('localhost', 9000),
            '[::1]:9000': ('[::1]', 9000),
            '[::1]': ('[::1]', 0),
            '127.0.0.1:8792': ('127.0.0.1', 8792),
            'some-random-hostname.com:7000': ('some-random-hostname.com', 7000),
            '[2001:db8:85a3:8d3:1319:8a2e:370:7348]:notaport': ('[2001:db8:85a3:8d3:1319:8a2e:370:7348]', 0)
        }

        for input_host, expected in testcase.items():
            result_host, result_port = parse_host_port(input_host)
            self.assertEqual(expected[0], result_host)
            self.assertEqual(expected[1], result_port)


class CommandTest(unittest.TestCase):
    def setUp(self):
        self.cli = CouchbaseCLI()
        self.stdout = sys.stdout
        self.stderr = sys.stderr
        self.sink = StringIO()
        self.sink_err = StringIO()
        self.str_output = ''
        self.str_error = ''
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

    def capture_error(self):
        sys.stderr = self.sink_err

    def stop_capture_error(self):
        sys.stderr = self.stderr
        if self.str_error == '':
            self.str_error = str(self.sink_err.getvalue())

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
        self.capture_error()
        with self.assertRaises(SystemExit):
            self.cli.execute(self.cli.parse(cmd_args))
        self.stop_capture()
        self.stop_capture_error()

    def rest_parameter_match(self, expected_params, length_match=True):
        if length_match:
            self.assertEqual(len(expected_params), len(self.server.rest_params))
        for parameter in expected_params:
            self.assertIn(parameter, self.server.rest_params)

    def deprecated_output(self):
        self.assertIn('DEPRECATED:', self.str_output)


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
            '--cluster-ramsize', '512', '--services', 'data,query,fts,eventing,analytics,backup',
            '--cluster-index-ramsize', '512', '--cluster-fts-ramsize', '512',
            '--cluster-eventing-ramsize', '512', '--cluster-query-ramsize', '512', '--cluster-name', 'name',
            '--index-storage-setting', 'memopt', '--update-notifications', '0',
            '--ip-family', 'ipv6-only', '--node-to-node-encryption', 'on'
        ]

        self.no_error_run(self.command + full_options, self.server_args)
        self.assertIn('SUCCESS', self.str_output)
        expected_params = ['memoryQuota=512', 'eventingMemoryQuota=512', 'ftsMemoryQuota=512', 'queryMemoryQuota=512',
                           'clusterName=name', 'indexMemoryQuota=512', 'indexerStorageMode=memory_optimized',
                           'username=Administrator', 'password=asdasd', 'port=6789',
                           'sendStats=false']
        self.rest_parameter_match(expected_params, False)

    def test_init_cluster_notification_default(self):
        self.server_args['init'] = False
        full_options = cluster_connect_args[:2] + self.command_args + [
            '--cluster-ramsize', '512', '--services', 'data,query,fts,eventing,analytics',
            '--cluster-index-ramsize', '512', '--cluster-fts-ramsize', '512',
            '--cluster-eventing-ramsize', '512', '--cluster-name', 'name',
            '--index-storage-setting', 'memopt',
        ]

        self.no_error_run(self.command + full_options, self.server_args)
        self.assertIn('SUCCESS', self.str_output)
        expected_params = ['memoryQuota=512', 'eventingMemoryQuota=512', 'ftsMemoryQuota=512', 'clusterName=name',
                           'indexMemoryQuota=512', 'indexerStorageMode=memory_optimized',
                           'username=Administrator', 'password=asdasd', 'port=6789',
                           'sendStats=true']
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

    def test_error_update_notifications_CE(self):
        self.server_args['init'] = False
        self.server_args['enterprise'] = False
        self.server_args['version'] = "7.2.3"
        self.system_exit_run(self.command + cluster_connect_args[:2] + self.command_args +
                             ['--update-notifications', '0'], self.server_args)
        self.assertIn('--update-notifications can only be configured on Enterprise Edition', self.str_output)


class TestBucketCompact(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-compact'] + cluster_connect_args
        self.command_args = ['--bucket', 'name']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True, 'buckets': []}

        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        self.bucket_memcached = {'name': 'name', 'bucketType': 'memcached'}
        super(TestBucketCompact, self).setUp()

    def test_basic_bucket_compact(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn('POST:/pools/default/buckets/' + self.bucket_membase['name'] + '/controller/compactBucket',
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

    def test_error_compact_memcached_bucket(self):
        self.server_args['buckets'].append(self.bucket_memcached)
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn('Cannot compact memcached buckets', self.str_output)

    def test_error_compact_non_existent_bucket(self):
        self.system_exit_run(self.command + self.command_args, self.server_args)


class TestBucketCreate(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-create'] + cluster_connect_args
        self.command_args = ['--bucket', 'name']
        self.command_couch_args = ['--bucket-type', 'couchbase', '--bucket-ramsize', '100', '--bucket-replica', '0',
                                   '--bucket-eviction-policy', 'fullEviction', '--rank', '3']
        self.command_EE_args = ['--compression-mode', 'active', '--max-ttl', '20']
        self.command_auto_compaction_args = ['--database-fragmentation-threshold-percentage', '25',
                                             '--view-fragmentation-threshold-percentage', '20',
                                             '--parallel-db-view-compaction', '1', '--purge-interval', '2']

        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets': []}

        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        self.bucket_memcached = {'name': 'name', 'bucketType': 'memcached'}
        super(TestBucketCreate, self).setUp()

    def test_basic_bucket_create(self):
        self.no_error_run(self.command + self.command_args + self.command_couch_args + self.command_EE_args,
                          self.server_args)
        expected_params = [
            'bucketType=couchbase', 'name=name', 'evictionPolicy=fullEviction', 'replicaNumber=0', 'maxTTL=20',
            'compressionMode=active', 'ramQuotaMB=100', 'storageBackend=couchstore', 'rank=3'
        ]
        self.rest_parameter_match(expected_params)

    def test_bucket_create_with_auto_compaction_settings(self):
        self.no_error_run(self.command + self.command_args + self.command_couch_args +
                          self.command_auto_compaction_args, self.server_args)

        expected_params = [
            'bucketType=couchbase', 'databaseFragmentationThreshold%5Bpercentage%5D=25', 'name=name',
            'evictionPolicy=fullEviction', 'autoCompactionDefined=true', 'parallelDBAndViewCompaction=true',
            'replicaNumber=0', 'purgeInterval=2.0', 'viewFragmentationThreshold%5Bpercentage%5D=20',
            'ramQuotaMB=100', 'storageBackend=couchstore', 'rank=3'
        ]

        self.rest_parameter_match(expected_params)

    def test_bucket_create_ephemeral(self):
        args = [
            '--bucket-type', 'ephemeral', '--bucket-ramsize', '100', '--bucket-replica', '0',
            '--bucket-eviction-policy', 'noEviction', '--bucket-replica', '0'
        ]
        self.no_error_run(self.command + self.command_args + args, self.server_args)
        expected_params = [
            'bucketType=ephemeral', 'replicaNumber=0', 'evictionPolicy=noEviction', 'name=name', 'ramQuotaMB=100',
            'storageBackend=couchstore'
        ]
        self.rest_parameter_match(expected_params)

    def test_bucket_create_memcached(self):
        args = [
            '--bucket-type', 'memcached', '--bucket-ramsize', '100'
        ]
        self.no_error_run(self.command + self.command_args + args, self.server_args)
        expected_params = [
            'bucketType=memcached', 'ramQuotaMB=100', 'name=name', 'storageBackend=couchstore'
        ]
        self.rest_parameter_match(expected_params)
        self.deprecated_output()

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

    def test_bucket_create_memcached_with_durability_min_level(self):
        args = [
            '--bucket-type', 'memcached', '--bucket-ramsize', '100',
            '--durability-min-level', 'majorityAndPersistActive',
        ]
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn('--durability-min-level cannot be specified for a memcached bucket', self.str_output)

    def test_bucket_create_ephemeral_with_history_retention_bytes(self):
        args = ['--bucket-type', 'ephemeral', '--bucket-ramsize', '100', '--history-retention-bytes', '1024']
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn("--history-retention-bytes cannot be specified for a ephemeral bucket", self.str_output)

    def test_bucket_create_couchstore_backend_with_history_retention_bytes(self):
        args = [
            '--bucket-type', 'couchbase', '--storage-backend', 'couchstore', '--bucket-ramsize', '1024',
            '--history-retention-bytes', '1024',
        ]
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn("--history-retention-bytes cannot be specified for a bucket with couchstore backend",
                      self.str_output)

    def test_bucket_create_ephemeral_with_history_retention_seconds(self):
        args = ['--bucket-type', 'ephemeral', '--bucket-ramsize', '100', '--history-retention-seconds', '10']
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn("--history-retention-seconds cannot be specified for a ephemeral bucket", self.str_output)

    def test_bucket_create_couchstore_backend_with_history_retention_seconds(self):
        args = [
            '--bucket-type', 'couchbase', '--storage-backend', 'couchstore', '--bucket-ramsize', '1024',
            '--history-retention-seconds', '10',
        ]
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn("--history-retention-seconds cannot be specified for a bucket with couchstore backend",
                      self.str_output)

    def test_bucket_create_ephemeral_with_enable_history_retention_by_default(self):
        args = ['--bucket-type', 'ephemeral', '--bucket-ramsize', '100', '--enable-history-retention-by-default', '0']
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn("--enable-history-retention-by-default cannot be specified for a ephemeral bucket",
                      self.str_output)

    def test_bucket_create_couchstore_backend_with_enable_history_retention_by_default(self):
        args = [
            '--bucket-type', 'couchbase', '--storage-backend', 'couchstore', '--bucket-ramsize', '1024',
            '--enable-history-retention-by-default', '0',
        ]
        self.system_exit_run(self.command + self.command_args + args, self.server_args)
        self.assertIn("--enable-history-retention-by-default cannot be specified for a bucket with couchstore backend",
                      self.str_output)

    def test_bucket_create_history_retention_options(self):
        args = [
            '--bucket-type', 'couchbase', '--storage-backend', 'magma', '--bucket-ramsize', '1024',
            '--history-retention-bytes', '1024', '--history-retention-seconds', '10',
            '--enable-history-retention-by-default', '0',
        ]
        self.no_error_run(self.command + self.command_args + args, self.server_args)
        expected_params = [
            'bucketType=couchbase', 'storageBackend=magma', 'name=name', 'ramQuotaMB=1024',
            "historyRetentionBytes=1024", "historyRetentionSeconds=10", "historyRetentionCollectionDefault=false"
        ]
        self.rest_parameter_match(expected_params)


class TestBucketDelete(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'bucket-delete'] + cluster_connect_args
        self.command_args = ['--bucket', 'name']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets': []}

        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        self.bucket_memcached = {'name': 'name', 'bucketType': 'memcached'}
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
                                   '--bucket-eviction-policy', 'fullEviction', '--enable-flush', '1', '--rank', '3']
        self.command_EE_args = ['--compression-mode', 'active', '--max-ttl', '20']
        self.command_auto_compaction_args = ['--database-fragmentation-threshold-percentage', '25',
                                             '--view-fragmentation-threshold-percentage', '20',
                                             '--abort-outside', '1', '--parallel-db-view-compaction', '1',
                                             '--purge-interval', '2']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            'buckets': []}
        self.bucket_membase = {'name': 'name', 'bucketType': 'membase'}
        self.bucket_memcached = {'name': 'name', 'bucketType': 'memcached'}
        super(TestBucketEdit, self).setUp()

    def test_bucket_edit(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + self.command_couch_args + self.command_EE_args,
                          self.server_args)
        expected_params = [
            'evictionPolicy=fullEviction', 'flushEnabled=1', 'threadsNumber=8', 'replicaNumber=0', 'maxTTL=20',
            'compressionMode=active', 'ramQuotaMB=100', 'rank=3'
        ]
        self.rest_parameter_match(expected_params)

    def test_bucket_edit_auto_compaction(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + self.command_couch_args + self.command_EE_args +
                          self.command_auto_compaction_args, self.server_args)

        expected_params = [
            'evictionPolicy=fullEviction', 'flushEnabled=1', 'threadsNumber=8', 'replicaNumber=0', 'maxTTL=20',
            'compressionMode=active', 'ramQuotaMB=100', 'databaseFragmentationThreshold%5Bpercentage%5D=25',
            'viewFragmentationThreshold%5Bpercentage%5D=20', 'allowedTimePeriod%5BabortOutside%5D=true',
            'parallelDBAndViewCompaction=true', 'purgeInterval=2.0', 'autoCompactionDefined=true', 'rank=3'
        ]

        self.rest_parameter_match(expected_params)

    def test_bucket_edit_value_only(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + ['--bucket-eviction-policy', 'valueOnly'],
                          self.server_args)
        self.rest_parameter_match(['evictionPolicy=valueOnly'])

    def test_bucket_edit_full_eviction(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + ['--bucket-eviction-policy', 'fullEviction'],
                          self.server_args)
        self.rest_parameter_match(['evictionPolicy=fullEviction'])

    def test_bucket_edit_no_eviction(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + ['--bucket-eviction-policy', 'noEviction'],
                          self.server_args)
        self.rest_parameter_match(['evictionPolicy=noEviction'])

    def test_bucket_edit_nru_eviction(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.no_error_run(self.command + self.command_args + ['--bucket-eviction-policy', 'nruEviction'],
                          self.server_args)
        self.rest_parameter_match(['evictionPolicy=nruEviction'])

    def test_bucket_edit_invalid_policy(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.system_exit_run(self.command + self.command_args + ['--bucket-eviction-policy', 'nope'], self.server_args)
        self.assertIn("ERROR: argument --bucket-eviction-policy: invalid choice: 'nope'" +
                      " (choose from 'valueOnly', 'fullEviction')", self.str_output)

    def test_memcached_bucket_edit(self):
        self.server_args['buckets'].append(self.bucket_memcached)
        self.memcahced_args = ['--enable-flush', '1']
        self.no_error_run(self.command + self.command_args + self.memcahced_args,
                          self.server_args)
        expected_params = ['flushEnabled=1']
        self.rest_parameter_match(expected_params)
        self.deprecated_output()

    def test_error_bucket_does_not_exist(self):
        self.system_exit_run(self.command + self.command_args + self.command_couch_args + self.command_EE_args,
                             self.server_args)

    def test_bucket_edit_ephemeral_with_history_retention_bytes(self):
        self.server_args['buckets'].append({'name': 'name', 'bucketType': 'ephemeral'})
        self.system_exit_run(self.command + self.command_args + ['--history-retention-bytes', '1024'], self.server_args)
        self.assertIn("--history-retention-bytes cannot be specified for a ephemeral bucket", self.str_output)

    def test_bucket_edit_couchstore_backend_with_history_retention_bytes(self):
        self.server_args['buckets'].append({'name': 'name', 'bucketType': 'membase', 'storageBackend': 'couchstore'})
        self.system_exit_run(self.command + self.command_args + ['--history-retention-bytes', '1024'], self.server_args)
        self.assertIn("--history-retention-bytes cannot be specified for a bucket with couchstore backend",
                      self.str_output)

    def test_bucket_edit_ephemeral_with_history_retention_seconds(self):
        self.server_args['buckets'].append({'name': 'name', 'bucketType': 'ephemeral'})
        self.system_exit_run(self.command + self.command_args + ['--history-retention-seconds', '10'], self.server_args)
        self.assertIn("--history-retention-seconds cannot be specified for a ephemeral bucket", self.str_output)

    def test_bucket_edit_couchstore_backend_with_history_retention_seconds(self):
        self.server_args['buckets'].append({'name': 'name', 'bucketType': 'membase', 'storageBackend': 'couchstore'})
        self.system_exit_run(self.command + self.command_args + ['--history-retention-seconds', '10'], self.server_args)
        self.assertIn("--history-retention-seconds cannot be specified for a bucket with couchstore backend",
                      self.str_output)

    def test_bucket_edit_ephemeral_with_enable_history_retention_by_default(self):
        self.server_args['buckets'].append({'name': 'name', 'bucketType': 'ephemeral'})
        self.system_exit_run(self.command + self.command_args + ['--enable-history-retention-by-default', '0'],
                             self.server_args)
        self.assertIn("--enable-history-retention-by-default cannot be specified for a ephemeral bucket",
                      self.str_output)

    def test_bucket_edit_couchstore_backend_with_enable_history_retention_by_default(self):
        self.server_args['buckets'].append({'name': 'name', 'bucketType': 'membase', 'storageBackend': 'couchstore'})
        self.system_exit_run(self.command + self.command_args + ['--enable-history-retention-by-default', '0'],
                             self.server_args)
        self.assertIn("--enable-history-retention-by-default cannot be specified for a bucket with couchstore backend",
                      self.str_output)

    def test_bucket_edit_history_retention_options(self):
        self.server_args['buckets'].append({'name': 'name', 'bucketType': 'membase', 'storageBackend': 'magma'})
        args = [
            '--history-retention-bytes', '1024', '--history-retention-seconds', '10',
            '--enable-history-retention-by-default', '0',
        ]
        self.no_error_run(self.command + self.command_args + args, self.server_args)
        expected_params = [
            "historyRetentionBytes=1024", "historyRetentionSeconds=10", "historyRetentionCollectionDefault=false"
        ]
        self.rest_parameter_match(expected_params)


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
        self.bucket_memcached = {'name': 'name1', 'bucketType': 'memcached', 'replicaNumber': '0',
                                 'quota': {'ram': '100'}, 'basicStats': {'memUsed': '100'}}
        super(TestBucketList, self).setUp()

    def test_bucket_list(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.server_args['buckets'].append(self.bucket_memcached)
        self.no_error_run(self.command, self.server_args)
        expected_out = ['name\n bucketType: membase\n numReplicas: 0\n ramQuota: 100\n ramUsed: 100',
                        'name1\n bucketType: memcached\n numReplicas: 0\n ramQuota: 100\n ramUsed: 100']
        self.assertIn(expected_out[0], self.str_output)
        self.assertIn(expected_out[1], self.str_output)

    def test_bucket_list_json(self):
        self.server_args['buckets'].append(self.bucket_membase)
        self.server_args['buckets'].append(self.bucket_memcached)
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
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'healthy', 'ports':
             {'httpsMgmt': '16789'}, 'clusterMembership': 'active'},
            {'otpNode': 'ns1@some-host', 'hostname': 'some-host:6789', 'status': 'healthy', 'ports':
             {'httpsMgmt': '16789'}, 'clusterMembership': 'active'}
        ]}
        self.server_args['tasks'] = [{'type': 'rebalance', 'status': 'notRunning'}]
        super(TestFailover, self).setUp()

    def test_failover_simple(self):
        self.no_error_run(self.command + self.basic_args, self.server_args)
        self.assertIn('POST:/controller/startGracefulFailover', self.server.trace)

    def test_failover_hard(self):
        self.no_error_run(self.command + self.basic_args + ['--hard'], self.server_args)
        self.assertIn('POST:/controller/failOver', self.server.trace)
        self.assertNotIn('allowUnsafe=true', self.server.rest_params)

    def test_failover_hard_non_existent_node(self):
        self.basic_args[1] = 'random-node:6789'
        self.system_exit_run(self.command + self.basic_args + ['--hard'], self.server_args)
        self.assertIn('Server random-node:6789 can\'t be failed over because it\'s not part of the cluster',
                      self.str_output)

    def test_failover_force_unhealthy_node(self):
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'unhealthy', 'ports':
             {'httpsMgmt': '16789'}, 'clusterMembership': 'active'}]}
        self.system_exit_run(self.command + self.basic_args, self.server_args)
        self.assertIn('can\'t be gracefully failed over because it is not healthy', self.str_output)

    def test_failover_hard_force(self):
        self.no_error_run(self.command + self.basic_args + ['--hard', '--force'], self.server_args)
        self.assertIn('POST:/controller/failOver', self.server.trace)
        self.assertIn('allowUnsafe=true', self.server.rest_params)

    def test_failover_force(self):
        self.system_exit_run(self.command + self.basic_args + ['--force'], self.server_args)
        self.assertIn('--hard is required with --force flag', self.str_output)

    def test_failover_force_inactive_failed(self):
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'unhealthy', 'ports':
             {'httpsMgmt': '16789'}, 'clusterMembership': 'inactiveFailed'}]}
        self.no_error_run(self.command + self.basic_args + ['--hard', '--force'], self.server_args)
        self.assertIn('POST:/controller/failOver', self.server.trace)
        self.assertIn('allowUnsafe=true', self.server.rest_params)

    def test_failover_force_inactive_added(self):
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'unhealthy', 'ports':
             {'httpsMgmt': '16789'}, 'clusterMembership': 'inactiveAdded'}]}
        self.no_error_run(self.command + self.basic_args + ['--hard', '--force'], self.server_args)
        self.assertIn('POST:/controller/failOver', self.server.trace)
        self.assertIn('allowUnsafe=true', self.server.rest_params)


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
            {"nodes": [{"hostname": "n1:8091"}, {"hostname": "n2:8091"}], "name": "name1",
             "uri": "/pools/default/serverGroups/1"}
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
                          '--node-init-java-home', '/foo/bar/java', '--node-init-analytics-path', '/foo/bar/analytics',
                          '--node-init-eventing-path', '/foo/bar/eventing']

        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestNodeInit, self).setUp()

    def test_node_init(self):
        self.no_error_run(self.command + self.name_args + self.path_args, self.server_args)
        self.assertIn('POST:/nodeInit', self.server.trace)
        expected_params = [
            'dataPath=%2Ffoo%2Fbar%2Fdata', 'indexPath=%2Ffoo%2Fbar%2Findex', 'analyticsPath=%2Ffoo%2Fbar%2Fanalytics',
            'eventingPath=%2Ffoo%2Fbar%2Feventing', 'javaHome=%2Ffoo%2Fbar%2Fjava', 'hostname=foo'
        ]
        self.rest_parameter_match(expected_params)

    def test_node_init_ipv6_and_ipv4(self):
        self.system_exit_run(self.command + self.name_args + ['--ipv4', '--ipv6'], self.server_args)
        self.assertIn('Use either --ipv4 or --ipv6', self.str_output)

    def test_node_init_ipv6(self):
        self.no_error_run(self.command + self.name_args + ['--ipv6'], self.server_args)
        self.assertIn('POST:/nodeInit', self.server.trace)
        expected_params = ['hostname=foo', 'afamily=ipv6']
        self.rest_parameter_match(expected_params)

    def test_node_init_ipv4(self):
        self.no_error_run(self.command + self.name_args + ['--ipv4'], self.server_args)
        self.assertIn('POST:/nodeInit', self.server.trace)
        expected_params = ['hostname=foo', 'afamily=ipv4']
        self.rest_parameter_match(expected_params)


class TestNodeReset(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'node-reset', '--force'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestNodeReset, self).setUp()

    def test_reset_node(self):
        self.no_error_run(self.command, self.server_args)
        self.assertIn('POST:/controller/hardResetNode', self.server.trace)


class TestRebalance(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'rebalance'] + cluster_connect_args
        self.cmd_args = ['--server-remove', 'localhost:6789', '--no-progress-bar']

        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.server_args['pools_default'] = {'nodes': [
            {'otpNode': 'ns1@localhost', 'hostname': 'localhost:6789', 'status': 'healthy', 'ports':
             {'httpsMgmt': '16789'}, 'clusterMembership': 'active'},
            {'otpNode': 'ns1@some-host', 'hostname': 'some-host:6789', 'status': 'healthy', 'ports':
             {'httpsMgmt': '16789'}, 'clusterMembership': 'active'}
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
             'addNodeURI': '/pools/default/serverGroups/1/addNode', 'nodes': []}
        ], 'uri': '/pools/default/serverGroups/rev=1'}
        self.server_args['indexes-settings'] = {'logLevel': 'info',
                                                'indexerThreads': 0,
                                                'storageMode': 'plasma',
                                                'stableSnapshotInterval': 5000,
                                                'maxRollbackPoints': 2,
                                                'memorySnapshotInterval': 200}
        self.server_args["pools_default"] = {"nodes": [{"version": "7.6.0-0000-enterprise"}]}

        super(TestServerAdd, self).setUp()

    def test_server_add_basic(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        self.assertIn('POST:/controller/addNode', self.server.trace)
        expected_params = ['hostname=some-host%3A6789', 'user=Administrator', 'password=asdasd', 'services=kv']
        self.rest_parameter_match(expected_params)

    def test_server_add_manager_only(self):
        self.no_error_run(self.command + self.cmd_args + ['--services', 'manager-only'], self.server_args)
        self.assertIn('POST:/controller/addNode', self.server.trace)
        expected_params = ['hostname=some-host%3A6789', 'user=Administrator', 'password=asdasd', 'services=']
        self.rest_parameter_match(expected_params)

    def test_server_add_group_name(self):
        self.no_error_run(self.command + self.cmd_args + ['--group-name', 'name1'], self.server_args)
        self.assertIn('POST:/pools/default/serverGroups/1/addNode', self.server.trace)
        expected_params = ['hostname=some-host%3A6789', 'user=Administrator', 'password=asdasd', 'services=kv']
        self.rest_parameter_match(expected_params)

    def test_server_add_services(self):
        self.no_error_run(self.command + self.cmd_args + ['--services', 'data,analytics,eventing,query,fts,backup'],
                          self.server_args)
        self.assertIn('POST:/controller/addNode', self.server.trace)
        expected_params = ['hostname=some-host%3A6789', 'user=Administrator', 'password=asdasd',
                           ['services=kv%2Cfts%2Ccbas%2Cn1ql%2Ceventing%2Cbackup']]
        self.assertEqual(len(expected_params), len(self.server.rest_params))
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
                            '--alert-communication-issue', '--alert-node-time', '--alert-disk-analyzer',
                            '--alert-bucket-history-size', '--alert-indexer-low-resident-percentage',
                            '--alert-memcached-connections']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingAlert, self).setUp()

    def test_set_all_alerts(self):
        self.no_error_run(self.command + self.all_allerts + ['--enable-email-alert', '0'], self.server_args)
        expected_params = ['alerts=auto_failover_node%2Cauto_failover_maximum_reached%2Cauto_failover_other' +
                           '_nodes_down%2Cauto_failover_cluster_too_small%2Cauto_failover_disabled%2Cip%2Cdisk%2' +
                           'Coverhead%2Cep_oom_errors%2Cep_item_commit_failed%2Caudit_dropped_events%2Cindexer_ram_' +
                           'max_usage%2Cep_clock_cas_drift_threshold_exceeded%2Ccommunication_issue' +
                           '%2Ctime_out_of_sync%2Cdisk_usage_analyzer_stuck%2Chistory_size_warning%2Cindexer_low_' +
                           'resident_percentage%2Cmemcached_connections', 'enabled=false', 'emailEncrypt=false']

        self.assertIn('POST:/settings/alerts', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_set_all_alerts_and_email(self):
        self.no_error_run(self.command + self.all_allerts + self.email_args, self.server_args)
        expected_params = ['alerts=auto_failover_node%2Cauto_failover_maximum_reached%2Cauto_failover_other' +
                           '_nodes_down%2Cauto_failover_cluster_too_small%2Cauto_failover_disabled%2Cip%2Cdisk%2' +
                           'Coverhead%2Cep_oom_errors%2Cep_item_commit_failed%2Caudit_dropped_events%2Cindexer_ram_' +
                           'max_usage%2Cep_clock_cas_drift_threshold_exceeded%2Ccommunication_issue' +
                           '%2Ctime_out_of_sync%2Cdisk_usage_analyzer_stuck%2Chistory_size_warning%2Cindexer_low_' +
                           'resident_percentage%2Cmemcached_connections', 'enabled=true', 'emailEncrypt=true',
                           'sender=email2', 'recipients=email1', 'emailUser=emailuser', 'emailPass=emailpwd',
                           'emailHost=emailhost', 'emailPort=3000']

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
        self.cmd_args = ['--set', '--audit-enabled', '1', '--audit-log-path', 'path', '--audit-log-rotate-interval',
                         '1', '--audit-log-rotate-size', '2', '--prune-age', '60']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingAudit, self).setUp()

    def test_setting_audit(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        expected_params = ['auditdEnabled=true', 'logPath=path', 'rotateInterval=1', 'rotateSize=2', 'pruneAge=60']
        self.assertIn('POST:/settings/audit', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_setting_audit_set_no_options(self):
        self.system_exit_run(self.command + ['--set'], self.server_args)
        self.assertIn('At least one of [--audit-enabled, --audit-log-path, --audit-log-rotate-interval,'
                      ' --audit-log-rotate-size, --disabled-users, --disable-events] is required with --set',
                      self.str_output)

    def test_setting_audit_set_disabled_users(self):
        self.no_error_run(self.command + ['--set', '--disabled-users=carlos/local'], self.server_args)
        self.assertIn('POST:/settings/audit', self.server.trace)
        expected_params = ['disabledUsers=carlos%2Flocal']
        self.rest_parameter_match(expected_params)

    def test_setting_audit_set_disabled_users_with_replaced_postfix(self):
        self.no_error_run(self.command + ['--set', '--disabled-users=carlos/local,james/couchbase'], self.server_args)
        self.assertIn('POST:/settings/audit', self.server.trace)
        expected_params = ['disabledUsers=carlos%2Flocal%2Cjames%2Flocal']
        self.rest_parameter_match(expected_params)

    def test_setting_audit_set_disable_events(self):
        self.no_error_run(self.command + ['--set', '--disable-events=4000'], self.server_args)
        self.assertIn('POST:/settings/audit', self.server.trace)
        expected_params = ['disabled=4000']
        self.rest_parameter_match(expected_params)

    def test_setting_audit_get_no_log_path(self):
        self.server_args['audit_settings'] = {
            'auditdEnabled': False,
            'uid': 'uuid',
            'rotateInterval': 0,
            'rotateSize': 0,
            'disabledUsers': [],
        }

        self.server_args['/settings/audit/descriptors'] = {}
        self.no_error_run(self.command + ['--get-settings'], self.server_args)
        self.assertIn('Log path: N/A', self.str_output)

    def test_setting_audit_clear_events_and_users(self):
        self.no_error_run(self.command + ['--set', '--disabled-users', '', '--disable-events', ''], self.server_args)
        self.assertIn('POST:/settings/audit', self.server.trace)
        expected_params = ['disabled=', 'disabledUsers=']
        self.rest_parameter_match(expected_params)


class TestSettingAutofailover(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-autofailover'] + cluster_connect_args
        self.cmd_args = ['--enable-auto-failover', '1', '--auto-failover-timeout', '10']
        self.EE_args = ['--max-failovers', '3', '--enable-failover-on-data-disk-issues', '1',
                        '--failover-data-disk-period', '20',
                        '--allow-failover-for-ephemeral-without-replica', '1']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingAutofailover, self).setUp()

    def test_setting_auto_failover_basic(self):
        self.no_error_run(self.command + self.cmd_args, self.server_args)
        expected_params = ['enabled=true', 'timeout=10']
        self.assertIn('POST:/settings/autoFailover', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_setting_auto_failover_EE(self):
        self.no_error_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        expected_params = ['enabled=true', 'timeout=10', 'failoverOnDataDiskIssues%5Benabled%5D=true',
                           'failoverOnDataDiskIssues%5BtimePeriod%5D=20', 'maxCount=3',
                           'allowFailoverEphemeralNoReplicas=true']
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

    def __test_enable_disk_issues_without_disk_period(self, cli_enabled_param, cli_period_param):
        self.EE_args = [cli_enabled_param, '1']
        self.system_exit_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        self.assertIn(
            f'{cli_period_param} must be set when {cli_enabled_param} is enabled',
            self.str_output)

    def test_enable_disk_issues_without_disk_period(self):
        self.__test_enable_disk_issues_without_disk_period(
            '--enable-failover-on-data-disk-issues', '--failover-data-disk-period')

    def test_enable_disk_non_responsive_without_period(self):
        self.__test_enable_disk_issues_without_disk_period(
            '--enable-failover-on-data-disk-non-responsive',
            '--failover-data-disk-non-responsive-period')

    def __test_enable_disk_issues_with_disk_period(self, cli_enabled_param, cli_period_param, rest_param):
        self.EE_args = [cli_enabled_param, '1', cli_period_param, '20']
        self.no_error_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        expected_params = ['enabled=true', 'timeout=10', f'{rest_param}%5Benabled%5D=true',
                           f'{rest_param}%5BtimePeriod%5D=20']
        self.assertIn('POST:/settings/autoFailover', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_enable_disk_issues_with_disk_period(self):
        self.__test_enable_disk_issues_with_disk_period(
            '--enable-failover-on-data-disk-issues', '--failover-data-disk-period',
            'failoverOnDataDiskIssues')

    def test_enable_disk_non_responsive_with_period(self):
        self.__test_enable_disk_issues_with_disk_period(
            '--enable-failover-on-data-disk-non-responsive',
            '--failover-data-disk-non-responsive-period',
            'failoverOnDataDiskNonResponsiveness')

    def __test_disable_disk_issues_without_disk_period(self, cli_enabled_param, rest_param):
        self.EE_args = [cli_enabled_param, '0']
        self.no_error_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        expected_params = ['enabled=true', 'timeout=10', f'{rest_param}%5Benabled%5D=false']
        self.assertIn('POST:/settings/autoFailover', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_disable_disk_issues_without_disk_period(self):
        self.__test_disable_disk_issues_without_disk_period(
            '--enable-failover-on-data-disk-issues', 'failoverOnDataDiskIssues')

    def test_disable_disk_non_responsive_without_period(self):
        self.__test_disable_disk_issues_without_disk_period(
            '--enable-failover-on-data-disk-non-responsive', 'failoverOnDataDiskNonResponsiveness')

    def __test_disable_disk_issues_with_disk_period(self, cli_enabled_param, cli_period_param):
        self.EE_args = [cli_enabled_param, '0', cli_period_param, '20']
        self.system_exit_run(self.command + self.cmd_args + self.EE_args, self.server_args)
        self.assertIn(
            f'{cli_enabled_param} must be set to 1 when {cli_period_param} is configured',
            self.str_output)

    def test_disable_disk_issues_with_disk_period(self):
        self.__test_disable_disk_issues_with_disk_period(
            '--enable-failover-on-data-disk-issues', '--failover-data-disk-period')

    def test_disable_disk_non_responsive_with_period(self):
        self.__test_disable_disk_issues_with_disk_period(
            '--enable-failover-on-data-disk-non-responsive',
            '--failover-data-disk-non-responsive-period')


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
                                 '--cluster-query-ramsize', '600', '--cluster-name', 'name']
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
                           'cbasMemoryQuota=500', 'indexMemoryQuota=200', 'queryMemoryQuota=600']
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
        self.no_error_run(self.command + self.basic_args + self.circular_args, self.server_args)
        expected_params = [
            'allowedTimePeriod%5BtoMinute%5D=0',
            'databaseFragmentationThreshold%5Bpercentage%5D=10',
            'allowedTimePeriod%5BtoHour%5D=15',
            'indexCircularCompaction%5Binterval%5D%5BabortOutside%5D=true',
            'indexCompactionMode=circular',
            'allowedTimePeriod%5BfromMinute%5D=0',
            'viewFragmentationThreshold%5Bsize%5D=31457280',
            'indexCircularCompaction%5Binterval%5D%5BfromMinute%5D=0',
            'parallelDBAndViewCompaction=false',
            'databaseFragmentationThreshold%5Bsize%5D=20971520',
            'allowedTimePeriod%5BabortOutside%5D=true',
            'allowedTimePeriod%5BfromHour%5D=12',
            'indexCircularCompaction%5Binterval%5D%5BtoMinute%5D=0',
            'purgeInterval=3.0',
            'viewFragmentationThreshold%5Bpercentage%5D=25',
            'indexCircularCompaction%5Binterval%5D%5BtoHour%5D=12',
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

    def test_7_0_settings(self):
        self.no_error_run(self.command + ['--replicas', '2', '--optimize-placement', '0'], self.server_args)
        self.assertIn('POST:/settings/indexes', self.server.trace)
        self.rest_parameter_match(['numReplica=2', 'redistributeIndexes=false'])


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
        self.server_args['pools_default'] = {'nodes': [{'version': '7.2.0'}]}
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

    def test_enable_notification_for_CE(self):
        self.server_args['enterprise'] = False
        self.server_args['pools_default'] = {'nodes': [{'version': '7.2.3'}]}
        self.system_exit_run(self.command + ['--enable-notifications', '1'], self.server_args)
        self.assertIn("Modifying notifications settings is an Enterprise Edition only feature", self.str_output)

    def test_enable_notification_for_EE(self):
        self.server_args['enterprise'] = True
        self.server_args['pools_default'] = {'nodes': [{'version': '7.6.0'}]}
        self.no_error_run(self.command + ['--enable-notifications', '1'], self.server_args)


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

    def test_set_settings_cluster_encryption_strict(self):
        self.no_error_run(self.command + ['--set', '--disable-http-ui', '1', '--cluster-encryption-level', 'strict',
                                          '--tls-honor-cipher-order', '1', '--tls-min-version', 'tlsv1.2'],
                          self.server_args)

        expected_params = ['disableUIOverHttp=true', 'clusterEncryptionLevel=strict', 'tlsMinVersion=tlsv1.2',
                           'honorCipherOrder=true']
        self.assertIn('POST:/settings/security', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_set_settings_hsts_max_age(self):
        self.no_error_run(self.command + ['--set', '--hsts-max-age', '42'], self.server_args)
        expected_params = ['responseHeaders=' + urllib.parse.quote('{"Strict-Transport-Security":"max-age=42"}')]
        self.assertIn('POST:/settings/security', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_set_settings_hsts_preload_enabled(self):
        self.no_error_run(self.command + ['--set', '--hsts-preload-enabled', '1'], self.server_args)
        expected_params = ['responseHeaders=' + urllib.parse.quote('{"Strict-Transport-Security":"preload"}')]
        self.assertIn('POST:/settings/security', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_set_settings_hsts_include_sub_domains_enabled(self):
        self.no_error_run(self.command + ['--set', '--hsts-include-sub-domains-enabled', '1'], self.server_args)
        expected_params = ['responseHeaders=' + urllib.parse.quote('{"Strict-Transport-Security":"includeSubDomains"}')]
        self.assertIn('POST:/settings/security', self.server.trace)
        self.rest_parameter_match(expected_params)

    def test_set_settings_hsts_max_age_and_preload_enabled(self):
        self.no_error_run(self.command + ['--set', '--hsts-max-age', '42', '--hsts-preload-enabled', '1'],
                          self.server_args)
        expected_params = ['responseHeaders=' +
                           urllib.parse.quote('{"Strict-Transport-Security":"max-age=42;preload"}')]
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
# TODO: TestRestCipherSuites

class TestResetAdminPassword(unittest.TestCase):
    @patch('cbmgr._exit_on_file_read_failure')
    @patch('cbmgr.ClusterManager')
    @patch('cbmgr._exit_if_errors')
    @patch('cbmgr._success')
    def test_execute(self, mock_success, mock_exit_errors, mock_ClusterManager, mock_exit_on_file_read_failure):
        # Arrange
        mock_exit_on_file_read_failure.return_value = 'mock_token\n'
        mock_ClusterManager.return_value = MagicMock()
        mock_ClusterManager.return_value.is_cluster_initialized.return_value = True, []
        mock_ClusterManager.return_value.pools.return_value = {'implementationVersion': '1.0.0'}, []
        mock_ClusterManager.return_value.regenerate_admin_password.return_value = {"password": "random_password"}, []
        mock_ClusterManager.return_value.set_admin_password.return_value = None, []

        # Test case 1: regenerate
        opts = MagicMock()
        opts.config_path = '/path/to/config'
        opts.ip = 'localhost'
        opts.port = '8091'
        opts.new_password = None
        opts.regenerate = True
        ResetAdminPassword().execute(opts)
        mock_ClusterManager.return_value.regenerate_admin_password.assert_called_once()

        # Test case 2: new password
        opts.new_password = 'new_password'
        opts.regenerate = False
        ResetAdminPassword().execute(opts)
        mock_ClusterManager.return_value.set_admin_password.assert_called_once_with('new_password')

        # Test case 3: regenerate with -I and -P
        opts.ip = '192.168.1.1'
        opts.port = '9000'
        opts.new_password = None
        opts.regenerate = True
        ResetAdminPassword().execute(opts)
        mock_ClusterManager.assert_called_with('http://192.168.1.1:9000', '@localtoken', 'mock_token')
        mock_ClusterManager.return_value.regenerate_admin_password.assert_called()

        # Test case 4: both new_password and regenerate specified
        opts.new_password = 'new_password'
        opts.regenerate = True
        ResetAdminPassword().execute(opts)
        mock_exit_errors.assert_called_with(["Cannot specify both --new-password and --regenerate at the same time"])

        # Test case 5: neither new_password nor regenerate specified
        opts.new_password = None
        opts.regenerate = False
        ResetAdminPassword().execute(opts)
        mock_exit_errors.assert_called_with(["No parameters specified"])

        # Check the number of calls to _exit_if_errors
        self.assertEqual(mock_exit_errors.call_count, 5)


class TestSslManage(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'ssl-manage'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super().setUp()

    def test_cluster_cert_info(self):
        certificates = [{"node": "127.0.0.1:9000",
                         "warnings": [{"message": "Out-of-the-box certificates are self-signed. To further secure your "
                                                  "system, you must create new X.509 certificates signed by a trusted "
                                                  "CA."}],
                         "subject": "CN=Couchbase Server Node (127.0.0.1)",
                         "expires": "2049-12-31T23:59:59.000Z",
                         "type": "generated",
                         "pem": "-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n",
                         "privateKeyPassphrase": {}},
                        {"node": "127.0.0.1:9001",
                         "warnings": [{"message": "Out-of-the-box certificates are self-signed. To further secure your "
                                                  "system, you must create new X.509 certificates signed by a trusted "
                                                  "CA."}],
                         "subject": "CN=Couchbase Server Node (127.0.0.1)",
                         "expires": "2049-12-31T23:59:59.000Z",
                         "type": "generated",
                         "pem": "-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n",
                         "privateKeyPassphrase": {}}]
        self.server_args['/pools/default/certificates'] = certificates
        self.no_error_run(self.command + ['--cluster-cert-info'], self.server_args)
        self.assertIn('GET:/pools/default/certificates', self.server.trace)
        self.assertIn('127.0.0.1:9000', self.str_output)
        self.assertIn('127.0.0.1:9001', self.str_output)

    def test_cluster_cert_info_not_init(self):
        certificates = [{"node": "127.0.0.1:9000",
                         "warnings": [{"message": "Out-of-the-box certificates are self-signed. To further secure your "
                                                  "system, you must create new X.509 certificates signed by a trusted "
                                                  "CA."}],
                         "subject": "CN=Couchbase Server Node (127.0.0.1)",
                         "expires": "2049-12-31T23:59:59.000Z",
                         "type": "generated",
                         "pem": "-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n",
                         "privateKeyPassphrase": {}},
                        {"node": "127.0.0.1:9001",
                         "warnings": [{"message": "Out-of-the-box certificates are self-signed. To further secure your "
                                                  "system, you must create new X.509 certificates signed by a trusted "
                                                  "CA."}],
                         "subject": "CN=Couchbase Server Node (127.0.0.1)",
                         "expires": "2049-12-31T23:59:59.000Z",
                         "type": "generated",
                         "pem": "-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n",
                         "privateKeyPassphrase": {}}]
        self.server_args['/pools/default/certificates'] = certificates
        self.server_args['init'] = False
        self.no_error_run(self.command + ['--cluster-cert-info'], self.server_args)
        self.assertIn('GET:/pools/default/certificates', self.server.trace)
        self.assertIn('127.0.0.1:9000', self.str_output)
        self.assertIn('127.0.0.1:9001', self.str_output)

    def test_cluster_ca_info(self):
        ca = [{"nodes": ["172.18.1.75:9000", "127.0.0.1:9001"],
               "warnings": [
                   {"message": "Out-of-the-box certificates are self-signed. To further secure your system, you "
                               "must create new X.509 certificates signed by a trusted CA."}],
               "subject": "CN=Couchbase Server e5b5a918",
               "id": 1,
               "loadTimestamp": "2021-10-11T13:49:52.000Z",
               "notBefore": "2013-01-01T00:00:00.000Z",
               "notAfter": "2049-12-31T23:59:59.000Z",
               "type": "generated",
               "pem": "-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n\n", }]
        self.server_args['/pools/default/trustedCAs'] = ca
        self.no_error_run(self.command + ['--cluster-ca-info'], self.server_args)
        self.assertIn('GET:/pools/default/trustedCAs', self.server.trace)
        self.assertIn('CN=Couchbase Server e5b5a918', self.str_output)

    def test_cluster_ca_info_not_init(self):
        ca = [{"nodes": ["172.18.1.75:9000", "127.0.0.1:9001"],
               "warnings": [
                   {"message": "Out-of-the-box certificates are self-signed. To further secure your system, you "
                               "must create new X.509 certificates signed by a trusted CA."}],
               "subject": "CN=Couchbase Server e5b5a918",
               "id": 1,
               "loadTimestamp": "2021-10-11T13:49:52.000Z",
               "notBefore": "2013-01-01T00:00:00.000Z",
               "notAfter": "2049-12-31T23:59:59.000Z",
               "type": "generated",
               "pem": "-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n\n", }]
        self.server_args['/pools/default/trustedCAs'] = ca
        self.server_args['init'] = False
        self.no_error_run(self.command + ['--cluster-ca-info'], self.server_args)
        self.assertIn('GET:/pools/default/trustedCAs', self.server.trace)
        self.assertIn('CN=Couchbase Server e5b5a918', self.str_output)

    def test_cluster_ca_info_missing_info(self):
        ca = [{"nodes": ["172.18.1.75:9000", "127.0.0.1:9001"],
               "warnings": [
                   {"message": "Out-of-the-box certificates are self-signed. To further secure your system, you "
                               "must create new X.509 certificates signed by a trusted CA."}],
               "subject": "CN=Couchbase Server e5b5a918",
               "id": 1,
               "notBefore": "2013-01-01T00:00:00.000Z",
               "notAfter": "2049-12-31T23:59:59.000Z",
               "type": "generated",
               "pem": "-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n\n", }]
        self.server_args['/pools/default/trustedCAs'] = ca
        self.no_error_run(self.command + ['--cluster-ca-info'], self.server_args)
        self.assertIn('GET:/pools/default/trustedCAs', self.server.trace)
        self.assertIn('unknown', self.str_output)

    def test_cluster_ca_load(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789', 'ports': {'httpsMgmt': '6789'}},
                                                      {'hostname': '127.0.0.1:6789', 'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--cluster-ca-load'], self.server_args)
        self.assertIn('POST:/node/controller/loadTrustedCAs', self.server.trace)
        self.assertIn('localhost', self.str_output)
        self.assertIn('127.0.0.1', self.str_output)

    def test_cluster_ca_load_not_init(self):
        self.server_args['/pools/nodes'] = 'unknown pool'
        self.server_args['override-status'] = 400
        self.server_args['init'] = False
        self.no_error_run(self.command + ['--cluster-ca-load'], self.server_args)
        self.assertIn('POST:/node/controller/loadTrustedCAs', self.server.trace)
        self.assertIn('127.0.0.1', self.str_output)
        self.assertIn('Successfully load CA from inbox/CA', self.str_output)

    def test_cluster_ca_delete(self):
        self.no_error_run(self.command + ['--cluster-ca-delete', '0'], self.server_args)
        self.assertIn('DELETE:/pools/default/trustedCAs/0', self.server.trace)
        self.assertIn('Certificate Authority with ID 0 has been deleted', self.str_output)

    def test_cluster_ca_delete_not_init(self):
        self.server_args['init'] = False
        self.no_error_run(self.command + ['--cluster-ca-delete', '0'], self.server_args)
        self.assertIn('DELETE:/pools/default/trustedCAs/0', self.server.trace)
        self.assertIn('Certificate Authority with ID 0 has been deleted', self.str_output)

    def test_cluster_ca_delete_with_204(self):
        self.server_args['/pools/default/trustedCAs/0'] = 'unknown pool'
        self.server_args['override-status'] = 204
        self.no_error_run(self.command + ['--cluster-ca-delete', '0'], self.server_args)
        self.assertIn('DELETE:/pools/default/trustedCAs/0', self.server.trace)
        self.assertIn('Certificate Authority with ID 0 has been deleted', self.str_output)

    def test_upload_cluster_ca(self):
        cert_file = tempfile.NamedTemporaryFile(delete=False)
        cert_file.write(b'this-is-the-cert-file')
        cert_file.close()
        self.no_error_run(self.command + ['--upload-cluster-ca', cert_file.name], self.server_args)
        os.remove(cert_file.name)
        self.assertIn('POST:/controller/uploadClusterCA', self.server.trace)
        self.assertIn('DEPRECATED:', self.str_output)
        self.assertIn('Uploaded cluster certificate to http://127.0.0.1:6789', self.str_output)

    def test_upload_cluster_ca_not_init(self):
        self.server_args['init'] = False
        cert_file = tempfile.NamedTemporaryFile(delete=False)
        cert_file.write(b'this-is-the-cert-file')
        cert_file.close()
        self.no_error_run(self.command + ['--upload-cluster-ca', cert_file.name], self.server_args)
        os.remove(cert_file.name)
        self.assertIn('POST:/controller/uploadClusterCA', self.server.trace)
        self.assertIn('DEPRECATED:', self.str_output)
        self.assertIn('Uploaded cluster certificate to http://127.0.0.1:6789', self.str_output)

    def test_node_cert_info(self):
        certificate = {'warnings': [{'message': 'Out-of-the-box certificates are self-signed. To further secure your '
                                                'system, you must create new X.509 certificates signed by a trusted '
                                                'CA.'}],
                       'subject': f'CN=Couchbase Server Node ({host})',
                       'expires': '2049-12-31T23:59:59.000Z',
                       'type': 'generated',
                       'pem': '-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n',
                       'privateKeyPassphrase': {}}
        self.server_args[f'/pools/default/certificate/node/{host}:{port}'] = certificate
        self.no_error_run(self.command + ['--node-cert-info'], self.server_args)
        self.assertIn(f'GET:/pools/default/certificate/node/{host}:{port}', self.server.trace)
        self.assertIn('127.0.0.1', self.str_output)

    def test_node_cert_info_not_init(self):
        self.server_args['init'] = False
        certificate = {'warnings': [{'message': 'Out-of-the-box certificates are self-signed. To further secure your '
                                                'system, you must create new X.509 certificates signed by a trusted '
                                                'CA.'}],
                       'subject': f'CN=Couchbase Server Node ({host})',
                       'expires': '2049-12-31T23:59:59.000Z',
                       'type': 'generated',
                       'pem': '-----BEGIN CERTIFICATE-----\nCert String\n-----END CERTIFICATE-----\n',
                       'privateKeyPassphrase': {}}
        self.server_args[f'/pools/default/certificate/node/{host}:{port}'] = certificate
        self.no_error_run(self.command + ['--node-cert-info'], self.server_args)
        self.assertIn(f'GET:/pools/default/certificate/node/{host}:{port}', self.server.trace)
        self.assertIn('127.0.0.1', self.str_output)

    def test_regenerate_cert(self):
        self.server_args['/controller/regenerateCertificate'] = 'This is a cert'
        self.no_error_run(self.command + ['--regenerate-cert', 'node1.pem'], self.server_args)
        os.remove('node1.pem')
        self.assertIn('POST:/controller/regenerateCertificate', self.server.trace)
        self.assertIn('Certificate regenerate and copied to `node1.pem`', self.str_output)

    def test_regenerate_cert_not_init(self):
        self.server_args['init'] = False
        self.server_args['/controller/regenerateCertificate'] = 'This is a cert'
        self.no_error_run(self.command + ['--regenerate-cert', 'node1.pem'], self.server_args)
        os.remove('node1.pem')
        self.assertIn('POST:/controller/regenerateCertificate', self.server.trace)
        self.assertIn('Certificate regenerate and copied to `node1.pem`', self.str_output)

    def test_set_node_certificate(self):
        self.no_error_run(self.command + ['--set-node-certificate'], self.server_args)
        self.assertIn('POST:/node/controller/reloadCertificate', self.server.trace)
        self.assertIn('Node certificate set', self.str_output)

    def test_set_node_certificate_not_init(self):
        self.server_args['init'] = False
        self.no_error_run(self.command + ['--set-node-certificate'], self.server_args)
        self.assertIn('POST:/node/controller/reloadCertificate', self.server.trace)
        self.assertIn('Node certificate set', self.str_output)

    def test_set_node_certificate_with_pkey_settings(self):
        pkey_settings_file = tempfile.NamedTemporaryFile(delete=False)
        pkey_settings_file.write(b'{"type":"plain","password":"asdf"}')
        pkey_settings_file.close()
        self.no_error_run(self.command + ['--set-node-certificate', '--pkey-passphrase-settings',
                                          pkey_settings_file.name], self.server_args)
        os.remove(pkey_settings_file.name)
        self.assertIn('POST:/node/controller/reloadCertificate', self.server.trace)
        self.assertIn('Node certificate set', self.str_output)

    def test_set_client_auth(self):
        client_json = tempfile.NamedTemporaryFile(delete=False)
        client_json.write(b'{"name":"json"}')
        client_json.close()
        self.no_error_run(self.command + ['--set-client-auth', client_json.name], self.server_args)
        self.assertIn('POST:/settings/clientCertAuth', self.server.trace)
        self.assertIn('SSL client auth updated', self.str_output)

    def test_set_client_auth_not_init(self):
        self.server_args['init'] = False
        client_json = tempfile.NamedTemporaryFile(delete=False)
        client_json.write(b'{"name":"json"}')
        client_json.close()
        self.no_error_run(self.command + ['--set-client-auth', client_json.name], self.server_args)
        self.assertIn('POST:/settings/clientCertAuth', self.server.trace)
        self.assertIn('SSL client auth updated', self.str_output)

    def test_client_auth(self):
        self.server_args['/settings/clientCertAuth'] = {'prefixes': [], 'state': 'disable'}
        self.no_error_run(self.command + ['--client-auth'], self.server_args)
        self.assertIn('GET:/settings/clientCertAuth', self.server.trace)
        self.assertIn('prefixes', self.str_output)

    def test_client_auth_not_init(self):
        self.server_args['init'] = False
        self.server_args['/settings/clientCertAuth'] = {'prefixes': [], 'state': 'disable'}
        self.no_error_run(self.command + ['--client-auth'], self.server_args)
        self.assertIn('GET:/settings/clientCertAuth', self.server.trace)
        self.assertIn('prefixes', self.str_output)


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

    def test_get_user_with_no_user(self):
        self.system_exit_run(self.command + ['--get'], self.server_args)
        self.assertIn('rbac-username is required', self.str_output)

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
                                          '--auth-domain', 'local', '--roles', 'admin', '--rbac-name', 'name',
                                          '--user-groups', ''],
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
                                          'external', '--roles', 'admin', '--rbac-name', 'name', '--user-groups', ''],
                          self.server_args)
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


class TestMasterPassword(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'master-password']
        self.cmd_args = ['--config-path', '.', '--send-password', 'asdasd']
        self.server_args = {}
        super(TestMasterPassword, self).setUp()

    def test_missing_portfile(self):
        self.system_exit_run(self.command + self.cmd_args, self.server_args)
        self.assertIn('The node is down', self.str_output)

    def test_cannot_read_port(self):
        portfile = open('couchbase-server.babysitter.smport', 'x')
        try:
            portfile.write('inet 12345')
            portfile.close()
            os.chmod(portfile.name, 0000)
            self.system_exit_run(self.command + self.cmd_args, self.server_args)
            self.assertIn('ERROR: Insufficient privileges', self.str_output)
        finally:
            os.remove(portfile.name)

    def test_succ_cmd_ipv4(self):
        self.succ_cmd(socket.AF_INET)

    def test_succ_cmd_ipv6(self):
        self.succ_cmd(socket.AF_INET6)

    def succ_cmd(self, addrFamily):
        with socket.socket(addrFamily, socket.SOCK_DGRAM) as sock:
            sock.bind(('', 0))
            port = sock.getsockname()[1]
            portfile = open('couchbase-server.babysitter.smport', 'x')
            try:
                addrFamilyStr = 'inet' if addrFamily == socket.AF_INET else "inet6"
                portfile.write(addrFamilyStr + ' ' + str(port))
                portfile.close()

                thread = threading.Thread(target=read_password, args=(sock,))
                thread.start()

                self.no_error_run(self.command + self.cmd_args, self.server_args)
                thread.join()
                self.assertIn('SUCCESS: Password accepted', self.str_output)
            finally:
                os.remove(portfile.name)


def read_password(sock):
    (result, remoteaddr) = sock.recvfrom(128)
    assert(result == b'asdasd')
    sock.sendto(b'ok', remoteaddr)


class TestXdcrReplicate(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'xdcr-replicate'] + cluster_connect_args
        self.cmd_args = ['--xdcr-cluster-name', 'name', '--xdcr-hostname', 'hostname', '--xdcr-username', 'username',
                         '--xdcr-password', 'pwd']
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestXdcrReplicate, self).setUp()

    def test_get_replication_settings(self):
        self.server_args['/settings/replications/1'] = {'test': 0}
        self.no_error_run(self.command + ['--get', '--xdcr-replicator', '1'], self.server_args)
        self.assertIn('GET:/settings/replications/1', self.server.trace)

    def test_create_EE(self):
        self.no_error_run(self.command + ['--create', '--xdcr-cluster-name', 'cluster1', '--xdcr-to-bucket', 'bucket2',
                                          '--xdcr-from-bucket', 'bucket1', '--filter-expression', 'key:[a-zA-z]+',
                                          '--enable-compression', '1'],
                          self.server_args)
        self.assertIn('POST:/controller/createReplication', self.server.trace)
        expected_params = ['toBucket=bucket2', 'fromBucket=bucket1', 'toCluster=cluster1', 'compressionType=Auto',
                           'filterExpression=key%3A%5Ba-zA-z%5D%2B', 'replicationType=continuous']

    def test_create_log_level(self):
        self.no_error_run(self.command + ['--create', '--xdcr-cluster-name', 'cluster1', '--xdcr-to-bucket', 'bucket2',
                                          '--xdcr-from-bucket', 'bucket1', '--filter-expression', 'key:[a-zA-z]+',
                                          '--enable-compression', '1', '--log-level', 'Warn'], self.server_args)
        self.assertIn('POST:/controller/createReplication', self.server.trace)
        expected_params = ['toBucket=bucket2', 'fromBucket=bucket1', 'toCluster=cluster1', 'compressionType=Auto',
                           'filterExpression=key%3A%5Ba-zA-z%5D%2B', 'replicationType=continuous', 'logLevel=Warn']

        self.rest_parameter_match(expected_params)

    def test_create_with_mutually_exclusive_args(self):
        self.system_exit_run(self.command + ['--create', '--xdcr-cluster-name', 'cluster1', '--xdcr-to-bucket',
                                             'bucket2', '--xdcr-from-bucket', 'bucket1', '--filter-expression',
                                             'key:[a-zA-z]+', '--enable-compression', '1',
                                             '--collection-explicit-mappings', '1', '--collection-migration', '1'],
                             self.server_args)
        self.assertIn('cannot enable both collection migration and explicit mappings', self.str_output)

    def test_create_CE_with_EE(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + ['--create',
                                             '--xdcr-cluster-name',
                                             'cluster1',
                                             '--xdcr-to-bucket',
                                             'bucket2',
                                             '--xdcr-from-bucket',
                                             'bucket1',
                                             '--filter-expression',
                                             'key:[a-zA-z]+',
                                             '--enable-compression',
                                             '1'],
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
                                          '--filter-deletion', '0', '--filter-expiration', '1', '--filter-binary', '1'],
                          self.server_args)
        self.assertIn('POST:/settings/replications/1', self.server.trace)
        expected_params = ['filterExpression=key%3A', 'filterSkipRestream=1', 'filterBypassExpiry=true',
                           'filterDeletion=false', 'filterExpiration=true', 'filterBinary=true']
        self.rest_parameter_match(expected_params)

    def test_settings_collection_args(self):
        self.no_error_run(self.command + ['--settings', '--xdcr-replicator', '1', '--collection-explicit-mappings',
                                          '1', '--collection-migration', '0', '--collection-mapping-rules',
                                          'mappings'], self.server_args)
        self.assertIn('POST:/settings/replications/1', self.server.trace)
        expected_params = ['collectionsExplicitMapping=true', 'collectionsMigrationMode=false',
                           'colMappingRules=mappings']
        self.rest_parameter_match(expected_params, False)

    def test_settings_collection_mutually_exclusive_args(self):
        self.system_exit_run(self.command + ['--settings', '--xdcr-replicator', '1', '--collection-explicit-mappings',
                                             '1', '--collection-migration', '1', '--collection-mapping-rules',
                                             'mappings'], self.server_args)
        self.assertIn('cannot enable both collection migration and explicit mappings', self.str_output)

    def test_migration_CE_(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + ['--settings', '--xdcr-replicator', '1', '--collection-explicit-mappings',
                                             '1', '--collection-migration', '0', '--collection-mapping-rules',
                                             'mappings'], self.server_args)
        self.assertIn('can only be configured on enterprise edition', self.str_output)

    def test_list_replicate(self):
        self.no_error_run(self.command + ['--list'], self.server_args)
        self.assertIn('GET:/pools/default/tasks', self.server.trace)

    def test_pause_resume(self):
        self.no_error_run(self.command + ['--resume', '--xdcr-replicator', '1'], self.server_args)
        self.assertIn('GET:/pools/default/tasks', self.server.trace)


class TestXdcrSetup(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'xdcr-setup'] + cluster_connect_args
        self.cmd_args_without_hostname = ['--xdcr-cluster-name', 'name', '--xdcr-username', 'username',
                                          '--xdcr-password', 'pwd']
        self.cmd_args = self.cmd_args_without_hostname + ['--xdcr-hostname', 'hostname']
        # TODO: encryption setting test
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestXdcrSetup, self).setUp()

    def test_create_xdcr(self):
        self.no_error_run(self.command + ['--create'] + self.cmd_args, self.server_args)
        self.assertIn('POST:/pools/default/remoteClusters', self.server.trace)
        expected_params = ['name=name', 'hostname=hostname', 'username=username', 'password=pwd', 'demandEncryption=0']
        self.rest_parameter_match(expected_params)

    def test_create_xdcr_encryption_passed_certificate(self):
        cert_file = tempfile.NamedTemporaryFile(delete=False)
        cert_file.write(b'this-is-the-cert-file')
        cert_file.close()

        self.no_error_run(self.command + ['--create'] + self.cmd_args +
                          ['--xdcr-demand-encryption', '1', '--xdcr-certificate', cert_file.name],
                          self.server_args)
        expected_params = ['name=name', 'hostname=hostname', 'username=username', 'password=pwd', 'demandEncryption=1',
                           'encryptionType=full', 'certificate=this-is-the-cert-file']
        self.rest_parameter_match(expected_params)

    def test_create_xdcr_encryption_needs_certificate(self):
        self.system_exit_run(self.command + ['--create'] + self.cmd_args + ['--xdcr-demand-encryption', '1'],
                             self.server_args)
        self.assertIn('certificate required if encryption is demanded', self.str_output)

    def test_create_xdcr_capella(self):
        args = ['--create'] + self.cmd_args + [
            '--xdcr-hostname', 'cb.abcdef12345678.cloud.couchbase.com', '--xdcr-demand-encryption', '1']
        self.no_error_run(self.command + args, self.server_args)
        self.assertIn('POST:/pools/default/remoteClusters', self.server.trace)
        expected_params = [
            'name=name', 'hostname=cb.abcdef12345678.cloud.couchbase.com', 'username=username', 'password=pwd',
            'demandEncryption=1', 'encryptionType=full']
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


class TestEventingFunctionSetup(CommandTest):
    def setUp(self):
        self.command = ["couchbase-cli", "eventing-function-setup"] + cluster_connect_args
        self.server_args = {"enterprise": True, "init": True, "is_admin": True,
                            'pools_default': {"nodes": [{"version": "0.0.0-0000-enterprise"}]},
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'eventingAdminPort': port,
                                }
                            }]}}
        super().setUp()

    def test_import_with_bucket_and_scope(self):
        self.command_args = ["--import", "--bucket", "bucket", "--scope", "scope", "--name", "function_name"]
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("This operation does not support the --bucket/--scope flags", self.str_output)

    def test_export_all_with_bucket_and_scope(self):
        self.command_args = ["--export-all", "--bucket", "bucket", "--scope", "scope"]
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("This operation does not support the --bucket/--scope flags", self.str_output)

    def test_list_with_bucket_and_scope(self):
        self.command_args = ["--list", "--bucket", "bucket", "--scope", "scope"]
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("This operation does not support the --bucket/--scope flags", self.str_output)

    def test_export_collection_unaware(self):
        self.server_args['eventing_export_payload'] = [
            {"appname": "name1"},
            {"appname": "name2"},
            {"appname": "name3"},
        ]
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.command_args = ["--export", "--name", "name1", "--file", tmp.name]

        self.no_error_run(self.command + self.command_args, self.server_args)

        with open(tmp.name) as f:
            dmp = json.loads(f.read())
            self.assertEqual(dmp, [{"appname": "name1"}])

        os.remove(tmp.name)

    def test_export_collection_unaware_7_1_0(self):
        self.server_args['eventing_export_payload'] = [
            {"appname": "name", "function_scope": {"bucket": "*", "scope": "*"}}]
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.command_args = ["--export", "--name", "name", "--file", tmp.name]

        self.no_error_run(self.command + self.command_args, self.server_args)

        with open(tmp.name) as f:
            dmp = json.loads(f.read())
            self.assertEqual(dmp, [{'appname': 'name', 'function_scope': {'bucket': '*', 'scope': '*'}}])

        os.remove(tmp.name)

    def test_export_collection_unaware_not_found(self):
        self.server_args['eventing_export_payload'] = [{"appname": "name"}]
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.command_args = ["--export", "--name", "not-the-name", "--file", tmp.name]

        self.system_exit_run(self.command + self.command_args, self.server_args)

        self.assertIn('Function "not-the-name" does not exist', self.str_output)

        os.remove(tmp.name)

    def test_export_collection_aware(self):
        self.server_args['eventing_export_payload'] = [
            {"appname": "name1", "function_scope": {"bucket": "*", "scope": "*"}},
            {"appname": "name1", "function_scope": {"bucket": "bucket1", "scope": "scope1"}},
            {"appname": "name2", "function_scope": {"bucket": "bucket2", "scope": "scope2"}},
        ]
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.command_args = [
            "--export",
            "--bucket",
            "bucket1",
            "--scope",
            "scope1",
            "--name",
            "name1",
            "--file",
            tmp.name]

        self.no_error_run(self.command + self.command_args, self.server_args)

        with open(tmp.name) as f:
            dmp = json.loads(f.read())
            self.assertEqual(dmp, [{'appname': 'name1', 'function_scope': {'bucket': 'bucket1', 'scope': 'scope1'}}])

        os.remove(tmp.name)

    def test_export_too_many_matches(self):
        self.server_args['eventing_export_payload'] = [
            {"appname": "name", "function_scope": {"bucket": "bucket", "scope": "scope"}},
            {"appname": "name", "function_scope": {"bucket": "bucket", "scope": "scope"}},
        ]
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.command_args = ["--export", "--bucket", "bucket", "--scope", "scope", "--name", "name", "--file", tmp.name]

        self.system_exit_run(self.command + self.command_args, self.server_args)

        self.assertIn('Unexpectedly found more than one function matching the name "bucket/scope/name"', self.str_output)

        os.remove(tmp.name)

    def test_export_collection_aware_not_found(self):
        self.server_args['eventing_export_payload'] = [
            {"appname": "name", "function_scope": {"bucket": "bucket", "scope": "scope"}}]
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.command_args = [
            "--export",
            "--bucket",
            "bucket",
            "--scope",
            "scope",
            "--name",
            "not-the-name",
            "--file",
            tmp.name]

        self.system_exit_run(self.command + self.command_args, self.server_args)

        self.assertIn('Function "bucket/scope/not-the-name" does not exist', self.str_output)

        os.remove(tmp.name)


class TestEventingFunctionSetupDeploy(CommandTest):
    def setUp(self):
        self.command = ["couchbase-cli", "eventing-function-setup"] + cluster_connect_args
        self.command_args = ["--deploy", "--name", "function_name"]
        self.server_args = {"enterprise": True, "init": True, "is_admin": True,
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'eventingAdminPort': port,
                                }
                            }]}}
        super().setUp()

    def test_deploy_deprecated(self):
        self.command_args += ["--boundary", "from-now"]
        self.server_args["pools_default"] = {"nodes": [{"version": "6.6.2-0000-enterprise"}]}
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn('POST:/api/v1/functions/function_name/deploy', self.server.trace)
        self.rest_parameter_match([json.dumps({})])
        self.assertIn("The --boundary option is deprecated and will be ignored; the function definition itself"
                      " now defines the feed boundary", self.str_output)

    def test_deploy_deprecated_unknown_version(self):
        self.command_args += ["--boundary", "from-now"]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn('POST:/api/v1/functions/function_name/deploy', self.server.trace)
        self.rest_parameter_match([json.dumps({})])
        self.assertIn("The --boundary option is deprecated and will be ignored; the function definition itself"
                      " now defines the feed boundary", self.str_output)

    def test_deploy_not_deprecated_still_required(self):
        self.server_args["pools_default"] = {"nodes": [{"version": "6.6.1-0000-enterprise"}]}
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("--boundary is needed to deploy a function", self.str_output)

    def test_deploy_mixed_mode(self):
        self.server_args["pools_default"] = {"nodes": [
            {"version": "6.6.1-0000-enterprise"}, {"version": "7.0.0-0000-enteenterprise"}]}
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("--boundary is needed to deploy a function", self.str_output)

    def test_deploy_with_no_bucket_and_scope_flags(self):
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn("Request to deploy the function was accepted", self.str_output)

    def test_deploy_with_only_bucket_flag(self):
        self.command_args += ["--bucket", "default"]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("You need to supply both --bucket and --scope, or neither for collection-unaware eventing "
                      "functions", self.str_output)

    def test_deploy_with_only_scope_flag(self):
        self.command_args += ["--scope", "default"]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("You need to supply both --bucket and --scope, or neither for collection-unaware eventing "
                      "functions", self.str_output)

    def test_deploy_with_both_bucket_and_scope_flags(self):
        self.command_args += ["--bucket", "default", "--scope", "default"]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn("Request to deploy the function was accepted", self.str_output)


class TestEventingFunctionQueryParameters(CommandTest):
    def setUp(self):
        self.command = ["couchbase-cli", "eventing-function-setup"] + cluster_connect_args
        self.command_args = ["--name", "function_name"]
        self.server_args = {"enterprise": True, "init": True, "is_admin": True,
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'eventingAdminPort': port,
                                }
                            }]}}
        super().setUp()

    def test_expected_parameters_deploy_no_bucket_and_scope(self):
        self.command_args += ["--deploy"]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.server_args['expected_query_parameters'] = {}
        self.no_error_run(self.command + self.command_args, self.server_args)

    def test_expected_parameters_deploy_bucket_and_scope(self):
        bucket_name = "test_bucket_deploy"
        scope_name = "test_scope_deploy"
        self.command_args += ["--deploy", "--bucket", bucket_name, "--scope", scope_name]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.server_args['expected_query_parameters'] = {'bucket': [bucket_name], 'scope': [scope_name]}
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn('POST:/api/v1/functions/function_name/deploy', self.server.trace)

    def test_expected_parameters_undeploy_bucket_and_scope(self):
        bucket_name = "test_bucket_undeploy"
        scope_name = "test_scope_undeploy"
        self.command_args += ["--undeploy", "--bucket", bucket_name, "--scope", scope_name]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.server_args['expected_query_parameters'] = {'bucket': [bucket_name], 'scope': [scope_name]}
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn('POST:/api/v1/functions/function_name/undeploy', self.server.trace)

    def test_expected_parameters_delete_bucket_and_scope(self):
        bucket_name = "test_bucket_delete"
        scope_name = "test_scope_delete"
        self.command_args += ["--delete", "--bucket", bucket_name, "--scope", scope_name]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.server_args['expected_query_parameters'] = {'bucket': [bucket_name], 'scope': [scope_name]}
        self.no_error_run(self.command + self.command_args, self.server_args)

    def test_expected_parameters_pause_bucket_and_scope(self):
        bucket_name = "test_bucket_pause"
        scope_name = "test_scope_pause"
        self.command_args += ["--pause", "--bucket", bucket_name, "--scope", scope_name]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.server_args['expected_query_parameters'] = {'bucket': [bucket_name], 'scope': [scope_name]}
        self.no_error_run(self.command + self.command_args, self.server_args)

    def test_expected_parameters_resume_bucket_and_scope(self):
        bucket_name = "test_bucket_resume"
        scope_name = "test_scope_resume"
        self.command_args += ["--resume", "--bucket", bucket_name, "--scope", scope_name]
        self.server_args["pools_default"] = {"nodes": [{"version": "0.0.0-0000-enterprise"}]}
        self.server_args['expected_query_parameters'] = {'bucket': [bucket_name], 'scope': [scope_name]}
        self.no_error_run(self.command + self.command_args, self.server_args)


class TestEventingFunctionList(CommandTest):
    def setUp(self):
        self.command = ["couchbase-cli", "eventing-function-setup", "--list"] + cluster_connect_args
        self.command_args = ["--name", "function_name"]
        self.server_args = {"enterprise": True, "init": True, "is_admin": True,
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'eventingAdminPort': port,
                                }
                            }]}}
        super().setUp()

    def test_eventing_function_list_one_function_collection_aware(self):
        self.server_args["functions_status_payload"] = {
            'apps':
            [
                {
                    'composite_status': 'undeployed',
                    'name': 'func_0',
                    'function_scope':
                    {
                        'bucket': 'bucket_0',
                        'scope': 'scope_0'
                    },
                }
            ],
        }
        self.server_args['functions_list_payload'] = [
            {
                'depcfg':
                {
                    'source_bucket': 'bucket_0',
                    'source_scope': '_default',
                    'source_collection': '_default',
                    'metadata_bucket': 'bucket_1',
                    'metadata_scope': '_default',
                    'metadata_collection': '_default'
                },
                'appname': 'func_0',
                'function_scope':
                {
                    'bucket': 'bucket_0',
                    'scope': 'scope_0'
                }
            }
        ]
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn(
            "bucket_0/scope_0/func_0\n Status: undeployed\n Source: bucket_0._default._default\n " +
            "Metadata: bucket_1._default._default\n",
            self.str_output)

    def test_eventing_function_list_one_function_collection_unaware(self):
        self.server_args["functions_status_payload"] = {
            'apps':
            [
                {
                    'composite_status': 'undeployed',
                    'name': 'func_0',
                    'function_scope':
                    {
                        'bucket': '*',
                        'scope': '*'
                    },
                }
            ],
        }
        self.server_args['functions_list_payload'] = [
            {
                'depcfg':
                {
                    'source_bucket': 'bucket_0',
                    'source_scope': '_default',
                    'source_collection': '_default',
                    'metadata_bucket': 'bucket_1',
                    'metadata_scope': '_default',
                    'metadata_collection': '_default'
                },
                'appname': 'func_0',
                'function_scope':
                {
                    'bucket': '*',
                    'scope': '*'
                }
            }
        ]
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn(
            "func_0\n Status: undeployed\n Source: bucket_0._default._default\n " +
            "Metadata: bucket_1._default._default\n",
            self.str_output)

    def test_eventing_function_list_one_function_legacy(self):
        self.server_args["functions_status_payload"] = {
            'apps':
            [
                {
                    'composite_status': 'undeployed',
                    'name': 'func_0',
                }
            ],
        }
        self.server_args['functions_list_payload'] = [
            {
                'depcfg':
                {
                    'source_bucket': 'bucket_0',
                    'source_scope': '_default',
                    'source_collection': '_default',
                    'metadata_bucket': 'bucket_1',
                    'metadata_scope': '_default',
                    'metadata_collection': '_default'
                },
                'appname': 'func_0',
            }
        ]
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn(
            "func_0\n Status: undeployed\n Source: bucket_0._default._default\n " +
            "Metadata: bucket_1._default._default\n",
            self.str_output)

    def test_eventing_function_list_two_functions(self):
        self.server_args["functions_status_payload"] = {
            'apps':
            [
                {
                    'composite_status': 'undeployed',
                    'name': 'func_0',
                    'function_scope':
                    {
                        'bucket': 'bucket_0',
                        'scope': 'scope_0'
                    },
                },
                {
                    'composite_status': 'deployed',
                    'name': 'func_1',
                    'function_scope':
                    {
                        'bucket': 'bucket_1',
                        'scope': 'scope_1'
                    },
                }
            ],
        }
        self.server_args['functions_list_payload'] = [
            {
                'depcfg':
                {
                    'source_bucket': 'bucket_0',
                    'source_scope': '_default',
                    'source_collection': '_default',
                    'metadata_bucket': 'bucket_1',
                    'metadata_scope': '_default',
                    'metadata_collection': '_default'
                },
                'appname': 'func_0',
                'function_scope':
                {
                    'bucket': 'bucket_0',
                    'scope': 'scope_0'
                }
            },
            {
                'depcfg':
                {
                    'source_bucket': 'bucket_1',
                    'source_scope': '_default',
                    'source_collection': '_default',
                    'metadata_bucket': 'bucket_0',
                    'metadata_scope': '_default',
                    'metadata_collection': '_default'
                },
                'appname': 'func_1',
                'function_scope':
                {
                    'bucket': 'bucket_1',
                    'scope': 'scope_1'
                }
            }
        ]
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn(
            "bucket_0/scope_0/func_0\n Status: undeployed\n Source: bucket_0._default._default\n " +
            "Metadata: bucket_1._default._default\n",
            self.str_output)
        self.assertIn(
            "bucket_1/scope_1/func_1\n Status: deployed\n Source: bucket_1._default._default\n " +
            "Metadata: bucket_0._default._default\n",
            self.str_output)

    def test_eventing_function_list_two_functions_same_name(self):
        self.server_args["functions_status_payload"] = {
            'apps':
            [
                {
                    'composite_status': 'undeployed',
                    'name': 'func_0',
                    'function_scope':
                    {
                        'bucket': 'bucket_0',
                        'scope': 'scope_0'
                    },
                },
                {
                    'composite_status': 'undeployed',
                    'name': 'func_0',
                    'function_scope':
                    {
                        'bucket': 'bucket_1',
                        'scope': 'scope_1'
                    },
                }
            ],
        }
        self.server_args['functions_list_payload'] = [
            {
                'depcfg':
                {
                    'source_bucket': 'bucket_0',
                    'source_scope': '_default',
                    'source_collection': '_default',
                    'metadata_bucket': 'bucket_1',
                    'metadata_scope': '_default',
                    'metadata_collection': '_default'
                },
                'appname': 'func_0',
                'function_scope':
                {
                    'bucket': 'bucket_0',
                    'scope': 'scope_0'
                }
            },
            {
                'depcfg':
                {
                    'source_bucket': 'bucket_1',
                    'source_scope': '_default',
                    'source_collection': '_default',
                    'metadata_bucket': 'bucket_0',
                    'metadata_scope': '_default',
                    'metadata_collection': '_default'
                },
                'appname': 'func_0',
                'function_scope':
                {
                    'bucket': 'bucket_1',
                    'scope': 'scope_1'
                }
            }
        ]
        self.no_error_run(self.command + self.command_args, self.server_args)
        self.assertIn(
            "bucket_0/scope_0/func_0\n Status: undeployed\n Source: bucket_0._default._default\n " +
            "Metadata: bucket_1._default._default\n",
            self.str_output)
        self.assertIn(
            "bucket_1/scope_1/func_0\n Status: undeployed\n Source: bucket_1._default._default\n " +
            "Metadata: bucket_0._default._default\n",
            self.str_output)


class TestEventingFunctionSetupSubcommandErrors(CommandTest):
    def setUp(self):
        self.command = ["couchbase-cli", "eventing-function-setup"] + cluster_connect_args
        self.server_args = {"enterprise": True, "init": True, "is_admin": True,
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'eventingAdminPort': port,
                                }
                            }]}}
        super().setUp()

    def test_handling_of_expected_error_payload_type_1(self):
        self.command_args = ['--pause', '--name', 'test_func']
        self.server_args['override_eventing_setup_status'] = 403
        self.server_args['override_eventing_setup_payload'] = {"message": "Expected payload type 1",
                                                               "permissions": ["Insufficient permission example"]}
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("ERROR: Expected payload type 1: Insufficient permission example\n", self.str_output)

    def test_handling_of_expected_error_payload_type_2(self):
        self.command_args = ['--pause', '--name', 'test_func']
        self.server_args['override_eventing_setup_status'] = 403
        self.server_args['override_eventing_setup_payload'] = {"runtime_info": {
            "info": "Expected payload type 2: Insufficient permission example"}}
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("ERROR: Expected payload type 2: Insufficient permission example\n", self.str_output)

    def test_handling_of_unexpected_error_payload(self):
        self.command_args = ['--pause', '--name', 'test_func']
        self.server_args['override_eventing_setup_status'] = 403
        self.server_args['override_eventing_setup_payload'] = "Unexpected response"
        self.system_exit_run(self.command + self.command_args, self.server_args)
        self.assertIn("Unexpected response", self.str_output)


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

    def test_missing_cmd(self):
        self.system_exit_run(self.command + ['--max-ttl', '100'], self.server_args)
        self.assertIn("Must specify one of the following: --create-scope, --drop-scope, --list-scopes, "
                      "--create-collection, --edit-collection, --drop-collection, or --list-collections",
                      self.str_output)

    def test_more_than_one_cmd(self):
        self.system_exit_run(self.command + ['--create-scope', 'scope_1', '--list-collections'], self.server_args)
        self.assertIn("Only one of the following may be specified: --create-scope, --drop-scope, --list-scopes, "
                      "--create-collection, --edit-collection, --drop-collection, or --list-collections",
                      self.str_output)

    def test_enable_history_set_with_not_create_collection_or_edit_collection(self):
        self.system_exit_run(self.command + ['--list-collections', '--enable-history-retention', '1'], self.server_args)
        self.assertIn("--enable-history-retention can only be set with --create-collection or --edit-collection",
                      self.str_output)

    def test_enable_history_or_max_ttl_or_no_expiry_should_be_set_with_edit_collection(self):
        self.system_exit_run(self.command + ['--edit-collection', 'scope_1.collection_1'], self.server_args)
        self.assertIn(
            "at least one of {--enable-history-retention, --max-ttl, --no-expiry} should be set with " +
            "--edit-collection", self.str_output)

    def test_create_scope(self):
        self.no_error_run(self.command + ['--create-scope', 'scope_1'], self.server_args)
        self.assertIn('POST:/pools/default/buckets/name/scopes', self.server.trace)
        expected_params = ['name=scope_1']
        self.rest_parameter_match(expected_params)

    def test_delete_scope(self):
        self.no_error_run(self.command + ['--drop-scope', 'scope_1'], self.server_args)
        self.assertIn('DELETE:/pools/default/buckets/name/scopes/scope_1', self.server.trace)

    def list_scopes(self):
        self.server_args['collection_manifest'] = {"scope_1": {"collection_1": {}}, "scope_2": {"collection_2": {}}}
        self.no_error_run(self.command + ['--list-scopes'], self.server_args)
        self.assertIn('GET:/pools/default/buckets/name/scopes', self.server.trace)
        expected_out = ['scope_1', 'scope_2']
        for p in expected_out:
            self.assertIn(p, self.str_output)

    def test_create_collection(self):
        self.no_error_run(self.command + ['--create-collection', 'scope_1.collection_1', '--max-ttl', '100',
                          '--enable-history-retention', '1'], self.server_args)
        self.assertIn('POST:/pools/default/buckets/name/scopes/scope_1/collections', self.server.trace)
        expected_params = ['name=collection_1', 'maxTTL=100', 'history=true']
        self.rest_parameter_match(expected_params)

    def test_create_collection_with_0_max_ttl(self):
        self.no_error_run(self.command + ['--create-collection', 'scope_1.collection_1', '--max-ttl', '0',
                          '--enable-history-retention', '1'], self.server_args)
        self.assertIn('POST:/pools/default/buckets/name/scopes/scope_1/collections', self.server.trace)
        expected_params = ['name=collection_1', 'maxTTL=0', 'history=true']
        self.rest_parameter_match(expected_params)

    def test_no_expiry_pre_7_6(self):
        self.server_args["pools_default"] = {"nodes": [{"version": "7.2.0-0000-enteenterprise"}]}
        self.system_exit_run(
            self.command +
            ['--create-collection', 'scope_1.collection_1', '--enable-history-retention', '1', '--no-expiry'],
            self.server_args)
        self.assertIn('--no-expiry can only be used on >= 7.6.0 clusters', self.str_output)

    def test_edit_collection(self):
        self.server_args["pools_default"] = {"nodes": [{"version": "7.6.0-0000-enteenterprise"}]}
        self.no_error_run(self.command + ['--edit-collection', 'scope_1.collection_1', '--enable-history-retention',
                                          '1', '--max-ttl', '60'], self.server_args)
        self.assertIn('PATCH:/pools/default/buckets/name/scopes/scope_1/collections/collection_1', self.server.trace)
        expected_params = ['history=true', 'maxTTL=60']
        self.rest_parameter_match(expected_params)

    def test_edit_collection_max_ttl_pre_7_6(self):
        self.server_args["pools_default"] = {"nodes": [{"version": "7.2.0-0000-enteenterprise"}]}
        self.system_exit_run(self.command + ['--edit-collection', 'scope_1.collection_1', '--enable-history-retention',
                                             '1', '--max-ttl', '60'], self.server_args)
        self.assertIn('--max-ttl can only be used with --edit-collection on >= 7.6.0 clusters',
                      self.str_output)

    def test_edit_collection_no_expiry(self):
        self.server_args["pools_default"] = {"nodes": [{"version": "7.6.0-0000-enteenterprise"}]}
        self.no_error_run(self.command + ['--edit-collection', 'scope_1.collection_1', '--enable-history-retention',
                                          '1', '--no-expiry'], self.server_args)
        self.assertIn('PATCH:/pools/default/buckets/name/scopes/scope_1/collections/collection_1', self.server.trace)
        expected_params = ['history=true', 'maxTTL=-1']
        self.rest_parameter_match(expected_params)

    def test_edit_collection_cant_use_max_ttl_and_no_expiry_together(self):
        self.server_args["pools_default"] = {"nodes": [{"version": "7.6.0-0000-enteenterprise"}]}
        self.system_exit_run(self.command + ['--edit-collection', 'scope_1.collection_1', '--enable-history-retention',
                                             '1', '--max-ttl', '60', '--no-expiry'], self.server_args)
        self.assertIn('Only one of --max-ttl and --no-expiry may be set', self.str_output)

    def test_delete_collection(self):
        self.no_error_run(self.command + ['--drop-collection', 'scope_1.collection_1'], self.server_args)
        self.assertIn('DELETE:/pools/default/buckets/name/scopes/scope_1/collections/collection_1', self.server.trace)

    def list_collections(self):
        self.server_args['collection_manifest'] = {
            'scopes': [{
                'name': 'scope_1',
                'collections': [{'name': 'collection_1'}, {'name': 'collection_2'}]
            }]
        }
        self.no_error_run(self.command + ['--list-collections', 'scope_1'], self.server_args)
        self.assertIn('GET:/pools/default/buckets/name/scopes', self.server.trace)
        expected_out = ['    - collection_1', '    - collection_2', 'Scope scope_1:']
        for p in expected_out:
            self.assertIn(p, self.str_output)

    def list_collections_json(self):
        self.server_args['collection_manifest'] = {
            'scopes': [
                {
                    'name': 'scope_1',
                    'collections': [{'name': 'collection_1'}, {'name': 'collection_2'}]
                },
                {
                    'name': 'scope_2',
                    'collections': [{'name': 'collection_1'}, {'name': 'collection_2'}]
                }
            ]
        }
        self.no_error_run(self.command + ['--list-collections', 'scope_1', '-o', 'json'], self.server_args)
        self.assertIn('GET:/pools/default/buckets/name/scopes', self.server.trace)
        expected = json.dumps({'scope_1': ['collection_1', 'collection_2'], 'scope_2': [
            'collection_1', 'collection_2']}, sort_keys=True)

        out = json.dumps(json.loads(self.str_output), sort_keys=True)
        self.assertEqual(expected, out)


class TestSettingLdap(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-ldap'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        self.authentication_args = ['--authentication-enabled', '1', '--hosts', '0.0.0.0', '--port', '369',
                                    '--encryption', 'none', '--request-timeout', '2000', '--max-parallel', '20',
                                    '--max-cache-size', '20', '--cache-value-lifetime', '2000000',
                                    '--server-cert-validation', '0']
        self.authorization_args = ['--authorization-enabled', '1', '--bind-dn', 'admin', '--bind-password', 'pass',
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
        # create fake user certificate file
        user_cert_file = tempfile.NamedTemporaryFile(delete=False)
        user_cert_file_name = user_cert_file.name
        user_cert_file.write(b'this-is-the-user-cert-file')
        user_cert_file.close()
        # create fake user key file
        user_key_file = tempfile.NamedTemporaryFile(delete=False)
        user_key_file_name = user_key_file.name
        user_key_file.write(b'this-is-the-user-key-file')
        user_key_file.close()
        cert_args = ['--ldap-client-cert', user_cert_file_name, '--ldap-client-key', user_key_file_name]

        self.no_error_run(self.command + self.authentication_args + self.authorization_args + cert_args,
                          self.server_args)

        # clean up the test files
        os.remove(user_cert_file_name)
        os.remove(user_key_file_name)

        self.assertIn('POST:/settings/ldap', self.server.trace)
        expected_params = ['authenticationEnabled=true', 'authorizationEnabled=true', 'hosts=0.0.0.0', 'port=369',
                           'encryption=None', 'requestTimeout=2000', 'maxParallelConnections=20',
                           'maxCacheSize=20', 'cacheValueLifetime=2000000', 'bindDN=admin', 'bindPass=pass',
                           'nestedGroupsEnabled=true', 'nestedGroupsMaxDepth=10',
                           'groupsQuery=%25D%3FmemberOf%3Fbase', 'serverCertValidation=false',
                           'clientTLSCert=this-is-the-user-cert-file',
                           'clientTLSKey=this-is-the-user-key-file']
        self.rest_parameter_match(expected_params)

    def test_set_ldap_user_dn_advanced(self):
        mapping_file = tempfile.NamedTemporaryFile(delete=False)
        file_contents = '[{"match":"","query":""}]'
        mapping_file.write(file_contents.encode())
        mapping_file.close()

        mapping = f'{{"advanced":{file_contents}}}'

        self.no_error_run(self.command + self.authentication_args + ['--user-dn-advanced', mapping_file.name],
                          self.server_args)
        self.assertIn('POST:/settings/ldap', self.server.trace)
        expected_params = ['authenticationEnabled=true', 'hosts=0.0.0.0', 'port=369',
                           'encryption=None', 'requestTimeout=2000', 'maxParallelConnections=20',
                           'maxCacheSize=20', 'cacheValueLifetime=2000000', 'serverCertValidation=false',
                           'userDNMapping=' + urllib.parse.quote(mapping)]
        self.rest_parameter_match(expected_params)

    def test_set_ldap_user_dn_advanced_invalid_json(self):
        mapping_file = tempfile.NamedTemporaryFile(delete=False)
        file_contents = 'this is not valid json'
        mapping_file.write(file_contents.encode())
        mapping_file.close()

        self.system_exit_run(self.command + self.authentication_args + ['--user-dn-advanced', mapping_file.name],
                             self.server_args)
        self.assertIn('does not contain valid JSON', self.str_output)


class TestIpFamily(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'ip-family'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestIpFamily, self).setUp()

    def test_get_single_node(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv4', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def test_get_single_node_unknown_pool(self):
        self.server_args['/pools/nodes'] = 'unknown pool'
        self.server_args['override-status'] = 400
        self.server_args['node-info'] = {'addressFamily': 'inet'}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv4', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def test_get_single_node_ipv4_only(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet', 'addressFamilyOnly': True}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv4only', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def test_get_single_node_ipv6_only(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet6', 'addressFamilyOnly': True}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv6only', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def test_get_multiple_nodes(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet6'}, {'addressFamily': 'inet6'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv6', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def test_get_multiple_node_TLS(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet6_tls'}, {'addressFamily': 'inet6_tls'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster using ipv6', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def test_get_multiple_node_mixed(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'addressFamily': 'inet'}, {'addressFamily': 'inet6_tls'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster is in mixed mode', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)

    def test_set_ipv4(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--set', '--ipv4'], self.server_args)
        self.assertIn('Switched IP family of the cluster', self.str_output)
        self.assertCountEqual(['GET:/pools', 'GET:/pools/nodes', 'GET:/settings/security',
                               'POST:/node/controller/enableExternalListener', 'POST:/node/controller/setupNetConfig',
                               'GET:/pools/nodes', 'POST:/node/controller/disableUnusedExternalListeners'],
                              self.server.trace)
        expected_params = ['afamily=ipv4', 'afamily=ipv4', 'afamilyOnly=false']
        self.rest_parameter_match(expected_params, True)

    def test_set_ipv4_unknown_pool(self):
        self.server_args['/pools/nodes'] = 'unknown pool'
        self.server_args['override-status'] = 400
        self.no_error_run(self.command + ['--set', '--ipv4'], self.server_args)
        self.assertIn('Switched IP family of the cluster', self.str_output)
        self.assertCountEqual(['GET:/pools', 'GET:/pools/nodes', 'POST:/node/controller/enableExternalListener',
                               'POST:/node/controller/setupNetConfig',
                               'POST:/node/controller/disableUnusedExternalListeners'],
                              self.server.trace)
        expected_params = ['afamily=ipv4', 'afamily=ipv4', 'afamilyOnly=false']
        self.rest_parameter_match(expected_params, True)

    def test_set_ipv4_only(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--set', '--ipv4only'], self.server_args)
        self.assertIn('Switched IP family of the cluster', self.str_output)
        self.assertCountEqual(['GET:/pools', 'GET:/pools/nodes', 'GET:/settings/security',
                               'POST:/node/controller/enableExternalListener', 'POST:/node/controller/setupNetConfig',
                               'GET:/pools/nodes', 'POST:/node/controller/disableUnusedExternalListeners'],
                              self.server.trace)
        expected_params = ['afamily=ipv4', 'afamily=ipv4', 'afamilyOnly=true']
        self.rest_parameter_match(expected_params, True)

    def test_set_ipv6(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--set', '--ipv6'], self.server_args)
        self.assertIn('Switched IP family of the cluster', self.str_output)
        self.assertCountEqual(['GET:/pools', 'GET:/pools/nodes', 'GET:/settings/security',
                               'POST:/node/controller/enableExternalListener', 'POST:/node/controller/setupNetConfig',
                               'GET:/pools/nodes', 'POST:/node/controller/disableUnusedExternalListeners'],
                              self.server.trace)
        expected_params = ['afamily=ipv6', 'afamily=ipv6', 'afamilyOnly=false']
        self.rest_parameter_match(expected_params, True)

    def test_set_ipv6_only(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--set', '--ipv6only'], self.server_args)
        self.assertIn('Switched IP family of the cluster', self.str_output)
        self.assertCountEqual(['GET:/pools', 'GET:/pools/nodes', 'GET:/settings/security',
                               'POST:/node/controller/enableExternalListener', 'POST:/node/controller/setupNetConfig',
                               'GET:/pools/nodes', 'POST:/node/controller/disableUnusedExternalListeners'],
                              self.server.trace)
        expected_params = ['afamily=ipv6', 'afamily=ipv6', 'afamilyOnly=true']
        self.rest_parameter_match(expected_params, True)

    def test_set_force_tls(self):
        # NOTE: We use a HTTP port that doesn't exist to ensure that we're using the HTTPS port
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:12345',
                                                       'ports': {'httpsMgmt': '6790'}}]}
        self.server_args['/settings/security'] = {"clusterEncryptionLevel": 'strict'}
        self.no_error_run(self.command + ['--set', '--ipv4', '--no-ssl-verify'], self.server_args)
        self.assertIn('Switched IP family of the cluster', self.str_output)
        self.assertIn('GET:/pools/nodes', self.server.trace)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableUnusedExternalListeners', self.server.trace)
        expected_params = ['afamily=ipv4', 'afamily=ipv4', 'afamilyOnly=false']
        self.rest_parameter_match(expected_params, True)


class TestClusterEncryption(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'node-to-node-encryption'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestClusterEncryption, self).setUp()

    def test_error_disable_and_enable(self):
        self.system_exit_run(self.command + ['--enable', '--disable'], self.server_args)

    def test_get_encryption_false(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'nodeEncryption': False, 'hostname': 'host1'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Node-to-node encryption is disabled', self.str_output)

    def test_get_encryption_true(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'nodeEncryption': True, 'hostname': 'host1'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Node-to-node encryption is enabled', self.str_output)

    def test_get_encryption_mixed_mode(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'nodeEncryption': True, 'hostname': 'host1'},
                                                      {'nodeEncryption': False, 'hostname': 'host2'}]}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('Cluster is in mixed mode', self.str_output)

    def test_enable_cluster_encryption_ipv4(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--enable'], self.server_args)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableUnusedExternalListeners', self.server.trace)
        self.rest_parameter_match(['nodeEncryption=on', 'nodeEncryption=on'])

    def test_enable_cluster_encryption_ipv4_unknown_pool(self):
        self.server_args['/pools/nodes'] = 'unknown pool'
        self.server_args['override-status'] = 400
        self.no_error_run(self.command + ['--enable'], self.server_args)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableUnusedExternalListeners', self.server.trace)
        self.rest_parameter_match(['nodeEncryption=on', 'nodeEncryption=on'])

    def test_disable_cluster_encryption_ipv4(self):
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:6789',
                                                       'ports': {'httpsMgmt': '6789'}}]}
        self.no_error_run(self.command + ['--disable'], self.server_args)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableUnusedExternalListeners', self.server.trace)
        self.rest_parameter_match(['nodeEncryption=off', 'nodeEncryption=off'])

    def test_enable_cluster_encryption_force_tls(self):
        # NOTE: We use a HTTP port that doesn't exist to ensure that we're using the HTTPS port
        self.server_args['/pools/nodes'] = {'nodes': [{'hostname': 'localhost:12345',
                                                       'ports': {'httpsMgmt': '6790'}}]}
        self.server_args['/settings/security'] = {"clusterEncryptionLevel": 'strict'}
        self.no_error_run(self.command + ['--enable', '--no-ssl-verify'], self.server_args)
        self.assertIn('POST:/node/controller/enableExternalListener', self.server.trace)
        self.assertIn('POST:/node/controller/setupNetConfig', self.server.trace)
        self.assertIn('POST:/node/controller/disableUnusedExternalListeners', self.server.trace)
        self.rest_parameter_match(['nodeEncryption=on', 'nodeEncryption=on'])


class TestSettingRebalance(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-rebalance'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingRebalance, self).setUp()

    def test_ce_invalid(self):
        self.server_args['enterprise'] = False
        self.system_exit_run(self.command + ['--set', '--enable', '1'], self.server_args)
        self.assertIn('Automatic rebalance retry configuration is an Enterprise Edition only feature', self.str_output)

    def test_ce_valid(self):
        self.server_args['enterprise'] = False
        self.no_error_run(self.command + ['--set', '--moves-per-node', '10'], self.server_args)
        self.assertIn('Rebalance settings updated', self.str_output)
        expected_params = ['rebalanceMovesPerNode=10']
        self.rest_parameter_match(expected_params)

    def test_more_than_one_action(self):
        self.system_exit_run(self.command + ['--get', '--set'], self.server_args)
        self.assertIn('Provide either --set, --get, --cancel or --pending-info', self.str_output)

    def test_get_human_friendly(self):
        self.server_args['/settings/retryRebalance'] = {"enabled": False, "afterTimePeriod": 300, "maxAttempts": 1}
        self.server_args['/settings/rebalance'] = {"rebalanceMovesPerNode": 4}
        self.no_error_run(self.command + ['--get'], self.server_args)
        expected_output = ['Automatic rebalance retry disabled', 'Retry wait time: 300', 'Maximum number of retries: 1',
                           'Maximum number of vBucket move per node: 4']
        for e in expected_output:
            self.assertIn(e, self.str_output)

    def test_get_json(self):
        self.server_args['/settings/retryRebalance'] = {"enabled": False, "afterTimePeriod": 300, "maxAttempts": 1}
        self.server_args['/settings/rebalance'] = {"rebalanceMovesPerNode": 4}
        self.no_error_run(self.command + ['--get', '--output', 'json'], self.server_args)
        self.assertIn('{"rebalanceMovesPerNode": 4, "enabled": false, "afterTimePeriod": 300, "maxAttempts": 1}',
                      self.str_output)

    def test_set(self):
        self.no_error_run(self.command + ['--set', '--enable', '1', '--wait-for', '5', '--max-attempts', '3',
                                          '--moves-per-node', '10'],
                          self.server_args)
        expected_params = ['enabled=true', 'afterTimePeriod=5', 'maxAttempts=3', 'rebalanceMovesPerNode=10']
        self.rest_parameter_match(expected_params)

    def test_set_wait_for_out_of_range(self):
        self.system_exit_run(self.command + ['--set', '--enable', '1', '--wait-for', '1', '--max-attempts', '3'],
                             self.server_args)
        self.assertIn('--wait-for must be a value between 5 and 3600', self.str_output)

    def test_set_rebalance_moves_per_node_above_64(self):
        self.system_exit_run(self.command + ['--set', '--enable', '1', '--moves-per-node', '65'],
                             self.server_args)
        self.assertIn('--moves-per-node must be a value between 1 and 64', self.str_output)

    def test_set_rebalance_moves_per_node_below_1(self):
        self.system_exit_run(self.command + ['--set', '--enable', '1', '--moves-per-node', '0'],
                             self.server_args)
        self.assertIn('--moves-per-node must be a value between 1 and 64', self.str_output)


class TestAnalyticsLinkSetup(CommandTest):
    def setUp(self):
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'cbas': port,
                                }
                            }]}}
        self.command = ['couchbase-cli', 'analytics-link-setup'] + cluster_connect_args
        super(TestAnalyticsLinkSetup, self).setUp()

    def test_more_than_one_action_flag(self):
        self.system_exit_run(self.command + ['--list', '--create'], self.server_args)

    def test_list_no_params(self):
        self.server_args['/analytics/link'] = []
        self.no_error_run(self.command + ['--list'], self.server_args)
        self.assertNotIn('query', self.server_args, 'did not expect any query arguments')

    def test_list_data_verse(self):
        self.server_args['/analytics/link/Default'] = []
        self.no_error_run(self.command + ['--list', '--dataverse', 'Default'], self.server_args)
        self.assertNotIn('query', self.server_args, 'did not expect any query arguments')
        self.assertIn('GET:/analytics/link/Default', self.server.trace)

    def test_list_all_options(self):
        self.server_args['/analytics/link/Default/name'] = []
        self.no_error_run(self.command + ['--list', '--dataverse', 'Default', '--name', 'name', '--type', 'couchbase'],
                          self.server_args)
        self.assertIn('query', self.server_args, 'expected query parameters to have been set')
        self.assertEqual(self.server_args['query'], 'type=couchbase')
        self.assertIn('GET:/analytics/link/Default/name', self.server.trace)

    def test_list_invalid_type(self):
        self.system_exit_run(self.command + ['--list', '--type', 'fire'], self.server_args)

    def test_create_no_options(self):
        self.system_exit_run(self.command + ['--create'], self.server_args)
        self.assertIn('scope is required', self.str_output)

    def test_create_no_type(self):
        self.system_exit_run(self.command + ['--create', '--dataverse', 'Default', '--name', 'east'], self.server_args)
        self.assertIn('type is required', self.str_output)

    def test_create_minimum(self):
        self.no_error_run(self.command + ['--create', '--dataverse', 'Default', '--name', 'east', '--type', 's3'],
                          self.server_args)
        self.assertIn('POST:/analytics/link/Default/east', self.server.trace)
        self.rest_parameter_match(['type=s3'])

    def test_create_s3(self):
        self.no_error_run(self.command + ['--create', '--scope', 'my/Scope', '--name', 'east', '--type', 's3',
                                          '--access-key-id', 'id-1', '--secret-access-key', 'my-secret', '--region',
                                          'us-east-0', '--service-endpoint', 'my-cool-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/my%2FScope/east', self.server.trace)
        self.rest_parameter_match(['type=s3', 'accessKeyId=id-1', 'secretAccessKey=my-secret', 'region=us-east-0',
                                   'serviceEndpoint=my-cool-endpoint.com'])

    def test_create_s3_session_token(self):
        self.no_error_run(self.command + ['--create', '--dataverse', 'Default%Scope', '--name', 'east', '--type', 's3',
                                          '--access-key-id', 'id-1', '--secret-access-key', 'my-secret', '--region',
                                          'us-east-0', '--session-token', 'my-token', '--service-endpoint',
                                          'my-cool-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/Default%25Scope/east', self.server.trace)
        self.rest_parameter_match(['type=s3', 'accessKeyId=id-1', 'secretAccessKey=my-secret', 'region=us-east-0',
                                   'sessionToken=my-token', 'serviceEndpoint=my-cool-endpoint.com'])

    def test_create_azureblob_account_name_present_account_key_present(self):
        self.no_error_run(self.command + ['--create', '--scope', 'Default', '--name', 'myLink', '--type',
                                          'azureblob', '--account-name', 'myAccountName', '--account-key',
                                          'myAccountKey', '--endpoint', 'my-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/Default/myLink', self.server.trace)
        self.rest_parameter_match(['type=azureblob', 'accountName=myAccountName', 'accountKey=myAccountKey',
                                   'endpoint=my-endpoint.com'])

    def test_create_azureblob_shared_access_signature_present(self):
        self.no_error_run(self.command + ['--create', '--scope', 'Default', '--name', 'myLink', '--type',
                                          'azureblob', '--shared-access-signature', 'mySharedAccessSignature',
                                          '--endpoint', 'my-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/Default/myLink', self.server.trace)
        self.rest_parameter_match(['type=azureblob', 'sharedAccessSignature=mySharedAccessSignature',
                                   'endpoint=my-endpoint.com'])

    def test_create_azureblob_managed_identity_id_present(self):
        self.no_error_run(self.command + ['--create', '--scope', 'Default', '--name', 'myLink', '--type',
                                          'azureblob', '--managed-identity-id', 'myManagedIdentityId', '--endpoint',
                                          'my-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/Default/myLink', self.server.trace)
        self.rest_parameter_match(['type=azureblob', 'managedIdentityId=myManagedIdentityId',
                                   'endpoint=my-endpoint.com'])

    def test_create_azuredatalake_managed_identity_id_present(self):
        self.no_error_run(self.command + ['--create', '--scope', 'Default', '--name', 'myLink', '--type',
                                          'azuredatalake', '--managed-identity-id', 'myManagedIdentityId', '--endpoint',
                                          'my-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/Default/myLink', self.server.trace)
        self.rest_parameter_match(['type=azuredatalake', 'managedIdentityId=myManagedIdentityId',
                                   'endpoint=my-endpoint.com'])

    def test_create_azureblob_client_id_present_client_secret_present(self):
        self.no_error_run(self.command + ['--create', '--scope', 'Default', '--name', 'myLink', '--type',
                                          'azureblob', '--client-id', 'myClientId', '--client-secret',
                                          'myClientSecret', '--tenant-id', 'myTenantId', '--endpoint',
                                          'my-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/Default/myLink', self.server.trace)
        self.rest_parameter_match(['type=azureblob', 'clientId=myClientId', 'clientSecret=myClientSecret',
                                   'tenantId=myTenantId', 'endpoint=my-endpoint.com'])

    def test_create_azureblob_client_id_present_client_certificate_present(self):
        self.no_error_run(self.command + ['--create', '--scope', 'Default', '--name', 'myLink', '--type',
                                          'azureblob', '--client-id', 'myClientId', '--client-certificate',
                                          'myClientCertificate', '--tenant-id', 'myTenantId', '--endpoint',
                                          'my-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/Default/myLink', self.server.trace)
        self.rest_parameter_match(['type=azureblob', 'clientId=myClientId', 'clientCertificate=myClientCertificate',
                                   'tenantId=myTenantId', 'endpoint=my-endpoint.com'])

    def test_create_azureblob_client_id_present_client_certificate_present_client_certificate_password(self):
        self.no_error_run(self.command + ['--create', '--scope', 'Default', '--name', 'myLink', '--type',
                                          'azureblob', '--client-id', 'myClientId', '--client-certificate',
                                          'myClientCertificate', '--client-certificate-password',
                                          'myClientCertificatePassword', '--tenant-id', 'myTenantId', '--endpoint',
                                          'my-endpoint.com'], self.server_args)
        self.assertIn('POST:/analytics/link/Default/myLink', self.server.trace)
        self.rest_parameter_match(['type=azureblob', 'clientId=myClientId', 'clientCertificate=myClientCertificate',
                                   'clientCertificatePassword=myClientCertificatePassword', 'tenantId=myTenantId',
                                   'endpoint=my-endpoint.com'])

    def test_create_gcs_anonymous(self):
        self.no_error_run(self.command + ['--create', '--scope', 'myScope', '--name', 'myLink', '--type', 'gcs'],
                          self.server_args)
        self.assertIn('POST:/analytics/link/myScope/myLink', self.server.trace)
        self.rest_parameter_match(['type=gcs'])

    def test_create_gcs_application_default_credentials(self):
        self.no_error_run(self.command + ['--create', '--scope', 'myScope', '--name', 'myLink', '--type', 'gcs',
                                          '--application-default-credentials'], self.server_args)
        self.assertIn('POST:/analytics/link/myScope/myLink', self.server.trace)
        self.rest_parameter_match(['type=gcs', 'applicationDefaultCredentials=True'])

    def test_create_gcs_json_credentials(self):
        self.no_error_run(self.command + ['--create', '--scope', 'myScope', '--name', 'myLink', '--type', 'gcs',
                                          '--json-credentials', 'myJsonCredentials'], self.server_args)
        self.assertIn('POST:/analytics/link/myScope/myLink', self.server.trace)
        self.rest_parameter_match(['type=gcs', 'jsonCredentials=myJsonCredentials'])

    def test_create_gcs_application_default_credentials_present_json_credentials_present(self):
        self.system_exit_run(self.command + ['--create', '--scope', 'myScope', '--name', 'myLink', '--type', 'gcs',
                                             '--application-default-credentials',
                                             '--json-credentials', 'myJsonCredentials'], self.server_args)
        self.assertIn('Parameter --json-credentials is not allowed if --application-default-credentials is provided',
                      self.str_output)

    def test_create_gcs_json_credentials_endpoint(self):
        self.no_error_run(self.command + ['--create', '--scope', 'myScope', '--name', 'myLink', '--type', 'gcs',
                                          '--json-credentials', 'myJsonCredentials', '--endpoint',
                                          'https://storage.googleapis.com'], self.server_args)
        self.assertIn('POST:/analytics/link/myScope/myLink', self.server.trace)
        self.rest_parameter_match(['type=gcs', 'jsonCredentials=myJsonCredentials',
                                   'endpoint=https%3A%2F%2Fstorage.googleapis.com'])

    def test_create_couchbase(self):
        self.no_error_run(self.command + ['--create', '--dataverse', 'Default', '--name', 'east', '--type',
                                          'couchbase', '--link-username', 'user', '--link-password', 'secret',
                                          '--encryption', 'none'], self.server_args)
        self.assertIn('POST:/analytics/link/Default/east', self.server.trace)
        self.rest_parameter_match(['type=couchbase', 'username=user',
                                   'password=secret', 'encryption=none'])

    def test_edit_no_type(self):
        self.no_error_run(self.command + ['--edit', '--dataverse', 'Default', '--name', 'east'], self.server_args)
        self.assertIn('PUT:/analytics/link/Default/east', self.server.trace)

    def test_edit_couchbase(self):
        self.no_error_run(self.command + ['--edit', '--dataverse', 'Default', '--name', 'east', '--type', 'couchbase',
                                          '--link-username', 'user', '--link-password', 'secret', '--encryption',
                                          'none'], self.server_args)
        self.assertIn('PUT:/analytics/link/Default/east', self.server.trace)
        self.rest_parameter_match(['type=couchbase', 'username=user', 'password=secret', 'encryption=none'])

    def testFailEditCouchbase(self):
        self.server_args['fail'] = True
        self.system_exit_run(self.command + ['--edit',
                                             '--dataverse',
                                             'Default',
                                             '--name',
                                             'east',
                                             '--type',
                                             'couchbase',
                                             '--link-username',
                                             'user',
                                             '--link-password',
                                             'secret',
                                             '--encryption',
                                             'none'],
                             self.server_args)
        self.assertIn('ERROR: CBAS0054: Operation cannot be performed while the link is connected', self.str_output)

    def testEditWithCerts(self):
        # create fake cert file
        cert_file = tempfile.NamedTemporaryFile(delete=False)
        cert_file_name = cert_file.name
        cert_file.write(b'this-is-the-cert-file')
        cert_file.close()

        # create fake user key file
        user_key_file = tempfile.NamedTemporaryFile(delete=False)
        user_key_file_name = user_key_file.name
        user_key_file.write(b'this-is-the-user-key-file')
        user_key_file.close()

        # create fake user key passphrase file
        user_key_passphrase_file = tempfile.NamedTemporaryFile(delete=False)
        user_key_passphrase_file_name = user_key_passphrase_file.name
        user_key_passphrase_file.write(b'{"type": "plain","password": "asdasd"}')
        user_key_passphrase_file.close()

        # create fake user certificate file
        user_cert_file = tempfile.NamedTemporaryFile(delete=False)
        user_cert_file_name = user_cert_file.name
        user_cert_file.write(b'this-is-the-user-cert-file')
        user_cert_file.close()

        self.no_error_run(self.command + ['--edit', '--dataverse', 'Default', '--name', 'east', '--type', 'couchbase',
                                          '--link-username', 'user', '--link-password', 'secret', '--encryption',
                                          'full', '--user-certificate', user_cert_file_name, '--certificate',
                                          cert_file_name, '--user-key', user_key_file_name, '--user-key-passphrase',
                                          user_key_passphrase_file_name], self.server_args)

        # clean up the test files
        os.remove(cert_file_name)
        os.remove(user_cert_file_name)
        os.remove(user_key_file_name)
        os.remove(user_key_passphrase_file_name)

        self.assertIn('PUT:/analytics/link/Default/east', self.server.trace)
        self.rest_parameter_match(
            ['type=couchbase', 'username=user', 'password=secret', 'encryption=full',
             'certificate=this-is-the-cert-file', 'clientKey=this-is-the-user-key-file',
             'clientCertificate=this-is-the-user-cert-file',
             'clientKeyPassphrase=%7B%22type%22%3A+%22plain%22%2C%22password%22%3A+%22asdasd%22%7D'])

    def testEditWithMultiCA(self):
        # create fake cert file
        cert_file = tempfile.NamedTemporaryFile(delete=False)
        cert_file_name = cert_file.name
        cert_file.write(b'this-is-the-cert-file')
        cert_file.close()

        # create second fake cert file
        cert2_file = tempfile.NamedTemporaryFile(delete=False)
        cert2_file_name = cert2_file.name
        cert2_file.write(b'this-is-the-other-cert-file')
        cert2_file.close()

        # create fake user key file
        user_key_file = tempfile.NamedTemporaryFile(delete=False)
        user_key_file_name = user_key_file.name
        user_key_file.write(b'this-is-the-user-key-file')
        user_key_file.close()

        # create fake user key passphrase file
        user_key_passphrase_file = tempfile.NamedTemporaryFile(delete=False)
        user_key_passphrase_file_name = user_key_passphrase_file.name
        user_key_passphrase_file.write(b'{"type": "plain","password": "asdasd"}')
        user_key_passphrase_file.close()

        # create fake user certificate file
        user_cert_file = tempfile.NamedTemporaryFile(delete=False)
        user_cert_file_name = user_cert_file.name
        user_cert_file.write(b'this-is-the-user-cert-file')
        user_cert_file.close()

        self.no_error_run(self.command + ['--edit', '--dataverse', 'Default', '--name', 'east', '--type', 'couchbase',
                                          '--link-username', 'user', '--link-password', 'secret', '--encryption',
                                          'full', '--user-certificate', user_cert_file_name, '--certificate',
                                          cert_file_name, '--certificate', cert2_file_name, '--user-key',
                                          user_key_file_name, '--user-key-passphrase', user_key_passphrase_file_name],
                          self.server_args)

        # clean up the test files
        os.remove(cert_file_name)
        os.remove(cert2_file_name)
        os.remove(user_cert_file_name)
        os.remove(user_key_file_name)
        os.remove(user_key_passphrase_file_name)

        self.assertIn('PUT:/analytics/link/Default/east', self.server.trace)
        self.rest_parameter_match(
            ['type=couchbase', 'username=user', 'password=secret', 'encryption=full',
             'certificate=this-is-the-cert-file%0Athis-is-the-other-cert-file', 'clientKey=this-is-the-user-key-file',
             'clientCertificate=this-is-the-user-cert-file',
             'clientKeyPassphrase=%7B%22type%22%3A+%22plain%22%2C%22password%22%3A+%22asdasd%22%7D'])

    def test_set_with_certs_that_dont_exist(self):
        # create fake cert file and deleted immediately guaranteeing it does not exists
        cert_file = tempfile.NamedTemporaryFile(delete=True)
        cert_file_name = cert_file.name
        cert_file.close()

        self.system_exit_run(self.command + ['--set', '--dataverse', 'Default', '--name', 'east', '--type', 'couchbase',
                                             '--link-username', 'user', '--link-password', 'secret', '--encryption',
                                             'full', '--user-certificate', cert_file_name, '--certificate',
                                             cert_file_name, '--user-key', cert_file_name], self.server_args)

    def test_delete_no_params(self):
        self.system_exit_run(self.command + ['--delete'], self.server_args)
        self.assertIn('scope is required', self.str_output)

    def test_delete_no_name(self):
        self.system_exit_run(self.command + ['--delete', '--dataverse', 'Default'], self.server_args)
        self.assertIn('name is required', self.str_output)

    def test_delete(self):
        self.no_error_run(self.command + ['--delete', '--dataverse', 'Default', '--name', 'me'], self.server_args)
        self.assertIn('DELETE:/analytics/link/Default/me', self.server.trace)


class VerifyAzureOptions(unittest.TestCase):
    def test_verify_azure_options(self):
        tests = {
            "MissingEndpoint": {
                "opts": [],
                "error": "Required parameter --endpoint not supplied",
            },
            "AccountNameRequiresAccountKey": {
                "opts": ["endpoint", "account_name"],
                "error": "Parameter --account-key is required if --account-name is provided",
            },
            "AccountKeyRequiresAccountName": {
                "opts": ["endpoint", "account_key"],
                "error": "Parameter --account-name is required if --account-key is provided",
            },
            "ANACNoSAS": {
                "opts": ["endpoint", "account_name", "account_key", "shared_access_signature"],
                "error": "Parameter --shared--access-signature is not allowed if --account-key is provided",
            },
            "ANACNoMI": {
                "opts": ["endpoint", "account_name", "account_key", "managed_identity_id"],
                "error": "Parameter --managed-identity-id is not allowed if --account-key is provided",
            },
            "ANACNoClientID": {
                "opts": ["endpoint", "account_name", "account_key", "client_id"],
                "error": "Parameter --client-id is not allowed if --account-key is provided",
            },
            "ANACNoCSecret": {
                "opts": ["endpoint", "account_name", "account_key", "client_secret"],
                "error": "Parameter --client-secret is not allowed if --account-key is provided",
            },
            "ANACNoClientCert": {
                "opts": ["endpoint", "account_name", "account_key", "client_certificate"],
                "error": "Parameter --client-certificate is not allowed if --account-key is provided",
            },
            "ANACNoClientCertPassword": {
                "opts": ["endpoint", "account_name", "account_key", "client_certificate_password"],
                "error": "Parameter --client-certificate-password is not allowed if --account-key is provided",
            },
            "ANACNoTenantID": {
                "opts": ["endpoint", "account_name", "account_key", "tenant_id"],
                "error": "Parameter --tenant-id is not allowed if --account-key is provided",
            },
            "SASNoMI": {
                "opts": ["endpoint", "shared_access_signature", "managed_identity_id"],
                "error": "Parameter --managed-identity-id is not allowed if --shared-access-signature is provided",
            },
            "SASNoClientID": {
                "opts": ["endpoint", "shared_access_signature", "client_id"],
                "error": "Parameter --client-id is not allowed if --shared-access-signature is provided",
            },
            "SASNoClientSecret": {
                "opts": ["endpoint", "shared_access_signature", "client_secret"],
                "error": "Parameter --client-secret is not allowed if --shared-access-signature is provided",
            },
            "SASNoClientCert": {
                "opts": ["endpoint", "shared_access_signature", "client_certificate"],
                "error": "Parameter --client-certificate is not allowed if --shared-access-signature is provided",
            },
            "SASNoClientCertPassword": {
                "opts": ["endpoint", "shared_access_signature", "client_certificate_password"],
                "error": "Parameter --client-certificate-password is not allowed if --shared-access-signature is provided",
            },
            "SASNoTenantID": {
                "opts": ["endpoint", "shared_access_signature", "tenant_id"],
                "error": "Parameter --tenant-id is not allowed if --shared-access-signature is provided",
            },
            "MINoClientID": {
                "opts": ["endpoint", "managed_identity_id", "client_id"],
                "error": "Parameter --client-id is not allowed if --managed-identity-id is provided",
            },
            "MINoClientSecret": {
                "opts": ["endpoint", "managed_identity_id", "client_secret"],
                "error": "Parameter --client-secret is not allowed if --managed-identity-id is provided",
            },
            "MINoClientCert": {
                "opts": ["endpoint", "managed_identity_id", "client_certificate"],
                "error": "Parameter --client-certificate is not allowed if --managed-identity-id is provided",
            },
            "MINoClientCertPassword": {
                "opts": ["endpoint", "managed_identity_id", "client_certificate_password"],
                "error": "Parameter --client-certificate-password is not allowed if --managed-identity-id is provided",
            },
            "MINoTenantID": {
                "opts": ["endpoint", "managed_identity_id", "tenant_id"],
                "error": "Parameter --tenant-id is not allowed if --managed-identity-id is provided",
            },
            "ClientSecretRequiresClientID": {
                "opts": ["endpoint", "client_secret"],
                "error": "Parameter --client-id is required if --client-secret is provided",
            },
            "ClientCertRequiresClientID": {
                "opts": ["endpoint", "client_certificate"],
                "error": "Parameter --client-id is required if --client-certificate is provided",
            },
            "ClientCertPasswordRequiresClientID": {
                "opts": ["endpoint", "client_certificate_password"],
                "error": "Parameter --client-id is required if --client-certificate-password is provided",
            },
            "TenantIDRequiresClientID": {
                "opts": ["endpoint", "tenant_id"],
                "error": "Parameter --client-id is required if --tenant-id is provided",
            },
            "ClientIDRequiresTenantID": {
                "opts": ["endpoint", "client_id"],
                "error": "Required parameter --tenant-id not supplied",
            },
            "ClientIDRequiresClientSecret": {
                "opts": ["endpoint", "client_id", "tenant_id", "client_certificate"],
            },
            "ClientIDRequiresClientCert": {
                "opts": ["endpoint", "client_id", "tenant_id", "client_secret"],
            },
            "ClientSecretAndClientCertMutuallyExclusive": {
                "opts": ["endpoint", "tenant_id", "client_id", "client_secret", "client_certificate"],
                "error": "The parameters --client-secret and --client-certificate cannot be provided at the same time",
            },
            "ClientSecretNoClientCertPassword": {
                "opts": ["endpoint", "tenant_id", "client_id", "client_secret", "client_certificate_password"],
                "error": "Parameter --client-certificate-password is not allowed if --client-secret is provided",
            },
        }

        def create_namespace(dic):
            defaults = {
                "account_key": None,
                "account_name": None,
                "client_certificate": None,
                "client_certificate_password": None,
                "client_id": None,
                "client_secret": None,
                "endpoint": None,
                "managed_identity_id": None,
                "shared_access_signature": None,
                "tenant_id": None,
            }

            return Namespace(**{**defaults, **dic})

        for name, test in tests.items():
            with self.subTest(name=name):
                # We only care about the presence/absence of a given key, therefore, we can use the same value
                namespace = create_namespace(dict.fromkeys(test["opts"], "value"))

                # pylint: disable=protected-access
                errors = AnalyticsLinkSetup._verify_azure_options(namespace)

                if "error" in test:
                    self.assertEqual([test["error"]], errors)
                else:
                    self.assertIsNone(errors)


class TestBackupServiceSettings(CommandTest):
    """Test the backup-service settings subcommand"""

    def setUp(self):
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'backupAPI': port,
                                }
                            }]}}
        self.command = ['couchbase-cli', 'backup-service'] + cluster_connect_args + ['settings']
        super(TestBackupServiceSettings, self).setUp()

    def test_no_action_flag_given(self):
        """Test that the command fails and returns an error if no action flag is given"""
        self.system_exit_run(self.command, self.server_args)
        self.assertIn('ERROR: Must use one and only one of [--get, --set]', self.str_output)

    def test_get(self):
        """Test that the --get action flag will retrieve the backups service configuration and print it out"""
        self.server_args['/api/v1/config'] = {'history_rotation_size': 10, 'history_rotation_period': 50}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('History rotation size: 10 MiB', self.str_output)
        self.assertIn('History rotation period: 50 days', self.str_output)

    def test_set_no_options(self):
        """Test that if no options are given to --set it will fail and print an error asking for other options"""
        self.system_exit_run(self.command + ['--set'], self.server_args)
        self.assertIn('At least one of', self.str_output)

    def test_set_all(self):
        """Test that set with all options will send the correct request to the correct endpoint"""
        self.no_error_run(self.command + ['--set', '--history-rotation-size', '10', '--history-rotation-period', '30'],
                          self.server_args)
        self.assertIn('PATCH:/api/v1/config', self.server.trace)
        self.rest_parameter_match([json.dumps({'history_rotation_size': 10, 'history_rotation_period': 30},
                                              sort_keys=True)])


class TestBackupServiceRepository(CommandTest):
    """Test the backup-service repository subcommand and all its actions
    """

    def setUp(self):
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            '/api/v1/cluster/self/repository/active': [],
                            '/api/v1/cluster/self/repository/archived': [],
                            '/api/v1/cluster/self/repository/imported': [],
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'backupAPI': port,
                                },
                            }]}}
        self.command = ['couchbase-cli', 'backup-service'] + cluster_connect_args + ['repository']
        super(TestBackupServiceRepository, self).setUp()

    def test_list_invalid_state(self):
        """Test that if a state not in [active, imported, archived] is provided the command exits with a non zero status
        code
        """
        self.system_exit_run(self.command + ['--list', '--state', 'state'], self.server_args)

    def test_list_no_repositories(self):
        """Test that if the are no repositories the command exits with 0 status and prints out that the are no repositories"""
        self.no_error_run(self.command + ['--list'], self.server_args)
        self.assertIn('GET:/api/v1/cluster/self/repository/active', self.server.trace)
        self.assertIn('GET:/api/v1/cluster/self/repository/archived', self.server.trace)
        self.assertIn('GET:/api/v1/cluster/self/repository/imported', self.server.trace)
        self.assertIn('No repositories found', self.str_output)

    def test_list_various_repositories(self):
        """Test that repository of all state are retireved and outputed"""
        self.server_args['/api/v1/cluster/self/repository/active'] = [{
            'id': 'active-repository',
            'state': 'active',
            'plan_name': 'plan1',
            'health': {'healthy': False},
            'repo': 'repo1',
        }]
        self.server_args['/api/v1/cluster/self/repository/imported'] = [{
            'id': 'imported-repository',
            'state': 'imported',
            'repo': 'repo2',
        }]
        self.server_args['/api/v1/cluster/self/repository/archived'] = [{
            'id': 'archived-repository',
            'state': 'archived',
            'plan_name': 'plan2',
            'repo': 'repo2',
        }]

        self.no_error_run(self.command + ['--list'], self.server_args)
        self.assertIn('GET:/api/v1/cluster/self/repository/active', self.server.trace)
        self.assertIn('GET:/api/v1/cluster/self/repository/archived', self.server.trace)
        self.assertIn('GET:/api/v1/cluster/self/repository/imported', self.server.trace)
        self.assertGreaterEqual(len(self.str_output.split('\n')), 4, 'Expected at least four lines')
        for repository_id in ['active-repository', 'imported-repository', 'archived-repository']:
            self.assertIn(repository_id, self.str_output)

    def test_list_various_repositories_state_filter(self):
        """Test that repository of the specific state are retireved and outputed"""
        self.server_args['/api/v1/cluster/self/repository/active'] = [{
            'id': 'active-repository',
            'state': 'active',
            'plan_name': 'plan1',
            'health': {'health': False},
            'repo': 'repo1',
        }]
        self.server_args['/api/v1/cluster/self/repository/imported'] = [{
            'id': 'imported-repository',
            'state': 'imported',
            'repo': 'repo2',
        }]
        self.server_args['/api/v1/cluster/self/repository/archived'] = [{
            'id': 'archived-repository',
            'state': 'archived',
            'plan_name': 'plan2',
            'repo': 'repo2',
        }]

        self.no_error_run(self.command + ['--list', '--state', 'imported'], self.server_args)
        self.assertNotIn('GET:/api/v1/cluster/self/repository/active', self.server.trace)
        self.assertNotIn('GET:/api/v1/cluster/self/repository/archived', self.server.trace)
        self.assertIn('GET:/api/v1/cluster/self/repository/imported', self.server.trace)
        self.assertIn('imported-repository', self.str_output)
        # as a bonus also check that N/A is printed for the plans
        self.assertIn('N/A', self.str_output)

    def test_get_repository_no_id(self):
        """Test that the --get action flag fails if the --id option is not given"""
        self.server_args['/api/v1/cluster/self/repository/active/repository'] = {}
        self.system_exit_run(self.command + ['--get', '--state', 'active'], self.server_args)
        self.assertIn('--id is required', self.str_output)

    def test_get_repository_no_state(self):
        """Test that the --get action flag fails if the --state flag is not given"""
        self.server_args['/api/v1/cluster/self/repository/active/repository'] = {}
        self.system_exit_run(self.command + ['--get', '--id', 'repository'], self.server_args)
        self.assertIn('--state is required', self.str_output)

    def test_get_repository(self):
        """Test get the repository when all valid options are valid"""
        self.server_args['/api/v1/cluster/self/repository/active/repository'] = {
            'id': 'repository',
            'state': 'active',
            'health': {'healthy': False},
            'archive': 'some/archive',
            'repo': 'the-repo',
            'bucket': {'name': 'beer-sample'},
            'plan_name': 'daily-plan',
            'creation_time': '10-01-2020T01:02:00.00001Z',
            'scheduled': {'task-1': {'name': 'task-1', 'task_type': 'BACKUP', 'next_run': '07-10-2020T00:00:00Z'}},
        }

        self.no_error_run(self.command + ['--get', '--id', 'repository', '--state', 'active'], self.server_args)
        self.assertIn('GET:/api/v1/cluster/self/repository/active/repository', self.server.trace)

        # check that the expected lines are there
        self.assertIn('ID: repository', self.str_output)
        self.assertIn('State: active', self.str_output)
        self.assertIn('Healthy: False', self.str_output)
        self.assertIn('Archive: some/archive', self.str_output)
        self.assertIn('Repository: the-repo', self.str_output)
        self.assertIn('Bucket: beer-sample', self.str_output)
        self.assertIn('plan: daily-plan', self.str_output)
        self.assertIn('Creation time: 10-01-2020T01:02:00.00001Z', self.str_output)
        self.assertIn('Scheduled tasks:', self.str_output)
        self.assertIn('Name   | Task type | Next run', self.str_output)
        self.assertIn('task-1 | Backup    | 07-10-2020T00:00:00Z', self.str_output)

    def test_archive_repository_no_id(self):
        """Test that the archive id fails if no id specified"""
        self.server_args['/api/v1/cluster/self/repository/active/repository'] = {
            'id': 'repository',
            'state': 'active',
            'archive': 'some/archive',
            'repo': 'the-repo',
            'plan_name': 'daily-plan',
            'creation_time': '10-01-2020T01:02:00.00001Z',
        }

        self.system_exit_run(self.command + ['--archive', '--new-id', 'new'], self.server_args)
        self.assertIn('--id is required', self.str_output)

    def test_archive_repository_no_new_id(self):
        """Test that the archive id fails if no new id specified"""
        self.server_args['/api/v1/cluster/self/repository/active/repository'] = {
            'id': 'repository',
            'state': 'active',
            'archive': 'some/archive',
            'repo': 'the-repo',
            'plan_name': 'daily-plan',
            'creation_time': '10-01-2020T01:02:00.00001Z',
        }

        self.system_exit_run(self.command + ['--archive', '--id', 'old'], self.server_args)
        self.assertIn('--new-id is required', self.str_output)

    def test_archive_repository(self):
        """Test that the parameters are passed to the POST request"""
        self.server_args['/api/v1/cluster/self/repository/active/repository'] = {
            'id': 'repository',
            'state': 'active',
            'archive': 'some/archive',
            'repo': 'the-repo',
            'plan_name': 'daily-plan',
            'creation_time': '10-01-2020T01:02:00.00001Z',
        }

        self.no_error_run(self.command + ['--archive', '--id', 'old', '--new-id', 'new'], self.server_args)
        self.assertIn('POST:/api/v1/cluster/self/repository/active/old/archive', self.server.trace)
        self.rest_parameter_match([json.dumps({'id': 'new'})])

    def test_add_repository_id_missing(self):
        """Test that the add process fails if any of the mandatory flags are missing"""
        self.system_exit_run(self.command + ['--add', '--plan', 'a', '--backup-archive', 'b'], self.server_args)
        self.assertIn('--id is required', self.str_output)

    def test_add_repository_plan_missing(self):
        """Test that the add process fails if any of the mandatory flags are missing"""
        self.system_exit_run(self.command + ['--add', '--id', 'a', '--backup-archive', 'b'], self.server_args)
        self.assertIn('--plan is required', self.str_output)

    def test_add_repository_backup_archive_missing(self):
        """Test that the add process fails if any of the mandatory flags are missing"""
        self.system_exit_run(self.command + ['--add', '--plan', 'a', '--id', 'b'], self.server_args)
        self.assertIn('--backup-archive is required', self.str_output)

    def test_simple_add(self):
        """Test that add works with the minimum amount of the flags given"""
        self.no_error_run(self.command + ['--add', '--id', 'a', '--plan', 'plan', '--backup-archive', 'archive'],
                          self.server_args)
        self.assertIn('POST:/api/v1/cluster/self/repository/active/a', self.server.trace)
        self.rest_parameter_match([json.dumps({'plan': 'plan', 'archive': 'archive'}, sort_keys=True)])

    def test_add_with_bucket_name(self):
        """Test that add works when given a bucket, it should ssend the bucket name to the service"""
        self.no_error_run(self.command + ['--add', '--id', 'a', '--plan', 'plan', '--backup-archive', 'archive',
                                          '--bucket-name', 'bucket'], self.server_args)
        self.assertIn('POST:/api/v1/cluster/self/repository/active/a', self.server.trace)
        self.rest_parameter_match([json.dumps({'plan': 'plan', 'archive': 'archive',
                                               'bucket_name': 'bucket'}, sort_keys=True)])

    def test_add_cloud_backup_repository_with_no_cloud_credentials(self):
        """"Test that if an S3 archive is given but no cloud credentials that the command throws a meaningfull error"""
        self.system_exit_run(self.command + ['--add', '--id', 'a', '--plan', 'plan', '--backup-archive',
                                             's3://archive', '--cloud-credentials-region', 'a', '--cloud-staging-dir',
                                             'b'], self.server_args)
        self.assertIn('must provide either --cloud-credentials-name or --cloud-credentials-key ', self.str_output)

    def test_add_cloud_backup_repository_with_no_staging_dir(self):
        """Test that if an S3 archive is given without a staging directory it fails"""
        self.system_exit_run(self.command + ['--add', '--id', 'a', '--plan', 'p', '--backup-archive', 's3://b',
                                             '--cloud-credentials-name', 'c'],
                             self.server_args)
        self.assertIn('--cloud-staging-dir is required', self.str_output)

    def test_add_cloud_backup_repository_all_options(self):
        """Test that creating a cloud repository with the maximum amount of options is allowed"""
        self.no_error_run(self.command + ['--add', '--id', 'a', '--plan', 'p', '--backup-archive', 's3://b/a',
                                          '--cloud-credentials-id', 'id', '--cloud-credentials-key', 'key',
                                          '--cloud-credentials-refresh-token', 'token', '--cloud-credentials-region',
                                          'region', '--cloud-staging-dir', 'dir', '--cloud-endpoint', 'endpoint',
                                          '--s3-force-path-style', '--bucket-name',
                                          'bucket'], self.server_args)
        self.assertIn('POST:/api/v1/cluster/self/repository/active/a', self.server.trace)
        self.rest_parameter_match([json.dumps({'plan': 'p', 'archive': 's3://b/a', 'cloud_credentials_id': 'id',
                                               'cloud_credentials_key': 'key', 'cloud_credentials_refresh_token':
                                               'token', 'cloud_region': 'region', 'cloud_staging_dir': 'dir',
                                               'cloud_endpoint': 'endpoint', 'cloud_force_path_style': True,
                                               'bucket_name': 'bucket'},
                                              sort_keys=True)])

    def test_remove_fails_with_no_id(self):
        """Test that the remove action fails if no id is given and that a valid error message is returned"""
        self.system_exit_run(self.command + ['--remove', '--state', 'imported'], self.server_args)
        self.assertIn('--id is required', self.str_output)

    def test_remove_fails_with_no_state(self):
        """Test that the remove action fails if no state is given and that a valid error message is returned"""
        self.system_exit_run(self.command + ['--remove', '--id', 'a'], self.server_args)
        self.assertIn('--state is required', self.str_output)

    def test_remove_active_repository_fails(self):
        """Test that the remove action fails if no state is given and that a valid error message is returned"""
        self.system_exit_run(self.command + ['--remove', '--id', 'a', '--state', 'active'], self.server_args)
        self.assertIn('can only delete archived or imported repositories', self.str_output)

    def test_remove_delete_data_imported_repository_failes(self):
        """Test that the CLI does not allow the option --remove-data to be given when deleting an imported repository"""
        self.system_exit_run(self.command + ['--remove', '--id', 'a', '--state', 'imported', '--remove-data'],
                             self.server_args)
        self.assertIn('cannot delete the repository for an imported repository', self.str_output)

    def test_remove_valid_request(self):
        """Test that if all valid options are given that the correct request is made"""
        self.no_error_run(self.command + ['--remove', '--id', 'r', '--state', 'archived', '--remove-data'],
                          self.server_args)
        self.assertIn('DELETE:/api/v1/cluster/self/repository/archived/r', self.server.trace)
        self.assertIn('remove_repository=True', self.server.queries)


class TestBackupServicePlan(CommandTest):
    """Test the backup-service plan subcommand and all its actions
    """

    def setUp(self):
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'backupAPI': port,
                                },
                            }]}}
        self.command = ['couchbase-cli', 'backup-service'] + cluster_connect_args + ['plan']
        super(TestBackupServicePlan, self).setUp()

    def test_plan_list_empty(self):
        """Test that if the are no plans a sensible message is returned"""
        self.server_args['api/v1/plans'] = []
        self.no_error_run(self.command + ['--list'], self.server_args)
        self.assertIn('GET:/api/v1/plan', self.server.trace)
        self.assertIn('No plans', self.str_output)

    def test_plan_list(self):
        """Test that the plan list calls the correct endpoint and prints the expected information"""
        self.server_args['/api/v1/plan'] = [
            {
                'name': 'p1',
                'description': 'somethings',
                'services': [],
                'tasks': [{}, {}],
                'default': True,
            },
            {
                'name': 'p2',
                'services': ['data', 'cbas'],
                'tasks': [{}],
            }
        ]
        self.no_error_run(self.command + ['--list'], self.server_args)
        self.assertIn('GET:/api/v1/plan', self.server.trace)
        self.assertIn('p1', self.str_output)
        self.assertIn('all', self.str_output)
        self.assertIn('2', self.str_output)
        self.assertIn('True', self.str_output)
        self.assertIn('p2', self.str_output)
        self.assertIn('Data, Analytics', self.str_output)
        self.assertIn('1', self.str_output)
        self.assertIn('False', self.str_output)

    def test_get_plan_no_name(self):
        """Test that the get plan operation fails if no name is given"""
        self.system_exit_run(self.command + ['--get'], self.server_args)
        self.assertIn('--name is required', self.str_output)

    def test_get_plan(self):
        """Test that get operatiopn hits the right endpoint and prints the correct information"""
        self.server_args['/api/v1/plan/p1'] = {
            'name': 'p1',
            'description': 'Some description',
            'services': ['data', 'cbas'],
            'tasks': [{
                'name': 't1',
                'task_type': 'BACKUP',
                'full_backup': True,
                'schedule': {
                    'job_type': 'BACKUP',
                    'frequency': 3,
                    'period': 'HOURS',
                    'time': '00:00',
                }
            }],
        }

        self.no_error_run(self.command + ['--get', '--name', 'p1'], self.server_args)
        self.assertIn('GET:/api/v1/plan/p1', self.server.trace)
        self.assertIn('Name: p1', self.str_output)
        self.assertIn('Description: Some description', self.str_output)
        self.assertIn('Services: Data, Analytics', self.str_output)
        self.assertIn('Default: False', self.str_output)
        self.assertIn('Tasks:', self.str_output)
        self.assertIn('t1', self.str_output)
        self.assertIn('backup every 3 hours at 00:00', self.str_output)

    def test_remove_plan_no_name(self):
        """Test that the remove action does not work if no plan name is given"""
        self.system_exit_run(self.command + ['--remove'], self.server_args)
        self.assertIn('--name is required', self.str_output)

    def test_remove_plan(self):
        """Test that given a name the CLI will hit the correct endpoint"""
        self.no_error_run(self.command + ['--remove', '--name', 'p1'], self.server_args)
        self.assertIn('DELETE:/api/v1/plan/p1', self.server.trace)
        self.assertIn('Plan removed', self.str_output)

    def test_add_plan_no_name(self):
        """Test that when no name is given to the add plan command it fails and gives a sensible error"""
        self.system_exit_run(self.command + ['--add'], self.server_args)
        self.assertIn('--name is required', self.str_output)

    def test_add_empty_plan(self):
        """Test that a plan with just a name is valid"""
        self.no_error_run(self.command + ['--add', '--name', 'prof'], self.server_args)
        self.assertIn('POST:/api/v1/plan/prof', self.server.trace)
        self.rest_parameter_match(['{}'])

    def test_add_plan_all_options(self):
        """Test that all options are valid when adding a plan"""
        task = {'name': 't1', 'task_type': 'BACKUP', 'schedule': {'frequency': 1, 'period': 'DAYS'}}
        self.no_error_run(self.command + ['--add', '--name', 'prof', '--description', 'some description',
                                          '--services', 'data,eventing', '--task', json.dumps(task)], self.server_args)
        self.assertIn('POST:/api/v1/plan/prof', self.server.trace)
        self.rest_parameter_match([json.dumps({'description': 'some description', 'services': ['data', 'eventing'],
                                               'tasks': [task]}, sort_keys=True)])


class TestBucketServiceNodeThreadsMap(CommandTest):
    """Test the bucket-service node-threads-map subcommand and all its actions
    """

    def setUp(self):
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True,
                            '/pools/default/nodeServices': {'nodesExt': [{
                                'hostname': host,
                                'services': {
                                    'backupAPI': port,
                                },
                            }]}}
        self.command = ['couchbase-cli', 'backup-service'] + cluster_connect_args + ['node-threads']
        super(TestBucketServiceNodeThreadsMap, self).setUp()

    def test_node_threads_empty(self):
        """Test that if the are no node threads map a sensible message is returned"""
        self.server_args['api/v1/nodesThreadsMap'] = {}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('GET:/api/v1/nodesThreadsMap', self.server.trace)
        self.assertIn('No node threads map found', self.str_output)

    def test_node_threads_get(self):
        """Test that the node threads get calls the correct endpoint and prints the expected information"""
        self.server_args['/api/v1/nodesThreadsMap'] = {
            '0e88b40a2e476cf8a95565c480465f71': 4,
            '11d925ec77c0f577bc38c41d315c4b07': 3
        }
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('GET:/api/v1/nodesThreadsMap', self.server.trace)
        self.assertIn('4', self.str_output)
        self.assertIn('0e88b40a2e476cf8a95565c480465f71', self.str_output)
        self.assertIn('3', self.str_output)
        self.assertIn('11d925ec77c0f577bc38c41d315c4b07', self.str_output)

    def test_node_threads_post(self):
        """Test that the node threads post calls the correct endpoint and prints the expected information"""
        self.no_error_run(
            self.command + ['--set', '--node', '0e88b40a2e476cf8a95565c480465f71', '--threads', '4'],
            self.server_args)
        self.assertIn('POST:/api/v1/nodesThreadsMap', self.server.trace)
        self.rest_parameter_match(
            [json.dumps({"nodes_threads_map": {'0e88b40a2e476cf8a95565c480465f71': 4}}, sort_keys=True)])

    def test_node_threads_post_invalid_threads(self):
        """Test that the node threads post fails if the threads are not an integer"""
        self.system_exit_run(
            self.command + ['--set', '--node', '0e88b40a2e476cf8a95565c480465f71', '--threads', 'four'],
            self.server_args)
        self.assertIn('ERROR: argument --threads: invalid int value', self.str_error)

    def test_node_threads_post_invalid_flags(self):
        """Test that the node threads post fails if the threads is not set"""
        self.system_exit_run(self.command + ['--set', '--node', '0e88b40a2e476cf8a95565c480465f71'], self.server_args)
        self.assertIn('--node and --threads are required', self.str_output)

    def test_node_threads_post_invalid_number_of_flags(self):
        """Test that the node threads post fails if there is a different number of --node and --threads flags"""
        self.system_exit_run(self.command + ['--set',
                                             '--node',
                                             '0e88b40a2e476cf8a95565c480465f71',
                                             '--threads',
                                             '4',
                                             '--threads',
                                             '5'],
                             self.server_args)
        self.assertIn('--node and --threads must have the same number of arguments', self.str_output)

    def test_node_threads_patch_invalid_flags(self):
        """Test that the node threads patch fails if the threads is not set"""
        self.system_exit_run(self.command + ['--set', '--node', '0e88b40a2e476cf8a95565c480465f71'], self.server_args)
        self.assertIn('--node and --threads are required', self.str_output)

    def test_node_threads_patch_invalid_number_of_flags(self):
        """Test that the node threads patch fails if there is a different number of --node and --threads flags"""
        self.system_exit_run(self.command + ['--add',
                                             '--node',
                                             '0e88b40a2e476cf8a95565c480465f71',
                                             '--threads',
                                             '4',
                                             '--threads',
                                             '5'],
                             self.server_args)
        self.assertIn('--node and --threads must have the same number of arguments', self.str_output)

    def test_node_threads_patch(self):
        """Test that the node threads patch calls the correct endpoint and prints the expected information"""
        self.no_error_run(
            self.command + ['--add', '--node', '0e88b40a2e476cf8a95565c480465f71', '--threads', '4'],
            self.server_args)
        self.assertIn('PATCH:/api/v1/nodesThreadsMap', self.server.trace)
        self.rest_parameter_match(
            [json.dumps({"nodes_threads_map": {'0e88b40a2e476cf8a95565c480465f71': 4}}, sort_keys=True)])


class TestSettingQuery(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-query'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super(TestSettingQuery, self).setUp()

    def test_access_list(self):
        self.no_error_run(self.command + ['--set', '--curl-access', 'restricted', '--allowed-urls', 'url1,url2',
                                          '--disallowed-urls', 'url3,url4'], self.server_args)
        self.assertIn('POST:/settings/querySettings/curlAllowlist', self.server.trace)
        self.rest_parameter_match([json.dumps({'all_access': False, 'allowed_urls': ['url1', 'url2'],
                                               'disallowed_urls': ['url3', 'url4']}, sort_keys=True)])

    def test_access_list_and_temp_query_dir(self):
        self.no_error_run(self.command + ['--set', '--curl-access', 'unrestricted', '--temp-dir', 'location',
                                          '--temp-dir-size', '9000'], self.server_args)
        self.assertIn('POST:/settings/querySettings/curlAllowlist', self.server.trace)
        self.assertIn('POST:/settings/querySettings', self.server.trace)
        self.rest_parameter_match([json.dumps({'all_access': True}, sort_keys=True),
                                   'queryTmpSpaceDir=location', 'queryTmpSpaceSize=9000'])

    def test_access_list_unrestricted(self):
        """Test that if disallowed-uls and allowed-urls parameters are given with curl-access 'unrestricted' it will
         fail"""
        self.system_exit_run(self.command + ['--set', '--curl-access', 'unrestricted', '--allowed-urls', 'url1',
                                             '--disallowed-urls', 'url3,url4'], self.server_args)
        self.assertIn('Can only provide --allowed-urls or --disallowed-urls with --curl-access restricted',
                      self.str_output)

    def test_access_list_no_access_parameter(self):
        """Test that if disallowed-uls and allowed-urls parameters are given without the curl-access parameter it will
         fail"""
        self.system_exit_run(self.command + ['--set', '--allowed-urls', 'url1', '--disallowed-urls', 'url3,url4'],
                             self.server_args)
        self.assertIn('Can only provide --allowed-urls or --disallowed-urls with --curl-access restricted',
                      self.str_output)

    def test_7_0_query_settings(self):
        """Test the settings added in CB 7.0.0 for the query service"""
        self.no_error_run(self.command + ['--set', '--cost-based-optimizer', '0', '--transaction-timeout', '10ms',
                                          '--memory-quota', '100'], self.server_args)

        self.assertIn('POST:/settings/querySettings', self.server.trace)
        self.rest_parameter_match(['queryUseCBO=false', 'queryTxTimeout=10ms', 'queryMemoryQuota=100'])

    def test_7_6_query_settings(self):
        """Test the settings added in CB 7.6 for the query service"""
        self.no_error_run(self.command + ['--set', '--node-quota', '256', '--node-quota-val-percent', '42',
                                          '--use-replica', 'off'], self.server_args)

        self.assertIn('POST:/settings/querySettings', self.server.trace)
        self.rest_parameter_match(['queryNodeQuota=256', 'queryNodeQuotaValPercent=42', 'queryUseReplica=off'])


class TestSettingAnalytics(CommandTest):
    def setUp(self):
        self.command = ['couchbase-cli', 'setting-analytics'] + cluster_connect_args
        self.server_args = {'enterprise': True, 'init': True, 'is_admin': True}
        super().setUp()

    def test_get_and_set(self):
        self.system_exit_run(self.command + ['--get', '--set'], self.server_args)
        self.assertIn('ERROR: Please provide --set or --get, both cannot be provided at the same time', self.str_output)

    def test_get(self):
        self.server_args['/settings/analytics'] = {'numReplicas': 3}
        self.no_error_run(self.command + ['--get'], self.server_args)
        self.assertIn('GET:/settings/analytics', self.server.trace)
        self.assertIn('numReplicas: 3', self.str_output)

    def test_setting_replicas(self):
        self.no_error_run(self.command + ['--set', '--replicas', '3'], self.server_args)
        self.assertIn('POST:/settings/analytics', self.server.trace)
        self.rest_parameter_match(['numReplicas=3'])


if __name__ == '__main__':
    unittest.main()
