"""A Couchbase  CLI subcommand"""

import json
import os
import random
import string
import subprocess
import sys
import time

from optparse import HelpFormatter, OptionContainer, OptionGroup, OptionParser
from cluster_manager import ClusterManager

try:
    from gettext import gettext
except ImportError:
    def gettext(message):
        """Stub gettext method"""
        return message

COUCHBASE_DEFAULT_PORT = 8091

BUCKET_PRIORITY_HIGH_INT = 8
BUCKET_PRIORITY_HIGH_STR = "high"
BUCKET_PRIORITY_LOW_INT = 3
BUCKET_PRIORITY_LOW_STR = "low"

BUCKET_TYPE_COUCHBASE = "membase"
BUCKET_TYPE_MEMCACHED = "memcached"

def parse_command():
    """Parses a couchbase-cli command and routes the request to the appropriate handler

    Returns true if the command was handled"""
    if len(sys.argv) == 1:
        return False

    subcommands = {
        "admin-role-manage": AdminRoleManage,
        "bucket-compact": BucketCompact,
        "bucket-create": BucketCreate,
        "bucket-delete": BucketDelete,
        "bucket-edit": BucketEdit,
        "bucket-flush": BucketFlush,
        "bucket-list": BucketList,
        "cluster-edit": ClusterEdit,
        "cluster-init": ClusterInit,
        "collect-logs-start": CollectLogsStart,
        "collect-logs-status": CollectLogsStatus,
        "collect-logs-stop": CollectLogsStop,
        "failover": Failover,
        "group-manage": GroupManage,
        "host-list": HostList,
        "node-init": NodeInit,
        "rebalance": Rebalance,
        "rebalance-status": RebalanceStatus,
        "rebalance-stop": RebalanceStop,
        "recovery": Recovery,
        "server-add": ServerAdd,
        "server-eshell": ServerEshell,
        "server-info": ServerInfo,
        "server-list": ServerList,
        "server-readd": ServerReadd,
        "setting-alert": SettingAlert,
        "setting-audit": SettingAudit,
        "setting-autofailover": SettingAutoFailover,
        "setting-cluster": SettingCluster,
        "setting-compaction": SettingCompaction,
        "setting-index": SettingIndex,
        "setting-ldap": SettingLdap,
        "setting-xdcr": SettingXDCR,
        "setting-notification": SettingNotification,
        "ssl-manage": SSLManage,
        "user-manage": UserManage,
        "xdcr-setup": XDCRSetup,
    }

    if sys.argv[1] not in subcommands:
        return False

    subcommand = subcommands[sys.argv[1]]()
    options, args = subcommand.parse(sys.argv[2:])
    errors = subcommand.execute(options, args)
    if errors:
        _exitIfErrors(errors)

    return True

def help_callback(option, opt_str, value, parser, *args, **kwargs):

    """Help callback allowing us to show short help or a man page"""
    command = args[0]
    if opt_str == "-h":
        command.short_help()
    elif opt_str == "--help":
        command.short_help()

def check_cluster_initialized(rest):
    """Checks to see if the cluster is initialized"""
    initialized, errors = rest.is_cluster_initialized()
    if not initialized:
        _exitIfErrors(["Cluster is not initialized, use cluster-init to initialize the cluster"])
    elif errors:
        _exitIfErrors(errors)

def host_port(url):
    "Splits a url into it's host and port"

    hostport = url.split(":", 1)
    if len(hostport) == 1:
        return hostport[0], COUCHBASE_DEFAULT_PORT

    return url.split(":")[0], url.split(":")[1]

def index_storage_mode_to_param(value):
    """Converts the index storage mode to what Couchbase understands"""
    if value == "default":
        return "forestdb"
    elif value == "memopt":
        return "memory_optimized"
    else:
        return value

def process_services(services, is_enterprise):
    """Converts services to a format Couchbase understands"""
    sep = ","
    if services.find(sep) < 0:
        #backward compatible when using ";" as separator
        sep = ";"
    svc_list = list(set([w.strip() for w in services.split(sep)]))
    svc_candidate = ["data", "index", "query", "fts"]
    for svc in svc_list:
        if svc not in svc_candidate:
            return None, ["`%s` is not a valid service" % svc]
    if not is_enterprise:
        if len(svc_list) != len(svc_candidate) and (len(svc_list) != 1 or "data" not in svc_list):
            return None, ["Community Edition requires that all nodes provision all services or data service only"]

    services = ",".join(svc_list)
    for old, new in [[";", ","], ["data", "kv"], ["query", "n1ql"]]:
        services = services.replace(old, new)
    return services, None

def _deprecated(msg):
    print "DEPRECATED: " + msg

def _warning(msg):
    print "WARNING: " + msg

def _exitIfErrors(errors):
    if errors:
        for error in errors:
            print "ERROR: " + error
        sys.exit(1)

def _exit_on_file_write_failure(fname, to_write):
    try:
        wfile = open(fname, 'w')
        wfile.write(to_write)
        wfile.close()
    except IOError, error:
        _exitIfErrors([error])

def _exit_on_file_read_failure(fname):
    try:
        rfile = open(fname, 'r')
        read_bytes = rfile.read()
        rfile.close()
        return read_bytes
    except IOError, error:
        _exitIfErrors([error])

class CLIOptionParser(OptionParser):
    """A custom parser for subcommands"""

    def __init__(self, *args, **kwargs):
        OptionParser.__init__(self, *args, **kwargs)

    def format_option_help(self, formatter=None):
        if formatter is None:
            formatter = self.formatter
        formatter.store_option_strings(self)
        result = []
        if self.option_list:
            result.append(OptionContainer.format_option_help(self, formatter))
            result.append("\n")
        for group in self.option_groups:
            result.append(group.format_help(formatter))
            result.append("\n")
        # Drop the last "\n", or the header if no options or option groups:
        return "".join(result[:-1])

    def error(self, msg):
        """error(msg : string)

        Print a usage message incorporating 'msg' to stderr and exit.
        If you override this in a subclass, it should not return -- it
        should either exit or raise an exception.
        """
        sys.stdout.write("ERROR: %s\n\n" % (msg))
        self.print_help()
        self.exit(1)

class CLIHelpFormatter(HelpFormatter):
    """Format help with indented section bodies"""

    def __init__(self,
                 indent_increment=2,
                 max_help_position=30,
                 width=None,
                 short_first=1):
        HelpFormatter.__init__(self, indent_increment, max_help_position,
                               width, short_first)
        self._short_opt_fmt = "%s"
        self._long_opt_fmt = "%s"

    def format_usage(self, usage):
        return gettext("usage: %s\n") % usage

    def format_heading(self, heading):
        return "%*s%s:\n" % (self.current_indent, "", heading)

    def format_option_strings(self, option):
        """Return a comma-separated list of option strings & metavariables."""
        if option.takes_value():
            short_opts = [self._short_opt_fmt % (sopt)
                          for sopt in option._short_opts]
            long_opts = [self._long_opt_fmt % (lopt)
                         for lopt in option._long_opts]
        else:
            short_opts = option._short_opts
            long_opts = option._long_opts

        if self.short_first:
            opts = short_opts + long_opts
        else:
            opts = long_opts + short_opts

        return ", ".join(opts)

class Command(object):
    """A Couchbase CLI Subcommand"""

    def __init__(self):
        self.parser = CLIOptionParser(formatter=CLIHelpFormatter(), add_help_option=False)
        self._required_opt_check = []

        self.required = OptionGroup(self.parser, "Required")
        self.add_required("-c", "--cluster", dest="cluster",
                          help="The hostname of the Couchbase cluster")
        self.add_required("-u", "--username", dest="username",
                          help="The username for the Couchbase cluster")
        self.add_required("-p", "--password", dest="password",
                          help="The password for the Couchbase cluster")
        self.parser.add_option_group(self.required)

        self.optional = OptionGroup(self.parser, "Optional")
        self.add_optional("-o", "--output", dest="output", default="standard",
                          choices=["json", "standard"],
                          help="The output type (json or standard)")
        self.add_optional("-d", "--debug", dest="debug", action="store_true",
                          help="Run the command with extra logging")
        self.add_optional("-s", "--ssl", dest="ssl", action="store_true",
                          help="Use ssl when connecting to Couchbase")
        self.add_optional("-h", "--help", action="callback",
                          help="Prints the short or long help message",
                          callback=help_callback, callback_args=(self,))
        self.parser.add_option_group(self.optional)

    def add_required(self, *args, **kwargs):
        """Adds a required option to the subcommand"""
        self.required.add_option(*args, **kwargs)
        if "dest" in kwargs:
            self._required_opt_check.append((kwargs["dest"], ", ".join(args)))

    def add_optional(self, *args, **kwargs):
        """Adds an optional option to the subcommand"""
        self.optional.add_option(*args, **kwargs)

    def parse(self, args):
        """Parses the subcommand"""
        if len(args) == 0:
            self.short_help()

        errored = False
        opts, args = self.parser.parse_args(args)
        for (req_opt, flags) in self._required_opt_check:
            if not hasattr(opts, req_opt) or getattr(opts, req_opt) is None:
                sys.stdout.write("ERROR: Option required, but not specified: %s\n" % flags)
                errored = True

        if errored:
            sys.stdout.write("\n")
            self.short_help(1)

        return opts, args

    def short_help(self, code=0):
        """Prints the short help message and exits"""
        self.parser.print_help()
        self.parser.exit(code)

    def execute(self, opts, args):
        """Executes the subcommand"""
        raise NotImplementedError


class AdminRoleManage(Command):
    """The administrator role manage subcommand"""

    def __init__(self):
        super(AdminRoleManage, self).__init__()
        self.parser.set_usage("couchbase-cli admin-role-manage [options]")
        self.add_optional("--my-roles", dest="my_roles", action="store_true",
                          help="Show the current users roles")
        self.add_optional("--get-roles", dest="get_roles", action="store_true",
                          help="Show all valid users and roles")
        self.add_optional("--set-users", dest="set_users",
                          help="A comma-delimited list of user ids to set " +
                          "acess-control roles for")
        self.add_optional("--set-names", dest="set_names",
                          help="A optional quoted, comma-delimited list names, " +
                          "one for each specified user id ")
        self.add_optional("--roles", dest="roles",
                          help="A comma-delimited list of roles to set for users")
        self.add_optional("--delete-users", dest="delete_users",
                          help="A comma-delimited list of users to remove from" +
                          " access control")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.my_roles is None and opts.get_roles is None and \
            opts.set_users is None and opts.delete_users is None:
            _exitIfErrors(["You must specify either '--my-roles', '--get-roles', " +
                           "'--set-users', or '--delete-users'"])

        if opts.my_roles and (opts.get_roles or opts.set_users or opts.roles or opts.delete_users):
            _exitIfErrors(["The '--my-roles' option may not be used with any other" +
                           " option."])

        if opts.get_roles and (opts.my_roles or opts.set_users or opts.roles or opts.delete_users):
            _exitIfErrors(["The '--get-roles' option may not be used with any " +
                           "other option."])

        if (opts.set_users and opts.roles is None) or (opts.set_users is None and opts.roles):
            _exitIfErrors(["You must specify lists of both users and roles for those" +
                           " users.\n--set-users=[comma delimited user list] " +
                           "--roles=[comma-delimited list of one or more from admin," +
                           " ro_admin, cluster_admin, replication_admin, " +
                           "bucket_admin[bucket name or '*'], views_admin[bucket" +
                           " name or '*']"])

        if opts.my_roles:
            data, errors = rest.myRoles()
            _exitIfErrors(errors)
            print json.dumps(data, indent=2)
        elif opts.get_roles:
            data, errors = rest.getRoles()
            _exitIfErrors(errors)
            print json.dumps(data, indent=2)
        elif opts.set_users:
            data, errors = rest.setRoles(opts.set_users, opts.roles, opts.set_names)
            _exitIfErrors(errors)
            print "SUCCESS: New users and roles added"
        else:
            data, errors = rest.deleteRoles(opts.delete_users)
            _exitIfErrors(errors)
            print "SUCCESS: Users deleted"


class ClusterInit(Command):
    """The cluster initialization subcommand"""

    def __init__(self):
        super(ClusterInit, self).__init__()
        self.parser.set_usage("couchbase-cli cluster-init [options]")
        self.add_required("--cluster-username", dest="username",
                          help="The cluster administrator username")
        self.add_required("--cluster-password", dest="password",
                          help="Only compact the data files")
        self.add_optional("--cluster-port", dest="port", type=(int),
                          help="The cluster administration console port")
        self.add_optional("--cluster-ramsize", dest="data_mem_quota", type=(int),
                          help="The data service memory quota (Megabytes)")
        self.add_optional("--cluster-index-ramsize", dest="index_mem_quota", type=(int),
                          help="The index service memory quota (Megabytes)")
        self.add_optional("--cluster-fts-ramsize", dest="fts_mem_quota", type=(int),
                          help="The full-text service memory quota (Megabytes)")
        self.add_optional("--cluster-name", dest="name", help="The cluster name")
        self.add_optional("--index-storage-setting", dest="index_storage_mode",
                          choices=["default", "memopt"],
                          help="The index storage backend (Defaults to \"default)\"")
        self.add_optional("--services", dest="services", default="data",
                          help="The services to run on this server")

    def execute(self, opts, args):
        # We need to ensure that creating the REST username/password is the
        # last REST API that is called because once that API succeeds the
        # cluster is initialized and cluster-init cannot be run again.

        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)

        initialized, errors = rest.is_cluster_initialized()
        _exitIfErrors(errors)
        if initialized:
            _exitIfErrors(["Cluster is already initialized, use setting-cluster to change settings"])

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        services, errors = process_services(opts.services, enterprise)
        _exitIfErrors(errors)

        if 'kv' not in services.split(','):
            _exitIfErrors(["Cannot set up first cluster node without the data service"])

        #Set memory quota
        msg = "Option required, but not specified when %s service enabled: %s"
        if 'kv' in services.split(',') and not opts.data_mem_quota:
            _exitIfErrors([msg % ("data", "--cluster-ramsize")])
        elif 'index' in services.split(',') and not opts.index_mem_quota:
            _exitIfErrors([msg % ("index", "--cluster-index-ramsize")])
        elif 'fts' in services.split(',') and not opts.fts_mem_quota:
            _exitIfErrors([msg % ("fts", "--cluster-fts-ramsize")])



        _, errors = rest.set_pools_default(opts.data_mem_quota, opts.index_mem_quota,
                                           opts.fts_mem_quota, opts.name)
        _exitIfErrors(errors)

        # Set the index storage mode
        if not opts.index_storage_mode and 'index' in services.split(','):
            opts.index_storage_mode = "default"

        if opts.index_storage_mode:
            param = index_storage_mode_to_param(opts.index_storage_mode)
            _, errors = rest.set_index_settings(param, None, None, None, None, None)
            _exitIfErrors(errors)

        # Setup services
        _, errors = rest.setup_services(services)
        _exitIfErrors(errors)

        # Enable notifications
        _, errors = rest.enable_notifications(True)
        _exitIfErrors(errors)

        # Setup Administrator credentials and Admin Console port
        _, errors = rest.set_admin_credentials(opts.username, opts.password,
                                               opts.port)
        _exitIfErrors(errors)

        print "SUCCESS: Cluster initialized"


class BucketCompact(Command):
    """The bucket compact subcommand"""

    def __init__(self):
        super(BucketCompact, self).__init__()
        self.parser.set_usage("couchbase-cli bucket-compact [options]")
        self.add_required("--bucket", dest="bucket_name",
                          help="The name of bucket to compact")
        self.add_optional("--data-only", dest="data_only", action="store_true",
                          help="Only compact the data files")
        self.add_optional("--view-only", dest="view_only", action="store_true",
                          help="Only compact the view files")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        bucket, errors = rest.get_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        if bucket["bucketType"] != BUCKET_TYPE_COUCHBASE:
            _exitIfErrors(["Cannot compact memcached buckets"])

        _, errors = rest.compact_bucket(opts.bucket_name, opts.data_only, opts.view_only)
        _exitIfErrors(errors)

        print "SUCCESS: Bucket compaction started"


class BucketCreate(Command):
    """The bucket create subcommand"""

    def __init__(self):
        super(BucketCreate, self).__init__()
        self.parser.set_usage("couchbase-cli bucket-create [options]")
        self.add_required("--bucket", dest="bucket_name",
                          help="The name of bucket to create")
        self.add_required("--bucket-type", dest="type",
                          choices=["couchbase", "memcached"],
                          help="The bucket type (memcached or couchbase)")
        self.add_required("--bucket-ramsize", dest="memory_quota",
                          type=(int), help="The amount of memory to allocate the bucket")
        self.add_optional("--bucket-replica", dest="replica_count",
                          choices=["0", "1", "2", "3"],
                          help="The replica count for the bucket")
        self.add_optional("--bucket-priority", dest="priority",
                          choices=[BUCKET_PRIORITY_LOW_STR, BUCKET_PRIORITY_HIGH_STR],
                          help="The bucket disk io priority (low or high)")
        self.add_optional("--bucket-password", default="",
                          dest="bucket_password", help="The bucket password")
        self.add_optional("--bucket-eviction-policy", dest="eviction_policy",
                          choices=["valueOnly", "fullEviction"],
                          help="The bucket eviction policy (valueOnly or fullEviction)")
        self.add_optional("--enable-flush", dest="enable_flush",
                          choices=["0", "1"], help="Enable bucket flush on this bucket (0 or 1)")
        self.add_optional("--enable-index-replica", dest="replica_indexes",
                          choices=["0", "1"], help="Enable replica indexes (0 or 1)")
        self.add_optional("--wait", dest="wait", action="store_true",
                          help="Wait for bucket creation to complete")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.type == "memcached":
            if opts.replica_count is not None:
                _exitIfErrors(["--bucket-replica cannot be specified for a memcached bucket"])
            if opts.replica_indexes is not None:
                _exitIfErrors(["--enable-index-replica cannot be specified for a memcached bucket"])
            if opts.priority is not None:
                _exitIfErrors(["--bucket-priority cannot be specified for a memcached bucket"])
            if opts.eviction_policy is not None:
                _exitIfErrors(["--bucket-eviction-policy cannot be specified for a memcached bucket"])

        priority = None
        if opts.priority is not None:
            if opts.priority == BUCKET_PRIORITY_HIGH_STR:
                priority = BUCKET_PRIORITY_HIGH_INT
            elif opts.priority == BUCKET_PRIORITY_LOW_STR:
                priority = BUCKET_PRIORITY_LOW_INT


        _, errors = rest.create_bucket(opts.bucket_name, opts.bucket_password,
                                       opts.type, opts.memory_quota,
                                       opts.eviction_policy, opts.replica_count,
                                       opts.replica_indexes, priority,
                                       opts.enable_flush, opts.wait)
        _exitIfErrors(errors)

        print "SUCCESS: Bucket created"


class BucketDelete(Command):
    """The bucket delete subcommand"""

    def __init__(self):
        super(BucketDelete, self).__init__()
        self.parser.set_usage("couchbase-cli bucket-delete [options]")
        self.add_required("--bucket", dest="bucket_name",
                          help="The name of bucket to delete")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        _, errors = rest.get_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        _, errors = rest.delete_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        print "SUCCESS: Bucket deleted"


class BucketEdit(Command):
    """The bucket edit subcommand"""

    def __init__(self):
        super(BucketEdit, self).__init__()
        self.parser.set_usage("couchbase-cli bucket-edit [options]")
        self.add_required("--bucket", dest="bucket_name",
                          help="The name of bucket to create")
        self.add_optional("--bucket-ramsize", dest="memory_quota",
                          type=(int), help="The amount of memory to allocate the bucket")
        self.add_optional("--bucket-replica", dest="replica_count",
                          choices=["0", "1", "2", "3"],
                          help="The replica count for the bucket")
        self.add_optional("--bucket-priority", dest="priority",
                          choices=["low", "high"], help="The bucket disk io priority (low or high)")
        self.add_optional("--bucket-password", default="",
                          dest="bucket_password", help="The bucket password")
        self.add_optional("--bucket-eviction-policy", dest="eviction_policy",
                          choices=["valueOnly", "fullEviction"],
                          help="The bucket eviction policy (valueOnly or fullEviction)")
        self.add_optional("--enable-flush", dest="enable_flush",
                          choices=["0", "1"], help="Enable bucket flush on this bucket (0 or 1)")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        bucket, errors = rest.get_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        if "bucketType" in bucket and bucket["bucketType"] == "memcached":
            if opts.memory_quota is not None:
                _exitIfErrors(["--bucket-ramsize cannot be specified for a memcached bucket"])
            if opts.replica_count is not None:
                _exitIfErrors(["--bucket-replica cannot be specified for a memcached bucket"])
            if opts.priority is not None:
                _exitIfErrors(["--bucket-priority cannot be specified for a memcached bucket"])
            if opts.eviction_policy is not None:
                _exitIfErrors(["--bucket-eviction-policy cannot be specified for a memcached bucket"])

        priority = None
        if opts.priority is not None:
            if opts.priority == BUCKET_PRIORITY_HIGH_STR:
                priority = BUCKET_PRIORITY_HIGH_INT
            elif opts.priority == BUCKET_PRIORITY_LOW_STR:
                priority = BUCKET_PRIORITY_LOW_INT

        _, errors = rest.edit_bucket(opts.bucket_name, opts.bucket_password,
                                     opts.memory_quota, opts.eviction_policy,
                                     opts.replica_count, priority,
                                     opts.enable_flush)
        _exitIfErrors(errors)

        print "SUCCESS: Bucket edited"


class BucketFlush(Command):
    """The bucket edit subcommand"""

    def __init__(self):
        super(BucketFlush, self).__init__()
        self.parser.set_usage("couchbase-cli bucket-flush [options]")
        self.add_required("--bucket", dest="bucket_name",
                          help="The name of bucket to delete")
        self.add_optional("--force", dest="force", action="store_true",
                          help="Execute the command without asking to confirm")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        _, errors = rest.get_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        if not opts.force:
            question = "Running this command will totally PURGE database data from disk. " + \
                       "Do you really want to do it? (Yes/No)"
            confirm = raw_input(question)
            if confirm not in ('y', 'Y', 'yes', 'Yes'):
                return

        _, errors = rest.flush_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        print "SUCCESS: Bucket flushed"


class BucketList(Command):
    """The bucket list subcommand"""

    def __init__(self):
        super(BucketList, self).__init__()
        self.parser.set_usage("couchbase-cli bucket-list [options]")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        result, errors = rest.list_buckets(extended=True)
        _exitIfErrors(errors)

        if opts.output == 'json':
            print result
        else:
            for bucket in result:
                print '%s' % bucket['name']
                print ' bucketType: %s' % bucket['bucketType']
                print ' authType: %s' % bucket['authType']
                print ' saslPassword: %s' % bucket['saslPassword']
                print ' numReplicas: %s' % bucket['replicaNumber']
                print ' ramQuota: %s' % bucket['quota']['ram']
                print ' ramUsed: %s' % bucket['basicStats']['memUsed']


class CollectLogsStart(Command):
    """The collect-logs-start subcommand"""

    def __init__(self):
        super(CollectLogsStart, self).__init__()
        self.parser.set_usage("couchbase-cli collect-logs-start [options]")
        self.add_optional("--all-nodes", dest="all_nodes", action="store_true",
                          default=False, help="Collect logs for all nodes")
        self.add_optional("--nodes", dest="nodes",
                          help="A comma separated list of nodes to collect logs from")
        self.add_optional("--upload", dest="upload", action="store_true",
                          default=False, help="Logs should be uploaded for Couchbase support")
        self.add_optional("--upload-host", dest="upload_host",
                          help="The host to upload logs to")
        self.add_optional("--customer", dest="upload_customer",
                          help="The name of the customer uploading logs")
        self.add_optional("--ticket", dest="upload_ticket",
                          help="The ticket number the logs correspond to")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.nodes is None and opts.all_nodes is False:
            _exitIfErrors(["Must specify either --all-nodes or --nodes"])

        if opts.nodes is not None and opts.all_nodes is True:
            _exitIfErrors(["Cannot specify both --all-nodes and --nodes"])

        if opts.upload:
            if opts.upload_host is None:
                _exitIfErrors(["--upload-host is required when --upload is specified"])
            if opts.upload_customer is None:
                _exitIfErrors(["--upload-customer is required when --upload is specified"])
        else:
            if opts.upload_host is not None:
                _warning("--upload-host has has no effect with specifying --upload")

            if opts.upload_customer is not None:
                _warning("--upload-customer has has no effect with specifying --upload")

        servers = opts.nodes
        if opts.all_nodes:
            servers = "*"

        _, errors = rest.collect_logs_start(servers, opts.upload, opts.upload_host,
                                            opts.upload_customer, opts.upload_ticket)
        _exitIfErrors(errors)

        print "SUCCESS: Log collection started"


class CollectLogsStatus(Command):
    """The collect-logs-status subcommand"""

    def __init__(self):
        super(CollectLogsStatus, self).__init__()
        self.parser.set_usage("couchbase-cli collect-logs-status [options]")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        tasks, errors = rest.get_tasks()
        _exitIfErrors(errors)

        found = False
        for task in tasks:
            if isinstance(task, dict) and 'type' in task and task['type'] == 'clusterLogsCollection':
                found = True
                self._print_task(task)

        if not found:
            print "No log collection tasks were found"

    def _print_task(self, task):
        print "Status: %s" % task['status']
        if 'perNode' in task:
            print "Details:"
            for node, node_status in task["perNode"].iteritems():
                print '\tNode:', node
                print '\tStatus:', node_status['status']
                for field in ["path", "statusCode", "url", "uploadStatusCode", "uploadOutput"]:
                    if field in node_status:
                        print '\t', field, ":", node_status[field]
            print

class CollectLogsStop(Command):
    """The collect-logs-stop subcommand"""

    def __init__(self):
        super(CollectLogsStop, self).__init__()
        self.parser.set_usage("couchbase-cli collect-logs-stop [options]")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        _, errors = rest.collect_logs_stop()
        _exitIfErrors(errors)

        print "SUCCESS: Log collection stopped"


class Failover(Command):
    """The failover subcommand"""

    def __init__(self):
        super(Failover, self).__init__()
        self.parser.set_usage("couchbase-cli failover [options]")
        self.add_required("--server-failover", dest="server_failover",
                          help="The server to failover")
        self.add_optional("--force", dest="force", action="store_true",
                          help="Hard failover the server")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        _, errors = rest.failover(opts.server_failover, opts.force)
        _exitIfErrors(errors)

        if not opts.force:
            _, errors = rest.rebalance([])
            _exitIfErrors(errors)

            time.sleep(1)
            status, errors = rest.rebalance_status()
            _exitIfErrors(errors)

            sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

            newline = False
            while status[0] in['running', 'unknown']:
                print ".",
                time.sleep(1)
                status, errors = rest.rebalance_status()
                _exitIfErrors(errors)
                newline = True

            if newline:
                print "\n"
            if status[1]:
                _exitIfErrors([status[1]])

        print "SUCCESS: Server failed over"


class GroupManage(Command):
    """The group manage subcommand"""

    def __init__(self):
        super(GroupManage, self).__init__()
        self.parser.set_usage("couchbase-cli host-list [options]")
        self.add_optional("--create", dest="create", action="store_true",
                          help="Create a new server group")
        self.add_optional("--delete", dest="delete", action="store_true",
                          help="Delete a server group")
        self.add_optional("--list", dest="list", action="store_true",
                          help="List all server groups")
        self.add_optional("--rename", dest="rename", help="Rename a server group")
        self.add_optional("--group-name", dest="name",
                          help="The name of the server group")
        self.add_optional("--move-servers", dest="move_servers",
                          help="A list of servers to move between groups")
        self.add_optional("--from-group", dest="from_group",
                          help="The group to move servers from")
        self.add_optional("--to-group", dest="to_group",
                          help="The group to move servers to")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        cmds = [opts.create, opts.delete, opts.list, opts.rename, opts.move_servers]
        if sum(cmd is not None for cmd in cmds) == 0:
            _exitIfErrors(["Must specify one of the following: --create, " +
                           "--delete, --list, --move-servers, or --rename"])
        elif sum(cmd is not None for cmd in cmds) != 1:
            _exitIfErrors(["Only one of the following may be specified: --create" +
                           ", --delete, --list, --move-servers, or --rename"])

        if opts.create:
            self._create(rest, opts, args)
        elif opts.delete:
            self._delete(rest, opts, args)
        elif opts.list:
            self._list(rest, opts, args)
        elif opts.rename:
            self._rename(rest, opts, args)
        elif opts.move_servers is not None:
            self._move(rest, opts, args)

    def _create(self, rest, opts, args):
        if opts.name is None:
            _exitIfErrors(["--group-name is required with --create flag"])
        _, errors = rest.create_server_group(opts.name)
        _exitIfErrors(errors)
        print "SUCCESS: Server group created"

    def _delete(self, rest, opts, args):
        if opts.name is None:
            _exitIfErrors(["--group-name is required with --delete flag"])
        _, errors = rest.delete_server_group(opts.name)
        _exitIfErrors(errors)
        print "SUCCESS: Server group deleted"

    def _list(self, rest, opts, args):
        groups, errors = rest.get_server_groups()
        _exitIfErrors(errors)

        found = False
        for group in groups["groups"]:
            if opts.name is None or opts.name == group['name']:
                found = True
                print '%s' % group['name']
                for node in group['nodes']:
                    print ' server: %s' % node["hostname"]
        if not found and opts.name:
            _exitIfErrors(["Invalid group name: %s" % opts.name])

    def _move(self, rest, opts, args):
        if opts.from_group is None:
            _exitIfErrors(["--from-group is required with --move-servers"])
        if opts.to_group is None:
            _exitIfErrors(["--to-group is required with --move-servers"])

        servers = opts.move_servers.split(",")
        _, errors = rest.move_servers_between_groups(servers, opts.from_group, opts.to_group)
        _exitIfErrors(errors)
        print "SUCCESS: Servers moved between groups"

    def _rename(self, rest, opts, args):
        if opts.name is None:
            _exitIfErrors(["--group-name is required with --rename option"])
        _, errors = rest.rename_server_group(opts.rename, opts.name)
        _exitIfErrors(errors)
        print "SUCCESS: Server group renamed"


class HostList(Command):
    """The host list subcommand"""

    def __init__(self):
        super(HostList, self).__init__()
        self.parser.set_usage("couchbase-cli host-list [options]")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        result, errors = rest.pools('default')
        _exitIfErrors(errors)

        for node in result['nodes']:
            print node['hostname']


class NodeInit(Command):
    """The node initialization subcommand"""

    def __init__(self):
        super(NodeInit, self).__init__()
        self.parser.set_usage("couchbase-cli node-init [options]")
        self.add_optional("--node-init-data-path", dest="data_path",
                          help="The path to store database files")
        self.add_optional("--node-init-index-path", dest="index_path",
                          help="The path to store index files")
        self.add_optional("--node-init-hostname", dest="hostname",
                          help="Sets the hostname for this server")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        # Cluster does not need to be initialized for this command

        if opts.data_path is None and opts.index_path is None and opts.hostname is None:
            _exitIfErrors(["No node initialization parameters specified"])

        if opts.data_path or opts.index_path:
            _, errors = rest.set_data_paths(opts.data_path, opts.index_path)
            _exitIfErrors(errors)

        if opts.hostname:
            _, errors = rest.set_hostname(opts.hostname)
            _exitIfErrors(errors)

        print "SUCCESS: Node initialized"


class Rebalance(Command):
    """The rebalance subcommand"""

    def __init__(self):
        super(Rebalance, self).__init__()
        self.parser.set_usage("couchbase-cli rebalance [options]")
        self.add_optional("--server-remove", dest="server_remove",
                          help="A list of servers to remove from the cluster")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        eject_nodes = []
        if opts.server_remove:
            eject_nodes = opts.server_remove.split(",")

        _, errors = rest.rebalance(eject_nodes)
        _exitIfErrors(errors)

        time.sleep(1)
        status, errors = rest.rebalance_status()
        _exitIfErrors(errors)

        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

        newline = False
        while status[0] in['running', 'unknown']:
            print ".",
            time.sleep(1)
            status, errors = rest.rebalance_status()
            _exitIfErrors(errors)
            newline = True

        if newline:
            print "\n"
        if status[1]:
            _exitIfErrors([status[1]])

        print "SUCCESS: Rebalance complete"


class RebalanceStatus(Command):
    """The rebalance status subcommand"""

    def __init__(self):
        super(RebalanceStatus, self).__init__()
        self.parser.set_usage("couchbase-cli rebalance-status [options]")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)
        status, errors = rest.rebalance_status()
        _exitIfErrors(errors)

        print status[0], status[1]


class RebalanceStop(Command):
    """The rebalance stop subcommand"""

    def __init__(self):
        super(RebalanceStop, self).__init__()
        self.parser.set_usage("couchbase-cli rebalance-stop [options]")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)
        _, errors = rest.stop_rebalance()
        _exitIfErrors(errors)

        print "SUCCESS: Rebalance stopped"


class Recovery(Command):
    """The recovery command"""

    def __init__(self):
        super(Recovery, self).__init__()
        self.parser.set_usage("couchbase-cli server-add [options]")
        self.add_required("--server-recovery", dest="servers",
                          help="The list of servers to recover")
        self.add_optional("--recovery-type", dest="recovery_type",
                          choices=["delta", "full"], default="delta",
                          help="The recovery type (delta or full)")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        servers = opts.servers.split(",")
        for server in servers:
            _, errors = rest.recovery(server, opts.recovery_type)
            _exitIfErrors(errors)

        print "SUCCESS: Servers recovered"


class ServerAdd(Command):
    """The server add command"""

    def __init__(self):
        super(ServerAdd, self).__init__()
        self.parser.set_usage("couchbase-cli server-add [options]")
        self.add_required("--server-add", dest="servers",
                          help="The list of servers to add")
        self.add_required("--server-add-username", dest="server_username",
                          help="The username for the server to add")
        self.add_required("--server-add-password", dest="server_password",
                          help="The password for the server to add")
        self.add_optional("--group-name", dest="group_name",
                          help="The server group to add this server into")
        self.add_optional("--services", dest="services", default="data",
                          help="The services this server will run")
        self.add_optional("--index-storage-setting", dest="storage_mode",
                          choices=["default", "memopt"], help="The index storage mode")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        opts.services, errors = process_services(opts.services, enterprise)
        _exitIfErrors(errors)

        settings, errors = rest.index_settings()
        _exitIfErrors(errors)

        if opts.storage_mode is None and settings['storageMode'] == "" and "index" in opts.services:
            opts.storage_mode = "default"

        if opts.storage_mode:
            param = index_storage_mode_to_param(opts.storage_mode)
            _, errors = rest.set_index_settings(param, None, None, None, None, None)
            _exitIfErrors(errors)

        for server in opts.servers.split(","):
            _, errors = rest.add_server(server, opts.group_name, opts.server_username,
                                        opts.server_password, opts.services)
            _exitIfErrors(errors)

        print "SUCCESS: Server added"


class ServerEshell(Command):
    """The server eshell subcommand"""

    def __init__(self):
        super(ServerEshell, self).__init__()
        self.parser.set_usage("couchbase-cli server-eshell [options]")
        self.add_optional("--vm", dest="vm", default="ns_server",
                          help="The vm to connect to")
        self.add_optional("--erl-path", dest="erl_path",
                          help="Override the path to the erl executable")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        # Cluster does not need to be initialized for this command

        result, errors = rest.node_info()
        _exitIfErrors(errors)

        node = result['otpNode']
        cookie = result['otpCookie']

        if opts.vm != 'ns_server':
            cookie, errors = rest.get_babysitter_cookie()
            _exitIfErrors(errors)

            [short, _] = node.split('@')

            if opts.vm == 'babysitter':
                node = 'babysitter_of_%s@127.0.0.1' % short
            elif opts.vm == 'couchdb':
                node = 'couchdb_%s@127.0.0.1' % short
            else:
                _exitIfErrors(["Unknown vm type `%s`" % opts.vm])

        rand_chars = ''.join(random.choice(string.ascii_letters) for i in xrange(20))
        name = 'ctl-%s@127.0.0.1' % rand_chars

        path = opts.erl_path
        if opts.erl_path is None:
            bin_dir = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), '..', '..', 'bin')
            cb_erl = os.path.join(bin_dir, 'erl')
            if os.path.isfile(cb_erl):
                path = cb_erl
            else:
                _warning("Cannot locate Couchbase erlang. Attempting to use non-Couchbase erlang")
                path = 'erl'

        try:
            subprocess.call([path, '-name', name, '-setcookie', cookie, '-hidden', '-remsh', node])
        except OSError:
            _exitIfErrors(["Unable to find the erl executable"])

class ServerInfo(Command):
    """The server info subcommand"""

    def __init__(self):
        super(ServerInfo, self).__init__()
        self.parser.set_usage("couchbase-cli server-info [options]")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        # Cluster does not need to be initialized for this command

        result, errors = rest.node_info()
        _exitIfErrors(errors)

        print json.dumps(result, sort_keys=True, indent=2)


class ServerList(Command):
    """The server list subcommand"""

    def __init__(self):
        super(ServerList, self).__init__()
        self.parser.set_usage("couchbase-cli server-list [options]")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        result, errors = rest.pools('default')
        _exitIfErrors(errors)

        for node in result['nodes']:
            if node.get('otpNode') is None:
                raise Exception("could not access node")

            print node['otpNode'], node['hostname'], node['status'], node['clusterMembership']


class ServerReadd(Command):
    """The setting alert subcommand"""

    def __init__(self):
        super(ServerReadd, self).__init__()
        self.parser.set_usage("couchbase-cli server-readd [options]")
        self.add_required("--server-add", dest="servers",
                          help="The list of servers to recover")
        # The parameters are unused, but kept for backwards compatibility
        self.add_optional("--server-username", dest="server_username",
                          help="The admin username for the server")
        self.add_optional("--server-password", dest="server_username",
                          help="The admin password for the server")
        self.add_optional("--group-name", dest="name",
                          help="The name of the server group")

    def execute(self, opts, args):
        _deprecated("This command is deprecated and has been replaced by the " +
                    "recovery command")
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        servers = opts.servers.split(",")
        for server in servers:
            _, errors = rest.readd_server(server)
            _exitIfErrors(errors)

        print "SUCCESS: Servers recovered"


class SettingAlert(Command):
    """The setting alert subcommand"""

    def __init__(self):
        super(SettingAlert, self).__init__()
        self.parser.set_usage("couchbase-cli setting-alert [options]")
        self.add_required("--enable-email-alert", dest="enabled",
                          choices=["0", "1"], help="Enable/disable email alerts")
        self.add_optional("--email-recipients", dest="email_recipients",
                          help="A comma separated list of email addresses")
        self.add_optional("--email-sender", dest="email_sender",
                          help="The sender email address")
        self.add_optional("--email-user", dest="email_username",
                          default="", help="The email server username")
        self.add_optional("--email-password", dest="email_password",
                          default="", help="The email server password")
        self.add_optional("--email-host", dest="email_host",
                          help="The email server host")
        self.add_optional("--email-port", dest="email_port",
                          help="The email server port")
        self.add_optional("--enable-email-encrypt", dest="email_encrypt",
                          choices=["0", "1"], help="Enable SSL encryption for emails")
        self.add_optional("--alert-auto-failover-node", dest="alert_af_node",
                          action="store_true", help="Alert when a node is auto-failed over")
        self.add_optional("--alert-auto-failover-max-reached", dest="alert_af_max_reached",
                          action="store_true",
                          help="Alert when the max number of auto-failover nodes was reached")
        self.add_optional("--alert-auto-failover-node-down", dest="alert_af_node_down",
                          action="store_true",
                          help="Alert when a node wasn't auto-failed over because other nodes were down")
        self.add_optional("--alert-auto-failover-cluster-small", dest="alert_af_small",
                          action="store_true",
                          help="Alert when a node wasn't auto-failed over because cluster was too small")
        self.add_optional("--alert-auto-failover-disable", dest="alert_af_disable",
                          action="store_true",
                          help="Alert when a node wasn't auto-failed over because auto-failover is disabled")
        self.add_optional("--alert-ip-changed", dest="alert_ip_changed",
                          action="store_true", help="Alert when a nodes IP address changed")
        self.add_optional("--alert-disk-space", dest="alert_disk_space",
                          action="store_true", help="Alert when disk usage on a node reaches 90%")
        self.add_optional("--alert-meta-overhead", dest="alert_meta_overhead",
                          action="store_true", help="Alert when metadata overhead is more than 50%")
        self.add_optional("--alert-meta-oom", dest="alert_meta_oom",
                          action="store_true", help="Alert when all bucket memory is used for metadata")
        self.add_optional("--alert-write-failed", dest="alert_write_failed",
                          action="store_true", help="Alert when writing data to disk has failed")
        self.add_optional("--alert-audit-msg-dropped", dest="alert_audit_dropped",
                          action="store_true", help="Alert when writing event to audit log failed")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.enabled == "1":
            if opts.email_recipients is None:
                _exitIfErrors(["--email-recipient must be set when email alerts are enabled"])
            if opts.email_sender is None:
                _exitIfErrors(["--email-sender must be set when email alerts are enabled"])
            if opts.email_host is None:
                _exitIfErrors(["--email-host must be set when email alerts are enabled"])
            if opts.email_port is None:
                _exitIfErrors(["--email-port must be set when email alerts are enabled"])

        alerts = list()
        if opts.alert_af_node:
            alerts.append('auto_failover_node')
        if opts.alert_af_max_reached:
            alerts.append('auto_failover_maximum_reached')
        if opts.alert_af_node_down:
            alerts.append('auto_failover_other_nodes_down')
        if opts.alert_af_small:
            alerts.append('auto_failover_cluster_too_small')
        if opts.alert_af_disable:
            alerts.append('auto_failover_disabled')
        if opts.alert_ip_changed:
            alerts.append('ip')
        if opts.alert_disk_space:
            alerts.append('disk')
        if opts.alert_meta_overhead:
            alerts.append('overhead')
        if opts.alert_meta_oom:
            alerts.append('ep_oom_errors')
        if opts.alert_write_failed:
            alerts.append('ep_item_commit_failed')
        if opts.alert_audit_dropped:
            alerts.append('audit_dropped_events')

        enabled = "true"
        if opts.enabled == "0":
            enabled = "false"

        _, errors = rest.set_alert_settings(enabled, opts.email_recipients,
                                            opts.email_sender, opts.email_username,
                                            opts.email_password, opts.email_host,
                                            opts.email_port, opts.email_encrypt,
                                            ",".join(alerts))
        _exitIfErrors(errors)

        print "SUCCESS: Email alert settings modified"

class SettingAudit(Command):
    """The settings audit subcommand"""

    def __init__(self):
        super(SettingAudit, self).__init__()
        self.parser.set_usage("couchbase-cli setting-audit [options]")
        self.add_optional("--audit-enabled", dest="enabled",
                          choices=["0", "1"], help="Enable/disable auditing")
        self.add_optional("--audit-log-path", dest="log_path",
                          help="The audit log path")
        self.add_optional("--audit-log-rotate-interval", dest="rotate_interval",
                          type=(int), help="The audit log rotate interval")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if not (opts.enabled or opts.log_path or opts.rotate_interval):
            _exitIfErrors(["No settings specified to be changed"])

        if opts.enabled == "1":
            opts.enabled = "true"
        elif opts.enabled == "0":
            opts.enabled = "false"

        _, errors = rest.set_audit_settings(opts.enabled, opts.log_path,
                                            opts.rotate_interval)
        _exitIfErrors(errors)

        print "SUCCESS: Audit settings modified"


class SettingAutoFailover(Command):
    """The settings auto-failover subcommand"""

    def __init__(self):
        super(SettingAutoFailover, self).__init__()
        self.parser.set_usage("couchbase-cli setting-autofailover [options]")
        self.add_optional("--enable-auto-failover", dest="enabled",
                          choices=["0", "1"], help="Enable/disable auto-failover")
        self.add_optional("--auto-failover-timeout", dest="timeout",
                          type=(int), help="The auto-failover timeout")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.enabled == "1":
            opts.enabled = "true"
        elif opts.enabled == "0":
            opts.enabled = "false"

        if not (opts.enabled or opts.timeout):
            _exitIfErrors(["No settings specified to be changed"])

        if (opts.enabled is None or opts.enabled == "false") and opts.timeout:
            _warning("Timeout specified will not take affect because auto-failover is being disabled")

        _, errors = rest.set_autofailover_settings(opts.enabled, opts.timeout)
        _exitIfErrors(errors)

        print "SUCCESS: Auto-failover settings modified"


class SettingCluster(Command):
    """The settings cluster subcommand"""

    def __init__(self):
        super(SettingCluster, self).__init__()
        self.parser.set_usage("couchbase-cli setting-cluster [options]")
        self.add_optional("--cluster-username", dest="new_username",
                          help="The cluster administrator username")
        self.add_optional("--cluster-password", dest="new_password",
                          help="Only compact the data files")
        self.add_optional("--cluster-port", dest="port", type=(int),
                          help="The cluster administration console port")
        self.add_optional("--cluster-ramsize", dest="data_mem_quota", type=(int),
                          help="The data service memory quota (Megabytes)")
        self.add_optional("--cluster-index-ramsize", dest="index_mem_quota", type=(int),
                          help="The index service memory quota (Megabytes)")
        self.add_optional("--cluster-fts-ramsize", dest="fts_mem_quota", type=(int),
                          help="The full-text service memory quota (Megabytes)")
        self.add_optional("--cluster-name", dest="name", help="The cluster name")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.data_mem_quota or opts.index_mem_quota or opts.fts_mem_quota or \
            opts.name is not None:
            _, errors = rest.set_pools_default(opts.data_mem_quota, opts.index_mem_quota,
                                               opts.fts_mem_quota, opts.name)
            _exitIfErrors(errors)

        if opts.new_username or opts.new_password or opts.port:
            username = opts.username
            if opts.new_username:
                username = opts.new_username

            password = opts.password
            if opts.new_password:
                password = opts.new_password

            _, errors = rest.set_admin_credentials(username, password, opts.port)
            _exitIfErrors(errors)

        print "SUCCESS: Cluster settings modified"


class ClusterEdit(SettingCluster):
    """The cluster edit subcommand (Deprecated)"""

    def __init__(self):
        super(ClusterEdit, self).__init__()
        self.parser.set_usage("couchbase-cli cluster-edit [options]")

    def execute(self, opts, args):
        _warning("The cluster-edit command is depercated, use setting-cluster instead")
        super(ClusterEdit, self).execute(opts, args)


class SettingCompaction(Command):
    """The setting compaction subcommand"""

    def __init__(self):
        super(SettingCompaction, self).__init__()
        self.parser.set_usage("couchbase-cli setting-compaction [options]")
        self.add_optional("--compaction-db-percentage", dest="db_perc", type=(int),
                          help="Compacts the db once the fragmentation reaches this percentage")
        self.add_optional("--compaction-db-size", dest="db_size", type=(int),
                          help="Compacts db once the fragmentation reaches this size (MB)")
        self.add_optional("--compaction-view-percentage", dest="view_perc", type=(int),
                          help="Compacts the view once the fragmentation reaches this percentage")
        self.add_optional("--compaction-view-size", dest="view_size", type=(int),
                          help="Compacts view once the fragmentation reaches this size (MB)")
        self.add_optional("--compaction-period-from", dest="from_period",
                          help="Only run compaction after this time")
        self.add_optional("--compaction-period-to", dest="to_period",
                          help="Only run compaction before this time")
        self.add_optional("--enable-compaction-abort", dest="enable_abort",
                          choices=["0", "1"], help="Allow compactions to be aborted")
        self.add_optional("--enable-compaction-parallel", dest="enable_parallel",
                          choices=["0", "1"], help="Allow parallel compactions")
        self.add_optional("--metadata-purge-interval", dest="purge_interval", type=(float),
                          help="The metadata purge interval")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.db_perc is not None and (opts.db_perc < 2 or opts.db_perc > 100):
            _exitIfErrors(["--compaction-db-percentage must be between 2 and 100"])

        if opts.view_perc is not None and (opts.view_perc < 2 or opts.view_perc > 100):
            _exitIfErrors(["--compaction-view-percentage must be between 2 and 100"])

        if opts.db_size is not None:
            if int(opts.db_size) < 1:
                _exitIfErrors(["--compaction-db-size must be between greater than 1 or infinity"])
            opts.db_size = int(opts.db_size) * 1024**2

        if opts.view_size is not None:
            if int(opts.view_size) < 1:
                _exitIfErrors(["--compaction-view-size must be between greater than 1 or infinity"])
            opts.view_size = int(opts.view_size) * 1024**2

        if opts.from_period and not (opts.to_period and opts.enable_abort):
            errors = []
            if opts.to_period is None:
                errors.append("--compaction-period-to is required when using --compaction-period-from")
            if opts.enable_abort is None:
                errors.append("--enable-compaction-abort is required when using --compaction-period-from")
            _exitIfErrors(errors)

        if opts.to_period and not (opts.from_period and opts.enable_abort):
            errors = []
            if opts.from_period is None:
                errors.append("--compaction-period-from is required when using --compaction-period-to")
            if opts.enable_abort is None:
                errors.append("--enable-compaction-abort is required when using --compaction-period-to")
            _exitIfErrors(errors)

        if opts.enable_abort and not (opts.from_period and opts.to_period):
            errors = []
            if opts.from_period is None:
                errors.append("--compaction-period-from is required when using --enable-compaction-abort")
            if opts.to_period is None:
                errors.append("--compaction-period-to is required when using --enable-compaction-abort")
            _exitIfErrors(errors)

        from_hour = None
        from_min = None
        if opts.from_period:
            if opts.from_period.find(':') == -1:
                _exitIfErrors(["Invalid value for --compaction-period-from, must be in form XX:XX"])
            from_hour, from_min = opts.from_period.split(':', 1)
            try:
                from_hour = int(from_hour)
            except ValueError:
                _exitIfErrors(["Invalid hour value for --compaction-period-from, must be an integer"])
            if from_hour not in range(24):
                _exitIfErrors(["Invalid hour value for --compaction-period-from, must be 0-23"])

            try:
                from_min = int(from_min)
            except ValueError:
                _exitIfErrors(["Invalid minute value for --compaction-period-from, must be an integer"])
            if from_min not in range(60):
                _exitIfErrors(["Invalid minute value for --compaction-period-from, must be 0-59"])


        to_hour = None
        to_min = None
        if opts.to_period:
            if opts.to_period.find(':') == -1:
                _exitIfErrors(["Invalid value for --compaction-period-to, must be in form XX:XX"])
            to_hour, to_min = opts.to_period.split(':', 1)
            try:
                to_hour = int(to_hour)
            except ValueError:
                _exitIfErrors(["Invalid hour value for --compaction-period-to, must be an integer"])
            if to_hour not in range(24):
                _exitIfErrors(["Invalid hour value for --compaction-period-to, must be 0-23"])

            try:
                to_min = int(to_min)
            except ValueError:
                _exitIfErrors(["Invalid minute value for --compaction-period-to, must be an integer"])
            if to_min not in range(60):
                _exitIfErrors(["Invalid minute value for --compaction-period-to, must be 0-59"])

        if opts.enable_abort == "1":
            opts.enable_abort = "true"
        elif opts.enable_abort == "0":
            opts.enable_abort = "false"

        if opts.enable_parallel == "1":
            opts.enable_parallel = "true"
        else:
            opts.enable_parallel = "false"

        if opts.purge_interval is not None and (opts.purge_interval < 0.04 or opts.purge_interval > 60.0):\
            _exitIfErrors(["--metadata-purge-interval must be between 0.04 and 60.0"])

        _, errors = rest.set_compaction_settings(opts.db_perc, opts.db_size, opts.view_perc,
                                                 opts.view_size, from_hour, from_min, to_hour,
                                                 to_min, opts.enable_abort, opts.enable_parallel,
                                                 opts.purge_interval)
        _exitIfErrors(errors)

        print "SUCCESS: Compaction settings modified"


class SettingIndex(Command):
    """The setting index subcommand"""

    def __init__(self):
        super(SettingIndex, self).__init__()
        self.parser.set_usage("couchbase-cli setting-index [options]")
        self.add_optional("--index-max-rollback-points", dest="max_rollback",
                          type=(int), help="Max rollback points")
        self.add_optional("--index-stable-snapshot-interval", dest="stable_snap",
                          type=(int), help="Stable snapshot interval in seconds")
        self.add_optional("--index-memory-snapshot-interval", dest="mem_snap",
                          type=(int), help="Stable snapshot interval in seconds")
        self.add_optional("--index-storage-setting", dest="storage_mode",
                          choices=["default", "memopt"], help="The index storage backend")
        self.add_optional("--index-threads", dest="threads",
                          type=(int), help="The number of indexer threads")
        self.add_optional("--index-log-level", dest="log_level",
                          choices=["debug", "silent", "fatal", "error", "warn",
                                   "info", "verbose", "timing", "trace"],
                          help="The indexer log level")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.max_rollback is None and opts.stable_snap is None \
            and opts.mem_snap is None and opts.storage_mode is None \
            and opts.threads is None and opts.log_level is None:
            _exitIfErrors(["No settings specified to be changed"])

        opts.storage_mode = index_storage_mode_to_param(opts.storage_mode)
        _, errors = rest.set_index_settings(opts.storage_mode, opts.max_rollback,
                                            opts.stable_snap, opts.mem_snap,
                                            opts.threads, opts.log_level)
        _exitIfErrors(errors)

        print "SUCCESS: Indexer settings modified"


class SettingLdap(Command):
    """The setting ldap subcommand"""

    def __init__(self):
        super(SettingLdap, self).__init__()
        self.parser.set_usage("couchbase-cli setting-ldap [options]")
        self.add_required("--ldap-enabled", dest="enabled",
                          choices=["0", "1"], help="Enable/disable LDAP")
        self.add_optional("--ldap-admins", dest="admins",
                          help="A comma separated list of full admins")
        self.add_optional("--ldap-roadmins", dest="roadmins",
                          help="A comma separated list of read only admins")
        self.add_optional("--ldap-default", dest="default", default="none",
                          choices=["admins", "roadmins", "none"],
                          help="Enable/disable LDAP")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        admins = ""
        if opts.admins:
            admins = opts.admins.replace(",", "\n")

        ro_admins = ""
        if opts.roadmins:
            ro_admins = opts.roadmins.replace(",", "\n")

        errors = None
        if opts.enabled == '1':
            if opts.default == 'admins':
                if ro_admins:
                    _warning("--ldap-ro-admins option ignored since default is read only admins")
                _, errors = rest.ldap_settings('true', ro_admins, None)
            elif opts.default == 'roadmins':
                if admins:
                    _warning("--ldap-admins option ignored since default is admins")
                _, errors = rest.ldap_settings('true', None, admins)
            else:
                _, errors = rest.ldap_settings('true', ro_admins, admins)
        else:
            if admins:
                _warning("--ldap-admins option ignored since ldap is being disabled")
            if ro_admins:
                _warning("--ldap-roadmins option ignored since ldap is being disabled")
            _, errors = rest.ldap_settings('false', "", "")

        _exitIfErrors(errors)

        print "SUCCESS: LDAP settings modified"

class SettingNotification(Command):
    """The settings notification subcommand"""

    def __init__(self):
        super(SettingNotification, self).__init__()
        self.parser.set_usage("couchbase-cli setting-notification [options]")
        self.add_required("--enable-notifications", dest="enabled",
                          choices=["0", "1"], help="Enables/disable notifications")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)

        enabled = None
        if opts.enabled == "1":
            enabled = True
        elif opts.enabled == "0":
            enabled = False

        _, errors = rest.enable_notifications(enabled)
        _exitIfErrors(errors)

        print "SUCCESS: Notification settings updated"


class SettingXDCR(Command):
    """The setting xdcr subcommand"""

    def __init__(self):
        super(SettingXDCR, self).__init__()
        self.parser.set_usage("couchbase-cli setting-xdcr [options]")
        self.add_optional("--checkpoint-interval", dest="chk_int", type=(int),
                          help="Intervals between checkpoints in seconds (60 to 14400)")
        self.add_optional("--worker-batch-size", dest="worker_batch_size", type=(int),
                          help="Doc batch size (500 to 10000)")
        self.add_optional("--doc-batch-size", dest="doc_batch_size", type=(int),
                          help="Document batching size in KB (10 to 100000)")
        self.add_optional("--failure-restart-interval", dest="fail_interval", type=(int),
                          help="Interval for restarting failed xdcr in seconds (1 to 300)")
        self.add_optional("--optimistic-replication-threshold", dest="rep_thresh", type=(int),
                          help="Document body size threshold (bytes) to trigger optimistic replication")
        self.add_optional("--source-nozzle-per-node", dest="src_nozzles", type=(int),
                          help="The number of source nozzles per source node (1 to 10)")
        self.add_optional("--target-nozzle-per-node", dest="dst_nozzles", type=(int),
                          help="The number of outgoing nozzles per target node (1 to 10)")
        self.add_optional("--log-level", dest="log_level",
                          choices=["Error", "Info", "Debug", "Trace"],
                          help="The XDCR log level")
        self.add_optional("--stats-interval", dest="stats_interval",
                          help="The interval for statistics updates (in milliseconds)")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        _, errors = rest.xdcr_global_settings(opts.chk_int, opts.worker_batch_size,
                                              opts.doc_batch_size, opts.fail_interval,
                                              opts.rep_thresh, opts.src_nozzles,
                                              opts.dst_nozzles, opts.log_level,
                                              opts.stats_interval)
        _exitIfErrors(errors)

        print "SUCCESS: Global XDCR settings updated"


class SSLManage(Command):
    """The user manage subcommand"""

    def __init__(self):
        super(SSLManage, self).__init__()
        self.parser.set_usage("couchbase-cli ssl-manage --cluster-cert-info [--extended]\n" +
                              "   or: couchbase-cli ssl-manage --node-cert-info\n" +
                              "   or: couchbase-cli ssl-manage --regenerate-cert <path>\n" +
                              "   or: couchbase-cli ssl-manage --set-node-certificate\n" +
                              "   or: couchbase-cli ssl-manage --upload-cluster-ca <path>")
        self.add_optional("--cluster-cert-info", dest="cluster_cert", action="store_true",
                          default=False, help="Gets the cluster certificate")
        self.add_optional("--node-cert-info", dest="node_cert", action="store_true",
                          default=False, help="Gets the node certificate")
        self.add_optional("--regenerate-cert", dest="regenerate",
                          help="Regenerate the cluster certificat and save it to a file")
        self.add_optional("--set-node-certificate", dest="set_cert", action="store_true",
                          default=False, help="Sets the node certificate")
        self.add_optional("--upload-cluster-ca", dest="upload_cert",
                          help="Upload a new cluster certificate")
        self.add_optional("--extended", dest="extended", action="store_true",
                          default=False, help="Print extended certificate information")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        if opts.regenerate is not None:
            try:
                open(opts.regenerate, 'a').close()
            except IOError:
                _exitIfErrors(["Unable to create file at `%s`" % opts.regenerate])
            certificate, errors = rest.regenerate_cluster_certificate()
            _exitIfErrors(errors)
            _exit_on_file_write_failure(opts.regenerate, certificate)
            print "SUCCESS: Certificate regenerate and copied to '%s'" % (opts.regenerate)
        elif opts.cluster_cert is not None:
            certificate, errors = rest.retrieve_cluster_certificate(opts.extended)
            _exitIfErrors(errors)
            if isinstance(certificate, dict):
                print json.dumps(certificate, sort_keys=True, indent=2)
            else:
                print certificate
        elif opts.node_cert is not None:
            certificate, errors = rest.retrieve_node_certificate('%s:%d' % (host, port))
            _exitIfErrors(errors)
            print json.dumps(certificate, sort_keys=True, indent=2)
        elif opts.upload_cert:
            certificate = _exit_on_file_read_failure(opts.upload_cert)
            _, errors = rest.upload_cluster_certificate(certificate)
            _exitIfErrors(errors)
            print "SUCCESS: Uploaded cluster certificate to %s:%d" % (host, port)
        elif opts.set_cert:
            _, errors = rest.set_node_certificate()
            _exitIfErrors(errors)
            print "SUCCESS: Node certificate set"
        else:
            _exitIfErrors(["No options specified"])

class UserManage(Command):
    """The user manage subcommand"""

    def __init__(self):
        super(UserManage, self).__init__()
        self.parser.set_usage("couchbase-cli user-manage [options]")
        self.add_optional("--list", dest="list", action="store_true",
                          default=False, help="List the local read-only user")
        self.add_optional("--delete", dest="delete", action="store_true",
                          default=False, help="Delete the local read-only user")
        self.add_optional("--set", dest="set", action="store_true",
                          default=False, help="Set the local read-only user")
        self.add_optional("--ro-username", dest="ro_user",
                          help="The read-only username")
        self.add_optional("--ro-password", dest="ro_pass",
                          help="The read-only password")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        num_selectors = sum([opts.list, opts.delete, opts.set])
        if num_selectors == 0:
            _exitIfErrors(["Must specify --delete, --list, or --set"])
        elif num_selectors != 1:
            _exitIfErrors(["Only one of the following can be specified: --delete, --list, or --set"])

        if opts.delete:
            self._delete(rest, opts, args)
        elif opts.list:
            self._list(rest, opts, args)
        elif opts.set:
            self._set(rest, opts, args)

    def _delete(self, rest, opts, args):
        if opts.ro_user is not None:
            _warning("--ro-username is not used with the --delete command")
        if opts.ro_pass is not None:
            _warning("--ro-password is not used with the --delete command")

        _, errors = rest.delete_local_read_only_user()
        _exitIfErrors(errors)
        print "SUCCESS: Local read-only user deleted"

    def _list(self, rest, opts, args):
        if opts.ro_user is not None:
            _warning("--ro-username is not used with the --list command")
        if opts.ro_pass is not None:
            _warning("--ro-password is not used with the --list command")

        result, errors = rest.list_local_read_only_user()
        if errors and errors[0] == "Requested resource not found.\r\n":
            errors[0] = "There is no internal read-only user"
        _exitIfErrors(errors)
        print result

    def _set(self, rest, opts, args):
        if opts.ro_user is None:
            _exitIfErrors(["--ro-username is required with the --set command"])
        if opts.ro_pass is None:
            _exitIfErrors(["--ro-password is required with the --set command"])

        cur_ro_user, errors = rest.list_local_read_only_user()
        if not errors and cur_ro_user != opts.ro_user:
            _exitIfErrors(["The internal read-only user already exists"])
        elif errors and errors[0] != "Requested resource not found.\r\n":
            _exitIfErrors(errors)

        _, errors = rest.set_local_read_only_user(opts.ro_user, opts.ro_pass)
        _exitIfErrors(errors)
        print "SUCCESS: Local read-only user created"


class XDCRSetup(Command):
    """The xdcr setup subcommand"""

    def __init__(self):
        super(XDCRSetup, self).__init__()
        self.parser.set_usage("couchbase-cli xdcr-setup [options]")
        self.add_optional("--create", dest="create", action="store_true",
                          default=False, help="Create an XDCR remote reference")
        self.add_optional("--delete", dest="delete", action="store_true",
                          default=False, help="Delete an XDCR remote reference")
        self.add_optional("--edit", dest="edit", action="store_true",
                          default=False, help="Set the local read-only user")
        self.add_optional("--list", dest="list", action="store_true",
                          default=False, help="List all XDCR remote references")
        self.add_optional("--xdcr-cluster-name", dest="name",
                          help="The name for the remote cluster reference")
        self.add_optional("--xdcr-hostname", dest="hostname",
                          help="The hostname of the remote cluster reference")
        self.add_optional("--xdcr-username", dest="r_username",
                          help="The username of the remote cluster reference")
        self.add_optional("--xdcr-password", dest="r_password",
                          help="The password of the remote cluster reference")
        self.add_optional("--xdcr-demand-encryption", dest="encrypt",
                          choices=["0", "1"], default="0",
                          help="Enable SSL when replicating with this cluster")
        self.add_optional("--xdcr-certificate", dest="certificate",
                          help="The certificate used for encryption")

    def execute(self, opts, args):
        host, port = host_port(opts.cluster)
        rest = ClusterManager(host, port, opts.username, opts.password, opts.ssl)
        check_cluster_initialized(rest)

        actions = sum([opts.create, opts.delete, opts.edit, opts.list])
        if actions == 0:
            _exitIfErrors(["Must specify one of --create, --delete, --edit, --list"])
        elif actions > 1:
            _exitIfErrors(["The --create, --delete, --edit, --list flags may not " +
                           "be specified at the same time"])
        elif opts.create or opts.edit:
            self._set(rest, opts, args)
        elif opts.delete:
            self._delete(rest, opts, args)
        elif opts.list:
            self._list(rest, opts, args)

    def _set(self, rest, opts, args):
        cmd = "create"
        if opts.edit:
            cmd = "edit"

        if opts.name is None:
            _exitIfErrors(["--xdcr-cluster-name is required to %s a cluster connection" % cmd])
        if opts.hostname is None:
            _exitIfErrors(["--xdcr-hostname is required to %s a cluster connections" % cmd])
        if opts.username is None:
            _exitIfErrors(["--xdcr-username is required to %s a cluster connections" % cmd])
        if opts.password is None:
            _exitIfErrors(["--xdcr-password is required to %s a cluster connections" % cmd])

        raw_cert = None
        if opts.encrypt == "1":
            if opts.certificate is None:
                _exitIfErrors(["certificate required if encryption is demanded"])
            raw_cert = _exit_on_file_read_failure(opts.certificate)

        if opts.create:
            _, errors = rest.create_xdcr_reference(opts.name, opts.hostname, opts.username,
                                                   opts.password, opts.encrypt, raw_cert)
            _exitIfErrors(errors)
            print "SUCCESS: Cluster reference created"
        else:
            _, errors = rest.edit_xdcr_reference(opts.name, opts.hostname, opts.username,
                                                 opts.password, opts.encrypt, raw_cert)
            _exitIfErrors(errors)
            print "SUCCESS: Cluster reference edited"

    def _delete(self, rest, opts, args):
        if opts.name is None:
            _exitIfErrors(["--xdcr-cluster-name is required to deleta a cluster connection"])

        _, errors = rest.delete_xdcr_reference(opts.name)
        _exitIfErrors(errors)

        print "SUCCESS: Cluster reference deleted"

    def _list(self, rest, opts, args):
        clusters, errors = rest.list_xdcr_references()
        _exitIfErrors(errors)

        for cluster in clusters:
            if not cluster["deleted"]:
                print "cluster name: %s" % cluster["name"]
                print "        uuid: %s" % cluster["uuid"]
                print "   host name: %s" % cluster["hostname"]
                print "   user name: %s" % cluster["username"]
                print "         uri: %s" % cluster["uri"]
