"""A Couchbase  CLI subcommand"""

import sys

from optparse import HelpFormatter, OptionContainer, OptionGroup, OptionParser
from cluster_manager import ClusterManager
from node import _exitIfErrors, _warning

try:
    from gettext import gettext
except ImportError:
    def gettext(message):
        """Stub gettext method"""
        return message

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
        "cluster-edit": ClusterEdit,
        "cluster-init": ClusterInit,
        "bucket-compact": BucketCompact,
        "bucket-create": BucketCreate,
        "bucket-delete": BucketDelete,
        "bucket-edit": BucketEdit,
        "bucket-flush": BucketFlush,
        "bucket-list": BucketList,
        "host-list": HostList,
        "rebalance-stop": RebalanceStop,
        "server-add": ServerAdd,
        "server-list": ServerList,
        "setting-audit": SettingAudit,
        "setting-autofailover": SettingAutoFailover,
        "setting-cluster": SettingCluster,
        "setting-index": SettingIndex,
        "setting-notification": SettingNotification,
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
        return gettext("%s\n") % usage

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


class ServerAdd(Command):
    """The server add command"""

    def __init__(self):
        super(ServerAdd, self).__init__()
        self.parser.set_usage("couchbase-cli server-list [options]")
        self.add_required("--server-add", dest="server",
                          help="The server to add")
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

        _, errors = rest.add_server(opts.server, opts.group_name, opts.server_username,
                                    opts.server_password, opts.services)
        _exitIfErrors(errors)

        print "SUCCESS: Server added"

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

        if not (opts.max_rollback or opts.stable_snap or opts.mem_snap or \
            opts.storage_mode or opts.threads or opts.log_level):
            _exitIfErrors(["No settings specified to be changed"])

        opts.storage_mode = index_storage_mode_to_param(opts.storage_mode)
        _, errors = rest.set_index_settings(opts.storage_mode, opts.max_rollback,
                                            opts.stable_snap, opts.mem_snap,
                                            opts.threads, opts.log_level)
        _exitIfErrors(errors)

        print "SUCCESS: Indexer settings modified"


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
