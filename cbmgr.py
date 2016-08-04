"""A Couchbase  CLI subcommand"""

import sys

from optparse import HelpFormatter, OptionContainer, OptionGroup, OptionParser
from cluster_manager import ClusterManager
from node import _exitIfErrors

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
        "bucket-compact": BucketCompact,
        "bucket-create": BucketCreate,
        "bucket-delete": BucketDelete,
        "bucket-edit": BucketEdit,
        "bucket-flush": BucketFlush,
        "bucket-list": BucketList,
        "host-list": HostList,
        "server-list": ServerList,
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
