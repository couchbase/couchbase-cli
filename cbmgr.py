"""A Couchbase  CLI subcommand"""

import getpass
import inspect
import ipaddress
import json
import os
import platform
import random
import re
import string
import subprocess
import sys
import urllib.parse
import time

from argparse import ArgumentError, ArgumentParser, HelpFormatter, Action, SUPPRESS
from cluster_manager import ClusterManager
from pbar import TopologyProgressBar

try:
    from cb_version import VERSION  # pylint: disable=import-error
except ImportError:
    VERSION = "0.0.0-0000-community"
    print(f'WARNING: Could not import cb_version, setting VERSION to {VERSION}')

COUCHBASE_DEFAULT_PORT = 8091

BUCKET_PRIORITY_HIGH_INT = 8
BUCKET_PRIORITY_HIGH_STR = "high"
BUCKET_PRIORITY_LOW_INT = 3
BUCKET_PRIORITY_LOW_STR = "low"

BUCKET_TYPE_COUCHBASE = "membase"
BUCKET_TYPE_MEMCACHED = "memcached"

CB_BIN_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
CB_ETC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "etc", "couchbase"))
CB_LIB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "lib"))

# On MacOS the config is store in the users home directory
if platform.system() == "Darwin":
    CB_CFG_PATH = os.path.expanduser("~/Library/Application Support/Couchbase/var/lib/couchbase")
else:
    CB_CFG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "var", "lib", "couchbase"))

CB_MAN_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "share"))

if os.name == "nt":
    CB_MAN_PATH = os.path.join(CB_MAN_PATH, "html")
else:
    CB_MAN_PATH = os.path.join(CB_MAN_PATH, "man", "man1")

def check_cluster_initialized(rest):
    """Checks to see if the cluster is initialized"""
    initialized, errors = rest.is_cluster_initialized()
    if errors:
        _exitIfErrors(errors)
    if not initialized:
        _exitIfErrors(["Cluster is not initialized, use cluster-init to initialize the cluster"])

def index_storage_mode_to_param(value, default="plasma"):
    """Converts the index storage mode to what Couchbase understands"""
    if value == "default":
        return default
    elif value == "memopt":
        return "memory_optimized"
    else:
        return value

def process_services(services, enterprise):
    """Converts services to a format Couchbase understands"""
    sep = ","
    if services.find(sep) < 0:
        #backward compatible when using ";" as separator
        sep = ";"
    svc_set = set([w.strip() for w in services.split(sep)])
    svc_candidate = ["data", "index", "query", "fts", "eventing", "analytics"]
    for svc in svc_set:
        if svc not in svc_candidate:
            return None, [f'`{svc}` is not a valid service']
        if not enterprise and svc in ["eventing", "analytics"]:
            return None, [f'{svc} service is only available on Enterprise Edition']

    if not enterprise:
        # Valid CE node service configuration
        ce_svc_30 = set(["data"])
        ce_svc_40 = set(["data", "index", "query"])
        ce_svc_45 = set(["data", "index", "query", "fts"])
        if svc_set not in [ce_svc_30, ce_svc_40, ce_svc_45]:
            return None, [f"Invalid service configuration. Community Edition only supports nodes with the following"
                          f" combinations of services: '{''.join(ce_svc_30)}', '{','.join(ce_svc_40)}' or "
                          f"'{','.join(ce_svc_45)}'"]

    services = ",".join(svc_set)
    for old, new in [[";", ","], ["data", "kv"], ["query", "n1ql"], ["analytics", "cbas"]]:
        services = services.replace(old, new)
    return services, None

def find_subcommands():
    """Finds all subcommand classes"""
    clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
    subclasses = [cls for cls in clsmembers if issubclass(cls[1], (Subcommand, LocalSubcommand)) and cls[1] not in [Subcommand, LocalSubcommand]]

    subcommands = []
    for subclass in subclasses:
        name = '-'.join([part.lower() for part in re.findall('[A-Z][a-z]*', subclass[0])])
        subcommands.append((name, subclass[1]))
    return subcommands

def _success(msg):
    print(f'SUCCESS: {msg}')

def _deprecated(msg):
    print(f'DEPRECATED: {msg}')

def _warning(msg):
    print(f'WARNING: {msg}')

def _exitIfErrors(errors):
    if errors:
        for error in errors:
            print(f'ERROR: {error}')
        sys.exit(1)

def _exit_on_file_write_failure(fname, to_write):
    try:
        wfile = open(fname, 'w')
        wfile.write(to_write)
        wfile.close()
    except IOError as error:
        _exitIfErrors([error])

def _exit_on_file_read_failure(fname, toReport = None):
    try:
        rfile = open(fname, 'r')
        read_bytes = rfile.read()
        rfile.close()
        return read_bytes
    except IOError as error:
        if toReport is None:
            _exitIfErrors([f'{error.strerror} `{fname}`'])
        else:
            _exitIfErrors([toReport])

def apply_default_port(nodes):
    """
    Adds the default port if the port is missing.

    @type  nodes: string
    @param nodes: A comma seprated list of nodes
    @rtype:       array of strings
    @return:      The nodes with the port postfixed on each one
    """
    nodes = nodes.split(',')
    def append_port(node):
        if re.match('.*:\d+$', node):
            return node
        return f'{node}:8091'
    return [append_port(x) for x in nodes]


def check_versions(rest):
    result, errors = rest.pools()
    if errors:
        return

    server_version = result['implementationVersion']
    if server_version is None or VERSION is None:
        return

    major_couch = server_version[: server_version.index('.')]
    minor_couch = server_version[server_version.index('.') + 1: server_version.index('.', len(major_couch) + 1)]
    major_cli = VERSION[: VERSION.index('.')]
    minor_cli = VERSION[VERSION.index('.') + 1: VERSION.index('.', len(major_cli) + 1)]

    if major_cli != major_couch or minor_cli != minor_couch:
        _warning(f'couchbase-cli version {VERSION} does not match couchbase server version {server_version}')

class CLIHelpFormatter(HelpFormatter):
    """Format help with indented section bodies"""

    def __init__(self, prog, indent_increment=2, max_help_position=30, width=None):
        HelpFormatter.__init__(self, prog, indent_increment, max_help_position, width)

    def add_argument(self, action):
        if action.help is not SUPPRESS:

            # find all invocations
            get_invocation = self._format_action_invocation
            invocations = [get_invocation(action)]
            for subaction in self._iter_indented_subactions(action):
                invocations.append(get_invocation(subaction))

            # update the maximum item length
            invocation_length = max([len(s) for s in invocations])
            action_length = invocation_length + self._current_indent + 2
            self._action_max_length = max(self._action_max_length,
                                          action_length)

            # add the item to the list
            self._add_item(self._format_action, [action])

    def _format_action_invocation(self, action):
        if not action.option_strings:
            metavar, = self._metavar_formatter(action, action.dest)(1)
            return metavar
        else:
            parts = []
            if action.nargs == 0:
                parts.extend(action.option_strings)
                return ','.join(parts)
            else:
                default = action.dest
                args_string = self._format_args(action, default)
                for option_string in action.option_strings:
                    parts.append(option_string)
                return ','.join(parts) + ' ' +  args_string


class CBDeprecatedAction(Action):
    """Indicates that a specific option is deprecated"""

    def __call__(self, parser, namespace, values, option_string=None):
        _deprecated('Specifying ' + '/'.join(self.option_strings) + ' is deprecated')
        if self.nargs == 0:
            setattr(namespace, self.dest, self.const)
        else:
            setattr(namespace, self.dest, values)


class CBHostAction(Action):
    """Allows the handling of hostnames on the command line"""

    def __call__(self, parser, namespace, values, option_string=None):
        parsed = urllib.parse.urlparse(values)

        # If the netloc is empty then it means that there was no scheme added
        # to the URI and we are parsing it as a path. In this case no scheme
        # means HTTP so we can add that scheme to the hostname provided.
        if parsed.netloc == "":
            parsed = urllib.parse.urlparse("http://" + values)

        if parsed.scheme == "":
            parsed = urllib.parse.urlparse("http://" + values)

        if parsed.path != "" or parsed.params != "" or parsed.query != "" or parsed.fragment != "":
            raise ArgumentError(self, f"{values} is not an accepted hostname")
        if not parsed.hostname:
            raise ArgumentError(self, f"{values} is not an accepted hostname")
        hostname_regex = re.compile("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$");
        if not hostname_regex.match(parsed.hostname):
            try:
                ipaddress.ip_address(parsed.hostname)
            except ValueError:
                raise ArgumentError(self, f"{values} is not an accepted hostname")


        scheme = parsed.scheme
        port = None
        if scheme in ["http", "couchbase"]:
            if not parsed.port:
                port = 8091
            if scheme == "couchbase":
                scheme = "http"
        elif scheme in ["https", "couchbases"]:
            if not parsed.port:
                port = 18091
            if scheme == "couchbases":
                scheme = "https"
        else:
            raise ArgumentError(self, "%s is not an accepted scheme" % scheme)

        if parsed.port:
            setattr(namespace, self.dest, (scheme + "://" + parsed.netloc))
        else:
            setattr(namespace, self.dest, (scheme + "://" + parsed.netloc + ":" + str(port)))


class CBEnvAction(Action):
    """Allows the custom handling of environment variables for command line options"""

    def __init__(self, envvar, required=True, default=None, **kwargs):
        if not default and envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        super(CBEnvAction, self).__init__(default=default, required=required,
                                          **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


class CBNonEchoedAction(CBEnvAction):
    """Allows an argument to be specified by use of a non-echoed value passed through
    stdin, through an environment variable, or as a value to the argument"""

    def __init__(self, envvar, prompt_text="Enter password:", confirm_text=None,
                 required=True, default=None, nargs='?', **kwargs):
        self.prompt_text = prompt_text
        self.confirm_text = confirm_text
        super(CBNonEchoedAction, self).__init__(envvar, required=required, default=default,
                                                nargs=nargs, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if values == None:
            values = getpass.getpass(self.prompt_text)
            if self.confirm_text is not None:
                confirm = getpass.getpass(self.prompt_text)
                if values != confirm:
                    raise ArgumentError(self, "Passwords entered do not match, please retry")
        super(CBNonEchoedAction, self).__call__(parser, namespace, values, option_string=None)


class CBHelpAction(Action):
    """Allows the custom handling of the help command line argument"""

    def __init__(self, option_strings, klass, dest=SUPPRESS, default=SUPPRESS, help=None):
        super(CBHelpAction, self).__init__(option_strings=option_strings, dest=dest,
                                           default=default, nargs=0, help=help)
        self.klass = klass

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string == "-h":
            parser.print_help()
        else:
            CBHelpAction._show_man_page(self.klass.get_man_page_name())
        parser.exit()

    @staticmethod
    def _show_man_page(page):
        exe_path = os.path.abspath(sys.argv[0])
        base_path = os.path.dirname(exe_path)

        if os.name == "nt":
            try:
                subprocess.call(["rundll32.exe", "url.dll,FileProtocolHandler", os.path.join(CB_MAN_PATH, page)])
            except OSError as e:
                _exitIfErrors(["Unable to open man page using your browser, %s" % e])
        else:
            try:
                subprocess.call(["man", os.path.join(CB_MAN_PATH, page)])
            except OSError:
                _exitIfErrors(["Unable to open man page using the 'man' command, ensure it " +
                               "is on your path or install a manual reader"])


class CliParser(ArgumentParser):

    def __init__(self, *args, **kwargs):
        super(CliParser, self).__init__(*args, **kwargs)

    def error(self, message):
        self.exit(2, f'ERROR: {message}\n')


class Command(object):
    """A Couchbase CLI Command"""

    def __init__(self):
        self.parser = CliParser(formatter_class=CLIHelpFormatter, add_help=False, allow_abbrev=False)

    def parse(self, args):
        """Parses the subcommand"""
        if len(args) == 0:
            self.short_help()

        return self.parser.parse_args(args)

    def short_help(self, code=0):
        """Prints the short help message and exits"""
        self.parser.print_help()
        self.parser.exit(code)

    def execute(self, opts):
        """Executes the subcommand"""
        raise NotImplementedError

    @staticmethod
    def get_man_page_name():
        """Returns the man page name"""
        raise NotImplementedError

    @staticmethod
    def get_description():
        """Returns the command description"""
        raise NotImplementedError


class CouchbaseCLI(Command):
    """A Couchbase CLI command"""

    def __init__(self):
        super(CouchbaseCLI, self).__init__()
        self.parser.prog = "couchbase-cli"
        subparser = self.parser.add_subparsers(title="Commands", metavar="")

        for (name, klass) in find_subcommands():
            if klass.is_hidden():
                subcommand = subparser.add_parser(name)
            else:
                subcommand = subparser.add_parser(name, help=klass.get_description())
            subcommand.set_defaults(klass=klass)

        group = self.parser.add_argument_group("Options")
        group.add_argument("-h", "--help", action=CBHelpAction, klass=self,
                           help="Prints the short or long help message")
        group.add_argument("--version",  help="Get couchbase-cli version")

    def parse(self, args):
        if len(sys.argv) == 1:
            self.parser.print_help()
            self.parser.exit(1)

        if args[1] == "--version":
            print (VERSION)
            sys.exit(0)

        if not args[1] in ["-h", "--help", "--version"] and  args[1].startswith("-"):
            _exitIfErrors([f"Unknown subcommand: '{args[1]}'. The first argument has to be a subcommand like"
                           f" 'bucket-list' or 'rebalance', please see couchbase-cli -h for the full list of commands"
                           f" and options"])


        l1_args = self.parser.parse_args(args[1:2])
        l2_args = l1_args.klass().parse(args[2:])
        setattr(l2_args, 'klass', l1_args.klass)
        return l2_args

    def execute(self, opts):
        opts.klass().execute(opts)

    @staticmethod
    def get_man_page_name():
        """Returns the man page name"""
        return "couchbase-cli" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "A Couchbase cluster administration utility"


class Subcommand(Command):
    """
    A Couchbase CLI Subcommand: This is for subcommand that interact with a remote Couchbase Server over the REST API.
    """

    def __init__(self, deprecate_username=False, deprecate_password=False, cluster_default=None):
        super(Subcommand, self).__init__()
        self.parser = CliParser(formatter_class=CLIHelpFormatter, add_help=False, allow_abbrev=False)
        group = self.parser.add_argument_group("Cluster options")
        group.add_argument("-c", "--cluster", dest="cluster", required=(cluster_default==None),
                           metavar="<cluster>", action=CBHostAction, default=cluster_default,
                           help="The hostname of the Couchbase cluster")

        if deprecate_username:
            group.add_argument("-u", "--username", dest="username",
                               action=CBDeprecatedAction, help=SUPPRESS)
        else:
            group.add_argument("-u", "--username", dest="username", required=True,
                               action=CBEnvAction, envvar='CB_REST_USERNAME',
                               metavar="<username>", help="The username for the Couchbase cluster")

        if deprecate_password:
            group.add_argument("-p", "--password", dest="password",
                               action=CBDeprecatedAction, help=SUPPRESS)
        else:
            group.add_argument("-p", "--password", dest="password", required=True,
                               action=CBNonEchoedAction, envvar='CB_REST_PASSWORD',
                               metavar="<password>", help="The password for the Couchbase cluster")

        group.add_argument("-o", "--output", dest="output", default="standard", metavar="<output>",
                           choices=["json", "standard"], help="The output type (json or standard)")
        group.add_argument("-d", "--debug", dest="debug", action="store_true",
                           help="Run the command with extra logging")
        group.add_argument("-s", "--ssl", dest="ssl", const=True, default=False,
                           nargs=0, action=CBDeprecatedAction,
                           help="Use ssl when connecting to Couchbase (Deprecated)")
        group.add_argument("--no-ssl-verify", dest="ssl_verify", action="store_false", default=True,
                           help="Skips SSL verification of certificates against the CA")
        group.add_argument("--cacert", dest="cacert", default=True,
                           help="Verifies the cluster identity with this certificate")
        group.add_argument("-h", "--help", action=CBHelpAction, klass=self,
                           help="Prints the short or long help message")


    def execute(self, opts):
        super(Subcommand, self).execute(opts)

    @staticmethod
    def get_man_page_name():
        return Command.get_man_page_name()

    @staticmethod
    def get_description():
        return Command.get_description()

    @staticmethod
    def is_hidden():
        """Whether or not the subcommand should be hidden from the help message"""
        return False

class LocalSubcommand(Command):
    """
    A Couchbase CLI Localcommand: This is for subcommands that interact with the local Couchbase Server via the
    filesystem or a local socket.
    """

    def __init__(self):
        super(LocalSubcommand, self).__init__()
        self.parser = CliParser(formatter_class=CLIHelpFormatter, add_help=False, allow_abbrev=False)
        group = self.parser.add_argument_group(title="Local command options",
                                               description="This command has to be execute on the locally running" +
                                                           " Couchbase Server.")
        group.add_argument("-h", "--help", action=CBHelpAction, klass=self,
                           help="Prints the short or long help message")
        group.add_argument("--config-path", dest="config_path", metavar="<path>",
                           default=CB_CFG_PATH, help=SUPPRESS)

    def execute(self, opts):
        super(LocalSubcommand, self).execute(opts)

    @staticmethod
    def get_man_page_name():
        return Command.get_man_page_name()

    @staticmethod
    def get_description():
        return Command.get_description()

    @staticmethod
    def is_hidden():
        """Whether or not the subcommand should be hidden from the help message"""
        return False

class ClusterInit(Subcommand):
    """The cluster initialization subcommand"""

    def __init__(self):
        super(ClusterInit, self).__init__(True, True, "http://127.0.0.1:8091")
        self.parser.prog = "couchbase-cli cluster-init"
        group = self.parser.add_argument_group("Cluster initialization options")
        group.add_argument("--cluster-username", dest="username", required=True,
                           metavar="<username>", help="The cluster administrator username")
        group.add_argument("--cluster-password", dest="password", required=True,
                           metavar="<password>", help="Only compact the data files")
        group.add_argument("--cluster-port", dest="port", type=(int),
                           metavar="<port>", help="The cluster administration console port")
        group.add_argument("--cluster-ramsize", dest="data_mem_quota", type=(int),
                           metavar="<quota>", help="The data service memory quota in megabytes")
        group.add_argument("--cluster-index-ramsize", dest="index_mem_quota", type=(int),
                           metavar="<quota>", help="The index service memory quota in megabytes")
        group.add_argument("--cluster-fts-ramsize", dest="fts_mem_quota", type=(int),
                           metavar="<quota>",
                           help="The full-text service memory quota in Megabytes")
        group.add_argument("--cluster-eventing-ramsize", dest="eventing_mem_quota", type=(int),
                           metavar="<quota>",
                           help="The Eventing service memory quota in Megabytes")
        group.add_argument("--cluster-analytics-ramsize", dest="cbas_mem_quota", type=(int),
                           metavar="<quota>",
                           help="The analytics service memory quota in Megabytes")
        group.add_argument("--cluster-name", dest="name", metavar="<name>", help="The cluster name")
        group.add_argument("--index-storage-setting", dest="index_storage_mode",
                           choices=["default", "memopt"], metavar="<mode>",
                           help="The index storage backend (Defaults to \"default)\"")
        group.add_argument("--services", dest="services", default="data", metavar="<service_list>",
                           help="The services to run on this server")

    def execute(self, opts):
        # We need to ensure that creating the REST username/password is the
        # last REST API that is called because once that API succeeds the
        # cluster is initialized and cluster-init cannot be run again.

        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)

        initialized, errors = rest.is_cluster_initialized()
        _exitIfErrors(errors)
        if initialized:
            _exitIfErrors(["Cluster is already initialized, use setting-cluster to change settings"])

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if not enterprise and opts.index_storage_mode == 'memopt':
            _exitIfErrors(["memopt option for --index-storage-setting can only be configured on enterprise edition"])

        services, errors = process_services(opts.services, enterprise)
        _exitIfErrors(errors)

        if 'kv' not in services.split(','):
            _exitIfErrors(["Cannot set up first cluster node without the data service"])

        if opts.data_mem_quota or opts.index_mem_quota or opts.fts_mem_quota or opts.cbas_mem_quota \
                or opts.eventing_mem_quota or opts.name is not None:
            _, errors = rest.set_pools_default(opts.data_mem_quota, opts.index_mem_quota, opts.fts_mem_quota,
                                               opts.cbas_mem_quota, opts.eventing_mem_quota, opts.name)
        _exitIfErrors(errors)

        # Set the index storage mode
        if not opts.index_storage_mode and 'index' in services.split(','):
            opts.index_storage_mode = "default"

        default = "plasma"
        if not enterprise:
            default = "forestdb"

        if opts.index_storage_mode:
            param = index_storage_mode_to_param(opts.index_storage_mode, default)
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

        _success("Cluster initialized")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-cluster-init" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Initialize a Couchbase cluster"


class BucketCompact(Subcommand):
    """The bucket compact subcommand"""

    def __init__(self):
        super(BucketCompact, self).__init__()
        self.parser.prog = "couchbase-cli bucket-compact"
        group = self.parser.add_argument_group("Bucket compaction options")
        group.add_argument("--bucket", dest="bucket_name", metavar="<name>",
                           help="The name of bucket to compact")
        group.add_argument("--data-only", dest="data_only", action="store_true",
                           help="Only compact the data files")
        group.add_argument("--view-only", dest="view_only", action="store_true",
                           help="Only compact the view files")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        bucket, errors = rest.get_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        if bucket["bucketType"] != BUCKET_TYPE_COUCHBASE:
            _exitIfErrors(["Cannot compact memcached buckets"])

        _, errors = rest.compact_bucket(opts.bucket_name, opts.data_only, opts.view_only)
        _exitIfErrors(errors)

        _success("Bucket compaction started")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-bucket-compact" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Compact database and view data"


class BucketCreate(Subcommand):
    """The bucket create subcommand"""

    def __init__(self):
        super(BucketCreate, self).__init__()
        self.parser.prog = "couchbase-cli bucket-create"
        group = self.parser.add_argument_group("Bucket create options")
        group.add_argument("--bucket", dest="bucket_name", metavar="<name>", required=True,
                           help="The name of bucket to create")
        group.add_argument("--bucket-type", dest="type", metavar="<type>", required=True,
                           choices=["couchbase", "ephemeral", "memcached"],
                           help="The bucket type (couchbase, ephemeral, or memcached)")
        group.add_argument("--bucket-ramsize", dest="memory_quota", metavar="<quota>", type=(int),
                           required=True, help="The amount of memory to allocate the bucket")
        group.add_argument("--bucket-replica", dest="replica_count", metavar="<num>",
                           choices=["0", "1", "2", "3"],
                           help="The replica count for the bucket")
        group.add_argument("--bucket-priority", dest="priority", metavar="<priority>",
                           choices=[BUCKET_PRIORITY_LOW_STR, BUCKET_PRIORITY_HIGH_STR],
                           help="The bucket disk io priority (low or high)")
        group.add_argument("--bucket-eviction-policy", dest="eviction_policy", metavar="<policy>",
                           choices=["valueOnly", "fullEviction", "noEviction", "nruEviction"],
                           help="The bucket eviction policy")
        group.add_argument("--conflict-resolution", dest="conflict_resolution", default=None,
                           choices=["sequence", "timestamp"], metavar="<type>",
                           help="The XDCR conflict resolution type (timestamp or sequence)")
        group.add_argument("--max-ttl", dest="max_ttl", default=None, type=(int), metavar="<seconds>",
                           help="Set the maximum TTL the bucket will accept. Couchbase server Enterprise Edition only.")
        group.add_argument("--compression-mode", dest="compression_mode",
                           choices=["off", "passive", "active"], metavar="<mode>",
                           help="Set the compression mode of the bucket")
        group.add_argument("--enable-flush", dest="enable_flush", metavar="<0|1>",
                           choices=["0", "1"], help="Enable bucket flush on this bucket (0 or 1)")
        group.add_argument("--enable-index-replica", dest="replica_indexes", metavar="<0|1>",
                           choices=["0", "1"], help="Enable replica indexes (0 or 1)")
        group.add_argument("--wait", dest="wait", action="store_true",
                           help="Wait for bucket creation to complete")
        group.add_argument("--database-fragmentation-threshold-percentage", dest="db_frag_perc",
                           metavar="<perc>", type=(int), help="Set Database Fragmentation level percent")

        group.add_argument("--database-fragmentation-threshold-size", dest="db_frag_size",
                           metavar="<megabytes>", type=(int), help="Set Database Fragmentation level")

        group.add_argument("--view-fragmentation-threshold-percentage", dest="view_frag_perc",
                           metavar="<perc>", type=(int), help="Set View Fragmentation level percent")

        group.add_argument("--view-fragmentation-threshold-size", dest="view_frag_size",
                           metavar="<megabytes>", type=(int), help="Set View Fragmentation level size")

        group.add_argument("--from-hour", dest="from_hour",
                           metavar="<quota>", type=(int), help="Set start time hour")
        group.add_argument("--from-minute", dest="from_min",
                           metavar="<quota>", type=(int), help="Set start time minutes")
        group.add_argument("--to-hour", dest="to_hour",
                           metavar="<quota>", type=(int), help="Set end time hour")
        group.add_argument("--to-minute", dest="to_min",
                           metavar="<quota>", type=(int), help="Set end time minutes")

        group.add_argument("--abort-outside", dest="abort_outside",
                           metavar="<0|1>", choices=["0", "1"], help="Allow Time period")
        group.add_argument("--parallel-db-view-compaction", dest="paralleldb_and_view_compact",
                           metavar="<0|1>", choices=["0", "1"], help="Set parallel DB and View Compaction")

        group.add_argument("--purge-interval", dest="purge_interval", type=(float),
                           metavar="<float>", help="Set parallel DB and View Compaction")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if opts.max_ttl and not enterprise:
            _exitIfErrors(["Maximum TTL can only be configured on enterprise edition"])
        if opts.compression_mode and not enterprise:
            _exitIfErrors(["Compression mode can only be configured on enterprise edition"])

        if opts.type == "memcached":
            if opts.replica_count is not None:
                _exitIfErrors(["--bucket-replica cannot be specified for a memcached bucket"])
            if opts.conflict_resolution is not None:
                _exitIfErrors(["--conflict-resolution cannot be specified for a memcached bucket"])
            if opts.replica_indexes is not None:
                _exitIfErrors(["--enable-index-replica cannot be specified for a memcached bucket"])
            if opts.priority is not None:
                _exitIfErrors(["--bucket-priority cannot be specified for a memcached bucket"])
            if opts.eviction_policy is not None:
                _exitIfErrors(["--bucket-eviction-policy cannot be specified for a memcached bucket"])
            if opts.max_ttl is not None:
                _exitIfErrors(["--max-ttl cannot be specified for a memcached bucket"])
            if opts.compression_mode is not None:
                _exitIfErrors(["--compression-mode cannot be specified for a memcached bucket"])
        elif opts.type == "ephemeral":
            if opts.eviction_policy in ["valueOnly", "fullEviction"]:
                _exitIfErrors(["--bucket-eviction-policy must either be noEviction or nruEviction"])
        elif opts.type == "couchbase":
            if opts.eviction_policy in ["noEviction", "nruEviction"]:
                _exitIfErrors(["--bucket-eviction-policy must either be valueOnly or fullEviction"])

        if ((opts.type == "memcached" or opts.type == "ephemeral") and (opts.db_frag_perc is not None or
                opts.db_frag_size is not None or opts.view_frag_perc is not None or
                opts.view_frag_size is not None or opts.from_hour is not None or opts.from_min is not None or
                opts.to_hour is not None or opts.to_min is not None or opts.abort_outside is not None or
                opts.paralleldb_and_view_compact is not None)):
            _warning(f'ignoring compaction settings as bucket type {opts.type} does not accept it')


        priority = None
        if opts.priority is not None:
            if opts.priority == BUCKET_PRIORITY_HIGH_STR:
                priority = BUCKET_PRIORITY_HIGH_INT
            elif opts.priority == BUCKET_PRIORITY_LOW_STR:
                priority = BUCKET_PRIORITY_LOW_INT

        conflict_resolution_type = None
        if opts.conflict_resolution is not None:
            if opts.conflict_resolution == "sequence":
                conflict_resolution_type = "seqno"
            elif opts.conflict_resolution == "timestamp":
                conflict_resolution_type = "lww"

        _, errors = rest.create_bucket(opts.bucket_name, opts.type, opts.memory_quota, opts.eviction_policy,
                                       opts.replica_count, opts.replica_indexes, priority, conflict_resolution_type,
                                       opts.enable_flush, opts.max_ttl, opts.compression_mode, opts.wait,
                                       opts.db_frag_perc, opts.db_frag_size, opts.view_frag_perc, opts.view_frag_size,
                                       opts.from_hour, opts.from_min, opts.to_hour, opts.to_min, opts.abort_outside,
                                       opts.paralleldb_and_view_compact, opts.purge_interval)
        _exitIfErrors(errors)
        _success("Bucket created")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-bucket-create" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Add a new bucket to the cluster"


class BucketDelete(Subcommand):
    """The bucket delete subcommand"""

    def __init__(self):
        super(BucketDelete, self).__init__()
        self.parser.prog = "couchbase-cli bucket-delete"
        group = self.parser.add_argument_group("Bucket delete options")
        group.add_argument("--bucket", dest="bucket_name", metavar="<name>", required=True,
                           help="The name of bucket to delete")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        _, errors = rest.get_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        _, errors = rest.delete_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        _success("Bucket deleted")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-bucket-delete" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Delete an existing bucket"


class BucketEdit(Subcommand):
    """The bucket edit subcommand"""

    def __init__(self):
        super(BucketEdit, self).__init__()
        self.parser.prog = "couchbase-cli bucket-edit"
        group = self.parser.add_argument_group("Bucket edit options")
        group.add_argument("--bucket", dest="bucket_name", metavar="<name>", required=True,
                           help="The name of bucket to create")
        group.add_argument("--bucket-ramsize", dest="memory_quota", metavar="<quota>",
                           type=(int), help="The amount of memory to allocate the bucket")
        group.add_argument("--bucket-replica", dest="replica_count", metavar="<num>",
                           choices=["0", "1", "2", "3"],
                           help="The replica count for the bucket")
        group.add_argument("--bucket-priority", dest="priority", metavar="<priority>",
                           choices=["low", "high"], help="The bucket disk io priority (low or high)")
        group.add_argument("--bucket-eviction-policy", dest="eviction_policy", metavar="<policy>",
                           choices=["valueOnly", "fullEviction"],
                           help="The bucket eviction policy (valueOnly or fullEviction)")
        group.add_argument("--max-ttl", dest="max_ttl", default=None, type=(int), metavar="<seconds>",
                           help="Set the maximum TTL the bucket will accept")
        group.add_argument("--compression-mode", dest="compression_mode",
                           choices=["off", "passive", "active"], metavar="<mode>",
                           help="Set the compression mode of the bucket")
        group.add_argument("--enable-flush", dest="enable_flush", metavar="<0|1>",
                           choices=["0", "1"], help="Enable bucket flush on this bucket (0 or 1)")
        group.add_argument("--remove-bucket-port", dest="remove_port", metavar="<0|1>",
                           choices=["0", "1"], help="Removes the bucket-port setting")
        group.add_argument("--database-fragmentation-threshold-percentage", dest="db_frag_perc",
                           metavar="<perc>", type=(int), help="Set Database Fragmentation level percent")

        group.add_argument("--database-fragmentation-threshold-size", dest="db_frag_size",
                           metavar="<megabytes>", type=(int), help="Set Database Fragmentation level")

        group.add_argument("--view-fragmentation-threshold-percentage", dest="view_frag_perc",
                           metavar="<perc>", type=(int), help="Set View Fragmentation level percent")

        group.add_argument("--view-fragmentation-threshold-size", dest="view_frag_size",
                           metavar="<megabytes>", type=(int), help="Set View Fragmentation level size")

        group.add_argument("--from-hour", dest="from_hour",
                           metavar="<hour>", type=(int), help="Set start time hour")
        group.add_argument("--from-minute", dest="from_min",
                           metavar="<min>", type=(int), help="Set start time minutes")
        group.add_argument("--to-hour", dest="to_hour",
                           metavar="<hour>", type=(int), help="Set end time hour")
        group.add_argument("--to-minute", dest="to_min",
                           metavar="<min>", type=(int), help="Set end time minutes")

        group.add_argument("--abort-outside", dest="abort_outside",
                           metavar="<0|1>", choices=["0", "1"], help="Allow Time period")
        group.add_argument("--parallel-db-view-compaction", dest="paralleldb_and_view_compact",
                           metavar="<0|1>", choices=["0", "1"], help="Set parallel DB and View Compaction")

        group.add_argument("--purge-interval", dest="purge_interval", type=(float),
                           metavar="<num>", help="Set parallel DB and View Compaction")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if opts.max_ttl and not enterprise:
            _exitIfErrors(["Maximum TTL can only be configured on enterprise edition"])

        if opts.compression_mode and not enterprise:
            _exitIfErrors(["Compression mode can only be configured on enterprise edition"])

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
            if opts.max_ttl is not None:
                _exitIfErrors(["--max-ttl cannot be specified for a memcached bucket"])
            if opts.compression_mode is not None:
                _exitIfErrors(["--compression-mode cannot be specified for a memcached bucket"])

        if (("bucketType" in bucket and (bucket["bucketType"] == "memcached" or bucket["bucketType"] == "ephemeral"))
                and (opts.db_frag_perc is not None or opts.db_frag_size is not None or
                     opts.view_frag_perc is not None or opts.view_frag_size is not None or opts.from_hour is not None or
                     opts.from_min is not None or opts.to_hour is not None or opts.to_min is not None or
                     opts.abort_outside is not None or opts.paralleldb_and_view_compact is not None)):
            _exitIfErrors([f'compaction settings can not be specified for a {bucket["bucketType"]} bucket'])

        priority = None
        if opts.priority is not None:
            if opts.priority == BUCKET_PRIORITY_HIGH_STR:
                priority = BUCKET_PRIORITY_HIGH_INT
            elif opts.priority == BUCKET_PRIORITY_LOW_STR:
                priority = BUCKET_PRIORITY_LOW_INT

        if opts.remove_port:
            if opts.remove_port == '1':
                opts.remove_port = True
            else:
                opts.remove_port = False

        _, errors = rest.edit_bucket(opts.bucket_name, opts.memory_quota, opts.eviction_policy, opts.replica_count,
                                     priority, opts.enable_flush, opts.max_ttl, opts.compression_mode, opts.remove_port,
                                     opts.db_frag_perc, opts.db_frag_size, opts.view_frag_perc, opts.view_frag_size,
                                     opts.from_hour, opts.from_min, opts.to_hour, opts.to_min, opts.abort_outside,
                                     opts.paralleldb_and_view_compact, opts.purge_interval)
        _exitIfErrors(errors)

        _success("Bucket edited")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-bucket-edit" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify settings for an existing bucket"


class BucketFlush(Subcommand):
    """The bucket edit subcommand"""

    def __init__(self):
        super(BucketFlush, self).__init__()
        self.parser.prog = "couchbase-cli bucket-flush"
        group = self.parser.add_argument_group("Bucket flush options")
        group.add_argument("--bucket", dest="bucket_name", metavar="<name>", required=True,
                           help="The name of bucket to delete")
        group.add_argument("--force", dest="force", action="store_true",
                           help="Execute the command without asking to confirm")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        _, errors = rest.get_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        if not opts.force:
            question = "Running this command will totally PURGE database data from disk. " + \
                       "Do you really want to do it? (Yes/No)"
            confirm = input(question)
            if confirm not in ('y', 'Y', 'yes', 'Yes'):
                return

        _, errors = rest.flush_bucket(opts.bucket_name)
        _exitIfErrors(errors)

        _success("Bucket flushed")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-bucket-flush" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Flush all data from disk for a given bucket"


class BucketList(Subcommand):
    """The bucket list subcommand"""

    def __init__(self):
        super(BucketList, self).__init__()
        self.parser.prog = "couchbase-cli bucket-list"

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)

        result, errors = rest.list_buckets(extended=True)
        _exitIfErrors(errors)

        if opts.output == 'json':
            print(json.dumps(result))
        else:
            for bucket in result:
                print(f'{bucket["name"]}')
                print(f' bucketType: {bucket["bucketType"]}')
                print(f' numReplicas: {bucket["replicaNumber"]}')
                print(f' ramQuota: {bucket["quota"]["ram"]}')
                print(f' ramUsed: {bucket["basicStats"]["memUsed"]}')

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-bucket-list" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "List all buckets in a cluster"


class CollectLogsStart(Subcommand):
    """The collect-logs-start subcommand"""

    def __init__(self):
        super(CollectLogsStart, self).__init__()
        self.parser.prog = "couchbase-cli collect-logs-start"
        group = self.parser.add_argument_group("Collect logs start options")
        group.add_argument("--all-nodes", dest="all_nodes", action="store_true",
                           default=False, help="Collect logs for all nodes")
        group.add_argument("--nodes", dest="nodes", metavar="<node_list>",
                           help="A comma separated list of nodes to collect logs from")
        group.add_argument("--redaction-level", dest="redaction_level", metavar="<none|partial>",
                           choices=["none", "partial"], help="Level of log redaction to apply")
        group.add_argument("--salt", dest="salt", metavar="<string>",
                           help="The salt to use to redact the log")
        group.add_argument("--output-directory", dest="output_dir", metavar="<directory>",
                           help="Output directory to place the generated logs file")
        group.add_argument("--temporary-directory", dest="tmp_dir", metavar="<directory>",
                           help="Temporary directory to use when generating the logs")
        group.add_argument("--upload", dest="upload", action="store_true",
                           default=False, help="Logs should be uploaded for Couchbase support")
        group.add_argument("--upload-host", dest="upload_host", metavar="<host>",
                           help="The host to upload logs to")
        group.add_argument("--upload-proxy", dest="upload_proxy", metavar="<proxy>",
                           help="The proxy to used to upload the logs via")
        group.add_argument("--customer", dest="upload_customer", metavar="<name>",
                           help="The name of the customer uploading logs")
        group.add_argument("--ticket", dest="upload_ticket", metavar="<num>",
                           help="The ticket number the logs correspond to")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        if not opts.nodes and not opts.all_nodes:
            _exitIfErrors(["Must specify either --all-nodes or --nodes"])

        if opts.nodes and opts.all_nodes:
            _exitIfErrors(["Cannot specify both --all-nodes and --nodes"])

        if opts.salt and opts.redaction_level != "partial":
            _exitIfErrors(["--redaction-level has to be set to 'partial' when --salt is specified"])

        servers = opts.nodes
        if opts.all_nodes:
            servers = "*"

        if opts.upload:
            if not opts.upload_host:
                _exitIfErrors(["--upload-host is required when --upload is specified"])
            if not opts.upload_customer:
                _exitIfErrors(["--upload-customer is required when --upload is specified"])
        else:
            if opts.upload_host:
                _warning("--upload-host has no effect with specifying --upload")
            if opts.upload_customer:
                _warning("--upload-customer has no effect with specifying --upload")
            if opts.upload_ticket:
                _warning("--upload_ticket has no effect with specifying --upload")
            if opts.upload_proxy:
                _warning("--upload_proxy has no effect with specifying --upload")

        _, errors = rest.collect_logs_start(servers, opts.redaction_level, opts.salt, opts.output_dir, opts.tmp_dir,
                                            opts.upload, opts.upload_host, opts.upload_proxy, opts.upload_customer,
                                            opts.upload_ticket)
        _exitIfErrors(errors)
        _success("Log collection started")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-collect-logs-start" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Start cluster log collection"


class CollectLogsStatus(Subcommand):
    """The collect-logs-status subcommand"""

    def __init__(self):
        super(CollectLogsStatus, self).__init__()
        self.parser.prog = "couchbase-cli collect-logs-status"

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        tasks, errors = rest.get_tasks()
        _exitIfErrors(errors)

        found = False
        for task in tasks:
            if isinstance(task, dict) and 'type' in task and task['type'] == 'clusterLogsCollection':
                found = True
                self._print_task(task)

        if not found:
            print("No log collection tasks were found")

    def _print_task(self, task):
        print(f'Status: {task["status"]}')
        if 'perNode' in task:
            print("Details:")
            for node, node_status in task["perNode"].items():
                print('\tNode:', node)
                print('\tStatus:', node_status['status'])
                for field in ["path", "statusCode", "url", "uploadStatusCode", "uploadOutput"]:
                    if field in node_status:
                        print('\t', field, ":", node_status[field])
            print()

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-collect-logs-status" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "View the status of cluster log collection"


class CollectLogsStop(Subcommand):
    """The collect-logs-stop subcommand"""

    def __init__(self):
        super(CollectLogsStop, self).__init__()
        self.parser.prog = "couchbase-cli collect-logs-stop"

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)

        _, errors = rest.collect_logs_stop()
        _exitIfErrors(errors)

        _success("Log collection stopped")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-collect-logs-stop" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Stop cluster log collection"


class Failover(Subcommand):
    """The failover subcommand"""

    def __init__(self):
        super(Failover, self).__init__()
        self.parser.prog = "couchbase-cli failover"
        group = self.parser.add_argument_group("Failover options")
        group.add_argument("--server-failover", dest="servers_to_failover", metavar="<server_list>",
                           required=True, help="A list of servers to fail over")
        group.add_argument("--force", dest="force", action="store_true",
                           help="Hard failover the server")
        group.add_argument("--no-progress-bar", dest="no_bar", action="store_true",
                           default=False, help="Disables the progress bar")
        group.add_argument("--no-wait", dest="wait", action="store_false",
                           default=True, help="Don't wait for rebalance completion")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        opts.servers_to_failover = apply_default_port(opts.servers_to_failover)
        _, errors = rest.failover(opts.servers_to_failover, opts.force)
        _exitIfErrors(errors)

        if not opts.force:
            time.sleep(1)
            if opts.wait:
                bar = TopologyProgressBar(rest, 'Gracefully failing over', opts.no_bar)
                errors = bar.show()
                _exitIfErrors(errors)
                _success("Server failed over")
            else:
                _success("Server failed over started")

        else:
            _success("Server failed over")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-failover" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Failover one or more servers"


class GroupManage(Subcommand):
    """The group manage subcommand"""

    def __init__(self):
        super(GroupManage, self).__init__()
        self.parser.prog = "couchbase-cli group-manage"
        group = self.parser.add_argument_group("Group manage options")
        group.add_argument("--create", dest="create", action="store_true",
                           default=None, help="Create a new server group")
        group.add_argument("--delete", dest="delete", action="store_true",
                           default=None, help="Delete a server group")
        group.add_argument("--list", dest="list", action="store_true",
                           default=None, help="List all server groups")
        group.add_argument("--rename", dest="rename", help="Rename a server group. It takes the new name of the group.")
        group.add_argument("--group-name", dest="name", metavar="<name>",
                           help="The name of the server group")
        group.add_argument("--move-servers", dest="move_servers", metavar="<server_list>",
                           help="A list of servers to move between groups")
        group.add_argument("--from-group", dest="from_group", metavar="<group>",
                           help="The group to move servers from")
        group.add_argument("--to-group", dest="to_group", metavar="<group>",
                           help="The group to move servers to")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        cmds = [opts.create, opts.delete, opts.list, opts.rename, opts.move_servers]
        if sum(cmd is not None for cmd in cmds) == 0:
            _exitIfErrors(["Must specify one of the following: --create, " +
                           "--delete, --list, --move-servers, or --rename"])
        elif sum(cmd is not None for cmd in cmds) != 1:
            _exitIfErrors(["Only one of the following may be specified: --create" +
                           ", --delete, --list, --move-servers, or --rename"])

        if opts.create:
            self._create(rest, opts)
        elif opts.delete:
            self._delete(rest, opts)
        elif opts.list:
            self._list(rest, opts)
        elif opts.rename:
            self._rename(rest, opts)
        elif opts.move_servers is not None:
            self._move(rest, opts)

    def _create(self, rest, opts):
        if opts.name is None:
            _exitIfErrors(["--group-name is required with --create flag"])
        _, errors = rest.create_server_group(opts.name)
        _exitIfErrors(errors)
        _success("Server group created")

    def _delete(self, rest, opts):
        if opts.name is None:
            _exitIfErrors(["--group-name is required with --delete flag"])
        _, errors = rest.delete_server_group(opts.name)
        _exitIfErrors(errors)
        _success("Server group deleted")

    def _list(self, rest, opts):
        groups, errors = rest.get_server_groups()
        _exitIfErrors(errors)

        found = False
        for group in groups["groups"]:
            if opts.name is None or opts.name == group['name']:
                found = True
                print(group['name'])
                for node in group['nodes']:
                    print(f' server: {node["hostname"]}')
        if not found and opts.name:
            _exitIfErrors([f'Invalid group name: {opts.name}'])

    def _move(self, rest, opts):
        if opts.from_group is None:
            _exitIfErrors(["--from-group is required with --move-servers"])
        if opts.to_group is None:
            _exitIfErrors(["--to-group is required with --move-servers"])

        servers = apply_default_port(opts.move_servers)
        _, errors = rest.move_servers_between_groups(servers, opts.from_group, opts.to_group)
        _exitIfErrors(errors)
        _success("Servers moved between groups")

    def _rename(self, rest, opts):
        if opts.name is None:
            _exitIfErrors(["--group-name is required with --rename option"])
        _, errors = rest.rename_server_group(opts.name, opts.rename)
        _exitIfErrors(errors)
        _success("Server group renamed")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-group-manage" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Manage server groups"


class HostList(Subcommand):
    """The host list subcommand"""

    def __init__(self):
        super(HostList, self).__init__()
        self.parser.prog = "couchbase-cli host-list"

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)
        result, errors = rest.pools('default')
        _exitIfErrors(errors)

        if opts.output == 'json':
            nodes_out = {'nodes': []}
            for node in result['nodes']:
                nodes_out['nodes'].append(node['configuredHostname'])
            print(json.dumps(nodes_out))
        else:
            for node in result['nodes']:
                print(node['configuredHostname'])

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-host-list" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "List all hosts in a cluster"


class ResetCipherSuites(LocalSubcommand):
    """The reset cipher suites subcommand """

    def __init__(self):
        super(ResetCipherSuites, self).__init__()
        self.parser.prog = "couchbase-cli reset-cipher-suites"
        group = self.parser.add_argument_group("Reset Cipher Suites")
        group.add_argument("--force", action='store_true', default=False, help="Force resetting of the cipher suites")
        group.add_argument("-P", "--port", metavar="<port>", default="8091",
                           help="The REST API port, defaults to 8091")

    def execute(self, opts):
        token = _exit_on_file_read_failure(os.path.join(opts.config_path, "localtoken")).rstrip()
        rest = ClusterManager("http://127.0.0.1:" + opts.port, "@localtoken", token)
        check_cluster_initialized(rest)
        check_versions(rest)

        if not opts.force:
            confirm = str(input("Are you sure that the cipher should be reset?: Y/[N]"))
            if confirm != "Y":
                _success("Cipher suites have not been reset to default")

        _, errors = rest.reset_cipher_suites()
        _exitIfErrors(errors)
        _success("Cipher suites have been reset to the default")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-reset-cipher-suites" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Rests cipher suites to the default"


class MasterPassword(LocalSubcommand):
    """The master password subcommand"""

    def __init__(self):
        super(MasterPassword, self).__init__()
        self.parser.prog = "couchbase-cli master-password"
        group = self.parser.add_argument_group("Master password options")
        group.add_argument("--send-password", dest="send_password", metavar="<password>",
                           required=False, action=CBNonEchoedAction, envvar=None,
                           prompt_text="Enter master password:",
                           help="Sends the master password to start the server")

    def execute(self, opts):
        if opts.send_password is not None:
            path = [CB_BIN_PATH, os.environ['PATH']]
            if os.name == 'posix':
                os.environ['PATH'] = ':'.join(path)
            else:
                os.environ['PATH'] = ';'.join(path)

            cookiefile = os.path.join(opts.config_path, "couchbase-server.babysitter.cookie")
            cookie = _exit_on_file_read_failure(cookiefile, "The node is down").rstrip()

            nodefile = os.path.join(opts.config_path, "couchbase-server.babysitter.node")
            node = _exit_on_file_read_failure(nodefile).rstrip()

            self.prompt_for_master_pwd(node, cookie, opts.send_password, opts.config_path)
        else :
            _exitIfErrors(["No parameters set"])

    def prompt_for_master_pwd(self, node, cookie, password, cb_cfg_path):
        ns_server_ebin_path = os.path.join(CB_LIB_PATH, "ns_server", "erlang", "lib", "ns_server", "ebin")
        babystr_ebin_path = os.path.join(CB_LIB_PATH, "ns_server", "erlang", "lib", "ns_babysitter", "ebin")
        inetrc_file = os.path.join(CB_ETC_PATH, "hosts.cfg")
        dist_cfg_file = os.path.join(cb_cfg_path, "config", "dist_cfg")

        if password == '':
            password = getpass.getpass("\nEnter master password:")

        name = f'executioner@cb.local'
        args = ['-pa', ns_server_ebin_path, babystr_ebin_path, '-noinput', '-name', name, \
                '-proto_dist', 'cb', '-epmd_module', 'cb_epmd', \
                '-kernel', 'inetrc', f'"{inetrc_file}"', 'dist_config_file', f'"{dist_cfg_file}"', \
                '-setcookie', cookie, \
                '-run', 'encryption_service', 'remote_set_password', node, password]

        rc, out, err = self.run_process("erl", args)

        if rc == 0:
            print("SUCCESS: Password accepted. Node started booting.")
        elif rc == 101:
            print("Incorrect password.")
            self.prompt_for_master_pwd(node, cookie, '', cb_cfg_path)
        elif rc == 102:
            _exitIfErrors(["Password was already supplied"])
        elif rc == 103:
            _exitIfErrors(["The node is down"])
        elif rc == 104:
            _exitIfErrors(["Incorrect password. Node shuts down."])
        else:
            _exitIfErrors([f'Unknown error: {rc} {out}, {err}'])

    def run_process(self, name, args):
        try:
            if os.name == "nt":
                name = name + ".exe"

            args.insert(0, name)
            p = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            output = p.stdout.read()
            error = p.stderr.read()
            p.wait()
            rc = p.returncode
            return rc, output, error
        except OSError:
            _exitIfErrors([f'Could not locate the {name} executable'])

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-master-password" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Unlocking the master password"


class NodeInit(Subcommand):
    """The node initialization subcommand"""

    def __init__(self):
        super(NodeInit, self).__init__()
        self.parser.prog = "couchbase-cli node-init"
        group = self.parser.add_argument_group("Node initialization options")
        group.add_argument("--node-init-data-path", dest="data_path", metavar="<path>",
                           help="The path to store database files")
        group.add_argument("--node-init-index-path", dest="index_path", metavar="<path>",
                           help="The path to store index files")
        group.add_argument("--node-init-analytics-path", dest="analytics_path", metavar="<path>", action="append",
                           help="The path to store analytics files (supply one parameter for each path desired)")
        group.add_argument("--node-init-java-home", dest="java_home", metavar="<path>",
                           help="The path of the Java Runtime Environment (JRE) to use on this server")
        group.add_argument("--node-init-hostname", dest="hostname", metavar="<hostname>",
                           help="Sets the hostname for this server")
        group.add_argument("--ipv6", dest="ipv6", action="store_true", default=False,
                           help="Configure the node to communicate via ipv6")
        group.add_argument("--ipv4", dest="ipv4", action="store_true", default=False,
                           help="Configure the node to communicate via ipv4")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        # Cluster does not need to be initialized for this command

        if opts.data_path is None and opts.index_path is None and opts.analytics_path is None \
            and opts.java_home is None and opts.hostname is None and opts.ipv6 is None and opts.ipv4 is None:
            _exitIfErrors(["No node initialization parameters specified"])

        if opts.data_path or opts.index_path or opts.analytics_path or opts.java_home is not None:
            _, errors = rest.set_data_paths(opts.data_path, opts.index_path, opts.analytics_path, opts.java_home)
            _exitIfErrors(errors)

        if opts.ipv4 and opts.ipv6:
            _exitIfErrors(["Use either --ipv4 or --ipv6"])

        if opts.ipv6:
            self._set_ipv(rest, "ipv6", "ipv4")
        elif opts.ipv4:
            self._set_ipv(rest, "ipv4", "ipv6")

        if opts.hostname:
            _, errors = rest.set_hostname(opts.hostname)
            _exitIfErrors(errors)

        _success("Node initialized")

    def _set_ipv(self, rest, ip_enable, ip_disable):
        _, err = rest.enable_external_listener(ipfamily=ip_enable)
        _exitIfErrors(err)
        _, err = rest.setup_net_config(ipfamily=ip_enable)
        _exitIfErrors(err)
        _, err = rest.disable_external_listener(ipfamily=ip_disable)
        _exitIfErrors(err)

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-node-init" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Set node specific settings"


class Rebalance(Subcommand):
    """The rebalance subcommand"""

    def __init__(self):
        super(Rebalance, self).__init__()
        self.parser.prog = "couchbase-cli rebalance"
        group = self.parser.add_argument_group("Rebalance options")
        group.add_argument("--server-remove", dest="server_remove", metavar="<server_list>",
                           help="A list of servers to remove from the cluster")
        group.add_argument("--no-progress-bar", dest="no_bar", action="store_true",
                           default=False, help="Disables the progress bar")
        group.add_argument("--no-wait", dest="wait", action="store_false",
                           default=True, help="Don't wait for rebalance completion")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        eject_nodes = []
        if opts.server_remove:
            eject_nodes = apply_default_port(opts.server_remove)

        _, errors = rest.rebalance(eject_nodes)
        _exitIfErrors(errors)

        time.sleep(1)

        if opts.wait:
            bar = TopologyProgressBar(rest, 'Rebalancing', opts.no_bar)
            errors = bar.show()
            _exitIfErrors(errors)
            _success("Rebalance complete")
        else:
            _success("Rebalance started")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-rebalance" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Start a cluster rebalancing"


class RebalanceStatus(Subcommand):
    """The rebalance status subcommand"""

    def __init__(self):
        super(RebalanceStatus, self).__init__()
        self.parser.prog = "couchbase-cli rebalance-status"

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        status, errors = rest.rebalance_status()
        _exitIfErrors(errors)

        print(json.dumps(status, indent=2))

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-rebalance-status" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Show rebalance status"


class RebalanceStop(Subcommand):
    """The rebalance stop subcommand"""

    def __init__(self):
        super(RebalanceStop, self).__init__()
        self.parser.prog = "couchbase-cli rebalance-stop"

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        _, errors = rest.stop_rebalance()
        _exitIfErrors(errors)

        _success("Rebalance stopped")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-rebalance-stop" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Stop a rebalance"


class Recovery(Subcommand):
    """The recovery command"""

    def __init__(self):
        super(Recovery, self).__init__()
        self.parser.prog = "couchbase-cli recovery"
        group = self.parser.add_argument_group("Recovery options")
        group.add_argument("--server-recovery", dest="servers", metavar="<server_list>",
                           required=True, help="The list of servers to recover")
        group.add_argument("--recovery-type", dest="recovery_type", metavar="type",
                           choices=["delta", "full"], default="delta",
                           help="The recovery type (delta or full)")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        servers = apply_default_port(opts.servers)
        for server in servers:
            _, errors = rest.recovery(server, opts.recovery_type)
            _exitIfErrors(errors)

        _success("Servers recovered")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-recovery" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Recover one or more servers"


class ResetAdminPassword(LocalSubcommand):
    """The reset admin password command"""

    def __init__(self):
        super(ResetAdminPassword, self).__init__()
        self.parser.prog = "couchbase-cli reset-admin-password"
        group = self.parser.add_argument_group("Reset password options")
        group.add_argument("--new-password", dest="new_password", metavar="<password>",
                           required=False, action=CBNonEchoedAction, envvar=None,
                           prompt_text="Enter new administrator password:",
                           confirm_text="Confirm new administrator password:",
                           help="The new administrator password")
        group.add_argument("--regenerate", dest="regenerate", action="store_true",
                           help="Generates a random administrator password")
        group.add_argument("-P", "--port", metavar="<port>", default="8091",
                           help="The REST API port, defaults to 8091")

    def execute(self, opts):
        token = _exit_on_file_read_failure(os.path.join(opts.config_path, "localtoken")).rstrip()
        rest = ClusterManager("http://127.0.0.1:" + opts.port, "@localtoken", token)
        check_cluster_initialized(rest)
        check_versions(rest)

        if opts.new_password is not None and opts.regenerate == True:
            _exitIfErrors(["Cannot specify both --new-password and --regenerate at the same time"])
        elif opts.new_password is not None:
            _, errors = rest.set_admin_password(opts.new_password)
            _exitIfErrors(errors)
            _success("Administrator password changed")
        elif opts.regenerate:
            result, errors = rest.regenerate_admin_password()
            _exitIfErrors(errors)
            print(result["password"])
        else:
            _exitIfErrors(["No parameters specified"])

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-reset-admin-password" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Resets the administrator password"


class ServerAdd(Subcommand):
    """The server add command"""

    def __init__(self):
        super(ServerAdd, self).__init__()
        self.parser.prog = "couchbase-cli server-add"
        group = self.parser.add_argument_group("Server add options")
        group.add_argument("--server-add", dest="servers", metavar="<server_list>", required=True,
                           help="The list of servers to add")
        group.add_argument("--server-add-username", dest="server_username", metavar="<username>",
                           required=True, help="The username for the server to add")
        group.add_argument("--server-add-password", dest="server_password", metavar="<password>",
                           required=True, help="The password for the server to add")
        group.add_argument("--group-name", dest="group_name", metavar="<name>",
                           help="The server group to add this server into")
        group.add_argument("--services", dest="services", default="data", metavar="<services>",
                           help="The services this server will run")
        group.add_argument("--index-storage-setting", dest="index_storage_mode", metavar="<mode>",
                           choices=["default", "memopt"], help="The index storage mode")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if not enterprise and opts.index_storage_mode == 'memopt':
            _exitIfErrors(["memopt option for --index-storage-setting can only be configured on enterprise edition"])

        opts.services, errors = process_services(opts.services, enterprise)
        _exitIfErrors(errors)

        settings, errors = rest.index_settings()
        _exitIfErrors(errors)

        if opts.index_storage_mode is None and settings['storageMode'] == "" and "index" in opts.services:
            opts.index_storage_mode = "default"

        # For supporting the default index backend changing from forestdb to plasma in Couchbase 5.0
        default = "plasma"
        if opts.index_storage_mode == "default" and settings['storageMode'] == "forestdb" or not enterprise:
            default = "forestdb"

        if opts.index_storage_mode:
            param = index_storage_mode_to_param(opts.index_storage_mode, default)
            _, errors = rest.set_index_settings(param, None, None, None, None, None)
            _exitIfErrors(errors)

        servers = opts.servers.split(',')
        for server in servers:
            _, errors = rest.add_server(server, opts.group_name, opts.server_username,
                                        opts.server_password, opts.services)
            _exitIfErrors(errors)

        _success("Server added")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-server-add" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Add servers to the cluster"


class ServerEshell(Subcommand):
    """The server eshell subcommand"""

    def __init__(self):
        super(ServerEshell, self).__init__()
        self.parser.prog = "couchbase-cli server-eshell"
        group = self.parser.add_argument_group("Server eshell options")
        group.add_argument("--vm", dest="vm", default="ns_server", metavar="<name>",
                           help="The vm to connect to")
        group.add_argument("--erl-path", dest="erl_path", metavar="<path>", default=CB_BIN_PATH,
                           help="Override the path to the erl executable")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        # Cluster does not need to be initialized for this command
        check_versions(rest)

        result, errors = rest.node_info()
        _exitIfErrors(errors)

        node = result['otpNode']
        cookie = result['otpCookie']

        if opts.vm != 'ns_server':
            cookie, errors = rest.get_babysitter_cookie()
            _exitIfErrors(errors)

            [short, _] = node.split('@')

            if opts.vm == 'babysitter':
                node = f'babysitter_of_{short}@127.0.0.1'
            elif opts.vm == 'couchdb':
                node = f'couchdb_{short}@127.0.0.1'
            else:
                _exitIfErrors([f'Unknown vm type `{opts.vm}`'])

        rand_chars = ''.join(random.choice(string.ascii_letters) for i in range(20))
        name = f'ctl-{rand_chars}@127.0.0.1'

        cb_erl = os.path.join(opts.erl_path, 'erl')
        if os.path.isfile(cb_erl):
            path = cb_erl
        else:
            _warning("Cannot locate Couchbase erlang. Attempting to use non-Couchbase erlang")
            path = 'erl'

        inetrc_file = os.path.join(CB_ETC_PATH, 'hosts.cfg')
        if os.path.isfile(inetrc_file):
            inetrc_opt = ['-kernel', 'inetrc', f'"{inetrc_file}"']
        else:
            inetrc_opt = []

        proto_dist = result['addressFamily'] + "_tcp"

        try:
            subprocess.call([path, '-name', name, '-setcookie', cookie, '-hidden', '-remsh', node, '-proto_dist', proto_dist] + inetrc_opt)
        except OSError:
            _exitIfErrors(["Unable to find the erl executable"])

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-server-eshell" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Opens a shell to the Couchbase cluster manager"

    @staticmethod
    def is_hidden():
        # Internal command not recommended for production use
        return True


class ServerInfo(Subcommand):
    """The server info subcommand"""

    def __init__(self):
        super(ServerInfo, self).__init__()
        self.parser.prog = "couchbase-cli server-info"

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        # Cluster does not need to be initialized for this command
        check_versions(rest)

        result, errors = rest.node_info()
        _exitIfErrors(errors)

        print(json.dumps(result, sort_keys=True, indent=2))

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-server-info" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Show details of a node in the cluster"


class ServerList(Subcommand):
    """The server list subcommand"""

    def __init__(self):
        super(ServerList, self).__init__()
        self.parser.prog = "couchbase-cli server-list"

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        result, errors = rest.pools('default')
        _exitIfErrors(errors)

        for node in result['nodes']:
            if node.get('otpNode') is None:
                raise Exception("could not access node")

            print(node['otpNode'], node['hostname'], node['status'], node['clusterMembership'])

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-server-list" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "List all nodes in a cluster"


class ServerReadd(Subcommand):
    """The server readd subcommand (Deprecated)"""

    def __init__(self):
        super(ServerReadd, self).__init__()
        self.parser.prog = "couchbase-cli server-readd"
        group = self.parser.add_argument_group("Server re-add options")
        group.add_argument("--server-add", dest="servers", metavar="<server_list>", required=True,
                           help="The list of servers to recover")
        # The parameters are unused, but kept for backwards compatibility
        group.add_argument("--server-username", dest="server_username", metavar="<username>",
                           help="The admin username for the server")
        group.add_argument("--server-password", dest="server_password", metavar="<password>",
                           help="The admin password for the server")
        group.add_argument("--group-name", dest="name", metavar="<name>",
                           help="The name of the server group")

    def execute(self, opts):
        _deprecated("Please use the recovery command instead")
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        servers = apply_default_port(opts.servers)
        for server in servers:
            _, errors = rest.readd_server(server)
            _exitIfErrors(errors)

        _success("Servers recovered")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-server-readd" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Add failed server back to the cluster"

    @staticmethod
    def is_hidden():
        # Deprecated command in 4.6, hidden in 5.0, pending removal
        return True


class SettingAlert(Subcommand):
    """The setting alert subcommand"""

    def __init__(self):
        super(SettingAlert, self).__init__()
        self.parser.prog = "couchbase-cli setting-alert"
        group = self.parser.add_argument_group("Alert settings")
        group.add_argument("--enable-email-alert", dest="enabled", metavar="<1|0>", required=True,
                           choices=["0", "1"], help="Enable/disable email alerts")
        group.add_argument("--email-recipients", dest="email_recipients", metavar="<email_list>",
                           help="A comma separated list of email addresses")
        group.add_argument("--email-sender", dest="email_sender", metavar="<email_addr>",
                           help="The sender email address")
        group.add_argument("--email-user", dest="email_username", metavar="<username>",
                           default="", help="The email server username")
        group.add_argument("--email-password", dest="email_password", metavar="<password>",
                           default="", help="The email server password")
        group.add_argument("--email-host", dest="email_host", metavar="<host>",
                           help="The email server host")
        group.add_argument("--email-port", dest="email_port", metavar="<port>",
                           help="The email server port")
        group.add_argument("--enable-email-encrypt", dest="email_encrypt", metavar="<1|0>",
                           choices=["0", "1"], help="Enable SSL encryption for emails")
        group.add_argument("--alert-auto-failover-node", dest="alert_af_node",
                           action="store_true", help="Alert when a node is auto-failed over")
        group.add_argument("--alert-auto-failover-max-reached", dest="alert_af_max_reached",
                           action="store_true",
                           help="Alert when the max number of auto-failover nodes was reached")
        group.add_argument("--alert-auto-failover-node-down", dest="alert_af_node_down",
                           action="store_true",
                           help="Alert when a node wasn't auto-failed over because other nodes " +
                           "were down")
        group.add_argument("--alert-auto-failover-cluster-small", dest="alert_af_small",
                           action="store_true",
                           help="Alert when a node wasn't auto-failed over because cluster was" +
                           " too small")
        group.add_argument("--alert-auto-failover-disable", dest="alert_af_disable",
                           action="store_true",
                           help="Alert when a node wasn't auto-failed over because auto-failover" +
                           " is disabled")
        group.add_argument("--alert-ip-changed", dest="alert_ip_changed", action="store_true",
                           help="Alert when a nodes IP address changed")
        group.add_argument("--alert-disk-space", dest="alert_disk_space", action="store_true",
                           help="Alert when disk usage on a node reaches 90%%")
        group.add_argument("--alert-meta-overhead", dest="alert_meta_overhead", action="store_true",
                           help="Alert when metadata overhead is more than 50%%")
        group.add_argument("--alert-meta-oom", dest="alert_meta_oom", action="store_true",
                           help="Alert when all bucket memory is used for metadata")
        group.add_argument("--alert-write-failed", dest="alert_write_failed", action="store_true",
                           help="Alert when writing data to disk has failed")
        group.add_argument("--alert-audit-msg-dropped", dest="alert_audit_dropped",
                           action="store_true", help="Alert when writing event to audit log failed")
        group.add_argument("--alert-indexer-max-ram", dest="alert_indexer_max_ram",
                           action="store_true", help="Alert when indexer is using all of its allocated memory")
        group.add_argument("--alert-timestamp-drift-exceeded", dest="alert_cas_drift",
                           action="store_true", help="Alert when clocks on two servers are more than five seconds apart")
        group.add_argument("--alert-communication-issue", dest="alert_communication_issue",
                           action="store_true", help="Alert when nodes are experiencing communication issues")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

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
        if opts.alert_indexer_max_ram:
            alerts.append('indexer_ram_max_usage')
        if opts.alert_cas_drift:
            alerts.append('ep_clock_cas_drift_threshold_exceeded')
        if opts.alert_communication_issue:
            alerts.append('communication_issue')

        enabled = "true"
        if opts.enabled == "0":
            enabled = "false"

        email_encrypt = "false"
        if opts.email_encrypt == "1":
            email_encrypt = "true"

        _, errors = rest.set_alert_settings(enabled, opts.email_recipients,
                                            opts.email_sender, opts.email_username,
                                            opts.email_password, opts.email_host,
                                            opts.email_port, email_encrypt,
                                            ",".join(alerts))
        _exitIfErrors(errors)

        _success("Email alert settings modified")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-alert" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify email alert settings"


class SettingAudit(Subcommand):
    """The settings audit subcommand"""

    def __init__(self):
        super(SettingAudit, self).__init__()
        self.parser.prog = "couchbase-cli setting-audit"
        self.parser.description = "Available only in Couchbase Server Enterprise Edition"
        group = self.parser.add_argument_group("Audit settings")
        group.add_argument("--audit-enabled", dest="enabled", metavar="<1|0>", choices=["0", "1"],
                           help="Enable/disable auditing")
        group.add_argument("--audit-log-path", dest="log_path", metavar="<path>",
                           help="The audit log path")
        group.add_argument("--audit-log-rotate-interval", dest="rotate_interval", type=(int),
                           metavar="<seconds>", help="The audit log rotate interval")
        group.add_argument("--audit-log-rotate-size", dest="rotate_size", type=(int),
                           metavar="<bytes>", help="The audit log rotate size")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        if not (opts.enabled or opts.log_path or opts.rotate_interval or opts.rotate_size):
            _exitIfErrors(["No settings specified to be changed"])

        if opts.enabled == "1":
            opts.enabled = "true"
        elif opts.enabled == "0":
            opts.enabled = "false"

        _, errors = rest.set_audit_settings(opts.enabled, opts.log_path,
                                            opts.rotate_interval, opts.rotate_size)
        _exitIfErrors(errors)

        _success("Audit settings modified")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-audit" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify audit settings"


class SettingAutofailover(Subcommand):
    """The settings auto-failover subcommand"""

    def __init__(self):
        super(SettingAutofailover, self).__init__()
        self.parser.prog = "couchbase-cli setting-autofailover"
        group = self.parser.add_argument_group("Auto-failover settings")
        group.add_argument("--enable-auto-failover", dest="enabled", metavar="<1|0>",
                           choices=["0", "1"], help="Enable/disable auto-failover")
        group.add_argument("--auto-failover-timeout", dest="timeout", metavar="<seconds>",
                           type=(int), help="The auto-failover timeout")
        group.add_argument("--enable-failover-of-server-groups", dest="enableFailoverOfServerGroups", metavar="<1|0>",
                           choices=["0", "1"], help="Enable/disable auto-failover of server Groups")
        group.add_argument("--max-failovers", dest="maxFailovers", metavar="<1|2|3>", choices=["1", "2", "3"],
                           help="Maximum number of times an auto-failover event can happen")
        group.add_argument("--enable-failover-on-data-disk-issues", dest="enableFailoverOnDataDiskIssues",
                           metavar="<1|0>", choices=["0", "1"],
                           help="Enable/disable auto-failover when the Data Service reports disk issues. " +
                                "Couchbase Server Enterprise Edition only.")
        group.add_argument("--failover-data-disk-period", dest="failoverOnDataDiskPeriod",
                           metavar="<seconds>", type=(int),
                           help="The amount of time the Data Serivce disk failures has to be happening for to trigger"
                                " an auto-failover")
        group.add_argument("--can-abort-rebalance", metavar="<1|0>", choices=["1", "0"], dest="canAbortRebalance",
                           help="Enables auto-failover to abort rebalance and perform the failover. (EE only)")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        if opts.enabled == "1":
            opts.enabled = "true"
        elif opts.enabled == "0":
            opts.enabled = "false"

        if opts.enableFailoverOnDataDiskIssues == "1":
            opts.enableFailoverOnDataDiskIssues = "true"
        elif opts.enableFailoverOnDataDiskIssues == "0":
            opts.enableFailoverOnDataDiskIssues = "false"

        if opts.enableFailoverOfServerGroups == "1":
            opts.enableFailoverOfServerGroups = "true"
        elif opts.enableFailoverOfServerGroups == "0":
            opts.enableFailoverOfServerGroups = "false"

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if not enterprise:
            if opts.enableFailoverOfServerGroups:
                _exitIfErrors(["--enable-failover-of-server-groups can only be configured on enterprise edition"])
            if opts.enableFailoverOnDataDiskIssues or opts.failoverOnDataDiskPeriod:
                _exitIfErrors(["Auto failover on Data Service disk issues can only be configured on enterprise edition"])
            if opts.maxFailovers:
                _exitIfErrors(["--max-count can only be configured on enterprise edition"])
            if opts.canAbortRebalance:
                _exitIfErrors(["--can-abort-rebalance can only be configured on enterprise edition"])

        if not any([opts.enabled, opts.timeout, opts.enableFailoverOnDataDiskIssues, opts.failoverOnDataDiskPeriod,
                    opts.enableFailoverOfServerGroups, opts.maxFailovers]):
            _exitIfErrors(["No settings specified to be changed"])

        if ((opts.enableFailoverOnDataDiskIssues is None or opts.enableFailoverOnDataDiskIssues == "false")
            and opts.failoverOnDataDiskPeriod):
            _exitIfErrors(["--enable-failover-on-data-disk-issues must be set to 1 when auto-failover Data"
                           " Service disk period has been set"])

        if opts.enableFailoverOnDataDiskIssues and opts.failoverOnDataDiskPeriod is None:
            _exitIfErrors(["--failover-data-disk-period must be set when auto-failover on Data Service disk"
                           " is enabled"])

        if opts.enabled == "false" or opts.enabled is None:
            if opts.enableFailoverOnDataDiskIssues or opts.failoverOnDataDiskPeriod:
                _exitIfErrors(["--enable-auto-failover must be set to 1 when auto-failover on Data Service disk issues"
                               " settings are being configured"])
            if opts.enableFailoverOfServerGroups:
                _exitIfErrors(["--enable-auto-failover must be set to 1 when enabling auto-failover of Server Groups"])
            if opts.timeout:
                _warning("Timeout specified will not take affect because auto-failover is being disabled")

        if opts.canAbortRebalance == '1':
            opts.canAbortRebalance = 'true'
        elif opts.canAbortRebalance == '0':
            opts.canAbortRebalance ='false'

        _, errors = rest.set_autofailover_settings(opts.enabled, opts.timeout, opts.enableFailoverOfServerGroups,
                                                   opts.maxFailovers, opts.enableFailoverOnDataDiskIssues,
                                                   opts.failoverOnDataDiskPeriod, opts.canAbortRebalance)
        _exitIfErrors(errors)

        _success("Auto-failover settings modified")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-autofailover" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify auto failover settings"


class SettingAutoreprovision(Subcommand):
    """The settings auto-reprovision subcommand"""

    def __init__(self):
        super(SettingAutoreprovision, self).__init__()
        self.parser.prog = "couchbase-cli setting-autoreprovision"
        group = self.parser.add_argument_group("Auto-reprovision settings")
        group.add_argument("--enabled", dest="enabled", metavar="<1|0>", required=True,
                           choices=["0", "1"], help="Enable/disable auto-reprovision")
        group.add_argument("--max-nodes", dest="max_nodes", metavar="<num>", type=(int),
                           help="The numbers of server that can be auto-reprovisioned before a rebalance")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        if opts.enabled == "1":
            opts.enabled = "true"
        elif opts.enabled == "0":
            opts.enabled = "false"

        if opts.enabled == "true" and opts.max_nodes is None:
            _exitIfErrors(["--max-nodes must be specified if auto-reprovision is enabled"])

        if not (opts.enabled or opts.max_nodes):
            _exitIfErrors(["No settings specified to be changed"])

        if (opts.enabled is None or opts.enabled == "false") and opts.max_nodes:
            _warning("--max-servers will not take affect because auto-reprovision is being disabled")

        _, errors = rest.set_autoreprovision_settings(opts.enabled, opts.max_nodes)
        _exitIfErrors(errors)

        _success("Auto-reprovision settings modified")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-autoreprovision" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify auto-reprovision settings"


class SettingCluster(Subcommand):
    """The settings cluster subcommand"""

    def __init__(self):
        super(SettingCluster, self).__init__()
        self.parser.prog = "couchbase-cli setting-cluster"
        group = self.parser.add_argument_group("Cluster settings")
        group.add_argument("--cluster-username", dest="new_username", metavar="<username>",
                           help="The cluster administrator username")
        group.add_argument("--cluster-password", dest="new_password", metavar="<password>",
                           help="Only compact the data files")
        group.add_argument("--cluster-port", dest="port", type=(int), metavar="<port>",
                           help="The cluster administration console port")
        group.add_argument("--cluster-ramsize", dest="data_mem_quota", metavar="<quota>",
                           type=(int), help="The data service memory quota in megabytes")
        group.add_argument("--cluster-index-ramsize", dest="index_mem_quota", metavar="<quota>",
                           type=(int), help="The index service memory quota in megabytes")
        group.add_argument("--cluster-fts-ramsize", dest="fts_mem_quota", metavar="<quota>",
                           type=(int), help="The full-text service memory quota in megabytes")
        group.add_argument("--cluster-eventing-ramsize", dest="eventing_mem_quota", metavar="<quota>",
                           type=(int), help="The Eventing service memory quota in megabytes")
        group.add_argument("--cluster-analytics-ramsize", dest="cbas_mem_quota", metavar="<quota>",
                           type=(int), help="The analytics service memory quota in megabytes")
        group.add_argument("--cluster-name", dest="name", metavar="<name>", help="The cluster name")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        if opts.data_mem_quota or opts.index_mem_quota or opts.fts_mem_quota or opts.cbas_mem_quota \
                or opts.eventing_mem_quota or opts.name:
            _, errors = rest.set_pools_default(opts.data_mem_quota, opts.index_mem_quota, opts.fts_mem_quota,
                                               opts.cbas_mem_quota, opts.eventing_mem_quota, opts.name)
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

        _success("Cluster settings modified")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-cluster" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify cluster settings"


class ClusterEdit(SettingCluster):
    """The cluster edit subcommand (Deprecated)"""

    def __init__(self):
        super(ClusterEdit, self).__init__()
        self.parser.prog = "couchbase-cli cluster-edit"

    def execute(self, opts):
        _deprecated("Please use the setting-cluster command instead")
        super(ClusterEdit, self).execute(opts)

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-cluster-edit" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def is_hidden():
        # Deprecated command in 4.6, hidden in 5.0, pending removal
        return True


class SettingCompaction(Subcommand):
    """The setting compaction subcommand"""

    def __init__(self):
        super(SettingCompaction, self).__init__()
        self.parser.prog = "couchbase-cli setting-compaction"
        group = self.parser.add_argument_group("Compaction settings")
        group.add_argument("--compaction-db-percentage", dest="db_perc", metavar="<perc>",
                           type=(int),
                           help="Compacts the db once the fragmentation reaches this percentage")
        group.add_argument("--compaction-db-size", dest="db_size", metavar="<megabytes>",
                           type=(int),
                           help="Compacts db once the fragmentation reaches this size (MB)")
        group.add_argument("--compaction-view-percentage", dest="view_perc", metavar="<perc>",
                           type=(int),
                           help="Compacts the view once the fragmentation reaches this percentage")
        group.add_argument("--compaction-view-size", dest="view_size", metavar="<megabytes>",
                           type=(int),
                           help="Compacts view once the fragmentation reaches this size (MB)")
        group.add_argument("--compaction-period-from", dest="from_period", metavar="<HH:MM>",
                           help="Only run compaction after this time")
        group.add_argument("--compaction-period-to", dest="to_period", metavar="<HH:MM>",
                           help="Only run compaction before this time")
        group.add_argument("--enable-compaction-abort", dest="enable_abort", metavar="<1|0>",
                           choices=["0", "1"], help="Allow compactions to be aborted")
        group.add_argument("--enable-compaction-parallel", dest="enable_parallel", metavar="<1|0>",
                           choices=["0", "1"], help="Allow parallel compactions")
        group.add_argument("--metadata-purge-interval", dest="purge_interval", metavar="<float>",
                           type=(float), help="The metadata purge interval")
        group.add_argument("--gsi-compaction-mode", dest="gsi_mode",
                          choices=["append", "circular"],
                          help="Sets the gsi compaction mode (append or circular)")
        group.add_argument("--compaction-gsi-percentage", dest="gsi_perc", type=(int), metavar="<perc>",
                          help="Starts compaction once gsi file fragmentation has reached this percentage (Append mode only)")
        group.add_argument("--compaction-gsi-interval", dest="gsi_interval", metavar="<days>",
                          help="A comma separated list of days compaction can run (Circular mode only)")
        group.add_argument("--compaction-gsi-period-from", dest="gsi_from_period", metavar="<HH:MM>",
                          help="Allow gsi compaction to run after this time (Circular mode only)")
        group.add_argument("--compaction-gsi-period-to", dest="gsi_to_period", metavar="<HH:MM>",
                          help="Allow gsi compaction to run before this time (Circular mode only)")
        group.add_argument("--enable-gsi-compaction-abort", dest="enable_gsi_abort", metavar="<1|0>",
                          choices=["0", "1"],
                          help="Abort gsi compaction if when run outside of the accepted interaval (Circular mode only)")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

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

        from_hour, from_min = self._handle_timevalue(opts.from_period,
                                                     "--compaction-period-from")
        to_hour, to_min = self._handle_timevalue(opts.to_period, "--compaction-period-to")

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

        g_from_hour = None
        g_from_min = None
        g_to_hour = None
        g_to_min = None
        if opts.gsi_mode == "append":
            opts.gsi_mode = "full"
            if opts.gsi_perc is None:
                _exitIfErrors(["--compaction-gsi-percentage must be specified when" +
                               " --gsi-compaction-mode is set to append"])
        elif opts.gsi_mode == "circular":
            if opts.gsi_from_period is not None and opts.gsi_to_period is None:
                _exitIfErrors(["--compaction-gsi-period-to is required with --compaction-gsi-period-from"])
            if opts.gsi_to_period is not None and opts.gsi_from_period is None:
                _exitIfErrors(["--compaction-gsi-period-from is required with --compaction-gsi-period-to"])

            g_from_hour, g_from_min = self._handle_timevalue(opts.gsi_from_period,
                                                             "--compaction-gsi-period-from")
            g_to_hour, g_to_min = self._handle_timevalue(opts.gsi_to_period,
                                                            "--compaction-gsi-period-to")

            if opts.enable_gsi_abort == "1":
                opts.enable_gsi_abort = "true"
            else:
                opts.enable_gsi_abort = "false"

        _, errors = rest.set_compaction_settings(opts.db_perc, opts.db_size, opts.view_perc,
                                                 opts.view_size, from_hour, from_min, to_hour,
                                                 to_min, opts.enable_abort, opts.enable_parallel,
                                                 opts.purge_interval, opts.gsi_mode,
                                                 opts.gsi_perc, opts.gsi_interval, g_from_hour,
                                                 g_from_min, g_to_hour, g_to_min,
                                                 opts.enable_gsi_abort)
        _exitIfErrors(errors)

        _success("Compaction settings modified")

    def _handle_timevalue(self, opt_value, opt_name):
        hour = None
        minute = None
        if opt_value:
            if opt_value.find(':') == -1:
                _exitIfErrors([f'Invalid value for {opt_name}, must be in form XX:XX'])
            hour, minute = opt_value.split(':', 1)
            try:
                hour = int(hour)
            except ValueError:
                _exitIfErrors([f'Invalid hour value for {opt_name}, must be an integer'])
            if hour not in range(24):
                _exitIfErrors([f'Invalid hour value for {opt_name}, must be 0-23'])

            try:
                minute = int(minute)
            except ValueError:
                _exitIfErrors([f'Invalid minute value for {opt_name}, must be an integer'])
            if minute not in range(60):
                _exitIfErrors([f'Invalid minute value for {opt_name}, must be 0-59'])
        return hour, minute

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-compaction" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify auto-compaction settings"


class SettingIndex(Subcommand):
    """The setting index subcommand"""

    def __init__(self):
        super(SettingIndex, self).__init__()
        self.parser.prog = "couchbase-cli setting-index"
        group = self.parser.add_argument_group("Index settings")
        group.add_argument("--index-max-rollback-points", dest="max_rollback", metavar="<num>",
                           type=(int), help="Max rollback points")
        group.add_argument("--index-stable-snapshot-interval", dest="stable_snap", type=(int),
                           metavar="<seconds>", help="Stable snapshot interval in seconds")
        group.add_argument("--index-memory-snapshot-interval", dest="mem_snap", metavar="<ms>",
                           type=(int), help="Stable snapshot interval in milliseconds")
        group.add_argument("--index-storage-setting", dest="storage_mode", metavar="<mode>",
                           choices=["default", "memopt"], help="The index storage backend")
        group.add_argument("--index-threads", dest="threads", metavar="<num>",
                           type=(int), help="The number of indexer threads")
        group.add_argument("--index-log-level", dest="log_level", metavar="<level>",
                           choices=["debug", "silent", "fatal", "error", "warn", "info", "verbose",
                                    "timing", "trace"],
                           help="The indexer log level")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if opts.max_rollback is None and opts.stable_snap is None \
            and opts.mem_snap is None and opts.storage_mode is None \
            and opts.threads is None and opts.log_level is None:
            _exitIfErrors(["No settings specified to be changed"])

        settings, errors = rest.index_settings()
        _exitIfErrors(errors)

        # For supporting the default index backend changing from forestdb to plasma in Couchbase 5.0
        default = "plasma"
        if opts.storage_mode == "default" and settings['storageMode'] == "forestdb" or not enterprise:
            default = "forestdb"

        opts.storage_mode = index_storage_mode_to_param(opts.storage_mode, default)
        _, errors = rest.set_index_settings(opts.storage_mode, opts.max_rollback,
                                            opts.stable_snap, opts.mem_snap,
                                            opts.threads, opts.log_level)
        _exitIfErrors(errors)

        _success("Indexer settings modified")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-index" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify index settings"


class SettingSaslauthd(Subcommand):
    """The setting sasl subcommand"""

    def __init__(self):
        super(SettingSaslauthd, self).__init__()
        self.parser.prog = "couchbase-cli setting-saslauthd"
        group = self.parser.add_argument_group("saslauthd settings")
        group.add_argument("--enabled", dest="enabled", metavar="<1|0>", required=True,
                           choices=["0", "1"], help="Enable/disable saslauthd")
        group.add_argument("--admins", dest="admins", metavar="<user_list>",
                           help="A comma separated list of full admins")
        group.add_argument("--roadmins", dest="roadmins", metavar="<user_list>",
                           help="A comma separated list of read only admins")
        group.add_argument("--default", dest="default", default="none",
                           choices=["admins", "roadmins", "none"], metavar="<default>",
                           help="Default roles for saslauthd users")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

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
                    _warning("--ro-admins option ignored since default is read only admins")
                _, errors = rest.sasl_settings('true', ro_admins, None)
            elif opts.default == 'roadmins':
                if admins:
                    _warning("--admins option ignored since default is admins")
                _, errors = rest.sasl_settings('true', None, admins)
            else:
                _, errors = rest.sasl_settings('true', ro_admins, admins)
        else:
            if admins:
                _warning("--admins option ignored since saslauthd is being disabled")
            if ro_admins:
                _warning("--roadmins option ignored since saslauthd is being disabled")
            _, errors = rest.sasl_settings('false', "", "")

        _exitIfErrors(errors)

        _success("saslauthd settings modified")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-saslauthd" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify saslauthd settings"


class SettingLdap(Subcommand):
    """The setting Ldap subcommand"""

    def __init__(self):
        super(SettingLdap, self).__init__()
        self.parser.prog = "couchbase-cli setting-ldap"
        group = self.parser.add_argument_group("LDAP settings")
        group.add_argument("--get", dest="get", default=False, action="store_true",
                           help='When the get flag is provided it will retrieve the current ldap settings')
        group.add_argument("--authentication-enabled", dest="authentication_enabled", metavar="<1|0>",
                           choices=["1", "0"], help="Enable LDAP authentication, otherwise it defaults to disable")
        group.add_argument("--authorization-enabled", dest="authorization_enabled", metavar="<1|0>",
                           choices=["1", "0"], help="Enable LDAP authorization, otherwise defaults to false")
        group.add_argument("--hosts", dest="hosts", metavar="<host_list>",
                           help="Coma separated list of LDAP servers")
        group.add_argument("--port", dest="port", metavar="<port>", help="LDAP port", type=int)
        group.add_argument("--encryption", dest="encryption", metavar="<tls|startTLS|none>",
                           choices=["tls", "startTLS", "none"], help="Encryption used")
        group.add_argument("--server-cert-validation", dest="server_cert_val", metavar="<1|0>", choices=["0", "1"],
                           help="Enable or disable certificate validation when connecting to LDAP server")
        group.add_argument("--ldap-cacert", dest="cacert_ldap", metavar="<path>",
                           help="CA certificate to be used for LDAP server certificate validation, required if" +
                                " certificate validation is not disabled")
        group.add_argument("--user-dn-query", metavar="<query>", dest="user_dn_query",
                           help="LDAP query to get user's DN. Must contains at least one instance of %%u")
        group.add_argument("--user-dn-template", metavar="<template>", dest="user_dn_template",
                           help="Template to construct user's DN. Must contain at least one instance of %%u")
        group.add_argument("--request-timeout", metavar="<ms>", dest="timeout",
                           help="Request time out in milliseconds")
        group.add_argument("--max-parallel", dest="max_parallel", metavar="<max>", type=int,
                           help="Maximum number of parallel connections that can be established")
        group.add_argument("--max-cache-size", dest="max_cache_size", metavar="<size>",
                           help="Maximum number of cached LDAP requests")
        group.add_argument("--cache-value-lifetime", dest="cache_value_lifetime", metavar="<ms>",
                           help="Cache value lifetime in milliseconds")
        group.add_argument("--bind-dn", dest="bind_dn", metavar="<DN>",
                           help="The DN of a user to bind as to performance lookups")
        group.add_argument("--bind-password", dest="bind_password", metavar="<password>", help="The password of the bind user")
        group.add_argument("--group-query", dest="group_query", metavar="<query>",
                           help="LDAP query to get user's groups by username")
        group.add_argument("--enable-nested-groups", dest="nested_groups", metavar="<1|0>",
                           choices=["0", "1"])
        group.add_argument("--nested-group-max-depth", dest="nested_max_depth", metavar="<max>", type=int,
                           help="Maximum number of recursive group requests allowed. [1 - 100]")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)
        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if not enterprise:
            _exitIfErrors(["LDAP settings are only available in enterprise edition"])

        if opts.get:
            data, rv = rest.get_ldap()
            _exitIfErrors(rv)
            print(json.dumps(data))
        else:
            self._set(opts, rest)

    def _set(self, opts, rest):
        if opts.authentication_enabled == '1':
            opts.authentication_enabled = 'true'
        elif opts.authentication_enabled == '0':
            opts.authentication_enabled = 'false'

        if opts.authorization_enabled == '1':
            opts.authorization_enabled = 'true'
        elif opts.authorization_enabled == '0':
            opts.authorization_enabled = 'false'

        if opts.server_cert_val == '1':
            opts.server_cert_val = 'true'
        elif opts.server_cert_val == '0':
            opts.server_cert_val = 'false'

        if opts.server_cert_val == 'false' and opts.cacert_ldap is not None:
            _exitIfErrors(['--server-cert-validation 0 and --ldap-cert can not be used together'])

        if opts.cacert_ldap is not None:
            opts.cacert_ldap = _exit_on_file_read_failure(opts.cacert_ldap)

        if opts.encryption == "tls":
            opts.encryption = "TLS"
        elif opts.encryption == "startTLS":
            opts.encryption = "StartTLSExtension"
        elif opts.encryption == "none":
            opts.encryption = "None"

        if opts.nested_groups == '1':
            opts.nested_groups = 'true'
        elif opts.nested_groups == '0':
            opts.nested_groups = 'false'

        if opts.user_dn_query is not None and opts.user_dn_template is not None:
            _exitIfErrors(['--user-dn-query and --user-dn-template can not be used together'])

        mapping = None
        if opts.user_dn_query is not None:
            mapping = f'{{"query": "{opts.user_dn_query}"}}'

        if opts.user_dn_template is not None:
            mapping = f'{{"template": "{opts.user_dn_template}"}}'

        _, errors = rest.ldap_settings(opts.authentication_enabled, opts.authorization_enabled, opts.hosts, opts.port,
                                       opts.encryption, mapping, opts.timeout, opts.max_parallel,
                                       opts.max_cache_size, opts.cache_value_lifetime, opts.bind_dn, opts.bind_password,
                                       opts.group_query, opts.nested_groups, opts.nested_max_depth,
                                       opts.server_cert_val, opts.cacert_ldap)

        _exitIfErrors(errors)
        _success("LDAP settings modified")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-ldap" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify LDAP settings"


class SettingNotification(Subcommand):
    """The settings notification subcommand"""

    def __init__(self):
        super(SettingNotification, self).__init__()
        self.parser.prog = "couchbase-cli setting-notification"
        group = self.parser.add_argument_group("Notification Settings")
        group.add_argument("--enable-notifications", dest="enabled", metavar="<1|0>", required=True,
                           choices=["0", "1"], help="Enables/disable notifications")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        enabled = None
        if opts.enabled == "1":
            enabled = True
        elif opts.enabled == "0":
            enabled = False

        _, errors = rest.enable_notifications(enabled)
        _exitIfErrors(errors)

        _success("Notification settings updated")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-notification" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify email notification settings"


class SettingPasswordPolicy(Subcommand):
    """The settings password policy subcommand"""

    def __init__(self):
        super(SettingPasswordPolicy, self).__init__()
        self.parser.prog = "couchbase-cli setting-password-policy"
        group = self.parser.add_argument_group("Password Policy Settings")
        group.add_argument("--get", dest="get", action="store_true", default=False,
                           help="Get the current password policy")
        group.add_argument("--set", dest="set", action="store_true", default=False,
                           help="Set a new password policy")
        group.add_argument("--min-length", dest="min_length", type=int, default=None, metavar="<num>",
                           help="Specifies the minimum password length for new passwords")
        group.add_argument("--uppercase", dest="upper_case", metavar="<0|1>", choices=["0", "1"],
                           help="Specifies new passwords must contain an upper case character")
        group.add_argument("--lowercase", dest="lower_case", metavar="<0|1>", choices=["0", "1"],
                           help="Specifies new passwords must contain a lower case character")
        group.add_argument("--digit", dest="digit", metavar="<0|1>", choices=["0", "1"],
                           help="Specifies new passwords must at least one digit")
        group.add_argument("--special-char", dest="special_char", metavar="<0|1>", choices=["0", "1"],
                           help="Specifies new passwords must at least one special character")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        actions = sum([opts.get, opts.set])
        if actions == 0:
            _exitIfErrors(["Must specify either --get or --set"])
        elif actions > 1:
            _exitIfErrors(["The --get and --set flags may not be specified at " +
                           "the same time"])
        elif opts.get:
            if opts.min_length is not None or any([opts.upper_case, opts.lower_case, opts.digit, opts.special_char]):
                _exitIfErrors(["The --get flag must be used without any other arguments"])
            self._get(rest)
        elif opts.set:
            if opts.min_length is None:
                _exitIfErrors(["--min-length is required when using --set flag"])
            if opts.min_length <= 0:
                    _exitIfErrors(["--min-length has to be greater than 0"])
            self._set(rest, opts)

    def _get(self, rest):
        policy, errors = rest.get_password_policy()
        _exitIfErrors(errors)
        print(json.dumps(policy, sort_keys=True, indent=2))

    def _set(self, rest, opts):
        _, errors = rest.set_password_policy(opts.min_length, opts.upper_case, opts.lower_case,
                                             opts.digit, opts.special_char)
        _exitIfErrors(errors)
        _success("Password policy updated")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-password-policy" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify the password policy"


class SettingSecurity(Subcommand):
    """The settings security subcommand"""

    def __init__(self):
        super(SettingSecurity, self).__init__()
        self.parser.prog = "couchbase-cli setting-security"
        group = self.parser.add_argument_group("Cluster Security Settings")
        group.add_argument('--get', default=False, action='store_true', help='Get security settings.')
        group.add_argument('--set', default=False, action='store_true', help='Set security settings.')
        group.add_argument("--disable-http-ui", dest="disable_http_ui", metavar="<0|1>", choices=['0', '1'],
                           default=None, help="Disables access to the UI over HTTP (0 or 1)")
        group.add_argument("--cluster-encryption-level", dest="cluster_encryption_level", metavar="<all|control>",
                          choices=['all', 'control'], default=None,
                          help="Set cluster encryption level, only used when cluster encryption enabled.")
        group.add_argument('--tls-min-version', dest='tls_min_version', metavar='<tlsv1|tlsv1.1|tlsv1.2>',
                           choices=['tlsv1','tlsv1.1', 'tlsv1.2'],
                           default=None, help='Set the minimum TLS version')
        group.add_argument('--tls-honor-cipher-order', dest='tls_honor_cipher_order', metavar='<1|0>', choices=['1', '0'],
                           help='Specify or not the cipher order has to be followed.', default=None)
        group.add_argument('--cipher-suites', metavar='<ciphers>', default=None,
                           help='Comma separated list of ciphers to use.If an empty string (e.g "") given it will'
                                ' reset ciphers to default.')

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        if sum([opts.get, opts.set]) != 1:
            _exitIfErrors(['Provided either --set or --get.'])

        if opts.get:
            val, err = rest.get_security_settings()
            _exitIfErrors(err)
            print(json.dumps(val))
        elif opts.set:
            self._set(rest, opts.disable_http_ui, opts.cluster_encryption_level, opts.tls_min_version,
                      opts.tls_honor_cipher_order, opts.cipher_suites)

    @staticmethod
    def _set(rest, disable_http_ui, encryption_level, tls_min_version, honor_order, cipher_suites):
        if not any([True if x is not None else False for x in [disable_http_ui, encryption_level, tls_min_version,
                                                    honor_order, cipher_suites]]):
            _exitIfErrors(['please provide at least one of -cluster-encryption-level,'
                          ' --disable-http-ui, --tls-min-version, --tls-honor-cipher-order or --cipher-suites'
                          ' together with --set'])

        if disable_http_ui == '1':
            disable_http_ui = 'true'
        elif disable_http_ui == '0':
            disable_http_ui = 'false'

        if honor_order == '1':
            honor_order = 'true'
        elif honor_order == '0':
            honor_order = 'false'

        if cipher_suites == '':
            cipher_suites = json.dumps([])
        elif cipher_suites is not None:
            cipher_suites = json.dumps(cipher_suites.split(','))

        _, errors = rest.set_security_settings(disable_http_ui, encryption_level, tls_min_version,
                                               honor_order, cipher_suites)
        _exitIfErrors(errors)
        _success("Security settings updated")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-security" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify security settings"


class SettingXdcr(Subcommand):
    """The setting xdcr subcommand"""

    def __init__(self):
        super(SettingXdcr, self).__init__()
        self.parser.prog = "couchbase-cli setting-xdcr"
        group = self.parser.add_argument_group("XDCR Settings")
        group.add_argument("--checkpoint-interval", dest="chk_int", type=(int), metavar="<num>",
                           help="Intervals between checkpoints in seconds (60 to 14400)")
        group.add_argument("--worker-batch-size", dest="worker_batch_size", metavar="<num>",
                           type=(int), help="Doc batch size (500 to 10000)")
        group.add_argument("--doc-batch-size", dest="doc_batch_size", type=(int), metavar="<KB>",
                           help="Document batching size in KB (10 to 100000)")
        group.add_argument("--failure-restart-interval", dest="fail_interval", metavar="<seconds>",
                           type=(int),
                           help="Interval for restarting failed xdcr in seconds (1 to 300)")
        group.add_argument("--optimistic-replication-threshold", dest="rep_thresh", type=(int),
                           metavar="<bytes>",
                           help="Document body size threshold (bytes) to trigger optimistic " +
                           "replication")
        group.add_argument("--source-nozzle-per-node", dest="src_nozzles", metavar="<num>",
                           type=(int),
                           help="The number of source nozzles per source node (1 to 10)")
        group.add_argument("--target-nozzle-per-node", dest="dst_nozzles", metavar="<num>",
                           type=(int),
                           help="The number of outgoing nozzles per target node (1 to 10)")
        group.add_argument("--bandwidth-usage-limit", dest="usage_limit", type=(int),
                           metavar="<num>", help="The bandwidth usage limit in MB/Sec")
        group.add_argument("--enable-compression", dest="compression", metavar="<1|0>", choices=["1", "0"],
                           help="Enable/disable compression")
        group.add_argument("--log-level", dest="log_level", metavar="<level>",
                           choices=["Error", "Info", "Debug", "Trace"],
                           help="The XDCR log level")
        group.add_argument("--stats-interval", dest="stats_interval", metavar="<ms>",
                           help="The interval for statistics updates (in milliseconds)")
        group.add_argument('--max-processes', dest='max_proc', metavar="<num>", type=int,
                           help='Number of processes to be allocated to XDCR. The default is 4.')

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)
        if not enterprise and opts.compression:
            _exitIfErrors(["--enable-compression can only be configured on enterprise edition"])

        if opts.compression == "0":
            opts.compression = "None"
        elif opts.compression =="1":
            opts.compression = "Auto"

        _, errors = rest.xdcr_global_settings(opts.chk_int, opts.worker_batch_size,
                                              opts.doc_batch_size, opts.fail_interval,
                                              opts.rep_thresh, opts.src_nozzles,
                                              opts.dst_nozzles, opts.usage_limit,
                                              opts.compression, opts.log_level,
                                              opts.stats_interval, opts.max_proc)
        _exitIfErrors(errors)

        _success("Global XDCR settings updated")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-xdcr" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Modify XDCR related settings"

class SettingMasterPassword(Subcommand):
    """The setting master password subcommand"""

    def __init__(self):
        super(SettingMasterPassword, self).__init__()
        self.parser.prog = "couchbase-cli setting-master-password"
        group = self.parser.add_argument_group("Master password options")
        group.add_argument("--new-password", dest="new_password", metavar="<password>",
                           required=False, action=CBNonEchoedAction, envvar=None,
                           prompt_text="Enter new master password:",
                           confirm_text="Confirm new master password:",
                           help="Sets a new master password")
        group.add_argument("--rotate-data-key", dest="rotate_data_key", action="store_true",
                           help="Rotates the master password data key")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        if opts.new_password is not None:
            _, errors = rest.set_master_pwd(opts.new_password)
            _exitIfErrors(errors)
            _success("New master password set")
        elif opts.rotate_data_key == True:
            _, errors = rest.rotate_master_pwd()
            _exitIfErrors(errors)
            _success("Data key rotated")
        else:
            _exitIfErrors(["No parameters set"])

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-master-password" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Changing the settings of the master password"


class SslManage(Subcommand):
    """The user manage subcommand"""

    def __init__(self):
        super(SslManage, self).__init__()
        self.parser.prog = "couchbase-cli ssl-manage"
        group = self.parser.add_argument_group("SSL manage options")
        group.add_argument("--cluster-cert-info", dest="cluster_cert", action="store_true",
                           default=False, help="Gets the cluster certificate")
        group.add_argument("--node-cert-info", dest="node_cert", action="store_true",
                           default=False, help="Gets the node certificate")
        group.add_argument("--regenerate-cert", dest="regenerate", metavar="<path>",
                           help="Regenerate the cluster certificate and save it to a file")
        group.add_argument("--set-node-certificate", dest="set_cert", action="store_true",
                           default=False, help="Sets the node certificate")
        group.add_argument("--upload-cluster-ca", dest="upload_cert", metavar="<path>",
                           help="Upload a new cluster certificate")
        group.add_argument("--set-client-auth", dest="client_auth_path", metavar="<path>",
                           help="A path to a file containing the client auth configuration")
        group.add_argument("--client-auth", dest="show_client_auth", action="store_true",
                           help="Show ssl client certificate authentication value")
        group.add_argument("--extended", dest="extended", action="store_true",
                           default=False, help="Print extended certificate information")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        if opts.regenerate is not None:
            try:
                open(opts.regenerate, 'a').close()
            except IOError:
                _exitIfErrors([f'Unable to create file at `{opts.regenerate}`'])
            certificate, errors = rest.regenerate_cluster_certificate()
            _exitIfErrors(errors)
            _exit_on_file_write_failure(opts.regenerate, certificate)
            _success(f'Certificate regenerate and copied to `{opts.regenerate}`')
        elif opts.cluster_cert:
            certificate, errors = rest.retrieve_cluster_certificate(opts.extended)
            _exitIfErrors(errors)
            if isinstance(certificate, dict):
                print(json.dumps(certificate, sort_keys=True, indent=2))
            else:
                print(certificate)
        elif opts.node_cert:
            host = urllib.parse.urlparse(opts.cluster).netloc
            certificate, errors = rest.retrieve_node_certificate(host)
            _exitIfErrors(errors)
            print(json.dumps(certificate, sort_keys=True, indent=2))
        elif opts.upload_cert:
            certificate = _exit_on_file_read_failure(opts.upload_cert)
            _, errors = rest.upload_cluster_certificate(certificate)
            _exitIfErrors(errors)
            _success(f'Uploaded cluster certificate to {opts.cluster}')
        elif opts.set_cert:
            _, errors = rest.set_node_certificate()
            _exitIfErrors(errors)
            _success("Node certificate set")
        elif opts.client_auth_path:
            data = _exit_on_file_read_failure(opts.client_auth_path)
            try:
                config = json.loads(data)
            except ValueError as e:
                _exitIfErrors([f'Client auth config does not contain valid json: {e}'])
            _, errors = rest.set_client_cert_auth(config)
            _exitIfErrors(errors)
            _success("SSL client auth updated")
        elif opts.show_client_auth:
            result, errors = rest.retrieve_client_cert_auth()
            _exitIfErrors(errors)
            print(json.dumps(result, sort_keys=True, indent=2))
        else:
            _exitIfErrors(["No options specified"])

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-ssl-manage" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Manage cluster certificates"


class UserManage(Subcommand):
    """The user manage subcommand"""

    def __init__(self):
        super(UserManage, self).__init__()
        self.parser.prog = "couchbase-cli user-manage"
        group = self.parser.add_argument_group("User manage options")
        group.add_argument("--delete", dest="delete", action="store_true", default=False,
                           help="Delete an existing RBAC user")
        group.add_argument("--get", dest="get", action="store_true", default=False,
                           help="Display RBAC user details")
        group.add_argument("--list", dest="list", action="store_true", default=False,
                           help="List all RBAC users and their roles")
        group.add_argument("--my-roles", dest="my_roles", action="store_true", default=False,
                           help="List my roles")
        group.add_argument("--set", dest="set", action="store_true", default=False,
                           help="Create or edit an RBAC user")
        group.add_argument("--set-group", dest="set_group", action="store_true", default=False,
                           help="Create or edit a user group")
        group.add_argument("--delete-group", dest="delete_group", action="store_true", default=False,
                           help="Delete a user group")
        group.add_argument("--list-groups", dest="list_group", action="store_true", default=False,
                           help="List all groups")
        group.add_argument("--get-group", dest="get_group", action="store_true", default=False,
                           help="Get group")
        group.add_argument("--rbac-username", dest="rbac_user", metavar="<username>",
                           help="The RBAC username")
        group.add_argument("--rbac-password", dest="rbac_pass", metavar="<password>",
                           help="The RBAC password")
        group.add_argument("--rbac-name", dest="rbac_name", metavar="<name>",
                           help="The full name of the user")
        group.add_argument("--roles", dest="roles", metavar="<roles_list>",
                           help="The roles for the specified user")
        group.add_argument("--auth-domain", dest="auth_domain", metavar="<domain>",
                           choices=["external", "local"],
                           help="The authentication type for the specified user")
        group.add_argument("--user-groups", dest="groups", metavar="<groups>",
                           help="List of groups for the user to be added to")
        group.add_argument("--group-name", dest="group", metavar="<group>", help="Group name")
        group.add_argument("--group-description", dest="description", metavar="<text>", help="Group description")
        group.add_argument("--ldap-ref", dest="ldap_ref", metavar="<ref>", help="LDAP group's distinguished name")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        num_selectors = sum([opts.delete, opts.list, opts.my_roles, opts.set, opts.get, opts.get_group,
                             opts.list_group, opts.delete_group, opts.set_group])
        if num_selectors == 0:
            _exitIfErrors(["Must specify --delete, --list, --my_roles, --set, --get, --get-group, --set-group, " +
                           "--list-groups or --delete-group"])
        elif num_selectors != 1:
            _exitIfErrors(["Only one of the following can be specified:--delete, --list, --my_roles, --set, --get," +
                           " --get-group, --set-group, --list-groups or --delete-group"])

        if opts.delete:
            self._delete(rest, opts)
        elif opts.list:
            self._list(rest, opts)
        elif opts.my_roles:
            self._my_roles(rest, opts)
        elif opts.set:
            self._set(rest, opts)
        elif opts.get:
            self._get(rest, opts)
        elif opts.get_group:
            self._get_group(rest, opts)
        elif opts.set_group:
            self._set_group(rest, opts)
        elif opts.list_group:
            self._list_groups(rest)
        elif opts.delete_group:
            self._delete_group(rest, opts)

    def _delete_group(self, rest, opts):
        if opts.group is None:
            _exitIfErrors(['--group-name is required with the --delete-group option'])

        _, errors = rest.delete_user_group(opts.group)
        _exitIfErrors(errors)
        _success(f"Group '{opts.group}' was deleted")

    def _get_group(self, rest, opts):
        if opts.group is None:
            _exitIfErrors(['--group-name is required with the --get-group option'])

        group, errors = rest.get_user_group(opts.group)
        _exitIfErrors(errors)
        print(json.dumps(group, indent=2))

    def _set_group(self, rest, opts):
        if opts.group is None:
            _exitIfErrors(['--group-name is required with --set-group'])

        _, errors = rest.set_user_group(opts.group, opts.roles, opts.description, opts.ldap_ref)
        _exitIfErrors(errors)
        _success(f"Group '{opts.group}' set")

    def _list_groups(self, rest):
        groups, errors = rest.list_user_groups()
        _exitIfErrors(errors)
        print(json.dumps(groups, indent=2))

    def _delete(self, rest, opts):
        if opts.rbac_user is None:
            _exitIfErrors(["--rbac-username is required with the --delete option"])
        if opts.rbac_pass is not None:
            _warning("--rbac-password is not used with the --delete option")
        if opts.rbac_name is not None:
            _warning("--rbac-name is not used with the --delete option")
        if opts.roles is not None:
            _warning("--roles is not used with the --delete option")
        if opts.auth_domain is None:
            _exitIfErrors(["--auth-domain is required with the --delete option"])

        _, errors = rest.delete_rbac_user(opts.rbac_user, opts.auth_domain)
        _exitIfErrors(errors)
        _success(f"User '{opts.rbac_user}' was removed")

    def _list(self, rest, opts):
        if opts.rbac_user is not None:
            _warning("--rbac-username is not used with the --list option")
        if opts.rbac_pass is not None:
            _warning("--rbac-password is not used with the --list option")
        if opts.rbac_name is not None:
            _warning("--rbac-name is not used with the --list option")
        if opts.roles is not None:
            _warning("--roles is not used with the --list option")
        if opts.auth_domain is not None:
            _warning("--auth-domain is not used with the --list option")

        result, errors = rest.list_rbac_users()
        _exitIfErrors(errors)
        print(json.dumps(result, indent=2))

    def _get(self, rest, opts):
        if opts.rbac_user is None:
            _warning("--rbac-username is required with the --get option")
        if opts.rbac_pass is not None:
            _warning("--rbac-password is not used with the --get option")
        if opts.rbac_name is not None:
            _warning("--rbac-name is not used with the --get option")
        if opts.roles is not None:
            _warning("--roles is not used with the --get option")
        if opts.auth_domain is not None:
            _warning("--auth-domain is not used with the --get option")

        result, errors = rest.list_rbac_users()
        _exitIfErrors(errors)
        user = [u for u in result if u['id'] == opts.rbac_user]

        if len(user) != 0:
            print(json.dumps(user, indent=2))
        else:
            _exitIfErrors([f'no user {opts.rbac_user}'])

    def _my_roles(self, rest, opts):
        if opts.rbac_user is not None:
            _warning("--rbac-username is not used with the --my-roles option")
        if opts.rbac_pass is not None:
            _warning("--rbac-password is not used with the --my-roles option")
        if opts.rbac_name is not None:
            _warning("--rbac-name is not used with the --my-roles option")
        if opts.roles is not None:
            _warning("--roles is not used with the --my-roles option")
        if opts.auth_domain is not None:
            _warning("--auth-domain is not used with the --my-roles option")

        result, errors = rest.my_roles()
        _exitIfErrors(errors)
        print(json.dumps(result, indent=2))

    def _set(self, rest, opts):
        if opts.rbac_user is None:
            _exitIfErrors(["--rbac-username is required with the --set option"])
        if opts.rbac_pass is not None and opts.auth_domain == "external":
            _warning("--rbac-password cannot be used with the external auth domain")
            opts.rbac_pass = None
        if opts.auth_domain is None:
            _exitIfErrors(["--auth-domain is required with the --set option"])

        _, errors = rest.set_rbac_user(opts.rbac_user, opts.rbac_pass, \
                                       opts.rbac_name, opts.roles, \
                                       opts.auth_domain, opts.groups)
        _exitIfErrors(errors)

        if opts.roles is not None and "query_external_access" in opts.roles:
            _warning("Granting the query_external_access role permits execution of the N1QL " +
                "function CURL() and may allow access to other network endpoints in the local " +
                "network and the Internet.")

        _success(f"User {opts.rbac_user} set")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-user-manage" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Manage RBAC users"


class XdcrReplicate(Subcommand):
    """The xdcr replicate subcommand"""

    def __init__(self):
        super(XdcrReplicate, self).__init__()
        self.parser.prog = "couchbase-cli xdcr-replicate"
        group = self.parser.add_argument_group("XDCR replicate options")
        group.add_argument("--create", dest="create", action="store_true",
                           default=False, help="Create an XDCR replication")
        group.add_argument("--delete", dest="delete", action="store_true",
                           default=False, help="Delete an XDCR replication")
        group.add_argument("--pause", dest="pause", action="store_true",
                           default=False, help="Pause an XDCR replication")
        group.add_argument("--list", dest="list", action="store_true",
                           default=False, help="List all XDCR replications")
        group.add_argument("--resume", dest="resume", action="store_true",
                           default=False, help="Resume an XDCR replication")
        group.add_argument("--settings", dest="settings", action="store_true",
                           default=False, help="Set advanced settings for an XDCR replication")
        group.add_argument("--xdcr-from-bucket", dest="from_bucket", metavar="<bucket>",
                           help="The name bucket to replicate data from")
        group.add_argument("--xdcr-to-bucket", dest="to_bucket", metavar="<bucket>",
                           help="The name bucket to replicate data to")
        group.add_argument("--xdcr-cluster-name", dest="cluster_name", metavar="<name>",
                           help="The name of the cluster reference to replicate to")
        group.add_argument("--xdcr-replication-mode", dest="rep_mode", metavar="<mode>",
                           choices=["xmem", "capi"],
                           help="The replication protocol (capi or xmem)")
        group.add_argument("--filter-expression", dest="filter", metavar="<regex>",
                           help="Regular expression to filter replication streams")
        group.add_argument("--filter-skip-restream", dest="filter_skip", action="store_true", default=False,
                           help="Restart the replication. It must be specified together with --filter-expression")
        group.add_argument("--xdcr-replicator", dest="replicator_id", metavar="<id>",
                           help="Replication ID")
        group.add_argument("--checkpoint-interval", dest="chk_int", type=(int), metavar="<seconds>",
                           help="Intervals between checkpoints in seconds (60 to 14400)")
        group.add_argument("--worker-batch-size", dest="worker_batch_size", type=(int),
                           metavar="<num>", help="Doc batch size (500 to 10000)")
        group.add_argument("--doc-batch-size", dest="doc_batch_size", type=(int), metavar="<KB>",
                           help="Document batching size in KB (10 to 100000)")
        group.add_argument("--failure-restart-interval", dest="fail_interval", type=(int),
                           metavar="<seconds>",
                           help="Interval for restarting failed xdcr in seconds (1 to 300)")
        group.add_argument("--optimistic-replication-threshold", dest="rep_thresh", type=(int),
                           metavar="<bytes>",
                           help="Document body size threshold to trigger optimistic replication" +
                           " (bytes)")
        group.add_argument("--source-nozzle-per-node", dest="src_nozzles", type=(int),
                           metavar="<num>",
                           help="The number of source nozzles per source node (1 to 10)")
        group.add_argument("--target-nozzle-per-node", dest="dst_nozzles", type=(int),
                           metavar="<num>",
                           help="The number of outgoing nozzles per target node (1 to 10)")
        group.add_argument("--bandwidth-usage-limit", dest="usage_limit", type=(int),
                           metavar="<num>", help="The bandwidth usage limit in MB/Sec")
        group.add_argument("--enable-compression", dest="compression", metavar="<1|0>", choices=["1", "0"],
                           help="Enable/disable compression")
        group.add_argument("--log-level", dest="log_level", metavar="<level>",
                           choices=["Error", "Info", "Debug", "Trace"],
                           help="The XDCR log level")
        group.add_argument("--stats-interval", dest="stats_interval", metavar="<ms>",
                           help="The interval for statistics updates (in milliseconds)")
        group.add_argument("--priority", dest="priority", choices=['High', 'Medium', 'Low'],
                           metavar="<High|Medium|Low>", help='XDCR priority, by default set to High')
        group.add_argument('--reset-expiry', choices=['1', '0'], metavar='<1|0>', dest='reset_expiry',
                           default=None, help='When set to true the expiry of mutations will be set to zero')
        group.add_argument('--filter-deletion', choices=['1', '0'], metavar='<1|0>', default=None, dest='filter_del',
                           help='When set to true delete mutations will be filter out and not sent to the target '
                                'cluster')
        group.add_argument('--filter-expiration', choices=['1', '0'], metavar='<1|0>', default=None, dest='filter_exp',
                           help='When set to true expiry mutations will be filter out and not sent to the target '
                                'cluster')

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if not enterprise and opts.compression:
            _exitIfErrors(["--enable-compression can only be configured on enterprise edition"])

        if opts.compression == "0":
            opts.compression = "None"
        elif opts.compression =="1":
            opts.compression = "Auto"

        actions = sum([opts.create, opts.delete, opts.pause, opts.list, opts.resume,
                       opts.settings])
        if actions == 0:
            _exitIfErrors(["Must specify one of --create, --delete, --pause, --list," +
                           " --resume, --settings"])
        elif actions > 1:
            _exitIfErrors(["The --create, --delete, --pause, --list, --resume, --settings" +
                           " flags may not be specified at the same time"])
        elif opts.create:
            self._create(rest, opts)
        elif opts.delete:
            self._delete(rest, opts)
        elif opts.pause or opts.resume:
            self._pause_resume(rest, opts)
        elif opts.list:
            self._list(rest, opts)
        elif opts.settings:
            self._settings(rest, opts)

    def _create(self, rest, opts):
        _, errors = rest.create_xdcr_replication(opts.cluster_name, opts.to_bucket, opts.from_bucket, opts.filter,
                                                 opts.rep_mode, opts.compression, opts.reset_expiry, opts.filter_del,
                                                 opts.filter_exp)
        _exitIfErrors(errors)

        _success("XDCR replication created")

    def _delete(self, rest, opts):
        if opts.replicator_id is None:
            _exitIfErrors(["--xdcr-replicator is needed to delete a replication"])

        _, errors = rest.delete_xdcr_replicator(opts.replicator_id)
        _exitIfErrors(errors)

        _success("XDCR replication deleted")

    def _pause_resume(self, rest, opts):
        if opts.replicator_id is None:
            _exitIfErrors(["--xdcr-replicator is needed to pause or resume a replication"])

        tasks, errors = rest.get_tasks()
        _exitIfErrors(errors)
        for task in tasks:
            if task["type"] == "xdcr" and task["id"] == opts.replicator_id:
                if opts.pause and task["status"] == "notRunning":
                    _exitIfErrors(["The replication is not running yet. Pause is not needed"])
                if opts.resume and task["status"] == "running":
                    _exitIfErrors(["The replication is running already. Resume is not needed"])
                break

        if opts.pause:
            _, errors = rest.pause_xdcr_replication(opts.replicator_id)
            _exitIfErrors(errors)
            _success("XDCR replication paused")
        elif opts.resume:
            _, errors = rest.resume_xdcr_replication(opts.replicator_id)
            _exitIfErrors(errors)
            _success("XDCR replication resume")

    def _list(self, rest, opts):
        tasks, errors = rest.get_tasks()
        _exitIfErrors(errors)
        for task in tasks:
            if task["type"] == "xdcr":
                print(f'stream id: {task["id"]}')
                print(f'   status: {task["status"]}')
                print(f'   source: {task["source"]}')
                print(f'   target: {task["target"]}')
                if "filterExpression" in task and task["filterExpression"] != "":
                    print(f'   filter: {task["filterExpression"]}')

    def _settings(self, rest, opts):
        if opts.replicator_id is None:
            _exitIfErrors(["--xdcr-replicator is needed to change a replicators settings"])
        if opts.filter_skip and opts.filter is None:
            _exitIfErrors(["--filter-expersion is needed with the --filter-skip-restream option"])
        _, errors = rest.xdcr_replicator_settings(opts.chk_int, opts.worker_batch_size,
                                                  opts.doc_batch_size, opts.fail_interval,
                                                  opts.rep_thresh, opts.src_nozzles,
                                                  opts.dst_nozzles, opts.usage_limit,
                                                  opts.compression, opts.log_level,
                                                  opts.stats_interval, opts.replicator_id, opts.filter,
                                                  opts.filter_skip, opts.priority, opts.reset_expiry,
                                                  opts.filter_del, opts.filter_exp)
        _exitIfErrors(errors)

        _success("XDCR replicator settings updated")

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-xdcr-replicate" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Manage XDCR cluster references"


class XdcrSetup(Subcommand):
    """The xdcr setup subcommand"""

    def __init__(self):
        super(XdcrSetup, self).__init__()
        self.parser.prog = "couchbase-cli xdcr-setup"
        group = self.parser.add_argument_group("XDCR setup options")
        group.add_argument("--create", dest="create", action="store_true",
                           default=False, help="Create an XDCR remote reference")
        group.add_argument("--delete", dest="delete", action="store_true",
                           default=False, help="Delete an XDCR remote reference")
        group.add_argument("--edit", dest="edit", action="store_true",
                           default=False, help="Set the local read-only user")
        group.add_argument("--list", dest="list", action="store_true",
                           default=False, help="List all XDCR remote references")
        group.add_argument("--xdcr-cluster-name", dest="name", metavar="<name>",
                           help="The name for the remote cluster reference")
        group.add_argument("--xdcr-hostname", dest="hostname", metavar="<hostname>",
                           help="The hostname of the remote cluster reference")
        group.add_argument("--xdcr-username", dest="r_username", metavar="<username>",
                           help="The username of the remote cluster reference")
        group.add_argument("--xdcr-password", dest="r_password", metavar="<password>",
                           help="The password of the remote cluster reference")
        group.add_argument("--xdcr-user-certificate", dest="r_certificate", metavar="<path>",
                           help="The user certificate for authentication")
        group.add_argument("--xdcr-user-key", dest="r_key", metavar="<path>",
                           help="The user key for authentication")
        group.add_argument("--xdcr-demand-encryption", dest="encrypt", choices=["0", "1"],
                           action=CBDeprecatedAction, help=SUPPRESS)
        group.add_argument("--xdcr-encryption-type", dest="encryption_type", choices=["full", "half"],
                           metavar="<type>", action=CBDeprecatedAction, help=SUPPRESS)
        group.add_argument("--xdcr-certificate", dest="certificate", metavar="<path>",
                           help="The certificate used for encryption")
        group.add_argument("--xdcr-secure-connection", dest="secure_connection", choices=["none", "full", "half"],
                           metavar="<type>", help="The XDCR secure connection type")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        actions = sum([opts.create, opts.delete, opts.edit, opts.list])
        if actions == 0:
            _exitIfErrors(["Must specify one of --create, --delete, --edit, --list"])
        elif actions > 1:
            _exitIfErrors(["The --create, --delete, --edit, --list flags may not " +
                           "be specified at the same time"])
        elif opts.create or opts.edit:
            self._set(rest, opts)
        elif opts.delete:
            self._delete(rest, opts)
        elif opts.list:
            self._list(rest, opts)

    def _set(self, rest, opts):
        cmd = "create"
        if opts.edit:
            cmd = "edit"

        if opts.name is None:
            _exitIfErrors([f'--xdcr-cluster-name is required to {cmd} a cluster connection'])
        if opts.hostname is None:
            _exitIfErrors([f'--xdcr-hostname is required to {cmd} a cluster connections'])
        if opts.username is None:
            _exitIfErrors([f'--xdcr-username is required to {cmd} a cluster connections'])
        if opts.password is None:
            _exitIfErrors([f'--xdcr-password is required to {cmd} a cluster connections'])
        if (opts.encrypt is not None or opts.encryption_type is not None) and opts.secure_connection is not None:
            _exitIfErrors(["Cannot use deprecated flags --xdcr-demand-encryption or --xdcr-encryption-type with"
                           " --xdcr-secure-connection"])

        if opts.secure_connection == "none":
            opts.encrypt = "0"
            opts.encryption_type = None
        elif opts.secure_connection == "half":
            opts.encrypt = "1"
            opts.encryption_type = "half"
        elif opts.secure_connection == "full":
            opts.encrypt = "1"
            opts.encryption_type = "full"
        elif opts.encrypt is None and opts.encryption_type is None:
            opts.encrypt = "0"
            opts.encryption_type = None

        raw_cert = None
        if opts.encrypt == "1":
            if opts.encryption_type == None:
                opts.encryption_type = "full"

            if opts.encryption_type == "full":
                if opts.certificate is None:
                    _exitIfErrors(["certificate required if encryption is demanded"])
                raw_cert = _exit_on_file_read_failure(opts.certificate)

        raw_user_key = None
        if opts.r_key:
            raw_user_key = _exit_on_file_read_failure(opts.r_key)
        raw_user_cert = None
        if opts.r_certificate:
            raw_user_cert = _exit_on_file_read_failure(opts.r_certificate)

        if opts.create:
            _, errors = rest.create_xdcr_reference(opts.name, opts.hostname, opts.r_username,
                                                   opts.r_password, opts.encrypt, opts.encryption_type,
                                                   raw_cert, raw_user_cert, raw_user_key)
            _exitIfErrors(errors)
            _success("Cluster reference created")
        else:
            _, errors = rest.edit_xdcr_reference(opts.name, opts.hostname, opts.r_username,
                                                 opts.r_password, opts.encrypt, opts.encryption_type,
                                                 raw_cert, raw_user_cert, raw_user_key)
            _exitIfErrors(errors)
            _success("Cluster reference edited")

    def _delete(self, rest, opts):
        if opts.name is None:
            _exitIfErrors(["--xdcr-cluster-name is required to deleta a cluster connection"])

        _, errors = rest.delete_xdcr_reference(opts.name)
        _exitIfErrors(errors)

        _success("Cluster reference deleted")

    def _list(self, rest, opts):
        clusters, errors = rest.list_xdcr_references()
        _exitIfErrors(errors)

        for cluster in clusters:
            if not cluster["deleted"]:
                print(f'cluster name: {cluster["name"]}')
                print(f'        uuid: {cluster["uuid"]}')
                print(f'   host name: {cluster["hostname"]}')
                print(f'   user name: {cluster["username"]}')
                print(f'         uri: {cluster["uri"]}')

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-xdcr-setup" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Manage XDCR replications"

class EventingFunctionSetup(Subcommand):
    """The Eventing Service Function setup subcommand"""

    def __init__(self):
        super(EventingFunctionSetup, self).__init__()
        self.parser.prog = "couchbase-cli eventing-function-setup"
        group = self.parser.add_argument_group("Eventing Service Function setup options")
        group.add_argument("--import", dest="_import", action="store_true",
                           default=False, help="Import functions")
        group.add_argument("--export", dest="export", action="store_true",
                           default=False, help="Export a function")
        group.add_argument("--export-all", dest="export_all", action="store_true",
                           default=False, help="Export all functions")
        group.add_argument("--delete", dest="delete", action="store_true",
                           default=False, help="Delete a function")
        group.add_argument("--list", dest="list", action="store_true",
                           default=False, help="List all functions")
        group.add_argument("--deploy", dest="deploy", action="store_true",
                           default=False, help="Deploy a function")
        group.add_argument("--undeploy", dest="undeploy", action="store_true",
                           default=False, help="Undeploy a function")
        group.add_argument("--boundary", dest="boundary", metavar="<from-everything|from-now>",
                           choices=["from-everything", "from-now"], default=False,
                           help="Set the function deployment boundary")
        group.add_argument("--name", dest="name", metavar="<name>",
                           default=False, help="The name of the function to take an action on")
        group.add_argument("--file", dest="filename", metavar="<file>",
                           default=False, help="The file to export and import function(s) to and from")
        group.add_argument("--pause", dest="pause", action="store_true", help="Pause a function")
        group.add_argument("--resume", dest="resume", action="store_true", help="Resume a function")

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        actions = sum([opts._import, opts.export, opts.export_all, opts.delete, opts.list, opts.deploy, opts.undeploy, opts.pause, opts.resume])
        if actions == 0:
            _exitIfErrors(["Must specify one of --import, --export, --export-all, --delete, --list, --deploy,"
                           " --undeploy, --pause, --resume"])
        elif actions > 1:
            _exitIfErrors(["The --import, --export, --export-all, --delete, --list, --deploy, --undeploy, --pause, --resume flags may"
                           " not be specified at the same time"])
        elif opts._import:
            self._import(rest, opts)
        elif opts.export:
            self._export(rest, opts)
        elif opts.export_all:
            self._export_all(rest, opts)
        elif opts.delete:
            self._delete(rest, opts)
        elif opts.list:
            self._list(rest)
        elif opts.deploy:
            self._deploy_undeploy(rest, opts, True)
        elif opts.undeploy:
            self._deploy_undeploy(rest, opts, False)
        elif opts.pause:
            self._pause_resume(rest, opts, True)
        elif opts.resume:
            self._pause_resume(rest, opts, False)

    def _pause_resume(self, rest, opts, pause):
        if not opts.name:
            _exitIfErrors([f"Flag --name is required with the {'--pause' if pause else '--resume'} flag"])
        _, err = rest.pause_resume_function(opts.name, pause)
        _exitIfErrors(err)
        _success(f"Function was {'paused' if pause else 'resumed'}")

    def _import(self, rest, opts):
        if not opts.filename:
            _exitIfErrors(["--file is needed to import functions"])
        import_functions = _exit_on_file_read_failure(opts.filename)
        import_functions = json.loads(import_functions)
        _, errors = rest.import_functions(import_functions)
        _exitIfErrors(errors)
        _success("Events imported")

    def _export(self, rest, opts):
        if not opts.filename:
            _exitIfErrors(["--file is needed to export a function"])
        if not opts.name:
            _exitIfErrors(["--name is needed to export a function"])
        functions, errors = rest.export_functions()
        _exitIfErrors(errors)
        exported_function = None
        for function in functions:
            if function["appname"] == opts.name:
                exported_function = [function]
        if not exported_function:
            _exitIfErrors([f'Function {opts.name} does not exist'])
        _exit_on_file_write_failure(opts.filename, json.dumps(exported_function, separators=(',',':')))
        _success("Function exported to: " + opts.filename)

    def _export_all(self, rest, opts):
        if not opts.filename:
            _exitIfErrors(["--file is needed to export all functions"])
        exported_functions, errors = rest.export_functions()
        _exitIfErrors(errors)
        _exit_on_file_write_failure(opts.filename, json.dumps(exported_functions, separators=(',',':')))
        _success(f'All functions exported to: {opts.filename}')

    def _delete(self, rest, opts):
        if not opts.name:
            _exitIfErrors(["--name is needed to delete a function"])
        _, errors = rest.delete_function(opts.name)
        _exitIfErrors(errors)
        _success("Request to delete the function was accepted")

    def _deploy_undeploy(self, rest, opts, deploy):
        if not opts.name:
            _exitIfErrors([f"--name is needed to {'deploy' if deploy else 'undeploy'} a function"])
        if deploy and not opts.boundary:
            _exitIfErrors([f"--boundary is needed to deploy a function"])
        _, errors = rest.deploy_undeploy_function(opts.name, deploy, opts.boundary)
        _exitIfErrors(errors)
        _success(f"Request to {'deploy' if deploy else 'undeploy'} the function was accepted")

    def _list(self, rest):
        functions, errors = rest.list_functions()
        _exitIfErrors(errors)

        for function in functions:
            print(function['appname'])
            status = ''
            if function['settings']['deployment_status']:
                status = 'Deployed'
            else:
                status = 'Undeployed'
            print(f' Status: {status}')
            print(f' Source Bucket: {function["depcfg"]["source_bucket"]}')
            print(f' Metadata Bucket: {function["depcfg"]["metadata_bucket"]}')

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-eventing-function-setup" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Manage Eventing Service Functions"

class UserChangePassword(Subcommand):
    """The change password subcommand"""

    def __init__(self):
        super(UserChangePassword, self).__init__()
        self.parser.prog = "couchbase-cli user-change-password"
        group = self.parser.add_argument_group("User password change option")
        group.add_argument("--new-password", dest="new_pass", metavar="<password>", required=True,
                           help="The new password")

    def execute(self, opts):
        if opts.new_pass is None:
            _exitIfErrors(["--new-password is required"])

        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        _, rv = rest.user_change_passsword(opts.new_pass)
        _exitIfErrors(rv)
        _success(f'Changed password for {opts.username}')

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-user-change-password" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Change user password"


class CollectionManage(Subcommand):
    """The collections-manage subcommand"""

    def __init__(self):
        super(CollectionManage, self).__init__()
        self.parser.prog = "couchbase-cli collection-manage"
        group = self.parser.add_argument_group("Collection manage option")
        group.add_argument("--bucket", dest="bucket", metavar="<bucket>", required=True, help="The bucket to use")
        group.add_argument("--create-scope", dest="create_scope", metavar="<scope>", default=None,
                           help="The name of the scope to make")
        group.add_argument("--drop-scope", dest="drop_scope", metavar="<scope>", default=None,
                           help="The name of the scope to remove")
        group.add_argument("--list-scopes", dest="list_scopes", action="store_true", default=None,
                           help="List all of the scopes in the bucket")
        group.add_argument("--create-collection", dest="create_collection", metavar="<collection>", default=None,
                           help="The path to the collection to make")
        group.add_argument("--drop-collection", dest="drop_collection", metavar="<collection>", default=None,
                           help="The path to the collection to remove")
        group.add_argument("--list-collections", dest="list_collections", metavar="<scope>", default=None,
                           const="_default", nargs='?', help="List all of the collections in the scope")
        group.add_argument("--max-ttl", dest="max_ttl", metavar="<seconds>", type=int,
                           help="Set the maximum TTL the collection will accept")

    def execute(self, opts):
        cmds = [opts.create_scope, opts.drop_scope, opts.list_scopes, opts.create_collection, opts.drop_collection,
                opts.list_collections]
        cmd_total = sum(cmd is not None for cmd in cmds)

        args = "--create-scope, --drop-scope, --list-scopes, --create-collection, --drop-collection, or " \
               "--list-collections"
        if cmd_total == 0:
            _exitIfErrors([f'Must specify one of the following: {args}'])
        elif cmd_total != 1:
            _exitIfErrors([f'Only one of the following may be specified: {args}'])

        if opts.max_ttl is not None and opts.create_collection is None:
            _exitIfErrors(["--max-ttl can only be set with --create-collection"])

        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        if opts.create_scope:
            self._create_scope(rest, opts)
        if opts.drop_scope:
            self._drop_scope(rest, opts)
        if opts.list_scopes:
            self._list_scopes(rest, opts)
        if opts.create_collection:
            self._create_collection(rest, opts)
        if opts.drop_collection:
            self._drop_collection(rest, opts)
        if opts.list_collections:
            self._list_collections(rest, opts)


    def _create_scope(self, rest, opts):
        _, errors = rest.create_scope(opts.bucket, opts.create_scope)
        _exitIfErrors(errors)
        _success("Scope created")

    def _drop_scope(self, rest, opts):
        _, errors = rest.drop_scope(opts.bucket, opts.drop_scope)
        _exitIfErrors(errors)
        _success("Scope deleted")

    def _list_scopes(self, rest, opts):
        manifest, errors = rest.get_manifest(opts.bucket)
        _exitIfErrors(errors)
        for scope in manifest['scopes']:
            print(scope['name'])

    def _create_collection(self, rest, opts):
        scope, collection = self._get_scope_collection(opts.create_collection)
        _, errors = rest.create_collection(opts.bucket, scope, collection, opts.max_ttl)
        _exitIfErrors(errors)
        _success("Collection created")

    def _drop_collection(self, rest, opts):
        scope, collection = self._get_scope_collection(opts.drop_collection)
        _, errors = rest.drop_collection(opts.bucket, scope, collection)
        _exitIfErrors(errors)
        _success("Collection deleted")

    def _list_collections(self, rest, opts):
        manifest, errors = rest.get_manifest(opts.bucket)
        _exitIfErrors(errors)
        found_scope = False
        for scope in manifest['scopes']:
            if opts.list_collections == scope['name']:
                found_scope = True
                for collection in scope['collections']:
                    print(collection['name'])
        if not found_scope:
            _exitIfErrors([f"Scope {opts.list_collections} does not exist"])

    def _get_scope_collection(self, path):
        paths = path.split('.')
        if len(paths) == 2:
            if not '' in paths:
                return paths[0], paths[1]
        _exitIfErrors(["Path is not valid. It should be: scope.collection"])


    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-collection-manage" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Manage collections in a bucket"


class EnableDeveloperPreview(Subcommand):
    """"The enable developer preview command"""

    def __init__(self):
        super(EnableDeveloperPreview, self).__init__()
        self.parser.prog = "couchbase-cli enable-developer-preview"
        group = self.parser.add_argument_group("Developer preview option")
        group.add_argument('--enable', dest='enable', required=False, action="store_true",
                           help='Enable developer preview mode in target cluster')
        group.add_argument('--list', dest='list', required=False, action="store_true",
                           help='Check if cluster is in developer preview mode')

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        if not (opts.enable or opts.list):
            _exitIfErrors(['--enable or --list must be provided'])
        if opts.enable and opts.list:
            _exitIfErrors(['cannot provide both --enable and --list'])

        if opts.enable:
            confirm = input('Developer preview cannot be disabled once it is enabled. ' +
                            'If you enter developer preview mode you will not be able to ' +
                            'upgrade. DO NOT USE IN PRODUCTION.\nAre you sure [y/n]: ')
            if confirm == 'y':
                _, errors = rest.set_dp_mode()
                _exitIfErrors(errors)
                _success("Cluster is in developer preview mode")
            elif confirm == 'n':
                _success("Developer preview mode has NOT been enabled")
            else:
                _exitIfErrors(["Unknown option provided"])

        if opts.list:
            pools, rv = rest.pools()
            _exitIfErrors(rv)
            if 'isDeveloperPreview' in pools and pools['isDeveloperPreview']:
                print('Cluster is in developer preview mode')
            else:
                print('Cluster is NOT in developer preview mode')

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-enable-developer-preview" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Enable developer preview mode in target cluster"


class SettingAlternateAddress(Subcommand):
    """"Setting alternate address command"""

    def __init__(self):
        super(SettingAlternateAddress, self).__init__()
        self.parser.prog = "couchbase-cli setting-alternate-address"
        group = self.parser.add_argument_group("Configure alternate addresses")
        group.add_argument('--set', dest='set', required=False, action="store_true",
                           help='Set external address configuration for the node')
        group.add_argument('--remove', dest='remove', required=False, action="store_true",
                           help='Remove external address configuration')
        group.add_argument('--list', dest='list', required=False, action='store_true',
                           help='Retrieve current alternate address configuration for all nodes')
        group.add_argument('--node', dest='node', metavar="<node>", help="Specify the node to update")
        group.add_argument('--hostname', dest='alternate_hostname', metavar="<host>", help='Alternate address')
        group.add_argument('--ports', dest='ports', metavar="<ports>",
                           help="A comma separated list specifying port mappings for the services")

    def execute(self, opts):
        flags_used = sum([opts.set, opts.list, opts.remove])
        if flags_used != 1:
            _exitIfErrors(['Use exactly one of --set, --list or --remove'])
        if opts.set or opts.remove:
            if not opts.node:
                _exitIfErrors(['--node has to be set when using --set or --remove'])
            # Alternate address can only be set on the node it self. The opts.cluster
            # is updated with the opts.node instead to allow ease of use.
            # The node name can have a port number (./cluster_run)
            hostname, port = self._get_host_port(opts.node)
            url = urllib.parse.urlparse(opts.cluster)
            if url.scheme:
                scheme = url.scheme
                if url.port and not port:
                    port = url.port
            elif not port:
                _, old_port = self._get_host_port(opts.cluster)
                if old_port:
                    port = old_port
            if scheme:
                cluster = f'{scheme}://'
            cluster += hostname
            if port:
                cluster += f':{port}'
            opts.cluster = cluster
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        if opts.set:
            ports, error = self._parse_ports(opts.ports)
            _exitIfErrors(error)
            _, error = rest.set_alternate_address(opts.alternate_hostname, ports)
            _exitIfErrors(error)
        if opts.remove:
            _, error = rest.delete_alternate_address()
            _exitIfErrors(error)
            _success('Alternate address configuration deleted')
        if opts.list:
            add, error = rest.get_alternate_address()
            _exitIfErrors(error)
            if opts.output == 'standard':
                port_names = set()
                for node in add:
                    if 'alternateAddresses' in node:
                        if 'ports' in node['alternateAddresses']['external']:
                            for port in node['alternateAddresses']['external']['ports'].keys():
                                port_names.add(port)
                print('{:20}{:20}{}'.format('Hostname', 'Alternate Address', 'Ports (Primary/Alternate)'))
                print('{:40}'.format(' '), end='')
                port_names = sorted(port_names)
                for port in port_names:
                    column_size = len(port) + 1
                    if column_size < 11:
                        column_size = 11
                    print(f'{port:{column_size}}', end='')
                print()
                for node in add:
                    if 'alternateAddresses' in node:
                        # For cluster_run and single node clusters there is no hostname
                        try:
                            print(f'{node["hostname"]:20}{node["alternateAddresses"]["external"]["hostname"]:20}', end='')
                        except KeyError:
                            host = 'UNKNOWN'
                            print(f'{host:20}{node["alternateAddresses"]["external"]["hostname"]:20}', end='')
                        for port in port_names:
                            column_size = len(port) + 1
                            if column_size < 11:
                                column_size = 11
                            ports = ' '
                            if port in node['alternateAddresses']['external']['ports']:
                                ports = f'{str(node["services"][port])}' \
                                        f'/{str(node["alternateAddresses"]["external"]["ports"][port])}'
                            print(f'{ports:{column_size}}', end='')
                        print()
                    else:
                        # For cluster_run and single node clusters there is no hostanme
                        try:
                            print(f'{node["hostname"]}')
                        except KeyError:
                            print('UNKNOWN')
            else:
                print(json.dumps(add))

    @staticmethod
    def _parse_ports(ports):
        if ports is None:
            return None, None

        port_mappings = ports.split(',')
        map = []
        for port_value_pair in port_mappings:
            p_v = port_value_pair.split('=')
            if len(p_v) != 2:
                return None, [f'invalid port mapping: {port_value_pair}']
            try:
                int(p_v[1])
            except ValueError:
                return None, [f'invalid port mapping: {port_value_pair}']
            map.append((p_v[0], p_v[1]))
        return map, None

    @staticmethod
    def _get_host_port(host):
        if ']' in host:
            host_port = host.split(']:')
            if len(host_port) == 2:
                return host_port[0] + ']', host_port[1]
            return host_port[0], None
        else:
            host_port = host.split(':')
            if len(host_port) == 2:
                return host_port[0], host_port[1]
            return host_port[0], None

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-alternate-address" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Configure alternate addresses"


class SettingQuery(Subcommand):
    """"Command to configure query settings"""

    def __init__(self):
        super(SettingQuery, self).__init__()
        self.parser.prog = "couchbase-cli setting-query"
        group = self.parser.add_argument_group("Query service settings")
        group.add_argument('--set', dest='set', action="store_true",
                           help='Set query settings')
        group.add_argument('--get', dest='get', action="store_true",
                           help='Retrieve current query settings')
        group.add_argument('--pipeline-batch', metavar='<num>', type=int, default=None,
                           help='Number of items execution operators can batch.')
        group.add_argument('--pipeline-cap', metavar='<num>', type=int, default=None,
                           help='Maximum number of items each execution operator can buffer.')
        group.add_argument('--scan-cap', metavar='<size>', type=int, default=None,
                           help='Maximum buffer size for index scans.')
        group.add_argument('--timeout', metavar='<ms>', type=int, default=None,
                           help='Server execution timeout.')
        group.add_argument('--prepared-limit', metavar='<max>', type=int, default=None,
                           help='Maximum number of prepared statements.')
        group.add_argument('--completed-limit', metavar='<max>', type=int, default=None,
                           help='Maximum number of completed requests.')
        group.add_argument('--completed-threshold', metavar='<ms>', type=int, default=None,
                           help='Cache completed query lasting longer than this many milliseconds.')
        group.add_argument('--log-level', choices=['trace', 'debug', 'info', 'warn', 'error', 'sever', 'none'],
                           default=None, metavar='<trace|debug|info|warn|error|server|none>',
                           help='Log level: debug, trace, info, warn, error, severe, none.')
        group.add_argument('--max-parallelism', metavar='<max>', type=int, default=None,
                           help='Maximum parallelism per query.')
        group.add_argument('--n1ql-feature-control', metavar='<num>', type=int, default=None,
                           help='N1QL Feature Controls')

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)
        if sum([opts.get, opts.set]) != 1:
            _exitIfErrors(['Please provide --set or --get, both can not be provided at the same time'])

        if opts.get:
            settings, err = rest.get_query_settings()
            _exitIfErrors(err)
            print(json.dumps(settings))
        if opts.set:
            if all(v is None for v in [opts.pipeline_batch, opts.pipeline_cap, opts.scan_cap, opts.timeout,
                                       opts.prepared_limit, opts.completed_limit, opts.completed_threshold,
                                       opts.log_level, opts.max_parallelism, opts.n1ql_feature_control]):
                _exitIfErrors(['Please provide at least one other option with --set'])

            _, err = rest.post_query_settings(opts.pipeline_batch, opts.pipeline_cap, opts.scan_cap, opts.timeout,
                                              opts.prepared_limit, opts.completed_limit, opts.completed_threshold,
                                              opts.log_level, opts.max_parallelism, opts.n1ql_feature_control)
            _exitIfErrors(err)
            _success('Updated the query settings')


    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-query" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Manage query settings"


class IpFamily(Subcommand):
    """"Command to switch between IP family for node to node communication"""

    def __init__(self):
        super(IpFamily, self).__init__()
        self.parser.prog = "couchbase-cli ip-family"
        group = self.parser.add_argument_group("IP family options")
        group.add_argument('--get', action="store_true", default=False, help='Retrieve current used IP family')
        group.add_argument('--set', action="store_true", default=False, help='Change current used IP family')
        group.add_argument('--ipv4', dest='ipv4', default=False, action="store_true",
                           help='Set IP family to IPv4')
        group.add_argument('--ipv6', dest='ipv6', default=False, action="store_true",
                           help='Set IP family to IPv6')

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        flags_used = sum([opts.set, opts.get])
        if flags_used == 0:
            _exitIfErrors(['Please provide one of --set, or --get'])
        elif flags_used > 1:
            _exitIfErrors(['Please provide only one of --set, or --get'])

        if opts.get:
            self._get(rest)
        if opts.set:
            if sum([opts.ipv6, opts.ipv4]) != 1:
                _exitIfErrors(['Provided exactly one of --ipv4 or --ipv6 together with the --set option'])

            self._set(rest, opts.ipv6, opts.ssl)

    @staticmethod
    def _set(rest, ipv6, ssl):
        ip_fam, ip_fam_disable = ('ipv6', 'ipv4') if ipv6 else ('ipv4', 'ipv6')
        node_data, err = rest.pools('nodes')

        if err and err[0] == '"unknown pool"':
            _, err = rest.enable_external_listener(ipfamily=ip_fam)
            _exitIfErrors(err)
            _, err = rest.setup_net_config(ipfamily=ip_fam)
            _exitIfErrors(err)
            _, err = rest.disable_external_listener(ipfamily=ip_fam_disable)
            _exitIfErrors(err)
            _success('Switched IP family of the cluster')
            return

        _exitIfErrors(err)

        hosts = []
        for n in node_data['nodes']:
            host = f'http://{n["hostname"]}'
            if ssl:
                addr = host.rsplit(":", 1)[0]
                host = f'https://{addr}:{n["ports"]["httpsMgmt"]}'
            _, err = rest.enable_external_listener(host=host, ipfamily=ip_fam)
            _exitIfErrors(err)
            hosts.append(host)

        for h in hosts:
            _, err = rest.setup_net_config(host=h, ipfamily=ip_fam)
            _exitIfErrors(err)
            print(f'Switched IP family for node: {h}')

        for h in hosts:
            _, err = rest.disable_external_listener(host=h, ipfamily=ip_fam_disable)
            _exitIfErrors(err)

        _success('Switched IP family of the cluster')

    @staticmethod
    def _get(rest):
        nodes, err = rest.nodes_info()
        _exitIfErrors(err)
        fam = {}
        for n in nodes:
            fam[n['addressFamily']] = True

        family = list(fam.keys())
        if len(family) == 1:
            ipvFam = 'UNKNOWN'
            if family[0] == 'inet' or family[0] == 'inet_tls':
                ipvFam = 'ipv4'
            elif family[0] == 'inet6' or family[0] == 'inet6_tls':
                ipvFam = 'ipv6'

            print(f'Cluster using {ipvFam}')
        else:
            print(f'Cluster is in mixed mode')

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-ip-family" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Change or get the address family"


class NodeToNodeEncryption(Subcommand):
    """"Command to enable/disable cluster encryption"""

    def __init__(self):
        super(NodeToNodeEncryption, self).__init__()
        self.parser.prog = "couchbase-cli node-to-node-encryption"
        group = self.parser.add_argument_group("Node-to-node encryption options")
        group.add_argument('--enable', action="store_true", default=False, help='Enable node-to-node encryption')
        group.add_argument('--disable', action="store_true", default=False, help='Disable node-to-node encryption')
        group.add_argument('--get', action="store_true", default=False,
                           help='Retrieve current status of node-to-node encryption (on or off)')

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_versions(rest)

        flags_used = sum([opts.enable, opts.disable, opts.get])
        if flags_used == 0:
            _exitIfErrors(['Please provide one of --enable, --disable or --get'])
        elif flags_used > 1:
            _exitIfErrors(['Please provide only one of --enable, --disable or --get'])

        if opts.get:
            self._get(rest)
        elif opts.enable:
            self._change_encryption(rest, 'on', opts.ssl)
        elif opts.disable:
            self._change_encryption(rest, 'off', opts.ssl)

    @staticmethod
    def _change_encryption(rest, encryption, ssl):
        node_data, err = rest.pools('nodes')
        encryption_disable = 'off' if encryption == 'on' else 'on'

        if err and err[0] == '"unknown pool"':
            _, err = rest.enable_external_listener(encryption=encryption)
            _exitIfErrors(err)
            _, err = rest.setup_net_config(encryption=encryption)
            _exitIfErrors(err)
            _, err = rest.disable_external_listener(encryption=encryption_disable)
            _exitIfErrors(err)
            _success(f'Switched node-to-node encryption {encryption}')
            return

        _exitIfErrors(err)

        hosts = []
        for n in node_data['nodes']:
            host = f'http://{n["hostname"]}'
            if ssl:
                addr = host.rsplit(":", 1)[0]
                host = f'https://{addr}:{n["ports"]["httpsMgmt"]}'
            _, err = rest.enable_external_listener(host=host, encryption=encryption)
            _exitIfErrors(err)
            hosts.append(host)

        for h in hosts:
            _, err = rest.setup_net_config(host=h, encryption=encryption)
            _exitIfErrors(err)
            print(f'Turned {encryption} encryption for node: {h}')

        for h in hosts:
            _, err = rest.disable_external_listener(host=h, encryption=encryption_disable)
            _exitIfErrors(err)

        _success(f'Switched node-to-node encryption {encryption}')

    @staticmethod
    def _get(rest):
        # this will start the correct listeners in all the nodes
        nodes, err = rest.nodes_info()
        _exitIfErrors(err)
        encrypted_nodes = []
        unencrpyted_nodes = []
        for n in nodes:
            if n['nodeEncryption']:
                encrypted_nodes.append(n['hostname'])
            else:
                unencrpyted_nodes.append(n['hostname'])

        if len(encrypted_nodes) == len(nodes):
            print('Node-to-node encryption is enabled')
        elif len(unencrpyted_nodes) == len(nodes):
            print('Node-to-node encryption is disabled')
        else:
            print('Cluster is in mixed mode')
            print(f'Nodes with encryption enabled: {encrypted_nodes}')
            print(f'Nodes with encryption disabled: {unencrpyted_nodes}')

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-node-to-node-encryption" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Change or get the cluster encryption configuration"


class SettingRebalance(Subcommand):
    """The rebalance subcommand"""

    def __init__(self):
        super(SettingRebalance, self).__init__()
        self.parser.prog = "couchbase-cli setting-rebalance"
        group = self.parser.add_argument_group("Rebalance configuration")
        group.add_argument("--set", default=False, action='store_true',
                           help='Set the automatic rebalance retry settings.')
        group.add_argument("--get", default=False, action='store_true',
                           help='Get the automatic rebalance retry settings.')
        group.add_argument('--cancel', default=False, action='store_true',
                           help='Cancel pending rebalance retry.')
        group.add_argument('--pending-info', default=False, action='store_true',
                           help='Get info for pending rebalance retry.')
        group.add_argument("--enable", metavar="<1|0>", choices=["1", "0"],
                           help="Enable or disable automatic rebalance retry")
        group.add_argument("--wait-for", metavar="<sec>", type=int,
                           help="Specify the time to wat before retrying the rebalance [5-3600] seconds.")
        group.add_argument("--max-attempts", metavar="<num>", type=int,
                           help="Maximum number of rebalance retires [1-3].")
        group.add_argument('--rebalance-id', metavar='<id>',
                           help='Specify the id of the failed rebalance to cancel the retry.')

    def execute(self, opts):
        rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                              opts.cacert, opts.debug)
        check_cluster_initialized(rest)
        check_versions(rest)

        enterprise, errors = rest.is_enterprise()
        _exitIfErrors(errors)

        if not enterprise:
            _exitIfErrors(["Automatic rebalance retry configuration is an Enterprise Edition only feature"])

        if sum([opts.set, opts.get, opts.cancel, opts.pending_info]) != 1:
            _exitIfErrors(['Provide either --set, --get, --cancel or --pending-info'])

        if opts.get:
            settings, err = rest.get_settings_rebalance_retry()
            _exitIfErrors(err)
            if opts.output == 'json':
                print(json.dumps(settings))
            else:
                print(f'Automatic rebalance retry {"enabled" if settings["enabled"] else "disabled"}')
                print(f'Retry wait time: {settings["afterTimePeriod"]}')
                print(f'Maximum number of retries: {settings["maxAttempts"]}')
        elif opts.set:
            if opts.enable == '1':
                opts.enable = 'true'
            else:
                opts.enable = 'false'

            if opts.wait_for is not None and (opts.wait_for < 5 or opts.wait_for > 3600):
                _exitIfErrors(['--wait-for must be a value between 5 and 3600'])
            if opts.max_attempts is not None and (opts.max_attempts < 1 or opts.max_attempts > 3):
                _exitIfErrors(['--max-attempts must be a value between 1 and 3'])

            _, err = rest.set_settings_rebalance_retry(opts.enable, opts.wait_for, opts.max_attempts)
            _exitIfErrors(err)
            _success('Automatic rebalance retry settings updated')
        elif opts.cancel:
            if opts.rebalance_id is None:
                _exitIfErrors(['Provide the failed rebalance id using --rebalance-id <id>'])
            _, err = rest.cancel_rebalance_retry(opts.rebalance_id)
            _exitIfErrors(err)
            _success('Rebalance retry canceled')
        else:
            rebalance_info, err = rest.get_rebalance_info()
            _exitIfErrors(err)
            print(json.dumps(rebalance_info))

    @staticmethod
    def get_man_page_name():
        return "couchbase-cli-setting-rebalance" + ".1" if os.name != "nt" else ".html"

    @staticmethod
    def get_description():
        return "Configure automatic rebalance settings"
