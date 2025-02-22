"""A Couchbase  CLI subcommand"""

import getpass
import inspect
import ipaddress
import json
import os
import platform
import random
import re
import socket
import string
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
from argparse import SUPPRESS, Action, ArgumentError, ArgumentParser, HelpFormatter
from operator import itemgetter
from typing import Any, Dict, List, Optional

import couchbaseConstants
from cluster_manager import ClusterManager
from pbar import TopologyProgressBar
from x509_adapter import X509AdapterError

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

VERSION_UNKNOWN = "0.0.0"
LATEST_VERSION = "8.0.0"


def check_base_path(base_path):
    required_dirs = ["bin", "etc", "lib"]
    return all(os.path.exists(os.path.join(base_path, d)) for d in required_dirs)


def get_base_cb_path():
    # Check if relative path exists
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if os.path.exists(base_path) and check_base_path(base_path):
        return base_path

    # If the base path is not at the relative path, then check the default path, which is platform dependent
    system = platform.system()
    if system == "Darwin":
        base_path = os.path.join(os.sep, "Applications", "Couchbase Server.app", "Contents", "Resources",
                                 "couchbase-core")
    elif system == "Windows":
        base_path = os.path.join("C:" + os.sep, "Program Files", "couchbase", "server")
    elif system == "Linux":
        base_path = os.path.join(os.sep, "opt", "couchbase")
    else:
        base_path = None

    if base_path and check_base_path(base_path):
        return base_path

    return None


def get_bin_path():
    base_path = get_base_cb_path()
    if not base_path:
        return None

    return os.path.join(base_path, "bin")


def get_hosts_path():
    base_path = get_base_cb_path()
    if not base_path:
        return None

    hosts_path = os.path.join(base_path, "etc", "couchbase", "hosts.cfg")
    if os.path.isfile(hosts_path):
        return hosts_path

    return None


def get_inetrc(hosts_path):
    inetrc_file = hosts_path.encode('unicode-escape').decode()
    return ['inetrc', f'"{inetrc_file}"']


def get_cfg_path():
    base_path = get_base_cb_path()
    if not base_path:
        return None

    if platform.system() == "Darwin":
        return os.path.expanduser("~/Library/Application Support/Couchbase/var/lib/couchbase")

    return os.path.join(base_path, "var", "lib", "couchbase")


def get_lib_path():
    base_path = get_base_cb_path()
    if not base_path:
        return None

    return os.path.join(base_path, "lib")


def get_ns_ebin_path():
    lib_path = get_lib_path()
    if not lib_path:
        return None

    if platform.system() == "Windows":
        return os.path.join(lib_path, "ns_server", "ebin")

    return os.path.join(lib_path, "ns_server", "erlang", "lib", "ns_server", "ebin")


def get_man_path():
    base_path = get_base_cb_path()
    if not base_path:
        return None

    share_path = os.path.join(base_path, "share")

    if platform.system() == "Windows":
        return os.path.join(share_path, "doc", "couchbase-cli")

    return os.path.join(share_path, "man", "man1")


def get_doc_page_name(command: str) -> str:
    return f'{command}.{"1" if os.name != "nt" else "html"}'


def remove_prefix(val: str, prefix: str) -> str:
    """This function removes a prefix from a string.

    Note this is a built-in function in Python 3.9 once we upgrade to it we should use it instead.
    """
    return val[len(prefix):] if val.startswith(prefix) else val


def force_communicate_tls(rest: ClusterManager) -> bool:
    """force_communicate_tls returns a boolean indicating whether we should communicate with other nodes using the TLS
    ports.

    When communicating with a cluster which has 'clusterEncryptionLevel' set to 'strict' the non-tls ports will only be
    open to 'localhost' meaning we must communicate via the non-tls ports.
    """
    settings, err = rest.get_security_settings()
    _exit_if_errors(err)

    # The cluster isn't using 'strict' cluster encryption, we shouldn't need to force enable TLS
    if 'clusterEncryptionLevel' not in settings or settings['clusterEncryptionLevel'] != 'strict':
        return False

    # The user might not have used a 'https://' scheme prefix, so communicating to other nodes via the secure ports may
    # lead to interesting/surprising errors; let them know beforehand.
    _warning("sub-command requires multi-node communication via TLS enabled ports, '--cacert' or "
             "'--no-ssl-verify' may need to be supplied")

    return True


def rest_initialiser(cluster_init_check=False, version_check=False, enterprise_check=None, credentials_required=True):
    """rest_initialiser is a decorator that does common subcommand tasks.

    The decorator will always creates a cluster manager and assign it to the subcommand variable rest
    :param cluster_init_check: if true it will check if the cluster is initialized before executing the subcommand
    :param version_check: if true it will check if the cluster and CLI version match if they do not it prints a warning
    :param enterprise_check: if true it will check if the cluster is enterprise and fail if not. If it is false it does
        the check but it does not fail if not enterprise. If none it does not perform the check. The result of the check
        is stored on the instance parameter enterprise
    """
    def inner(fn):
        def decorator(self, opts):
            _exit_if_errors(validate_credential_flags(opts.cluster, opts.username, opts.password, opts.client_ca,
                                                      opts.client_ca_password, opts.client_pk, opts.client_pk_password,
                                                      credentials_required))

            try:
                self.rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                                           opts.cacert, opts.debug, client_ca=opts.client_ca,
                                           client_ca_password=opts.client_ca_password, client_pk=opts.client_pk,
                                           client_pk_password=opts.client_pk_password)
            except X509AdapterError as error:
                _exit_if_errors([f"failed to setup client certificate encryption, {error}"])

            if cluster_init_check:
                check_cluster_initialized(self.rest)
            if version_check:
                check_versions(self.rest)
            if enterprise_check is not None:
                # check columnar when we check enterprise to avoid a duplicate fetch on pools
                enterprise, columnar, errors = self.rest.is_enterprise_columnar()
                _exit_if_errors(errors)

                if enterprise_check and not enterprise:
                    _exit_if_errors(['Command only available in enterprise edition'])

                self.enterprise = enterprise
                self.columnar = columnar

            return fn(self, opts)
        return decorator
    return inner


def validate_credential_flags(host, username, password, client_ca, client_ca_password,
                              client_pk, client_pk_password, credentials_required: bool = True):
    """ValidateCredentialFlags - Performs validation to ensure the user has provided the flags required to connect to
    their cluster.
    """
    using_cert_auth = not (client_ca is None and
                           client_ca_password is None and
                           client_pk is None and
                           client_pk_password is None)

    if using_cert_auth:
        return validate_certificate_flags(
            host,
            username,
            password,
            client_ca,
            client_ca_password,
            client_pk,
            client_pk_password)

    if (username is None and password is None):
        if credentials_required is False:
            return None

        return ["cluster credentials required, expected --username/--password or --client-cert/--client-key"]

    if (username is None or password is None):
        return ["the --username/--password flags must be supplied together"]

    return None


def validate_certificate_flags(host, username, password, client_ca, client_ca_password, client_pk, client_pk_password):
    """Validate that the user is correctly using certificate authentication.
    """
    if username is not None or password is not None:
        return ["expected either --username and --password or --client-cert and --client-key but not both"]

    if not (host.startswith("https://") or host.startswith("couchbases://")):
        return ["certificate authentication requires a secure connection, use https:// or couchbases://"]

    if client_ca is None:
        return ["certificate authentication requires a certificate to be supplied with the --client-cert flag"]

    if client_ca_password is not None and client_pk_password is not None:
        return ["--client-cert-password and  --client-key-password can't be supplied together"]

    unencrypted = client_ca_password is None and client_pk_password is None

    if unencrypted and (client_ca is None or client_pk is None):
        return ["when no cert/key password is provided, the --client-cert/--client-key flags must be supplied together"]

    if client_pk_password is not None and client_pk is None:
        return ["--client-key-password provided without --client-key"]

    return None


def check_cluster_initialized(rest):
    initialized, errors = rest.is_cluster_initialized()
    if errors:
        _exit_if_errors(errors)
    if not initialized:
        _exit_if_errors(["Cluster is not initialized, use cluster-init to initialize the cluster"])


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


def index_storage_mode_to_param(value, default="plasma"):
    """Converts the index storage mode to what Couchbase understands"""
    if value == "default":
        return default

    if value == "memopt":
        return "memory_optimized"

    return value


def process_services(services, enterprise, columnar, cluster_version="0.0.0"):
    """Converts services to a format Couchbase understands"""
    sep = ","
    if services.find(sep) < 0:
        # backward compatible when using ";" as separator
        sep = ";"
    svc_set = set([w.strip() for w in services.split(sep)])

    manager_only = "manager-only"
    svc_candidate = ["data", "index", "query", "fts", "eventing", "analytics", "backup", manager_only]
    for svc in svc_set:
        if svc not in svc_candidate:
            return None, [f'`{svc}` is not a valid service']
        if not enterprise and svc in ["eventing", "analytics", "backup"]:
            return None, [f'{svc} service is only available on Enterprise Edition']

    if len(svc_set) > 1 and manager_only in svc_set:
        return None, ["Invalid service configuration. A manager only node cannot run any other services."]

    versionCheck = compare_versions(cluster_version, "7.6.0")
    if manager_only in svc_set and not columnar and versionCheck == -1:
        return None, ["The manager only service can only be used with >= 7.6.0 clusters"]

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
    for old, new in [[";", ","], ["data", "kv"], ["query", "n1ql"], ["analytics", "cbas"], ["manager-only", ""]]:
        services = services.replace(old, new)
    return services, None


def find_subcommands():
    """Finds all subcommand classes"""
    clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
    subclasses = [cls for cls in clsmembers if issubclass(cls[1], (Subcommand, LocalSubcommand))
                  and cls[1] not in [Subcommand, LocalSubcommand]]

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


def _error(msg):
    print(f"ERROR: {msg}")


def _exit_if_errors(errors):
    if not errors:
        return

    for error in errors:
        # Some endpoints return errors prefixed with '_ -' this has to be stripped out. For more information see
        # MB-42801.
        _error(remove_prefix(str(error), "_ -").strip())

    sys.exit(1)


def _exit_on_file_write_failure(fname, to_write):
    try:
        wfile = open(fname, 'w', encoding="utf-8")
        wfile.write(to_write)
        wfile.close()
    except IOError as error:
        _exit_if_errors([error])


def _exit_on_encrypted_file_read_failure(fname, password, config_path):
    cbcat_path = os.path.join(get_bin_path(), 'cbcat')
    if not os.path.isfile(cbcat_path):
        _exit_if_errors([f'`{cbcat_path}` does not exist'])
    gosecrets_cfg_path = os.path.join(config_path, 'config', 'gosecrets.cfg')
    args = [cbcat_path, '--with-gosecrets', gosecrets_cfg_path,
            '--password', '-', fname]
    r = subprocess.run(args, input=password, text=True, capture_output=True,
                       check=False)

    if r.returncode == 2:  # cbcat returns 2 if and only it is incorrect password
        _exit_if_errors(['Invalid master password'])

    if r.returncode != 0:
        _exit_if_errors([f'cbcat returned non zero return code: {r.stderr}'])

    return r.stdout


def _exit_on_file_read_failure(fname, to_report=None):
    try:
        rfile = open(fname, 'r', encoding="utf-8")
        read_bytes = rfile.read()
        rfile.close()
        return read_bytes
    except IOError as error:
        if to_report is None:
            _exit_if_errors([f'{error.strerror} `{fname}`'])
        else:
            _exit_if_errors([to_report])


def _exit_on_json_file_read_failure(fname, to_report=None):
    raw = _exit_on_file_read_failure(fname, to_report)

    try:
        decoded = json.loads(raw)
    except ValueError as error:
        if to_report is None:
            _exit_if_errors([f'`{fname}` does not contain valid JSON data: {error}'])
        else:
            _exit_if_errors([to_report])

    return decoded


def _read_json_file_if_provided(fname, to_report=None):
    if fname is None or fname == "":
        return None

    return _exit_on_json_file_read_failure(fname, to_report)


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
        if re.match(r'.*:\d+$', node):
            return node
        return f'{node}:8091'
    return [append_port(x) for x in nodes]


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
                return ','.join(parts) + ' ' + args_string


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
        hostname_regex = re.compile(r'^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*'
                                    + r'([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$')
        if not hostname_regex.match(parsed.hostname):
            try:
                ipaddress.ip_address(parsed.hostname)
            except ValueError as val_error:
                raise ArgumentError(self, f"{values} is not an accepted hostname") from val_error

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

    def __init__(self, envvar, required=False, default=None, **kwargs):
        if not default and envvar and envvar in os.environ:
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

    def __init__(self, envvar, prompt_text="Enter password: ", confirm_text=None,
                 required=False, default=None, nargs='?', **kwargs):
        self.prompt_text = prompt_text
        self.confirm_text = confirm_text
        super(CBNonEchoedAction, self).__init__(envvar, required=required, default=default,
                                                nargs=nargs, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if values is None:
            values = getpass.getpass(self.prompt_text)
            if self.confirm_text is not None:
                confirm = getpass.getpass(self.prompt_text)
                if values != confirm:
                    raise ArgumentError(self, "Passwords entered do not match, please retry")
        super(CBNonEchoedAction, self).__call__(parser, namespace, values, option_string=None)


class CBHelpAction(Action):
    """Allows the custom handling of the help command line argument"""

    # pylint: disable=redefined-builtin
    def __init__(self, option_strings, klass, dest=SUPPRESS, default=SUPPRESS, help=None):
        super(CBHelpAction, self).__init__(option_strings=option_strings, dest=dest,
                                           default=default, nargs=0, help=help)  # pylint: disable=redefined-builtin
        self.klass = klass

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string == "-h":
            parser.print_help()
        else:
            CBHelpAction._show_man_page(self.klass.get_man_page_name())
        parser.exit()

    @staticmethod
    def _show_man_page(page):
        man_path = get_man_path()
        if os.name == "nt":
            try:
                subprocess.call(["rundll32.exe", "url.dll,FileProtocolHandler", os.path.join(man_path, page)])
            except OSError as e:
                _exit_if_errors(["Unable to open man page using your browser, %s" % e])
        else:
            try:
                subprocess.call(["man", os.path.join(man_path, page)])
            except OSError:
                _exit_if_errors(["Unable to open man page using the 'man' command, ensure it is on your path or"
                                 + "install a manual reader"])


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
        group.add_argument("--version", help="Get couchbase-cli version")

    def parse(self, args):
        if len(sys.argv) == 1:
            self.parser.print_help()
            self.parser.exit(1)

        if args[1] == "--version":
            print(VERSION)
            sys.exit(0)

        if not args[1] in ["-h", "--help", "--version"] and args[1].startswith("-"):
            _exit_if_errors([f"Unknown subcommand: '{args[1]}'. The first argument has to be a subcommand like"
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
        return get_doc_page_name("couchbase-cli")

    @staticmethod
    def get_description():
        return "A Couchbase cluster administration utility"


class Subcommand(Command):
    """
    A Couchbase CLI Subcommand: This is for subcommand that interacts with a remote Couchbase Server over the REST API.
    """

    def __init__(self, deprecate_username=False, deprecate_password=False, cluster_default=None):
        super(Subcommand, self).__init__()
        # Filled by the decorators
        self.rest = None
        self.enterprise = None

        self.parser = CliParser(formatter_class=CLIHelpFormatter, add_help=False, allow_abbrev=False)
        group = self.parser.add_argument_group("Cluster options")
        group.add_argument("-c", "--cluster", dest="cluster", required=(cluster_default is None),
                           metavar="<cluster>", action=CBHostAction, default=cluster_default,
                           help="The hostname of the Couchbase cluster")

        if deprecate_username:
            group.add_argument("-u", "--username", dest="username",
                               action=CBDeprecatedAction, help=SUPPRESS)
        else:
            group.add_argument("-u", "--username", dest="username",
                               action=CBEnvAction, envvar='CB_REST_USERNAME',
                               metavar="<username>", help="The username for the Couchbase cluster")

        if deprecate_password:
            group.add_argument("-p", "--password", dest="password",
                               action=CBDeprecatedAction, help=SUPPRESS)
        else:
            group.add_argument("-p", "--password", dest="password",
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

        # Certificate based authentication
        group.add_argument("--client-cert", dest="client_ca", default=None, metavar="<path>",
                           action=CBEnvAction, envvar="CB_CLIENT_CERT",
                           help="The path to a client certificate used during certificate authentication")
        group.add_argument("--client-cert-password", dest="client_ca_password", default=None, metavar="<password>",
                           action=CBNonEchoedAction, prompt_text="Enter password for --client-cert-password: ",
                           envvar="CB_CLIENT_CERT_PASSWORD",
                           help="The password for the client certificate provided to '--client-cert'")

        group.add_argument("--client-key", dest="client_pk", default=None, metavar="<path>",
                           action=CBEnvAction, envvar="CB_CLIENT_KEY",
                           help="The path to the client private key used during certificate authentication")
        group.add_argument("--client-key-password", dest="client_pk_password", default=None, metavar="<password>",
                           action=CBNonEchoedAction, prompt_text="Enter password for --client-key-password: ",
                           envvar="CB_CLIENT_KEY_PASSWORD",
                           help="The password for the client key provided to '--client-key'")

    def execute(self, opts):  # pylint: disable=useless-super-delegation
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
                                               description="This command has to be execute on the locally running"
                                               + " Couchbase Server.")
        group.add_argument("-h", "--help", action=CBHelpAction, klass=self,
                           help="Prints the short or long help message")
        group.add_argument("--config-path", dest="config_path", metavar="<path>",
                           default=get_cfg_path(), help="Overrides the default configuration path")

    def execute(self, opts):  # pylint: disable=useless-super-delegation
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
                           metavar="<password>", help="The cluster administrator password")
        group.add_argument("--cluster-port", dest="port", type=(int),
                           metavar="<port>", help="The cluster administration console port")
        group.add_argument("--cluster-ramsize", dest="data_mem_quota", type=(int),
                           metavar="<quota>", help="The data service memory quota in mebibytes")
        group.add_argument("--cluster-index-ramsize", dest="index_mem_quota", type=(int),
                           metavar="<quota>", help="The index service memory quota in mebibytes")
        group.add_argument("--cluster-fts-ramsize", dest="fts_mem_quota", type=(int),
                           metavar="<quota>",
                           help="The full-text service memory quota in mebibytes")
        group.add_argument("--cluster-eventing-ramsize", dest="eventing_mem_quota", type=(int),
                           metavar="<quota>",
                           help="The Eventing service memory quota in mebibytes")
        group.add_argument("--cluster-analytics-ramsize", dest="cbas_mem_quota", type=(int),
                           metavar="<quota>",
                           help="The analytics service memory quota in mebibytes")
        group.add_argument('--cluster-query-ramsize', dest="query_mem_quota", type=(int),
                           metavar="<quota>", help="The query service memory quota in mebibytes")
        group.add_argument("--cluster-name", dest="name", metavar="<name>", help="The cluster name")
        group.add_argument("--index-storage-setting", dest="index_storage_mode",
                           choices=["default", "memopt"], metavar="<mode>",
                           help="The index storage backend (Defaults to \"default)\"")
        group.add_argument("--services", dest="services", default="data", metavar="<service_list>",
                           help="The services to run on this server")
        group.add_argument("--update-notifications", dest="notifications", metavar="<1|0>", choices=["0", "1"],
                           default="1", help="Enables/disable software update notifications (EE only)")
        group.add_argument("--ip-family", dest="ip_family", metavar="<ipv4|ipv6|ipv4-only|ipv6-only>", default="ipv4",
                           choices=["ipv4", "ipv6", "ipv4-only", "ipv6-only"],
                           help="Set the IP family for the cluster")
        group.add_argument("--node-to-node-encryption", dest="encryption", metavar="<on|off>", default="off",
                           choices=["on", "off"], help="Enable node to node encryption")

    @rest_initialiser(enterprise_check=False)
    def execute(self, opts):
        initialized, errors = self.rest.is_cluster_initialized()
        _exit_if_errors(errors)
        if initialized:
            _exit_if_errors(["Cluster is already initialized, use setting-cluster to change settings"])

        if not self.enterprise and opts.index_storage_mode == 'memopt':
            _exit_if_errors(["memopt option for --index-storage-setting can only be configured on enterprise edition"])

        if "manager-only" in opts.services:
            _exit_if_errors(["Cannot initialize cluster with the manager only service"])

        services, errors = process_services(opts.services, self.enterprise, self.columnar)
        _exit_if_errors(errors)

        if 'kv' not in services.split(','):
            _exit_if_errors(["Cannot set up first cluster node without the data service"])

        if 'ipv4' in opts.ip_family:
            ip_family = 'ipv4'
        elif 'ipv6' in opts.ip_family:
            ip_family = 'ipv6'
        ip_only = True if 'only' in opts.ip_family else False

        if not opts.index_storage_mode and 'index' in services.split(','):
            opts.index_storage_mode = "default"

        default = "plasma"
        if not self.enterprise:
            default = "forestdb"

        indexer_storage = None
        if opts.index_storage_mode:
            indexer_storage = index_storage_mode_to_param(opts.index_storage_mode, default)

        min_version, errors = self.rest.node_version()
        _exit_if_errors(errors)

        versionCheck = compare_versions(min_version, "7.2.3")
        if not self.columnar and versionCheck == -1:
            _exit_if_errors(["--cluster-init can only be used against >= 7.2.3 clusters"])

        if not self.enterprise and opts.notifications == "0":
            _exit_if_errors(["--update-notifications can only be configured on Enterprise Edition"])

        _, errors = self.rest.cluster_init(
            services=services,
            username=opts.username,
            password=opts.password,
            port=opts.port,
            cluster_name=opts.name,
            data_ramsize=opts.data_mem_quota,
            index_ramsize=opts.index_mem_quota,
            fts_ramsize=opts.fts_mem_quota,
            cbas_ramsize=opts.cbas_mem_quota,
            eventing_ramsize=opts.eventing_mem_quota,
            query_ramsize=opts.query_mem_quota,
            ipfamily=ip_family,
            ipfamilyonly=ip_only,
            encryption=opts.encryption,
            indexer_storage_mode=indexer_storage,
            send_stats=opts.notifications == "1")
        _exit_if_errors(errors)

        _success("Cluster initialized")

    def setup_ip_family_and_encryption(self, opts):
        """Setups the IP family and node to node encryption"""
        if 'ipv4' in opts.ip_family:
            ip_family = 'ipv4'
        elif 'ipv6' in opts.ip_family:
            ip_family = 'ipv6'
        ip_only = True if 'only' in opts.ip_family else False

        _, errors = self.rest.enable_external_listener(ipfamily=ip_family, encryption=opts.encryption)
        _exit_if_errors(errors)

        _, errors = self.rest.setup_net_config(ipfamily=opts.ip_family, encryption=opts.encryption,
                                               ipfamilyonly=ip_only)
        _exit_if_errors(errors)

        _, errors = self.rest.disable_unused_external_listeners()
        _exit_if_errors(errors)

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-cluster-init")

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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        bucket, errors = self.rest.get_bucket(opts.bucket_name)
        _exit_if_errors(errors)

        if bucket["bucketType"] != BUCKET_TYPE_COUCHBASE:
            _exit_if_errors(["Cannot compact memcached buckets"])

        _, errors = self.rest.compact_bucket(opts.bucket_name, opts.data_only, opts.view_only)
        _exit_if_errors(errors)

        _success("Bucket compaction started")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-bucket-compact")

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
                           help="The bucket type (couchbase or ephemeral)")
        group.add_argument("--storage-backend", dest="storage", metavar="<storage>",
                           choices=["couchstore", "magma"],
                           help="Type of storage backend (only for couchbase buckets)")
        group.add_argument("--bucket-ramsize", dest="memory_quota", metavar="<quota>", type=(int),
                           required=True, help="The amount of memory to allocate the bucket")
        group.add_argument("--bucket-replica", dest="replica_count", metavar="<num>",
                           choices=["0", "1", "2", "3"],
                           help="The replica count for the bucket")
        group.add_argument("--bucket-priority", dest="priority", metavar="<priority>",
                           choices=[BUCKET_PRIORITY_LOW_STR, BUCKET_PRIORITY_HIGH_STR],
                           help="The bucket disk io priority (low or high)")
        group.add_argument("--durability-min-level", dest="durability_min_level", metavar="<level>",
                           choices=["none", "majority", "majorityAndPersistActive",
                                    "persistToMajority"],
                           help="The bucket durability minimum level")
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
                           metavar="<mebibytes>", type=(int), help="Set Database Fragmentation level")

        group.add_argument("--view-fragmentation-threshold-percentage", dest="view_frag_perc",
                           metavar="<perc>", type=(int), help="Set View Fragmentation level percent")

        group.add_argument("--view-fragmentation-threshold-size", dest="view_frag_size",
                           metavar="<mebibytes>", type=(int), help="Set View Fragmentation level size")

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
                           metavar="<float>", help="Sets the frequency of the tombstone purge interval")
        group.add_argument("--history-retention-bytes", dest="history_retention_bytes", default=None, type=(int),
                           metavar="<bytes>", help="Set the maximum size of retained document history in bytes")
        group.add_argument("--history-retention-seconds", dest="history_retention_seconds", default=None, type=(int),
                           metavar="<seconds>", help="Set the maximum age of retained document history in seconds")
        group.add_argument("--enable-history-retention-by-default",
                           dest="enable_history_retention", metavar="<0|1>", choices=["0", "1"],
                           help="Enable history retention for new collections created in this bucket by default "
                           "(0 or 1)")

        group.add_argument("--rank", dest="rank", metavar="<num>", type=(int),
                           help="Sets the rank of this bucket in case of failover/rebalance. Buckets with larger "
                           "ranks are prioritised over buckets with smaller ranks")

        group.add_argument("--magma-configuration", dest="magma_configuration", metavar="<configuration>", default=None,
                           choices=["standard", "large-scale"], help="Sets the configuration for magma buckets, "
                           "either 'standard' (the default) or 'large-scale'")

        group.add_argument("--encryption-key", dest="encryption_key", metavar="<keyid>",
                           help="The key id of the encryption key to use on this bucket")
        group.add_argument("--dek-rotate-every", dest="dek_rotate_interval", metavar="<days>", type=(int),
                           help="How often to rotate the DEK for this bucket")
        group.add_argument("--dek-lifetime", dest="dek_lifetime", metavar="<days>", type=(int),
                           help="How long the DEK should be kept for")

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=False)
    def execute(self, opts):
        if opts.max_ttl and not self.enterprise:
            _exit_if_errors(["Maximum TTL can only be configured on enterprise edition"])
        if opts.compression_mode and not self.enterprise:
            _exit_if_errors(["Compression mode can only be configured on enterprise edition"])

        version, errors = self.rest.min_version()
        _exit_if_errors(errors)

        if opts.type == "memcached":
            if compare_versions(version, "8.0.0") >= 0:
                _exit_if_errors(["Memcached buckets have been removed in versions 8.0 and above, please use ephemeral "
                                 "buckets instead"])

            _deprecated("Memcached buckets are deprecated, please use ephemeral buckets instead")
            if opts.replica_count is not None:
                _exit_if_errors(["--bucket-replica cannot be specified for a memcached bucket"])
            if opts.conflict_resolution is not None:
                _exit_if_errors(["--conflict-resolution cannot be specified for a memcached bucket"])
            if opts.replica_indexes is not None:
                _exit_if_errors(["--enable-index-replica cannot be specified for a memcached bucket"])
            if opts.priority is not None:
                _exit_if_errors(["--bucket-priority cannot be specified for a memcached bucket"])
            if opts.eviction_policy is not None:
                _exit_if_errors(["--bucket-eviction-policy cannot be specified for a memcached bucket"])
            if opts.max_ttl is not None:
                _exit_if_errors(["--max-ttl cannot be specified for a memcached bucket"])
            if opts.compression_mode is not None:
                _exit_if_errors(["--compression-mode cannot be specified for a memcached bucket"])
            if opts.durability_min_level is not None:
                _exit_if_errors(["--durability-min-level cannot be specified for a memcached bucket"])
        elif opts.type == "ephemeral" and opts.eviction_policy in ["valueOnly", "fullEviction"]:
            _exit_if_errors(["--bucket-eviction-policy must either be noEviction or nruEviction"])
        elif opts.type == "couchbase" and opts.eviction_policy in ["noEviction", "nruEviction"]:
            _exit_if_errors(["--bucket-eviction-policy must either be valueOnly or fullEviction"])

        if ((opts.type == "memcached" or opts.type == "ephemeral")
                and (opts.db_frag_perc is not None
                     or opts.db_frag_size is not None or opts.view_frag_perc is not None
                     or opts.view_frag_size is not None or opts.from_hour is not None or opts.from_min is not None
                     or opts.to_hour is not None or opts.to_min is not None or opts.abort_outside is not None
                     or opts.paralleldb_and_view_compact is not None)):
            _warning(f'ignoring compaction settings as bucket type {opts.type} does not accept it')

        storage_type = None
        if opts.type == "couchbase":
            storage_type = "magma"

        if opts.storage is not None:
            if opts.type != "couchbase":
                _exit_if_errors(["--storage-backend is only valid for couchbase buckets"])
            if opts.storage == "couchstore":
                storage_type = "couchstore"

        min_version, error = self.rest.min_version()
        _exit_if_errors(error)

        if storage_type != "magma" and opts.magma_configuration is not None:
            _exit_if_errors(["--magma-configuration is only valid for Magma buckets"])
        if opts.magma_configuration is not None and compare_versions(min_version, "8.0.0") < 0:
            _exit_if_errors(["--magma-configuration can only be passed on 8.0 and above"])

        if storage_type == "magma" and opts.magma_configuration is None:
            opts.magma_configuration = "standard"

        vbuckets = None
        if opts.magma_configuration is not None and opts.type == "couchbase":
            vbuckets = 128 if opts.magma_configuration == "standard" else 1024

        if opts.type != "couchbase":
            if opts.history_retention_bytes is not None:
                _exit_if_errors([f"--history-retention-bytes cannot be specified for a {opts.type} bucket"])
            if opts.history_retention_seconds is not None:
                _exit_if_errors([f"--history-retention-seconds cannot be specified for a {opts.type} bucket"])
            if opts.enable_history_retention is not None:
                _exit_if_errors([f"--enable-history-retention-by-default cannot be specified for a {opts.type} bucket"])

        if storage_type != "magma":
            if opts.history_retention_bytes is not None:
                _exit_if_errors([f"--history-retention-bytes cannot be specified for a bucket with {storage_type} "
                                 "backend"])
            if opts.history_retention_seconds is not None:
                _exit_if_errors([f"--history-retention-seconds cannot be specified for a bucket with {storage_type} "
                                 "backend"])
            if opts.enable_history_retention is not None:
                _exit_if_errors(["--enable-history-retention-by-default cannot be specified for a bucket with "
                                 f"{storage_type} backend"])

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

        if (opts.dek_rotate_interval or opts.dek_lifetime) and not opts.encryption_key:
            _exit_if_errors(["--dek-rotate-every and/or --dek-lifetime can only be passed if an encryption key is "
                             "specified (--encryption-key)"])

        dek_rotate_interval, dek_lifetime = None, None
        if opts.dek_rotate_interval:
            dek_rotate_interval = opts.dek_rotate_interval * 24 * 60 * 60
        if opts.dek_lifetime:
            dek_lifetime = opts.dek_lifetime * 24 * 60 * 60

        _, errors = self.rest.create_bucket(opts.bucket_name, opts.type, storage_type, opts.memory_quota,
                                            opts.durability_min_level, opts.eviction_policy, opts.replica_count,
                                            opts.replica_indexes, priority, conflict_resolution_type, opts.enable_flush,
                                            opts.max_ttl, opts.compression_mode, opts.wait, opts.db_frag_perc,
                                            opts.db_frag_size, opts.view_frag_perc, opts.view_frag_size,
                                            opts.from_hour, opts.from_min, opts.to_hour, opts.to_min,
                                            opts.abort_outside, opts.paralleldb_and_view_compact, opts.purge_interval,
                                            opts.history_retention_bytes, opts.history_retention_seconds,
                                            opts.enable_history_retention, opts.rank, vbuckets, opts.encryption_key,
                                            dek_rotate_interval, dek_lifetime)
        _exit_if_errors(errors)
        _success("Bucket created")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-bucket-create")

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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        _, errors = self.rest.get_bucket(opts.bucket_name)
        _exit_if_errors(errors)

        _, errors = self.rest.delete_bucket(opts.bucket_name)
        _exit_if_errors(errors)

        _success("Bucket deleted")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-bucket-delete")

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
        group.add_argument("--durability-min-level", dest="durability_min_level", metavar="<level>",
                           choices=["none", "majority", "majorityAndPersistActive", "persistToMajority"],
                           help="The bucket durability minimum level")
        group.add_argument("--bucket-eviction-policy", dest="eviction_policy", metavar="<policy>",
                           type=(str), help="The bucket eviction policy (valueOnly or fullEviction)")
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
                           metavar="<mebibytes>", type=(int), help="Set Database Fragmentation level")

        group.add_argument("--view-fragmentation-threshold-percentage", dest="view_frag_perc",
                           metavar="<perc>", type=(int), help="Set View Fragmentation level percent")

        group.add_argument("--view-fragmentation-threshold-size", dest="view_frag_size",
                           metavar="<mebibytes>", type=(int), help="Set View Fragmentation level size")

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
                           metavar="<num>", help="Set the bucket metadata purge interval")

        group.add_argument("--history-retention-bytes", dest="history_retention_bytes", default=None, type=(int),
                           metavar="<bytes>", help="Set the maximum size of retained document history in bytes")
        group.add_argument("--history-retention-seconds", dest="history_retention_seconds", default=None, type=(int),
                           metavar="<seconds>", help="Set the maximum age of retained document history in seconds")
        group.add_argument("--enable-history-retention-by-default",
                           dest="enable_history_retention", metavar="<0|1>", choices=["0", "1"],
                           help="Enable history retention for new collections created in this bucket by default "
                           "(0 or 1)")

        group.add_argument("--rank", dest="rank", metavar="<num>", type=(int),
                           help="Sets the rank of this bucket in case of failover/rebalance. Buckets with larger "
                           "ranks are prioritised over buckets with smaller ranks")

        group.add_argument("--encryption-key", dest="encryption_key", metavar="<keyid>",
                           help="The key id of the encryption key to use on this bucket")
        group.add_argument("--dek-rotate-every", dest="dek_rotate_interval", metavar="<days>", type=(int),
                           help="How often to rotate the DEK for this bucket")
        group.add_argument("--dek-lifetime", dest="dek_lifetime", metavar="<days>", type=(int),
                           help="How long the DEK should be kept for")

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=False)
    def execute(self, opts):
        if opts.max_ttl and not self.enterprise:
            _exit_if_errors(["Maximum TTL can only be configured on enterprise edition"])

        if opts.compression_mode and not self.enterprise:
            _exit_if_errors(["Compression mode can only be configured on enterprise edition"])

        # Note that we accept 'noEviction' and 'nruEviction' as valid values even though they are undocumented; this is
        # so that users attempting to modify the eviction policy of an ephemeral bucket will receive a meaningful
        # message from 'ns_server'. See MB-39036 for more information.
        if (opts.eviction_policy is not None
                and opts.eviction_policy not in ["valueOnly", "fullEviction", "noEviction", "nruEviction"]):
            _exit_if_errors([f"argument --bucket-eviction-policy: invalid choice: '{opts.eviction_policy}'" +
                             " (choose from 'valueOnly', 'fullEviction')"])

        bucket, errors = self.rest.get_bucket(opts.bucket_name)
        _exit_if_errors(errors)

        if "bucketType" in bucket and bucket["bucketType"] == "memcached":
            _deprecated("Memcached buckets are deprecated, please use ephemeral buckets instead")
            if opts.memory_quota is not None:
                _exit_if_errors(["--bucket-ramsize cannot be specified for a memcached bucket"])
            if opts.replica_count is not None:
                _exit_if_errors(["--bucket-replica cannot be specified for a memcached bucket"])
            if opts.priority is not None:
                _exit_if_errors(["--bucket-priority cannot be specified for a memcached bucket"])
            if opts.eviction_policy is not None:
                _exit_if_errors(["--bucket-eviction-policy cannot be specified for a memcached bucket"])
            if opts.max_ttl is not None:
                _exit_if_errors(["--max-ttl cannot be specified for a memcached bucket"])
            if opts.compression_mode is not None:
                _exit_if_errors(["--compression-mode cannot be specified for a memcached bucket"])
            if opts.durability_min_level is not None:
                _exit_if_errors(["--durability-min-level cannot be specified for a memcached bucket"])

        if (("bucketType" in bucket and (bucket["bucketType"] == "memcached" or bucket["bucketType"] == "ephemeral"))
                and (opts.db_frag_perc is not None or opts.db_frag_size is not None
                     or opts.view_frag_perc is not None or opts.view_frag_size is not None or opts.from_hour is not None
                     or opts.from_min is not None or opts.to_hour is not None or opts.to_min is not None
                     or opts.abort_outside is not None or opts.paralleldb_and_view_compact is not None)):
            _exit_if_errors([f'compaction settings can not be specified for a {bucket["bucketType"]} bucket'])

        is_couchbase_bucket = "bucketType" in bucket and bucket["bucketType"] == "membase"

        if "bucketType" in bucket and bucket["bucketType"] != "membase":
            if opts.history_retention_bytes is not None:
                _exit_if_errors([f"--history-retention-bytes cannot be specified for a {bucket['bucketType']} bucket"])
            if opts.history_retention_seconds is not None:
                _exit_if_errors([f"--history-retention-seconds cannot be specified for a {bucket['bucketType']} "
                                 "bucket"])
            if opts.enable_history_retention is not None:
                _exit_if_errors(["--enable-history-retention-by-default cannot be specified for a "
                                 f"{bucket['bucketType']} bucket"])
        if "storageBackend" in bucket and bucket["storageBackend"] != "magma":
            if opts.history_retention_bytes is not None:
                _exit_if_errors(["--history-retention-bytes cannot be specified for a bucket with "
                                 f"{bucket['storageBackend']} backend"])
            if opts.history_retention_seconds is not None:
                _exit_if_errors(["--history-retention-seconds cannot be specified for a bucket with "
                                 f"{bucket['storageBackend']} backend"])
            if opts.enable_history_retention is not None:
                _exit_if_errors(["--enable-history-retention-by-default cannot be specified for a bucket with "
                                 f"{bucket['storageBackend']} backend"])

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

        if (opts.dek_rotate_interval or opts.dek_lifetime) and not opts.encryption_key:
            _exit_if_errors(["--dek-rotate-every and/or --dek-lifetime can only be passed if an encryption key is "
                             "specified (--encryption-key)"])

        dek_rotate_interval, dek_lifetime = None, None
        if opts.dek_rotate_interval:
            dek_rotate_interval = opts.dek_rotate_interval * 24 * 60 * 60
        if opts.dek_lifetime:
            dek_lifetime = opts.dek_lifetime * 24 * 60 * 60

        _, errors = self.rest.edit_bucket(opts.bucket_name, opts.memory_quota, opts.durability_min_level,
                                          opts.eviction_policy, opts.replica_count, priority, opts.enable_flush,
                                          opts.max_ttl, opts.compression_mode, opts.remove_port, opts.db_frag_perc,
                                          opts.db_frag_size, opts.view_frag_perc, opts.view_frag_size, opts.from_hour,
                                          opts.from_min, opts.to_hour, opts.to_min, opts.abort_outside,
                                          opts.paralleldb_and_view_compact, opts.purge_interval,
                                          opts.history_retention_bytes, opts.history_retention_seconds,
                                          opts.enable_history_retention, opts.rank, opts.encryption_key, dek_rotate_interval, dek_lifetime, is_couchbase_bucket)
        _exit_if_errors(errors)

        _success("Bucket edited")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-bucket-edit")

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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        _, errors = self.rest.get_bucket(opts.bucket_name)
        _exit_if_errors(errors)

        if not opts.force:
            question = "Running this command will totally PURGE database data from disk. " + \
                       "Do you really want to do it? (Yes/No)"
            confirm = input(question)
            if confirm not in ('y', 'Y', 'yes', 'Yes'):
                return

        _, errors = self.rest.flush_bucket(opts.bucket_name)
        _exit_if_errors(errors)

        _success("Bucket flushed")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-bucket-flush")

    @staticmethod
    def get_description():
        return "Flush all data from disk for a given bucket"


class BucketList(Subcommand):
    """The bucket list subcommand"""

    def __init__(self):
        super(BucketList, self).__init__()
        self.parser.prog = "couchbase-cli bucket-list"

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        result, errors = self.rest.list_buckets(extended=True)
        _exit_if_errors(errors)

        if opts.output == 'json':
            print(json.dumps(result))
        else:
            for bucket in result:
                print(f'{bucket["name"]}')
                print(f' bucketType: {bucket["bucketType"]}')
                print(f' numReplicas: {bucket["replicaNumber"]}')
                print(f' ramQuota: {bucket["quota"]["ram"]}')
                print(f' ramUsed: {bucket["basicStats"]["memUsed"]}')
                print(f' vBuckets: {bucket["numVBuckets"]}')

                if 'encryptionAtRestInfo' in bucket and 'dataStatus' in bucket['encryptionAtRestInfo']:
                    print(f' encryptionAtRestStatus: {bucket["encryptionAtRestInfo"]["dataStatus"]}')

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-bucket-list")

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
        group.add_argument("--encryption-password", dest="encryption_password",
                           action=CBNonEchoedAction, envvar=None, metavar="<encryption_password>",
                           help="The password to encrypt the logs with")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if not opts.nodes and not opts.all_nodes:
            _exit_if_errors(["Must specify either --all-nodes or --nodes"])

        if opts.nodes and opts.all_nodes:
            _exit_if_errors(["Cannot specify both --all-nodes and --nodes"])

        if opts.salt and opts.redaction_level != "partial":
            _exit_if_errors(["--redaction-level has to be set to 'partial' when --salt is specified"])

        servers = opts.nodes
        if opts.all_nodes:
            servers = "*"

        if opts.upload:
            if not opts.upload_host:
                _exit_if_errors(["--upload-host is required when --upload is specified"])
            if not opts.upload_customer:
                _exit_if_errors(["--upload-customer is required when --upload is specified"])
        else:
            if opts.upload_host:
                _warning("--upload-host has no effect with specifying --upload")
            if opts.upload_customer:
                _warning("--upload-customer has no effect with specifying --upload")
            if opts.upload_ticket:
                _warning("--upload_ticket has no effect with specifying --upload")
            if opts.upload_proxy:
                _warning("--upload_proxy has no effect with specifying --upload")

        _, errors = self.rest.collect_logs_start(servers, opts.redaction_level, opts.salt, opts.output_dir,
                                                 opts.tmp_dir, opts.upload, opts.upload_host, opts.upload_proxy,
                                                 opts.upload_customer, opts.upload_ticket, opts.encryption_password)
        _exit_if_errors(errors)
        _success("Log collection started")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-collect-logs-start")

    @staticmethod
    def get_description():
        return "Start cluster log collection"


class CollectLogsStatus(Subcommand):
    """The collect-logs-status subcommand"""

    def __init__(self):
        super(CollectLogsStatus, self).__init__()
        self.parser.prog = "couchbase-cli collect-logs-status"

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        tasks, errors = self.rest.get_tasks()
        _exit_if_errors(errors)

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
        return get_doc_page_name("couchbase-cli-collect-logs-status")

    @staticmethod
    def get_description():
        return "View the status of cluster log collection"


class CollectLogsStop(Subcommand):
    """The collect-logs-stop subcommand"""

    def __init__(self):
        super(CollectLogsStop, self).__init__()
        self.parser.prog = "couchbase-cli collect-logs-stop"

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        _, errors = self.rest.collect_logs_stop()
        _exit_if_errors(errors)

        _success("Log collection stopped")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-collect-logs-stop")

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
        group.add_argument("--hard", dest="hard", action="store_true",
                           help="Hard failover the server")
        group.add_argument("--force", dest="force", action="store_true",
                           help="Force a hard failover")
        group.add_argument("--no-progress-bar", dest="no_bar", action="store_true",
                           default=False, help="Disables the progress bar")
        group.add_argument("--no-wait", dest="wait", action="store_false",
                           default=True, help="Don't wait for rebalance completion")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if opts.force and not opts.hard:
            _exit_if_errors(["--hard is required with --force flag"])
        opts.servers_to_failover = apply_default_port(opts.servers_to_failover)
        _, errors = self.rest.failover(opts.servers_to_failover, opts.hard, opts.force)
        _exit_if_errors(errors)

        if not opts.hard:
            time.sleep(1)
            if opts.wait:
                bar = TopologyProgressBar(self.rest, 'Gracefully failing over', opts.no_bar)
                errors = bar.show()
                _exit_if_errors(errors)
                _success("Server failed over")
            else:
                _success("Server failed over started")

        else:
            _success("Server failed over")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-failover")

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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        cmds = [opts.create, opts.delete, opts.list, opts.rename, opts.move_servers]
        if sum(cmd is not None for cmd in cmds) == 0:
            _exit_if_errors(["Must specify one of the following: --create, "
                             + "--delete, --list, --move-servers, or --rename"])
        elif sum(cmd is not None for cmd in cmds) != 1:
            _exit_if_errors(["Only one of the following may be specified: --create"
                             + ", --delete, --list, --move-servers, or --rename"])

        if opts.create:
            self._create(opts)
        elif opts.delete:
            self._delete(opts)
        elif opts.list:
            self._list(opts)
        elif opts.rename:
            self._rename(opts)
        elif opts.move_servers is not None:
            self._move(opts)

    def _create(self, opts):
        if opts.name is None:
            _exit_if_errors(["--group-name is required with --create flag"])
        _, errors = self.rest.create_server_group(opts.name)
        _exit_if_errors(errors)
        _success("Server group created")

    def _delete(self, opts):
        if opts.name is None:
            _exit_if_errors(["--group-name is required with --delete flag"])
        _, errors = self.rest.delete_server_group(opts.name)
        _exit_if_errors(errors)
        _success("Server group deleted")

    def _list(self, opts):
        groups, errors = self.rest.get_server_groups()
        _exit_if_errors(errors)

        found = False
        for group in groups["groups"]:
            if opts.name is None or opts.name == group['name']:
                found = True
                print(group['name'])
                for node in group['nodes']:
                    print(f' server: {node["hostname"]}')
        if not found and opts.name:
            _exit_if_errors([f'Invalid group name: {opts.name}'])

    def _move(self, opts):
        if opts.from_group is None:
            _exit_if_errors(["--from-group is required with --move-servers"])
        if opts.to_group is None:
            _exit_if_errors(["--to-group is required with --move-servers"])

        servers = apply_default_port(opts.move_servers)
        _, errors = self.rest.move_servers_between_groups(servers, opts.from_group, opts.to_group)
        _exit_if_errors(errors)
        _success("Servers moved between groups")

    def _rename(self, opts):
        if opts.name is None:
            _exit_if_errors(["--group-name is required with --rename option"])
        _, errors = self.rest.rename_server_group(opts.name, opts.rename)
        _exit_if_errors(errors)
        _success("Server group renamed")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-group-manage")

    @staticmethod
    def get_description():
        return "Manage server groups"


class HostList(Subcommand):
    """The host list subcommand"""

    def __init__(self):
        super(HostList, self).__init__()
        self.parser.prog = "couchbase-cli host-list"

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        result, errors = self.rest.pools('default')
        _exit_if_errors(errors)

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
        return get_doc_page_name("couchbase-cli-host-list")

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
        group.add_argument("--master-password", dest="master_password", metavar="<master_password>",
                           required=False, action=CBNonEchoedAction, envvar=None,
                           prompt_text="Enter master password:",
                           confirm_text=None,
                           help="Node's master password")

    def execute(self, opts):
        if opts.config_path is None:
            _exit_if_errors(["Unable to locate the configuration path, please specify it using --config-path"])

        token_path = os.path.join(opts.config_path, "localtoken")
        master_pass = ''
        if opts.master_password is not None:
            master_pass = opts.master_password
        token = _exit_on_encrypted_file_read_failure(token_path, master_pass,
                                                     opts.config_path).rstrip()
        rest = ClusterManager("http://127.0.0.1:" + opts.port, "@localtoken", token)
        check_cluster_initialized(rest)
        check_versions(rest)

        if not opts.force:
            confirm = str(input("Are you sure that the cipher should be reset?: Y/[N]"))
            if confirm != "Y":
                _success("Cipher suites have not been reset to default")

        _, errors = rest.reset_cipher_suites()
        _exit_if_errors(errors)
        _success("Cipher suites have been reset to the default")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-reset-cipher-suites")

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
            if opts.config_path is None:
                _exit_if_errors(["Unable to locate the configuration path, please specify it using --config-path"])

            portfile = os.path.join(opts.config_path, "couchbase-server.babysitter.smport")
            if not os.path.isfile(portfile):
                _exit_if_errors(["The node is down"])
            familyport = _exit_on_file_read_failure(
                portfile, "Insufficient privileges to send master password - Please"
                " execute this command as an operating system user who has"
                " file system read permission on the Couchbase Server "
                " configuration").rstrip()
            [afamilystr, portstr] = familyport.split()
            port = int(portstr)
            afamily = socket.AF_INET6 if afamilystr == "inet6" else socket.AF_INET
            self.prompt_for_master_pwd(afamily, port, opts.send_password)
        else:
            _exit_if_errors(["No parameters set"])

    def prompt_for_master_pwd(self, afamily, port, password):
        if password == '':
            password = getpass.getpass("\nEnter master password:")

        sock = socket.socket(afamily, socket.SOCK_DGRAM)
        try:
            sock.settimeout(5)
            addr = "::1" if afamily == socket.AF_INET6 else "127.0.0.1"
            sock.sendto(password.encode('utf-8'), (addr, port))
            (result, _) = sock.recvfrom(128)
        except socket.timeout:
            result = b'timeout'
        finally:
            sock.close()

        if result == b'ok':
            print("SUCCESS: Password accepted. Node started booting.")
        elif result == b'retry':
            print("Incorrect password.")
            self.prompt_for_master_pwd(afamily, port, '')
        elif result == b'timeout':
            _exit_if_errors(["Timeout"])
        elif result == b'auth_failure':
            _exit_if_errors(["Incorrect password. Node shuts down."])
        else:
            _exit_if_errors([f'Unknown error: {result}'])

    def run_process(self, name, args, extra_env=None):
        try:
            if os.name == "nt":
                name = name + ".exe"

            env = None
            if extra_env is not None:
                env = os.environ.copy()
                env.update(extra_env)
            args.insert(0, name)
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            output = p.stdout.read()
            error = p.stderr.read()
            p.wait()
            rc = p.returncode
            return rc, output, error
        except OSError:
            _exit_if_errors([f'Could not locate the {name} executable'])

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-master-password")

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
        group.add_argument("--node-init-eventing-path", dest="eventing_path", metavar="<path>",
                           help="The path to store eventing files")
        group.add_argument("--node-init-java-home", dest="java_home", metavar="<path>",
                           help="The path of the Java Runtime Environment (JRE) to use on this server")
        group.add_argument("--node-init-hostname", dest="hostname", metavar="<hostname>",
                           help="Sets the hostname for this server")
        group.add_argument("--ipv6", dest="ipv6", action="store_true", default=False,
                           help="Configure the node to communicate via ipv6")
        group.add_argument("--ipv4", dest="ipv4", action="store_true", default=False,
                           help="Configure the node to communicate via ipv4")

    @rest_initialiser(credentials_required=False)
    def execute(self, opts):
        # Cluster does not need to be initialized for this command

        if (opts.data_path is None and opts.index_path is None and opts.analytics_path is None
                and opts.eventing_path is None and opts.java_home is None and opts.hostname is None
                and opts.ipv6 is None and opts.ipv4 is None):
            _exit_if_errors(["No node initialization parameters specified"])

        if opts.ipv4 and opts.ipv6:
            _exit_if_errors(["Use either --ipv4 or --ipv6"])

        if opts.ipv4:
            afamily = 'ipv4'
        elif opts.ipv6:
            afamily = 'ipv6'
        else:
            afamily = None

        _, errors = self.rest.node_init(hostname=opts.hostname,
                                        afamily=afamily,
                                        data_path=opts.data_path,
                                        index_path=opts.index_path,
                                        cbas_path=opts.analytics_path,
                                        eventing_path=opts.eventing_path,
                                        java_home=opts.java_home)

        _exit_if_errors(errors)
        _success("Node initialized")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-node-init")

    @staticmethod
    def get_description():
        return "Set node specific settings"


class NodeReset(Subcommand):
    """The node reset subcommand"""

    def __init__(self):
        super(NodeReset, self).__init__()
        self.parser.prog = "couchbase-cli node-reset"
        group = self.parser.add_argument_group("Node reset options")
        group.add_argument("--force", dest="force", action="store_true", help="Reset node without asking to confirm")

    @rest_initialiser(cluster_init_check=True)
    def execute(self, opts):
        if not opts.force:
            confirm = input('This command will purge all data on this node.\nAre you sure? [y/n]: ')
            if confirm == 'n':
                print("Node has not been reset")
                sys.exit(0)
            elif confirm != 'y':
                _exit_if_errors(["Unknown option provided"])

        _, errors = self.rest.reset_node()
        _exit_if_errors(errors)
        _success("Node reset")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-node-reset")

    @staticmethod
    def get_description():
        return "Resets node - will delete all data from node"


class Rebalance(Subcommand):
    """The rebalance subcommand"""

    def __init__(self):
        super(Rebalance, self).__init__()
        self.parser.prog = "couchbase-cli rebalance"
        group = self.parser.add_argument_group("Rebalance options")
        group.add_argument("--server-remove", dest="server_remove", metavar="<server_list>",
                           help="A list of servers to remove from the cluster")
        group.add_argument("--update-services", dest="update_services", action="store_true",
                           default=False, help="Update services used by nodes from the cluster")
        group.add_argument("--fts-add", dest="fts_add", metavar="<server_list>",
                           help="A list of servers which should have the Search Service added")
        group.add_argument("--fts-remove", dest="fts_remove", metavar="<server_list>",
                           help="A list of servers which should have the Search Service removed")
        group.add_argument("--index-add", dest="index_add", metavar="<server_list>",
                           help="A list of servers which should have the Index Service added")
        group.add_argument("--index-remove", dest="index_remove", metavar="<server_list>",
                           help="A list of servers which should have the Index Service removed")
        group.add_argument("--query-add", dest="n1ql_add", metavar="<server_list>",
                           help="A list of servers which should have the Query Service added")
        group.add_argument("--query-remove", dest="n1ql_remove", metavar="<server_list>",
                           help="A list of servers which should have the Query Service removed")
        group.add_argument("--backup-add", dest="backup_add", metavar="<server_list>",
                           help="A list of servers which should have the Backup Service added")
        group.add_argument("--backup-remove", dest="backup_remove", metavar="<server_list>",
                           help="A list of servers which should have the Backup Service removed")
        group.add_argument("--analytics-add", dest="cbas_add", metavar="<server_list>",
                           help="A list of servers which should have the Analytics Service added")
        group.add_argument("--analytics-remove", dest="cbas_remove", metavar="<server_list>",
                           help="A list of servers which should have the Analytics Service removed")
        group.add_argument("--no-progress-bar", dest="no_bar", action="store_true",
                           default=False, help="Disables the progress bar")
        group.add_argument("--no-wait", dest="wait", action="store_false",
                           default=True, help="Don't wait for rebalance completion")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if opts.server_remove and opts.update_services:
            _exit_if_errors(["Cannot specify both --server-remove and --update-services at the same time"])

        services_modified = opts.fts_add or opts.fts_remove or \
            opts.index_add or opts.index_remove or \
            opts.n1ql_add or opts.n1ql_remove or \
            opts.backup_add or opts.backup_remove or \
            opts.cbas_add or opts.cbas_remove

        if opts.update_services and not services_modified:
            _exit_if_errors(["--update-services requires services to be added/removed from nodes"])

        if services_modified and not opts.update_services:
            _exit_if_errors(["Services can only be modified when using --update-services"])

        eject_nodes = []
        if opts.server_remove:
            eject_nodes = apply_default_port(opts.server_remove)

        if opts.server_remove or not opts.update_services:
            _, errors = self.rest.rebalance(eject_nodes)
            _exit_if_errors(errors)

        if opts.update_services:
            fts_add, fts_remove = [], []
            if opts.fts_add is not None:
                fts_add = apply_default_port(opts.fts_add)
            if opts.fts_remove is not None:
                fts_remove = apply_default_port(opts.fts_remove)

            index_add, index_remove = [], []
            if opts.index_add is not None:
                index_add = apply_default_port(opts.index_add)
            if opts.index_remove is not None:
                index_remove = apply_default_port(opts.index_remove)

            n1ql_add, n1ql_remove = [], []
            if opts.n1ql_add is not None:
                n1ql_add = apply_default_port(opts.n1ql_add)
            if opts.n1ql_remove is not None:
                n1ql_remove = apply_default_port(opts.n1ql_remove)

            backup_add, backup_remove = [], []
            if opts.backup_add is not None:
                backup_add = apply_default_port(opts.backup_add)
            if opts.backup_remove is not None:
                backup_remove = apply_default_port(opts.backup_remove)

            cbas_add, cbas_remove = [], []
            if opts.cbas_add is not None:
                cbas_add = apply_default_port(opts.cbas_add)
            if opts.cbas_remove is not None:
                cbas_remove = apply_default_port(opts.cbas_remove)

            _, errors = self.rest.rebalance_services(fts_add, fts_remove,
                                                     index_add, index_remove,
                                                     n1ql_add, n1ql_remove,
                                                     backup_add, backup_remove,
                                                     cbas_add, cbas_remove)
            _exit_if_errors(errors)

        time.sleep(1)

        if opts.wait:
            bar = TopologyProgressBar(self.rest, 'Rebalancing', opts.no_bar)
            errors = bar.show()
            _exit_if_errors(errors)
            _success("Rebalance complete")
        else:
            _success("Rebalance started")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-rebalance")

    @staticmethod
    def get_description():
        return "Start a cluster rebalancing"


class RebalanceStatus(Subcommand):
    """The rebalance status subcommand"""

    def __init__(self):
        super(RebalanceStatus, self).__init__()
        self.parser.prog = "couchbase-cli rebalance-status"

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        status, errors = self.rest.rebalance_status()
        _exit_if_errors(errors)

        print(json.dumps(status, indent=2))

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-rebalance-status")

    @staticmethod
    def get_description():
        return "Show rebalance status"


class RebalanceStop(Subcommand):
    """The rebalance stop subcommand"""

    def __init__(self):
        super(RebalanceStop, self).__init__()
        self.parser.prog = "couchbase-cli rebalance-stop"

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        _, errors = self.rest.stop_rebalance()
        _exit_if_errors(errors)

        _success("Rebalance stopped")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-rebalance-stop")

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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        servers = apply_default_port(opts.servers)
        for server in servers:
            _, errors = self.rest.recovery(server, opts.recovery_type)
            _exit_if_errors(errors)

        _success("Servers recovered")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-recovery")

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
        group.add_argument("--master-password", dest="master_password", metavar="<master_password>",
                           required=False, action=CBNonEchoedAction, envvar=None,
                           prompt_text="Enter master password:",
                           confirm_text=None,
                           help="Node's master password")
        group.add_argument("--regenerate", dest="regenerate", action="store_true",
                           help="Generates a random administrator password")
        group.add_argument("-P", "--port", metavar="<port>", default="8091",
                           help="The REST API port, defaults to 8091")
        group.add_argument("-I", "--ip", metavar="<ip>", default="localhost",
                           help="The IP address of the cluster, defaults to localhost")

    def execute(self, opts):
        if opts.config_path is None:
            _exit_if_errors(["Unable to locate the configuration path, please specify it using --config-path"])

        token_path = os.path.join(opts.config_path, "localtoken")
        master_pass = ''
        if opts.master_password is not None:
            master_pass = opts.master_password
        token = _exit_on_encrypted_file_read_failure(token_path, master_pass,
                                                     opts.config_path).rstrip()
        ip = opts.ip if ":" not in opts.ip else "[" + opts.ip + "]"
        rest = ClusterManager("http://" + ip + ":" + opts.port, "@localtoken", token)
        check_cluster_initialized(rest)
        check_versions(rest)

        if opts.new_password is not None and opts.regenerate:
            _exit_if_errors(["Cannot specify both --new-password and --regenerate at the same time"])
        elif opts.new_password is not None:
            _, errors = rest.set_admin_password(opts.new_password)
            _exit_if_errors(errors)
            _success("Administrator password changed")
        elif opts.regenerate:
            result, errors = rest.regenerate_admin_password()
            _exit_if_errors(errors)
            print(result["password"])
        else:
            _exit_if_errors(["No parameters specified"])

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-reset-admin-password")

    @staticmethod
    def get_description():
        return "Resets the administrator password"


class AdminManage(LocalSubcommand):
    """The admin manage command"""

    def __init__(self):
        super(AdminManage, self).__init__()
        self.parser.prog = "couchbase-cli admin-manage"
        group = self.parser.add_argument_group("Manage admin options")
        group.add_argument("-P", "--port", metavar="<port>", default="8091",
                           help="The REST API port, defaults to 8091")
        group.add_argument("-I", "--ip", metavar="<ip>", default="localhost",
                           help="The IP address of the cluster, defaults to localhost")
        me_group = group.add_mutually_exclusive_group(required=True)
        me_group.add_argument("--lock", action="store_true",
                              help="Locks the built-in administrator")
        me_group.add_argument("--unlock", action="store_true",
                              help="Unlocks the built-in administrator")
        group.add_argument("--master-password", dest="master_password", metavar="<master_password>",
                           required=False, action=CBNonEchoedAction, envvar=None,
                           prompt_text="Enter master password:",
                           confirm_text=None,
                           help="Node's master password")

    def execute(self, opts):
        token_path = os.path.join(opts.config_path, "localtoken")
        master_pass = ''
        if opts.master_password is not None:
            master_pass = opts.master_password
        token = _exit_on_encrypted_file_read_failure(token_path, master_pass,
                                                     opts.config_path).rstrip()
        ip = opts.ip if ":" not in opts.ip else "[" + opts.ip + "]"
        rest = ClusterManager("http://" + ip + ":" + opts.port, "@localtoken", token)
        check_cluster_initialized(rest)
        check_versions(rest)

        if opts.lock:
            self._lock_admin(rest)
        elif opts.unlock:
            self._unlock_admin(rest)

    @staticmethod
    def _lock_admin(rest):
        _, errors = rest.lock_admin()
        _exit_if_errors(errors)
        _success("Administrator locked")

    @staticmethod
    def _unlock_admin(rest):
        _, errors = rest.unlock_admin()
        _exit_if_errors(errors)
        _success("Administrator unlocked")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-admin-manage")

    @staticmethod
    def get_description():
        return "Manages the built-in administrator"


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

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=False)
    def execute(self, opts):
        if not self.enterprise and opts.index_storage_mode == 'memopt':
            _exit_if_errors(["memopt option for --index-storage-setting can only be configured on enterprise edition"])

        min_version, error = self.rest.min_version()
        _exit_if_errors(error)

        opts.services, errors = process_services(opts.services, self.enterprise, self.columnar, min_version)
        _exit_if_errors(errors)

        settings, errors = self.rest.index_settings()
        _exit_if_errors(errors)

        if opts.index_storage_mode is None and settings['storageMode'] == "" and "index" in opts.services:
            opts.index_storage_mode = "default"

        # For supporting the default index backend changing from forestdb to plasma in Couchbase 5.0
        default = "plasma"
        if opts.index_storage_mode == "default" and settings['storageMode'] == "forestdb" or not self.enterprise:
            default = "forestdb"

        if opts.index_storage_mode:
            param = index_storage_mode_to_param(opts.index_storage_mode, default)
            _, errors = self.rest.set_index_settings(param, None, None, None, None, None, None, None)
            _exit_if_errors(errors)

        servers = opts.servers.split(',')
        for server in servers:
            _, errors = self.rest.add_server(server, opts.group_name, opts.server_username, opts.server_password,
                                             opts.services)
            _exit_if_errors(errors)

        _success("Server added")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-server-add")

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
        group.add_argument("--erl-path", dest="erl_path", metavar="<path>", default=get_bin_path(),
                           help="Override the default path to the erl executable")
        group.add_argument("--ns-ebin-path", dest="ns_ebin_path", metavar="<path>", default=get_ns_ebin_path(),
                           help="Override the default path to the ns_server ebin directory")
        group.add_argument("--hosts-path", dest="hosts_path", metavar="<path>", default=get_hosts_path(),
                           help="Override the default path to the hosts.cfg file")
        group.add_argument("--base-path", dest="base_path", metavar="<path>", default=get_base_cb_path(),
                           help="Override the default path to the Couchbase Server base directory")
        group.add_argument("--eval", dest="eval", metavar="<command>",
                           help="Evaluate the given command and exit")

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        # Cluster does not need to be initialized for this command
        result, errors = self.rest.node_info()
        _exit_if_errors(errors)

        node = result['otpNode']
        cookie, errors = self.rest.get_ns_server_cookie()
        _exit_if_errors(errors)

        if opts.vm != 'ns_server':
            cookie, errors = self.rest.get_babysitter_cookie()
            _exit_if_errors(errors)

            [short, _] = node.split('@')

            if opts.vm == 'babysitter':
                node = f'babysitter_of_{short}@cb.local'
            elif opts.vm == 'couchdb':
                node = f'couchdb_{short}@cb.local'
            else:
                _exit_if_errors([f'Unknown vm type `{opts.vm}`'])

        rand_chars = ''.join(random.choice(string.ascii_letters) for _ in range(20))
        name = f'ctl-{rand_chars}@127.0.0.1'

        if os.name == 'nt':
            cb_erl = os.path.join(opts.erl_path, 'erl.exe')
        else:
            cb_erl = os.path.join(opts.erl_path, 'erl')

        if os.path.isfile(cb_erl):
            path = cb_erl
        else:
            _warning("Cannot locate Couchbase erlang. Attempting to use non-Couchbase erlang")
            path = 'erl'

        if not opts.erl_path:
            _exit_if_errors(["directory containing erl executable not found, please specify it using --erl-path"])

        if not opts.ns_ebin_path:
            _exit_if_errors(["ns_server ebin directory not found, please specify it using --ns-ebin-path"])

        if not opts.hosts_path:
            _exit_if_errors(["hosts.cfg file not found, please specify it using --hosts-path"])

        if not opts.base_path:
            _exit_if_errors(["Couchbase Server base directory not found, please specify it using --base-path"])

        with tempfile.NamedTemporaryFile() as temp:
            temp.write(f'[{{preferred_local_proto,{result["addressFamily"]}_tcp_dist}}].'.encode())
            temp.flush()
            temp_name = temp.name
            env = os.environ.copy()
            env["CB_COOKIE"] = cookie
            if platform.system() == "Darwin":
                # erl script fails in OSX as it is unable to find COUCHBASE_TOP
                env["COUCHBASE_TOP"] = opts.base_path

            eval_str = 'erlang:set_cookie(list_to_atom(os:getenv("CB_COOKIE"))), '
            if opts.eval is not None:
                cmd = opts.eval.replace('"', '\\"')
                eval_str += \
                    '{value, Res, _} = ' \
                    f'rpc:call(\'{node}\', ' \
                    f'misc, eval, ["{cmd}", erl_eval:new_bindings()]),' \
                    'io:format("~p~n", [Res]),' \
                    'init:stop().'
            else:
                eval_str += f'shell:start_interactive({{remote, "{node}"}}).'

            args = [
                path,
                '-name',
                name,
                '-setcookie',
                'nocookie',
                '-hidden',
                '-eval',
                eval_str,
                '-noshell',
                '-proto_dist',
                'cb',
                '-epmd_module',
                'cb_epmd',
                '-no_epmd',
                '-pa',
                opts.ns_ebin_path,
                '-kernel',
                'dist_config_file',
                f'"{temp_name}"'] + get_inetrc(opts.hosts_path)

            if opts.debug:
                print(f'Running {" ".join(args)}')

            try:
                subprocess.call(args, env=env)
            except OSError:
                _exit_if_errors(["Unable to find the erl executable"])

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-server-eshell")

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

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        # Cluster does not need to be initialized for this command
        result, errors = self.rest.node_info()
        _exit_if_errors(errors)

        print(json.dumps(result, sort_keys=True, indent=2))

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-server-info")

    @staticmethod
    def get_description():
        return "Show details of a node in the cluster"


class ServerList(Subcommand):
    """The server list subcommand"""

    def __init__(self):
        super(ServerList, self).__init__()
        self.parser.prog = "couchbase-cli server-list"

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        result, errors = self.rest.pools('default')
        _exit_if_errors(errors)

        for node in result['nodes']:
            if node.get('otpNode') is None:
                raise Exception("could not access node")

            print(node['otpNode'], node['hostname'], node['status'], node['clusterMembership'])

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-server-list")

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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        _deprecated("Please use the recovery command instead")

        servers = apply_default_port(opts.servers)
        for server in servers:
            _, errors = self.rest.readd_server(server)
            _exit_if_errors(errors)

        _success("Servers recovered")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-server-readd")

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
                           help="Alert when a node wasn't auto-failed over because other nodes "
                           + "were down")
        group.add_argument("--alert-auto-failover-cluster-small", dest="alert_af_small",
                           action="store_true",
                           help="Alert when a node wasn't auto-failed over because cluster was"
                           + " too small")
        group.add_argument("--alert-auto-failover-disable", dest="alert_af_disable",
                           action="store_true",
                           help="Alert when a node wasn't auto-failed over because auto-failover"
                           + " is disabled")
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
        group.add_argument("--alert-timestamp-drift-exceeded", dest="alert_cas_drift", action="store_true",
                           help="Alert when should be sent if the remote mutation timestamps exceeds drift threshold")
        group.add_argument("--alert-communication-issue", dest="alert_communication_issue",
                           action="store_true", help="Alert when nodes are experiencing communication issues")
        group.add_argument("--alert-node-time", dest="alert_node_time", action="store_true",
                           help="Alert when time on one node is out of sync with time on another "
                                                     "node")
        group.add_argument("--alert-disk-analyzer", dest="alert_disk_analyzer", action="store_true",
                           help="Alert when the disk analyzer gets stuck")
        group.add_argument("--alert-memory-threshold", dest="alert_memory_threshold",
                           action="store_true", help="Alert when system memory usage exceeds threshold")
        group.add_argument("--alert-bucket-history-size", dest="alert_bucket_history_size",
                           action="store_true", help="Alert when history size for a bucket reaches 90%%")
        group.add_argument("--alert-indexer-low-resident-percentage", dest="alert_indexer_low_resident_percentage",
                           action="store_true", help="Alert when approaching indexer low resident percentage")
        group.add_argument("--alert-memcached-connections", dest="alert_memcached_connections",
                           action="store_true", help="Alert when the memcached connection threshold is exceeded")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if opts.enabled == "1":
            if opts.email_recipients is None:
                _exit_if_errors(["--email-recipients must be set when email alerts are enabled"])
            if opts.email_sender is None:
                _exit_if_errors(["--email-sender must be set when email alerts are enabled"])
            if opts.email_host is None:
                _exit_if_errors(["--email-host must be set when email alerts are enabled"])
            if opts.email_port is None:
                _exit_if_errors(["--email-port must be set when email alerts are enabled"])

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
        if opts.alert_node_time:
            alerts.append('time_out_of_sync')
        if opts.alert_disk_analyzer:
            alerts.append('disk_usage_analyzer_stuck')
        if opts.alert_memory_threshold:
            alerts.append('memory_threshold')
        if opts.alert_bucket_history_size:
            alerts.append('history_size_warning')
        if opts.alert_indexer_low_resident_percentage:
            alerts.append('indexer_low_resident_percentage')
        if opts.alert_memcached_connections:
            alerts.append('memcached_connections')

        enabled = "true"
        if opts.enabled == "0":
            enabled = "false"

        email_encrypt = "false"
        if opts.email_encrypt == "1":
            email_encrypt = "true"

        _, errors = self.rest.set_alert_settings(enabled, opts.email_recipients, opts.email_sender, opts.email_username,
                                                 opts.email_password, opts.email_host, opts.email_port, email_encrypt,
                                                 ",".join(alerts))
        _exit_if_errors(errors)

        _success("Email alert settings modified")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-alert")

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
        group.add_argument("--list-filterable-events", dest="list_events", action="store_true",
                           help="Retrieve a list of filterable event IDs and the descriptions")
        group.add_argument("--get-settings", dest="get_settings", action="store_true",
                           help="Retrieve current audit settings")
        group.add_argument("--set", dest="set_settings", action="store_true",
                           help="Set current audit settings")
        group.add_argument("--audit-enabled", dest="enabled", metavar="<1|0>", choices=["0", "1"],
                           help="Enable/disable auditing")
        group.add_argument("--audit-log-path", dest="log_path", metavar="<path>",
                           help="The audit log path")
        group.add_argument("--audit-log-rotate-interval", dest="rotate_interval", type=(int),
                           metavar="<seconds>", help="The audit log rotate interval")
        group.add_argument("--audit-log-rotate-size", dest="rotate_size", type=(int),
                           metavar="<bytes>", help="The audit log rotate size")
        group.add_argument("--disabled-users", dest="disabled_users", default=None,
                           help="A comma-separated list of users to ignore events from")
        group.add_argument("--disable-events", dest="disable_events", default=None,
                           help="A comma-separated list of audit-event IDs to not audit")
        group.add_argument("--prune-age", dest="prune_age", default=None,
                           help="Prune audit logs older than the specified age in minutes")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        flags = sum([opts.list_events, opts.get_settings, opts.set_settings])
        if flags != 1:
            _exit_if_errors(["One of the following is required: --list-filterable-events, --get-settings or --set"])

        if opts.list_events:
            descriptors, errors = self.rest.get_id_descriptors()
            _exit_if_errors(errors)
            if opts.output == 'json':
                print(json.dumps(descriptors, indent=4))
                return

            self.format_descriptors_in_table(descriptors)
        elif opts.get_settings:
            audit_settings, errors = self.rest.get_audit_settings()
            _exit_if_errors(errors)
            if opts.output == 'json':
                print(json.dumps(audit_settings, indent=4))
                return

            descriptors, errors = self.rest.get_id_descriptors()
            _exit_if_errors(errors)
            self.format_audit_settings(audit_settings, descriptors)
        elif opts.set_settings:
            if not (opts.enabled or opts.log_path or opts.rotate_interval or opts.rotate_size
                    or opts.disable_events is not None or opts.disabled_users is not None):
                _exit_if_errors(["At least one of [--audit-enabled, --audit-log-path, --audit-log-rotate-interval,"
                                 " --audit-log-rotate-size, --disabled-users, --disable-events] is required with"
                                 " --set"])

            # Match the behavior in the WebUI, which is to internally translate the '/couchbase' postfix into '/local'
            # see MB-46970 for more information.
            if opts.disabled_users is not None:
                opts.disabled_users = re.sub(r'\/couchbase', '/local', opts.disabled_users)

            _, errors = self.rest.set_audit_settings(opts.enabled, opts.log_path, opts.rotate_interval,
                                                     opts.rotate_size, opts.disable_events, opts.disabled_users,
                                                     opts.prune_age)
            _exit_if_errors(errors)
            _success("Audit settings modified")

    @staticmethod
    def format_audit_settings(audit_settings, json_descriptors):
        print(f'Audit enabled: {audit_settings["auditdEnabled"]}')
        print(f'UUID: {audit_settings["uid"]}')
        print(f'Log path: {audit_settings["logPath"] if "logPath" in audit_settings else "N/A"}')
        print(f'Rotate interval: {audit_settings["rotateInterval"]}')
        print(f'Rotate size: {audit_settings["rotateSize"]}')
        print(f'Disabled users: {audit_settings["disabledUsers"]}')

        if not audit_settings["auditdEnabled"]:
            return

        # change id lists to maps to make lookup o(1)
        disable_map = {eventID for eventID in audit_settings['disabled']}
        json_descriptors.sort(key=itemgetter('module', 'id'))
        all_descriptors_sets = {events["id"] for events in json_descriptors}

        padding_name = 12
        for descriptor in json_descriptors:
            if len(descriptor['name']) > padding_name:
                padding_name = len(descriptor['name'])

        padding_name += 2

        header = f'{"ID":<6}| {"Module":<15}| {"Name":<{padding_name}}| Enabled'
        print(header)
        print('-' * len(header))
        for descriptor in json_descriptors:
            print(f'{descriptor["id"]:<6}| {descriptor["module"]:<15}| {descriptor["name"]:<{padding_name}}| '
                  f'{"False" if descriptor["id"] in disable_map else "True"}')

        not_recognized = disable_map - all_descriptors_sets
        for unrecognized in not_recognized:
            print(f'{unrecognized:<6}| {"unknown":<15}| {"unknown":<{padding_name}}| False')

    @staticmethod
    def format_descriptors_in_table(json_descriptors):
        sorted_descriptors = sorted(json_descriptors, key=itemgetter('module', 'id'))
        padding_name = 15
        for descriptor in sorted_descriptors:
            if len(descriptor['name']) > padding_name:
                padding_name = len(descriptor['name'])

        padding_name += 2

        header = f'{"ID":<6}| {"Module":<15}| {"Name":<{padding_name}}| Description'
        print(header)
        print('-' * len(header))
        for descriptor in sorted_descriptors:
            print(f'{descriptor["id"]:<6}| {descriptor["module"]:<15}| {descriptor["name"]:<{padding_name}}| '
                  f'{descriptor["description"]}')

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-audit")

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
        group.add_argument("--max-failovers", dest="max_failovers",
                           help="Maximum number of times an auto-failover event can happen")
        group.add_argument("--enable-failover-on-data-disk-issues", dest="enable_failover_on_data_disk_issues",
                           metavar="<1|0>", choices=["0", "1"],
                           help="Enable/disable auto-failover when the Data Service reports disk issues. "
                           + "Couchbase Server Enterprise Edition only.")
        group.add_argument("--failover-data-disk-period", dest="failover_on_data_disk_period",
                           metavar="<seconds>", type=(int),
                           help="The amount of time the Data Serivce disk failures has to be happening for to trigger"
                                " an auto-failover")
        group.add_argument("--enable-failover-on-data-disk-non-responsive",
                           dest="enable_failover_on_data_disk_non_responsive", metavar="<1|0>", choices=["0", "1"],
                           help="Enable/disable auto-failover when the Data Service reports disk is not responsive. "
                           + "Couchbase Server Enterprise Edition only.")
        group.add_argument("--failover-data-disk-non-responsive-period",
                           dest="failover_on_data_disk_non_responsive_period", metavar="<seconds>", type=(int),
                           help="The amount of time the Data Serivce disk non-responsiveness has to be happening for to"
                                " trigger an auto-failover")
        group.add_argument("--allow-failover-for-ephemeral-without-replica", metavar="<1|0>", choices=["0", "1"],
                           help="Whether to allow autofailover on nodes with an ephemeral buckets that do not have at "
                                "least one replica")
        group.add_argument("--can-abort-rebalance", metavar="<1|0>", choices=["1", "0"], dest="can_abort_rebalance",
                           help="Enables auto-failover to abort rebalance and perform the failover. (EE only)")

    def __validate_failover_reason(self, enable, period, cli_enable_name, cli_period_name):
        if not self.enterprise and (enable or period):
            _exit_if_errors([f"--{cli_enable_name} and --{cli_period_name} can only be configured on "
                             "Enterprise Edition"])

        if (enable is None or enable == "false") and period:
            _exit_if_errors([
                f"{cli_enable_name} must be set to 1 when {cli_period_name} is configured"])

        if enable == "true" and period is None:
            _exit_if_errors([f"{cli_period_name} must be set when {cli_enable_name} is enabled"])

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=False)
    def execute(self, opts):
        if opts.enabled == "1":
            opts.enabled = "true"
        elif opts.enabled == "0":
            opts.enabled = "false"

        if opts.enable_failover_on_data_disk_issues == "1":
            opts.enable_failover_on_data_disk_issues = "true"
        elif opts.enable_failover_on_data_disk_issues == "0":
            opts.enable_failover_on_data_disk_issues = "false"

        if opts.enable_failover_on_data_disk_non_responsive == "1":
            opts.enable_failover_on_data_disk_non_responsive = "true"
        elif opts.enable_failover_on_data_disk_non_responsive == "0":
            opts.enable_failover_on_data_disk_non_responsive = "false"

        if opts.allow_failover_for_ephemeral_without_replica == "1":
            opts.allow_failover_for_ephemeral_without_replica = "true"
        elif opts.allow_failover_for_ephemeral_without_replica == "0":
            opts.allow_failover_for_ephemeral_without_replica = "false"

        if not self.enterprise:
            if opts.max_failovers:
                _exit_if_errors(["--max-count can only be configured on enterprise edition"])
            if opts.can_abort_rebalance:
                _exit_if_errors(["--can-abort-rebalance can only be configured on enterprise edition"])
            if opts.allow_failover_for_ephemeral_without_replica:
                _exit_if_errors(["--allow-failover-for-ephemeral-without-replica can only be configured on enterprise"
                                 " edition"])

        if not any([opts.enabled, opts.timeout, opts.enable_failover_on_data_disk_issues,
                    opts.failover_on_data_disk_period, opts.enable_failover_on_data_disk_non_responsive,
                    opts.failover_on_data_disk_non_responsive_period, opts.max_failovers,
                    opts.allow_failover_for_ephemeral_without_replica]):
            _exit_if_errors(["No settings specified to be changed"])

        self.__validate_failover_reason(
            opts.enable_failover_on_data_disk_issues, opts.failover_on_data_disk_period,
            "--enable-failover-on-data-disk-issues", "--failover-data-disk-period")
        self.__validate_failover_reason(
            opts.enable_failover_on_data_disk_non_responsive,
            opts.failover_on_data_disk_non_responsive_period,
            "--enable-failover-on-data-disk-non-responsive",
            "--failover-data-disk-non-responsive-period")

        if opts.enabled == "false" or opts.enabled is None:
            if opts.enable_failover_on_data_disk_issues or opts.failover_on_data_disk_period or \
                    opts.enable_failover_on_data_disk_non_responsive or \
                    opts.failover_on_data_disk_non_responsive_period or \
                    opts.allow_failover_for_ephemeral_without_replica:
                _exit_if_errors([
                    "--enable-auto-failover must be set to 1 when auto-failover on Data Service disk "
                    "issues/non-responsive settings or allowing auto-failover for ephemeral buckets without replicas "
                    "are being configured"])
            if opts.timeout:
                _warning("Timeout specified will not take affect because auto-failover is being disabled")

        if opts.can_abort_rebalance == '1':
            opts.can_abort_rebalance = 'true'
        elif opts.can_abort_rebalance == '0':
            opts.can_abort_rebalance = 'false'

        _, errors = self.rest.set_autofailover_settings(opts.enabled, opts.timeout, opts.max_failovers,
                                                        opts.enable_failover_on_data_disk_issues,
                                                        opts.failover_on_data_disk_period,
                                                        opts.enable_failover_on_data_disk_non_responsive,
                                                        opts.failover_on_data_disk_non_responsive_period,
                                                        opts.can_abort_rebalance,
                                                        opts.allow_failover_for_ephemeral_without_replica)
        _exit_if_errors(errors)

        _success("Auto-failover settings modified")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-autofailover")

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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if opts.enabled == "1":
            opts.enabled = "true"
        elif opts.enabled == "0":
            opts.enabled = "false"

        if opts.enabled == "true" and opts.max_nodes is None:
            _exit_if_errors(["--max-nodes must be specified if auto-reprovision is enabled"])

        if not (opts.enabled or opts.max_nodes):
            _exit_if_errors(["No settings specified to be changed"])

        if (opts.enabled is None or opts.enabled == "false") and opts.max_nodes:
            _warning("--max-servers will not take affect because auto-reprovision is being disabled")

        _, errors = self.rest.set_autoreprovision_settings(opts.enabled, opts.max_nodes)
        _exit_if_errors(errors)

        _success("Auto-reprovision settings modified")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-autoreprovision")

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
                           type=(int), help="The data service memory quota in mebibytes")
        group.add_argument("--cluster-index-ramsize", dest="index_mem_quota", metavar="<quota>",
                           type=(int), help="The index service memory quota in mebibytes")
        group.add_argument("--cluster-fts-ramsize", dest="fts_mem_quota", metavar="<quota>",
                           type=(int), help="The full-text service memory quota in mebibytes")
        group.add_argument("--cluster-eventing-ramsize", dest="eventing_mem_quota", metavar="<quota>",
                           type=(int), help="The Eventing service memory quota in mebibytes")
        group.add_argument("--cluster-analytics-ramsize", dest="cbas_mem_quota", metavar="<quota>",
                           type=(int), help="The analytics service memory quota in mebibytes")
        group.add_argument("--cluster-query-ramsize", dest="query_mem_quota", metavar="<quota>",
                           type=(int), help="The query service memory quota in mebibytes")
        group.add_argument("--cluster-name", dest="name", metavar="<name>", help="The cluster name")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if (opts.data_mem_quota or opts.index_mem_quota or opts.fts_mem_quota or opts.cbas_mem_quota
                or opts.eventing_mem_quota or opts.query_mem_quota or opts.name):
            _, errors = self.rest.set_pools_default(opts.data_mem_quota, opts.index_mem_quota, opts.fts_mem_quota,
                                                    opts.cbas_mem_quota, opts.eventing_mem_quota, opts.query_mem_quota,
                                                    opts.name)
            _exit_if_errors(errors)

        if opts.new_username or opts.new_password or opts.port:
            username = opts.username
            if opts.new_username:
                username = opts.new_username

            password = opts.password
            if opts.new_password:
                password = opts.new_password

            _, errors = self.rest.set_admin_credentials(username, password, opts.port)
            _exit_if_errors(errors)

        _success("Cluster settings modified")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-cluster")

    @staticmethod
    def get_description():
        return "Modify cluster settings"


class SettingEncryption(Subcommand):
    """The setting encryption subcommand"""

    def __init__(self):
        super(SettingEncryption, self).__init__()
        self.parser.prog = "couchbase-cli setting-encryption"
        group = self.parser.add_argument_group("Encryption settings")

        group_me = group.add_mutually_exclusive_group(required=True)
        group_me.add_argument("--get", dest="get", action="store_true",
                              help="Get the encryption settings of config/logs/audit")
        group_me.add_argument("--set", dest="set", action="store_true",
                              help="Set the encryption settings of config/log/audit")
        group_me.add_argument("--list-keys", dest="list_keys", action="store_true", help="List the encryption keys")
        group_me.add_argument("--add-key", dest="add_key", action="store_true", help="Create a new encryption key")
        group_me.add_argument("--edit-key", dest="edit_key", metavar="<keyid>", help="Edit an encryption key")
        group_me.add_argument("--rotate-key", dest="rotate_key", metavar="<keyid>",
                              help="Rotate the specified encryption key")
        group_me.add_argument("--delete-key", dest="delete_key", metavar="<keyid>",
                              help="Delete the specified encryption key")

        # --set arguments
        group.add_argument("--target", dest="target", choices=["config", "log", "audit"],
                           help="The type of files to configure the encryption of")
        group.add_argument("--type", dest="encryption_type", choices=["disabled", "master-password", "key"],
                           help="The type of encryption to use")
        group.add_argument("--key", dest="key", metavar="<keyid>",
                           help="The id of the key to encrypt these files with")
        group.add_argument("--dek-rotate-every", dest="dek_rotation_interval", metavar="<days>", type=(int),
                           help="The number of days the DEK should be rotated")
        group.add_argument("--dek-lifetime", dest="dek_lifetime", metavar="<days>", type=(int),
                           help="How long the DEK should be kept for")

        # --add-key/--edit-key arguments
        group.add_argument("--name", dest="name", metavar="<name>", help="The name of the key")
        group.add_argument("--key-type", dest="key_type", choices=["aws", "kmip", "auto-generated"],
                           help="The type of key to create")
        group.add_argument("--kek-usage", dest="kek_usage", action="store_true",
                           help="Allow the key to be used as a KEK")
        group.add_argument("--config-usage", dest="config_usage", action="store_true",
                           help="Allow the key to be used to encrypt the config")
        group.add_argument("--log-usage", dest="log_usage", action="store_true",
                           help="Allow the key to be used to encrypt logs")
        group.add_argument("--audit-usage", dest="audit_usage", action="store_true",
                           help="Allow the key to be used to encrypt the audit logs")

        group_usage_me = group.add_mutually_exclusive_group(required=False)
        group_usage_me.add_argument("--all-bucket-usage", dest="all_bucket_usage", action="store_true",
                                    help="Allow the key to be used to encrypt any bucket")
        group_usage_me.add_argument("--bucket-usage", dest="bucket_usage", action="append", metavar="<bucket>",
                                    help="Allow the key to be used to encrypt the given bucket. Can be used multiple "
                                    "times.")

        group.add_argument("--cloud-key-arn", dest="cloud_key_arn", metavar="<arn>", help="The arn of the cloud key")
        group.add_argument("--cloud-region", dest="cloud_region", metavar="<region>",
                           help="The region the cloud key is in")
        group.add_argument("--cloud-auth-by-instance-metadata", dest="cloud_imds", action="store_true",
                           help="When authenticating with the cloud provider, use IMDS")
        group.add_argument("--cloud-creds-path", dest="cloud_creds_path", metavar="<path>",
                           help="The path to the cloud credentials")
        group.add_argument("--cloud-config-path", dest="cloud_config_path", metavar="<path>",
                           help="The path to the cloud config")
        group.add_argument("--cloud-profile-path", dest="cloud_profile_path", metavar="<path>",
                           help="The path to the cloud profile")

        group_encrypt_me = group.add_mutually_exclusive_group(required=False)
        group_encrypt_me.add_argument("--encrypt-with-master-password", dest="encrypt_with_master", action="store_true",
                                      help="Encrypt this key with the master password")
        group_encrypt_me.add_argument("--encrypt-with-key", dest="encrypt_with_key", metavar="<keyid>",
                                      help="Encrypt this key with another key")

        group.add_argument("--kmip-operations", dest="kmip_ops", choices=["get", "encrypt-decrypt"],
                           help="What operations to use with the KMIP server")
        group.add_argument("--kmip-key", dest="kmip_key", metavar="<keyid>", help="The key on the KMIP server to use")
        group.add_argument("--kmip-host", dest="kmip_host", metavar="<host>", help="The hostname of the KMIP server")
        group.add_argument("--kmip-port", dest="kmip_port", metavar="<port>", type=(int),
                           help="The port of the KMIP server")
        group.add_argument("--kmip-key-path", dest="kmip_key_path", metavar="<path>",
                           help="The path to the client key to use")
        group.add_argument("--kmip-cert-path", dest="kmip_cert_path", metavar="<path>",
                           help="The path to the certificate to use")
        group.add_argument("--kmip-key-passphrase", dest="kmip_key_passphrase", metavar="<passphrase>",
                           action=CBNonEchoedAction, envvar="CB_KMIP_KEY_PASSPHRASE",
                           help="The passphrase to use to decode the key")

        group.add_argument("--auto-rotate-every", dest="auto_rotate_every", metavar="<days>",
                           help="How often to rotate the generated key")
        group.add_argument("--auto-rotate-start-on", dest="auto_rotate_start", metavar="<iso8601date>",
                           help="When to start rotating")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if opts.get:
            settings, errors = self.rest.get_encryption_settings()
            _exit_if_errors(errors)
            print(json.dumps(settings, indent=2))
        elif opts.set:
            self._set(opts)
        elif opts.add_key:
            self._add_key(opts)
        elif opts.edit_key:
            self._edit_key(opts)
        elif opts.rotate_key:
            _, errors = self.rest.rotate_key(opts.rotate_key)
            _exit_if_errors(errors)
            _success(f"Rotated key {opts.rotate_key}")
        elif opts.delete_key:
            _, errors = self.rest.delete_key(opts.delete_key)
            _exit_if_errors(errors)
            _success(f"Deleted key {opts.delete_key}")
        elif opts.list_keys:
            keys, errors = self.rest.list_keys()
            _exit_if_errors(errors)
            print(json.dumps(keys, indent=2))

    def _set(self, opts):
        if not opts.target:
            _exit_if_errors(["--target must be specified if setting encryption settings"])

        if not opts.encryption_type:
            _exit_if_errors(["--type must be specified if setting encryption settings"])

        if (opts.encryption_type == "key" and not opts.key) or (opts.encryption_type != "key" and opts.key):
            _exit_if_errors(["when --type is 'key', --key must be specified, and vice versa"])

        dek_rotation_interval, dek_lifetime = None, None
        if opts.dek_rotation_interval:
            dek_rotation_interval = opts.dek_rotation_interval * 24 * 60 * 60
        if opts.dek_lifetime:
            dek_lifetime = opts.dek_lifetime * 24 * 60 * 60

        _, errors = self.rest.set_encryption_settings(
            opts.target, opts.encryption_type, opts.key, dek_rotation_interval, dek_lifetime)
        _exit_if_errors(errors)

        _success(f"Set the encryptition settings for {opts.target}")

    def _add_edit_parse_opts(self, opts):
        if not opts.name:
            _exit_if_errors(["--name must be specified"])

        usages = []
        if opts.config_usage:
            usages.append("config-encryption")
        if opts.log_usage:
            usages.append("log-encryption")
        if opts.audit_usage:
            usages.append("audit_usage")
        if opts.kek_usage:
            usages.append("KEK-encryption")
        if opts.all_bucket_usage:
            usages.append("bucket-encryption-*")
        if opts.bucket_usage:
            usages += [f"bucket-encryption-{b}" for b in opts.bucket_usage]

        if not usages:
            _exit_if_errors(["at least one of --config-usage, --log-usage, --audit-usage, --kek-usage, "
                             "--all-bucket-usage and/or --bucket-usage must be passed"])

        data = {}
        typ = ""
        if opts.key_type == "aws":
            typ = "awskms-aes-key-256"

            if not opts.cloud_key_arn or not opts.cloud_region:
                _exit_if_errors(["--cloud-key-arn and --cloud-region must be specified"])

            data["keyARN"] = opts.cloud_key_arn
            data["region"] = opts.cloud_region
            data["useIMDS"] = opts.cloud_imds

            if opts.cloud_creds_path:
                data["credentialsFile"] = opts.cloud_creds_path
            if opts.cloud_config_path:
                data["configFile"] = opts.cloud_config_path
            if opts.cloud_profile_path:
                data["profile"] = opts.cloud_profile_path
        elif opts.key_type == "kmip":
            typ = "kmip-aes-key-256"

            if not (opts.kmip_ops and opts.kmip_key and opts.kmip_host and opts.kmip_port and opts.kmip_key_path
                    and opts.kmip_cert_path):
                _exit_if_errors(["--kmip-operations, --kmip-key, --kmip-host --kmip-port, --kmip-key-path, "
                                 "--kmip-cert-path must be specified"])

            if not (opts.encrypt_with_master or opts.encrypt_with_key):
                _exit_if_errors(["one of --encrypt-with-master-password, --encrypt-with-key must be specified"])

            if opts.encrypt_with_master:
                data["encryptWith"] = "nodeSecretManager"
            else:
                data["encryptWith"] = "encryptionKey"
                data["encryptWithKeyId"] = int(opts.encrypt_with_key)

            data["activeKey"] = {"kmipId": opts.kmip_key}
            data["host"] = opts.kmip_host
            data["port"] = int(opts.kmip_port)

            if opts.kmip_ops == "get":
                data["encryptionApproach"] = "useGet"
            else:
                data["encryptionApproach"] = "useEncryptDecrypt"

            if opts.kmip_key_path:
                data["keyPath"] = opts.kmip_key_path
            if opts.kmip_cert_path:
                data["certPath"] = opts.kmip_cert_path
            if opts.kmip_key_passphrase:
                data["keyPassphrase"] = opts.kmip_key_passphrase

        elif opts.key_type == "auto-generated":
            typ = "auto-generated-aes-key-256"

            if not (opts.encrypt_with_master or opts.encrypt_with_key):
                _exit_if_errors(["one of --encrypt-with-master-password, --encrypt-with-key must be specified"])

            if opts.encrypt_with_master:
                data["encryptWith"] = "nodeSecretManager"
            else:
                data["encryptWith"] = "encryptionKey"
                data["encryptWithKeyId"] = int(opts.encrypt_with_key)

            if (opts.auto_rotate_every and not opts.auto_rotate_start) or \
               (opts.auto_rotate_start and not opts.auto_rotate_every):
                _exit_if_errors(["--auto-rotate-every must be provided with --auto-rotate-start-on"])

            data["autoRotation"] = False
            if opts.auto_rotate_every:
                data["autoRotation"] = True
                data["rotationIntervalInDays"] = int(opts.auto_rotate_every)
                data["nextRotationTime"] = opts.auto_rotate_start
        else:
            _exit_if_errors(["--key-type must be specified"])

        return typ, usages, data

    def _add_key(self, opts):
        typ, usages, data = self._add_edit_parse_opts(opts)
        _, errors = self.rest.create_key(opts.name, typ, usages, data)
        _exit_if_errors(errors)

        _success(f"Created key {opts.name}")

    def _edit_key(self, opts):
        typ, usages, data = self._add_edit_parse_opts(opts)
        _, errors = self.rest.edit_key(opts.edit_key, opts.name, typ, usages, data)
        _exit_if_errors(errors)

        _success(f"Edited key {opts.name}")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-encryption")

    @staticmethod
    def get_description():
        return "Manage encryption at-rest"


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
        return get_doc_page_name("couchbase-cli-cluster-edit")

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
        group.add_argument("--compaction-db-size", dest="db_size", metavar="<mebibytes>",
                           type=(int),
                           help="Compacts db once the fragmentation reaches this size (MiB)")
        group.add_argument("--compaction-view-percentage", dest="view_perc", metavar="<perc>",
                           type=(int),
                           help="Compacts the view once the fragmentation reaches this percentage")
        group.add_argument("--compaction-view-size", dest="view_size", metavar="<mebibytes>",
                           type=(int),
                           help="Compacts view once the fragmentation reaches this size (MiB)")
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
        group.add_argument("--gsi-compaction-mode", dest="gsi_mode", choices=["append", "circular"],
                           help="Sets the gsi compaction mode (append or circular)")
        group.add_argument("--compaction-gsi-percentage", dest="gsi_perc", type=(int), metavar="<perc>",
                           help="Starts compaction once gsi file fragmentation has reached this percentage"
                           + "(Append mode only)")
        group.add_argument("--compaction-gsi-interval", dest="gsi_interval", metavar="<days>",
                           help="A comma separated list of days compaction can run (Circular mode only)")
        group.add_argument("--compaction-gsi-period-from", dest="gsi_from_period", metavar="<HH:MM>",
                           help="Allow gsi compaction to run after this time (Circular mode only)")
        group.add_argument("--compaction-gsi-period-to", dest="gsi_to_period", metavar="<HH:MM>",
                           help="Allow gsi compaction to run before this time (Circular mode only)")
        group.add_argument("--enable-gsi-compaction-abort", dest="enable_gsi_abort", metavar="<1|0>",
                           choices=["0", "1"],
                           help="Abort gsi compaction if when run outside of the accepted interaval"
                           + "(Circular mode only)")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if opts.db_perc is not None and (opts.db_perc < 2 or opts.db_perc > 100):
            _exit_if_errors(["--compaction-db-percentage must be between 2 and 100"])

        if opts.view_perc is not None and (opts.view_perc < 2 or opts.view_perc > 100):
            _exit_if_errors(["--compaction-view-percentage must be between 2 and 100"])

        if opts.db_size is not None:
            if int(opts.db_size) < 1:
                _exit_if_errors(["--compaction-db-size must be between greater than 1 or infinity"])
            opts.db_size = int(opts.db_size) * 1024**2

        if opts.view_size is not None:
            if int(opts.view_size) < 1:
                _exit_if_errors(["--compaction-view-size must be between greater than 1 or infinity"])
            opts.view_size = int(opts.view_size) * 1024**2

        if opts.from_period and not (opts.to_period and opts.enable_abort):
            errors = []
            if opts.to_period is None:
                errors.append("--compaction-period-to is required when using --compaction-period-from")
            if opts.enable_abort is None:
                errors.append("--enable-compaction-abort is required when using --compaction-period-from")
            _exit_if_errors(errors)

        if opts.to_period and not (opts.from_period and opts.enable_abort):
            errors = []
            if opts.from_period is None:
                errors.append("--compaction-period-from is required when using --compaction-period-to")
            if opts.enable_abort is None:
                errors.append("--enable-compaction-abort is required when using --compaction-period-to")
            _exit_if_errors(errors)

        if opts.enable_abort and not (opts.from_period and opts.to_period):
            errors = []
            if opts.from_period is None:
                errors.append("--compaction-period-from is required when using --enable-compaction-abort")
            if opts.to_period is None:
                errors.append("--compaction-period-to is required when using --enable-compaction-abort")
            _exit_if_errors(errors)

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

        g_from_hour = None
        g_from_min = None
        g_to_hour = None
        g_to_min = None
        if opts.gsi_mode == "append":
            opts.gsi_mode = "full"
            if opts.gsi_perc is None:
                _exit_if_errors(['--compaction-gsi-percentage must be specified when --gsi-compaction-mode is set '
                                 'to append'])
        elif opts.gsi_mode == "circular":
            if opts.gsi_from_period is not None and opts.gsi_to_period is None:
                _exit_if_errors(["--compaction-gsi-period-to is required with --compaction-gsi-period-from"])
            if opts.gsi_to_period is not None and opts.gsi_from_period is None:
                _exit_if_errors(["--compaction-gsi-period-from is required with --compaction-gsi-period-to"])

            g_from_hour, g_from_min = self._handle_timevalue(opts.gsi_from_period, "--compaction-gsi-period-from")
            g_to_hour, g_to_min = self._handle_timevalue(opts.gsi_to_period, "--compaction-gsi-period-to")

            if opts.enable_gsi_abort == "1":
                opts.enable_gsi_abort = "true"
            else:
                opts.enable_gsi_abort = "false"

        _, errors = self.rest.set_compaction_settings(opts.db_perc, opts.db_size, opts.view_perc, opts.view_size,
                                                      from_hour, from_min, to_hour, to_min, opts.enable_abort,
                                                      opts.enable_parallel, opts.purge_interval, opts.gsi_mode,
                                                      opts.gsi_perc, opts.gsi_interval, g_from_hour, g_from_min,
                                                      g_to_hour, g_to_min, opts.enable_gsi_abort)
        _exit_if_errors(errors)

        _success("Compaction settings modified")

    def _handle_timevalue(self, opt_value, opt_name):
        hour = None
        minute = None
        if opt_value:
            if opt_value.find(':') == -1:
                _exit_if_errors([f'Invalid value for {opt_name}, must be in form XX:XX'])
            hour, minute = opt_value.split(':', 1)
            try:
                hour = int(hour)
            except ValueError:
                _exit_if_errors([f'Invalid hour value for {opt_name}, must be an integer'])
            if hour not in range(24):
                _exit_if_errors([f'Invalid hour value for {opt_name}, must be 0-23'])

            try:
                minute = int(minute)
            except ValueError:
                _exit_if_errors([f'Invalid minute value for {opt_name}, must be an integer'])
            if minute not in range(60):
                _exit_if_errors([f'Invalid minute value for {opt_name}, must be 0-59'])
        return hour, minute

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-compaction")

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
        group.add_argument('--replicas', metavar='<num>', type=int, help='Number of index replicas')
        group.add_argument('--optimize-placement', metavar='<1|0>', type=str,
                           help='Optimize index placement on a rebalance.')

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=False)
    def execute(self, opts):
        if (opts.max_rollback is None and opts.stable_snap is None
                and opts.mem_snap is None and opts.storage_mode is None
                and opts.threads is None and opts.log_level is None and opts.replicas is None
                and opts.optimize_placement is None):
            _exit_if_errors(["No settings specified to be changed"])

        settings, errors = self.rest.index_settings()
        _exit_if_errors(errors)

        # For supporting the default index backend changing from forestdb to plasma in Couchbase 5.0
        default = "plasma"
        if opts.storage_mode == "default" and settings['storageMode'] == "forestdb" or not self.enterprise:
            default = "forestdb"

        opts.storage_mode = index_storage_mode_to_param(opts.storage_mode, default)
        _, errors = self.rest.set_index_settings(opts.storage_mode, opts.max_rollback, opts.stable_snap, opts.mem_snap,
                                                 opts.threads, opts.log_level, opts.replicas, opts.optimize_placement)
        _exit_if_errors(errors)

        _success("Indexer settings modified")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-index")

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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
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
                _, errors = self.rest.sasl_settings('true', ro_admins, None)
            elif opts.default == 'roadmins':
                if admins:
                    _warning("--admins option ignored since default is admins")
                _, errors = self.rest.sasl_settings('true', None, admins)
            else:
                _, errors = self.rest.sasl_settings('true', ro_admins, admins)
        else:
            if admins:
                _warning("--admins option ignored since saslauthd is being disabled")
            if ro_admins:
                _warning("--roadmins option ignored since saslauthd is being disabled")
            _, errors = self.rest.sasl_settings('false', "", "")

        _exit_if_errors(errors)

        _success("saslauthd settings modified")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-saslauthd")

    @staticmethod
    def get_description():
        return "Modify saslauthd settings"


class SettingLdap(Subcommand):
    """The setting Ldap subcommand"""

    def __init__(self):
        super(SettingLdap, self).__init__()
        self.parser.prog = "couchbase-cli setting-ldap"
        group = self.parser.add_argument_group("LDAP settings")
        group_me = group.add_mutually_exclusive_group(required=False)
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
                           help="CA certificate to be used for LDAP server certificate validation, required if"
                           + " certificate validation is not disabled")
        group.add_argument("--ldap-client-cert", metavar="<path>", dest="ldap_client_cert",
                           help="The client TLS certificate for authentication")
        group.add_argument("--ldap-client-key", metavar="<path>", dest="ldap_client_key",
                           help="The client TLS key for authentication")
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
        group.add_argument("--bind-password", dest="bind_password", metavar="<password>",
                           help="The password of the bind user")
        group.add_argument("--group-query", dest="group_query", metavar="<query>",
                           help="LDAP query to get user's groups by username")
        group.add_argument("--enable-nested-groups", dest="nested_groups", metavar="<1|0>",
                           choices=["0", "1"])
        group.add_argument("--nested-group-max-depth", dest="nested_max_depth", metavar="<max>", type=int,
                           help="Maximum number of recursive group requests allowed. [1 - 100]")
        group_me.add_argument("--user-dn-query", metavar="<query>", dest="user_dn_query",
                              help="LDAP query to get user's DN. Must contains at least one instance of %%u")
        group_me.add_argument("--user-dn-template", metavar="<template>", dest="user_dn_template",
                              help="Template to construct user's DN. Must contain at least one instance of %%u")
        group_me.add_argument("--user-dn-advanced", metavar="<path>", dest="user_dn_advanced",
                              help="A path to a JSON file with a list of mappings")

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=True)
    def execute(self, opts):
        if opts.get:
            data, rv = self.rest.get_ldap()
            _exit_if_errors(rv)
            print(json.dumps(data))
        else:
            self._set(opts)

    def _set(self, opts):
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
            _exit_if_errors(['--server-cert-validation 0 and --ldap-cert can not be used together'])

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

        mapping = None
        if opts.user_dn_query is not None:
            mapping = f'{{"query": "{opts.user_dn_query}"}}'

        if opts.user_dn_template is not None:
            mapping = f'{{"template": "{opts.user_dn_template}"}}'

        if opts.user_dn_advanced is not None:
            json_mapping = {'advanced': _read_json_file_if_provided(opts.user_dn_advanced)}
            mapping = json.dumps(json_mapping, separators=(',', ':'))

        if (opts.ldap_client_cert and not opts.ldap_client_key) or (not opts.ldap_client_cert and opts.ldap_client_key):
            _exit_if_errors(['--client-cert and --client--key have to be used together'])

        if opts.ldap_client_cert is not None:
            opts.ldap_client_cert = _exit_on_file_read_failure(opts.ldap_client_cert)

        if opts.ldap_client_key is not None:
            opts.ldap_client_key = _exit_on_file_read_failure(opts.ldap_client_key)

        _, errors = self.rest.ldap_settings(opts.authentication_enabled, opts.authorization_enabled, opts.hosts,
                                            opts.port, opts.encryption, mapping, opts.timeout, opts.max_parallel,
                                            opts.max_cache_size, opts.cache_value_lifetime, opts.bind_dn,
                                            opts.bind_password, opts.ldap_client_cert, opts.ldap_client_key, opts.group_query,
                                            opts.nested_groups, opts.nested_max_depth, opts.server_cert_val,
                                            opts.cacert_ldap)

        _exit_if_errors(errors)
        _success("LDAP settings modified")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-ldap")

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
                           choices=["0", "1"], help="Enables/disable software notifications")

    @rest_initialiser(version_check=True, enterprise_check=False)
    def execute(self, opts):
        min_version, errors = self.rest.min_version()
        if errors is not None:
            # We fallback to node_version as min_version checks the cluster version and would fail
            # if the cluster is not initialised. node_version checks the version of the node instead.
            # See MB-60592 for more information.
            min_version, errors = self.rest.node_version()
        _exit_if_errors(errors)

        versionCheck = compare_versions(min_version, "7.2.3")
        if not self.enterprise and not self.columnar and versionCheck != -1:
            _exit_if_errors(["Modifying notifications settings is an Enterprise Edition only feature"])

        enabled = None
        if opts.enabled == "1":
            enabled = True
        elif opts.enabled == "0":
            enabled = False

        _, errors = self.rest.enable_notifications(enabled)
        _exit_if_errors(errors)

        _success("Software notification settings updated")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-notification")

    @staticmethod
    def get_description():
        return "Modify software notification settings"


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

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        actions = sum([opts.get, opts.set])
        if actions == 0:
            _exit_if_errors(["Must specify either --get or --set"])
        elif actions > 1:
            _exit_if_errors(["The --get and --set flags may not be specified at the same time"])
        elif opts.get:
            if opts.min_length is not None or any([opts.upper_case, opts.lower_case, opts.digit, opts.special_char]):
                _exit_if_errors(["The --get flag must be used without any other arguments"])
            self._get()
        elif opts.set:
            if opts.min_length is None:
                _exit_if_errors(["--min-length is required when using --set flag"])
            if opts.min_length <= 0:
                _exit_if_errors(["--min-length has to be greater than 0"])
            self._set(opts)

    def _get(self):
        policy, errors = self.rest.get_password_policy()
        _exit_if_errors(errors)
        print(json.dumps(policy, sort_keys=True, indent=2))

    def _set(self, opts):
        _, errors = self.rest.set_password_policy(opts.min_length, opts.upper_case, opts.lower_case, opts.digit,
                                                  opts.special_char)
        _exit_if_errors(errors)
        _success("Password policy updated")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-password-policy")

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
        group.add_argument("--disable-www-authenticate", dest="disable_www_authenticate",
                           metavar="<0|1>", choices=['0', '1'], default=None,
                           help="Disables use of WWW-Authenticate (0 or 1")
        group.add_argument(
            "--cluster-encryption-level",
            dest="cluster_encryption_level",
            metavar="<all|control|strict>",
            choices=[
                'all',
                'control',
                'strict'],
            default=None,
            help="Set cluster encryption level, only used when cluster encryption enabled.")
        group.add_argument('--tls-min-version', dest='tls_min_version', default=None,
                           help='Set the minimum TLS version')
        group.add_argument('--tls-honor-cipher-order', dest='tls_honor_cipher_order', metavar='<1|0>',
                           choices=['1', '0'], help='Specify or not the cipher order has to be followed.', default=None)
        group.add_argument('--cipher-suites', metavar='<ciphers>', default=None,
                           help='Comma separated list of ciphers to use.If an empty string (e.g "") given it will'
                                ' reset ciphers to default.')
        group.add_argument('--hsts-max-age', dest='hsts_max_age', metavar='<seconds>', type=int,
                           help='Sets the max-ages directive the server uses in the Strict-Transport-Security header',
                           default=None)
        group.add_argument("--hsts-preload-enabled", dest="hsts_preload",
                           metavar="<0|1>", choices=['0', '1'], default=None,
                           help="Enable the preloadDirectives directive the server uses in the"
                                " Strict-Transport-Security header")
        group.add_argument("--hsts-include-sub-domains-enabled", dest="hsts_include_sub_domains",
                           metavar="<0|1>", choices=['0', '1'], default=None,
                           help="Enable the includeSubDomains directive the server uses in the"
                                " Strict-Transport-Security header")

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        if sum([opts.get, opts.set]) != 1:
            _exit_if_errors(['Provided either --set or --get.'])

        if opts.get:
            val, err = self.rest.get_security_settings()
            _exit_if_errors(err)
            print(json.dumps(val))
        elif opts.set:
            self._set(self.rest, opts.disable_http_ui, opts.cluster_encryption_level, opts.tls_min_version,
                      opts.tls_honor_cipher_order, opts.cipher_suites, opts.disable_www_authenticate,
                      opts.hsts_max_age, opts.hsts_preload, opts.hsts_include_sub_domains)

    @staticmethod
    def _set(rest, disable_http_ui, encryption_level, tls_min_version, honor_order, cipher_suites,
             disable_www_authenticate, hsts_max_age, hsts_preload, hsts_include_sub_domains):
        if not any([True if x is not None else False for x in [disable_http_ui, encryption_level, tls_min_version,
                                                               honor_order, cipher_suites, disable_www_authenticate,
                                                               hsts_max_age, hsts_preload, hsts_include_sub_domains]]):
            _exit_if_errors(['please provide at least one of --cluster-encryption-level, --disable-http-ui,'
                             ' --tls-min-version, --tls-honor-cipher-order, --cipher-suites, --hsts-max-age,'
                             ' --hsts-preload-enabled or --hsts-includeSubDomains-enabled together with --set'])

        if disable_http_ui == '1':
            disable_http_ui = 'true'
        elif disable_http_ui == '0':
            disable_http_ui = 'false'

        if disable_www_authenticate == '1':
            disable_www_authenticate = 'true'
        elif disable_www_authenticate == '0':
            disable_www_authenticate = 'false'

        if honor_order == '1':
            honor_order = 'true'
        elif honor_order == '0':
            honor_order = 'false'

        if cipher_suites == '':
            cipher_suites = json.dumps([])
        elif cipher_suites is not None:
            cipher_suites = json.dumps(cipher_suites.split(','))

        if hsts_preload == '1':
            hsts_preload = True
        elif hsts_preload == '0':
            hsts_preload = False

        if hsts_include_sub_domains == '1':
            hsts_include_sub_domains = True
        elif hsts_include_sub_domains == '0':
            hsts_include_sub_domains = False

        _, errors = rest.set_security_settings(disable_http_ui, encryption_level, tls_min_version, honor_order,
                                               cipher_suites, disable_www_authenticate, hsts_max_age, hsts_preload,
                                               hsts_include_sub_domains)
        _exit_if_errors(errors)
        _success("Security settings updated")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-security")

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
                           help="Document body size threshold (bytes) to trigger optimistic "
                           + "replication")
        group.add_argument("--source-nozzle-per-node", dest="src_nozzles", metavar="<num>",
                           type=(int),
                           help="The number of source nozzles per source node (1 to 10)")
        group.add_argument("--target-nozzle-per-node", dest="dst_nozzles", metavar="<num>",
                           type=(int),
                           help="The number of outgoing nozzles per target node (1 to 10)")
        group.add_argument("--bandwidth-usage-limit", dest="usage_limit", type=(int),
                           metavar="<num>", help="The bandwidth usage limit in MiB/Sec")
        group.add_argument("--enable-compression", dest="compression", metavar="<1|0>", choices=["1", "0"],
                           help="Enable/disable compression")
        group.add_argument("--log-level", dest="log_level", metavar="<level>",
                           choices=["Error", "Info", "Debug", "Trace"],
                           help="The XDCR log level")
        group.add_argument("--stats-interval", dest="stats_interval", metavar="<ms>",
                           help="The interval for statistics updates (in milliseconds)")
        group.add_argument('--max-processes', dest='max_proc', metavar="<num>", type=int,
                           help='Number of processes to be allocated to XDCR. The default is 4.')

    @rest_initialiser(version_check=True, cluster_init_check=True, enterprise_check=False)
    def execute(self, opts):
        if not self.enterprise and opts.compression:
            _exit_if_errors(["--enable-compression can only be configured on enterprise edition"])

        if opts.compression == "0":
            opts.compression = "None"
        elif opts.compression == "1":
            opts.compression = "Auto"

        _, errors = self.rest.xdcr_global_settings(opts.chk_int, opts.worker_batch_size, opts.doc_batch_size,
                                                   opts.fail_interval, opts.rep_thresh, opts.src_nozzles,
                                                   opts.dst_nozzles, opts.usage_limit, opts.compression, opts.log_level,
                                                   opts.stats_interval, opts.max_proc)
        _exit_if_errors(errors)

        _success("Global XDCR settings updated")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-xdcr")

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

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        if opts.new_password is not None:
            _, errors = self.rest.set_master_pwd(opts.new_password)
            _exit_if_errors(errors)
            _success("New master password set")
        elif opts.rotate_data_key:
            _, errors = self.rest.rotate_master_pwd()
            _exit_if_errors(errors)
            _success("Data key rotated")
        else:
            _exit_if_errors(["No parameters set"])

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-master-password")

    @staticmethod
    def get_description():
        return "Changing the settings of the master password"


class SslManage(Subcommand):
    """The user manage subcommand"""

    def __init__(self):
        super(SslManage, self).__init__()
        self.parser.prog = "couchbase-cli ssl-manage"
        group = self.parser.add_argument_group("SSL manage options")
        me_group = group.add_mutually_exclusive_group(required=True)
        me_group.add_argument("--cluster-cert-info", dest="cluster_cert", action="store_true",
                              default=False, help="Gets the cluster certificate(s)")
        me_group.add_argument("--cluster-ca-info", dest="cluster_ca", action="store_true",
                              default=False, help="Displays the Certificate Authorities the cluster is using")
        me_group.add_argument("--cluster-ca-load", dest="load_ca", action="store_true",
                              default=False, help="Loads the Certificate Authorities")
        me_group.add_argument("--cluster-ca-delete", dest="delete_ca", metavar="<id>",
                              help="Deletes a Certificate Authority")
        me_group.add_argument("--upload-cluster-ca", dest="upload_cert", metavar="<path>",
                              help="Upload a new cluster certificate", action=CBDeprecatedAction)
        me_group.add_argument("--node-cert-info", dest="node_cert", action="store_true",
                              default=False, help="Gets the node certificate")
        me_group.add_argument("--regenerate-cert", dest="regenerate", metavar="<path>",
                              help="Regenerate the cluster certificate and save it to a file")
        me_group.add_argument("--set-node-certificate", dest="set_cert", action="store_true",
                              default=False, help="Sets the node certificate")
        group.add_argument("--pkey-passphrase-settings", dest="pkey_settings", metavar="<path>",
                           help="Optional path to a JSON file containing private key passphrase settings")
        me_group.add_argument("--set-client-auth", dest="client_auth_path", metavar="<path>",
                              help="A path to a file containing the client auth configuration")
        me_group.add_argument("--client-auth", dest="show_client_auth", action="store_true",
                              help="Show ssl client certificate authentication value")

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        if opts.regenerate is not None:
            try:
                open(opts.regenerate, 'a', encoding="utf-8").close()
            except IOError:
                _exit_if_errors([f'Unable to create file at `{opts.regenerate}`'])
            certificate, errors = self.rest.regenerate_cluster_certificate()
            _exit_if_errors(errors)
            _exit_on_file_write_failure(opts.regenerate, certificate)
            _success(f'Certificate regenerate and copied to `{opts.regenerate}`')
        elif opts.cluster_cert:
            certificates, errors = self.rest.retrieve_cluster_certificates()
            _exit_if_errors(errors)
            if opts.output == 'standard':
                print("The certificate being used by the cluster")
                print(f'{"Node":21.21} {"Expires":24} {"Type":9} Subject')
                for cert in certificates:
                    print(f'{cert["node"]:21.21} {cert["expires"]:24} {cert["type"]:9} {cert["subject"]}')
                    if 'warnings' in cert:
                        for warning in cert["warnings"]:
                            print(f'{" ":4} Warning: {warning["message"]}')
            else:
                print(json.dumps(certificates, sort_keys=True, indent=2))
        elif opts.cluster_ca:
            cert_authorities, errors = self.rest.retrieve_cluster_ca()
            _exit_if_errors(errors)

            if opts.output != 'standard':
                print(json.dumps(cert_authorities, sort_keys=True, indent=2))
                return

            print("The Certificate Authorities being used by the cluster")
            print(f'{"ID":4} {"NotBefore":24} {"NotAfter":24} {"Loaded Date":24} {"Type":9} Subject')

            def val_or_unknown(ca, key):
                return ca[key] if key in ca else "unknown"  # Must be less than 9 characters

            for ca in cert_authorities:
                print(f'{ca["id"]:<4} '
                      f'{val_or_unknown(ca, "notBefore"):24} '
                      f'{val_or_unknown(ca, "notAfter"):24} '
                      f'{val_or_unknown(ca, "loadTimestamp"):24} '
                      f'{val_or_unknown(ca, "type"):9} '
                      f'{val_or_unknown(ca, "subject")}')

                if 'warnings' in ca:
                    for warning in ca["warnings"]:
                        print(f'{" ":4} Warning: {warning["message"]}')

        elif opts.load_ca:
            nodes_data, errors = self.rest.pools('nodes')
            if errors and errors[0] == 'unknown pool':
                hostnames = [self.rest.hostname]
            else:
                _exit_if_errors(errors)
                scheme = 'http'
                if opts.ssl:
                    scheme = 'https'
                hostnames = [f'{scheme}://{n["hostname"]}' for n in nodes_data['nodes']]
            loaded_none = True
            for hostname in hostnames:
                _, errors = self.rest.load_cluster_ca(hostname)
                if not errors:
                    loaded_none = False
                    print(f'{hostname}: Successfully load CA from inbox/CA')
                # If a CA is not loaded ns_server returns a error, this error is handled and the next node is tried
                elif "Couldn't load CA certificate" in errors[0]:
                    break
                else:
                    _exit_if_errors(errors)
            if loaded_none:
                _exit_if_errors(['Could not load CA from any nodes, please place the CA placed at ./inbox/CA'])
        elif opts.delete_ca:
            _, errors = self.rest.delete_cluster_ca(opts.delete_ca)
            _exit_if_errors(errors)
            _success(f'Certificate Authority with ID {opts.delete_ca} has been deleted')
        elif opts.node_cert:
            host = urllib.parse.urlparse(opts.cluster).netloc
            cert, errors = self.rest.retrieve_node_certificate(host)
            _exit_if_errors(errors)
            if opts.output == 'standard':
                print(f'The certificate being used by node {host}')
                print(f'{"Expires":24} {"Type":9} Subject')
                print(f'{cert["expires"]:24} {cert["type"]:9} {cert["subject"]}')
                if 'warnings' in cert:
                    for warning in cert["warnings"]:
                        print(f'{" ":4} Warning: {warning["message"]}')
            else:
                print(json.dumps(cert, sort_keys=True, indent=2))
        elif opts.upload_cert:
            certificate = _exit_on_file_read_failure(opts.upload_cert)
            _, errors = self.rest.upload_cluster_certificate(certificate)
            _exit_if_errors(errors)
            _success(f'Uploaded cluster certificate to {opts.cluster}')
        elif opts.set_cert:
            _, errors = self.rest.set_node_certificate(_read_json_file_if_provided(opts.pkey_settings))
            _exit_if_errors(errors)
            _success("Node certificate set")
        elif opts.client_auth_path:
            data = _exit_on_file_read_failure(opts.client_auth_path)
            try:
                config = json.loads(data)
            except ValueError as e:
                _exit_if_errors([f'Client auth config does not contain valid json: {e}'])
            _, errors = self.rest.set_client_cert_auth(config)
            _exit_if_errors(errors)
            _success("SSL client auth updated")
        elif opts.show_client_auth:
            result, errors = self.rest.retrieve_client_cert_auth()
            _exit_if_errors(errors)
            print(json.dumps(result, sort_keys=True, indent=2))

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-ssl-manage")

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
        group.add_argument("--lock", dest="lock", action="store_true", default=False,
                           help="Lock an RBAC user")
        group.add_argument("--unlock", dest="unlock", action="store_true", default=False,
                           help="Unlock an RBAC user")
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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        num_selectors = sum([opts.delete, opts.list, opts.my_roles, opts.set, opts.get, opts.get_group,
                             opts.list_group, opts.delete_group, opts.set_group, opts.lock, opts.unlock])
        if num_selectors != 1:
            _exit_if_errors([
                'Exactly one of the following must be specified:--delete, --list, --my-roles, --set, --get,'
                ' --get-group, --set-group, --list-groups, --delete-group, --lock or --unlock'])

        if opts.delete:
            self._delete(opts)
        elif opts.list:
            self._list(opts)
        elif opts.my_roles:
            self._my_roles(opts)
        elif opts.set:
            self._set(opts)
        elif opts.get:
            self._get(opts)
        elif opts.get_group:
            self._get_group(opts)
        elif opts.set_group:
            self._set_group(opts)
        elif opts.list_group:
            self._list_groups()
        elif opts.delete_group:
            self._delete_group(opts)
        elif opts.lock or opts.unlock:
            self._lock_unlock(opts)

    def _delete_group(self, opts):
        if opts.group is None:
            _exit_if_errors(['--group-name is required with the --delete-group option'])

        _, errors = self.rest.delete_user_group(opts.group)
        _exit_if_errors(errors)
        _success(f"Group '{opts.group}' was deleted")

    def _get_group(self, opts):
        if opts.group is None:
            _exit_if_errors(['--group-name is required with the --get-group option'])

        group, errors = self.rest.get_user_group(opts.group)
        _exit_if_errors(errors)
        print(json.dumps(group, indent=2))

    def _set_group(self, opts):
        if opts.group is None:
            _exit_if_errors(['--group-name is required with --set-group'])

        _, errors = self.rest.set_user_group(opts.group, opts.roles, opts.description, opts.ldap_ref)
        _exit_if_errors(errors)
        _success(f"Group '{opts.group}' set")

    def _list_groups(self):
        groups, errors = self.rest.list_user_groups()
        _exit_if_errors(errors)
        print(json.dumps(groups, indent=2))

    def _delete(self, opts):
        if opts.rbac_user is None:
            _exit_if_errors(["--rbac-username is required with the --delete option"])
        if opts.rbac_pass is not None:
            _warning("--rbac-password is not used with the --delete option")
        if opts.rbac_name is not None:
            _warning("--rbac-name is not used with the --delete option")
        if opts.roles is not None:
            _warning("--roles is not used with the --delete option")
        if opts.auth_domain is None:
            _exit_if_errors(["--auth-domain is required with the --delete option"])

        _, errors = self.rest.delete_rbac_user(opts.rbac_user, opts.auth_domain)
        _exit_if_errors(errors)
        _success(f"User '{opts.rbac_user}' was removed")

    def _list(self, opts):
        if opts.rbac_user is not None:
            _warning(["--rbac-username is not used with the --list option"])
        if opts.rbac_pass is not None:
            _warning(["--rbac-password is not used with the --list option"])
        if opts.rbac_name is not None:
            _warning("--rbac-name is not used with the --list option")
        if opts.roles is not None:
            _warning("--roles is not used with the --list option")
        if opts.auth_domain is not None:
            _warning("--auth-domain is not used with the --list option")

        result, errors = self.rest.list_rbac_users()
        _exit_if_errors(errors)
        print(json.dumps(result, indent=2))

    def _get(self, opts):
        if opts.rbac_user is None:
            _exit_if_errors(["--rbac-username is required with the --get option"])
        if opts.rbac_pass is not None:
            _warning("--rbac-password is not used with the --get option")
        if opts.rbac_name is not None:
            _warning("--rbac-name is not used with the --get option")
        if opts.roles is not None:
            _warning("--roles is not used with the --get option")
        if opts.auth_domain is not None:
            _warning("--auth-domain is not used with the --get option")

        result, errors = self.rest.list_rbac_users()
        _exit_if_errors(errors)
        user = [u for u in result if u['id'] == opts.rbac_user]

        if len(user) != 0:
            print(json.dumps(user, indent=2))
        else:
            _exit_if_errors([f'no user {opts.rbac_user}'])

    def _my_roles(self, opts):
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

        result, errors = self.rest.my_roles()
        _exit_if_errors(errors)
        print(json.dumps(result, indent=2))

    def _set(self, opts):
        if opts.rbac_user is None:
            _exit_if_errors(["--rbac-username is required with the --set option"])
        if opts.rbac_pass is not None and opts.auth_domain == "external":
            _warning("--rbac-password cannot be used with the external auth domain")
            opts.rbac_pass = None
        if opts.auth_domain is None:
            _exit_if_errors(["--auth-domain is required with the --set option"])

        _, errors = self.rest.set_rbac_user(opts.rbac_user, opts.rbac_pass, opts.rbac_name, opts.roles,
                                            opts.auth_domain, opts.groups)
        _exit_if_errors(errors)

        if opts.roles is not None and "query_external_access" in opts.roles:
            _warning('Granting the query_external_access role permits execution of the N1QL '
                     'function CURL() and may allow access to other network endpoints in the local network and'
                     'the Internet.')

        _success(f"User {opts.rbac_user} set")

    def _lock_unlock(self, opts):
        if opts.rbac_user is None:
            _exit_if_errors(["--rbac-username is required when using the --lock/--unlock option"])

        _, errors = self.rest.lock_rbac_user(opts.rbac_user, opts.lock)
        _exit_if_errors(errors)

        _success(f"User {opts.rbac_user} {'locked' if opts.lock else 'unlocked'}")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-user-manage")

    @staticmethod
    def get_description():
        return "Manage RBAC users"


class XdcrReplicate(Subcommand):
    """The xdcr replicate subcommand"""

    def __init__(self):
        super(XdcrReplicate, self).__init__()
        self.parser.prog = "couchbase-cli xdcr-replicate"
        group = self.parser.add_argument_group("XDCR replicate options")
        group.add_argument("--get", action="store_true", help="Retrieve the settings of a XDCR replication.")
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
                           help="Document body size threshold to trigger optimistic replication"
                           + " (bytes)")
        group.add_argument("--source-nozzle-per-node", dest="src_nozzles", type=(int),
                           metavar="<num>",
                           help="The number of source nozzles per source node (1 to 10)")
        group.add_argument("--target-nozzle-per-node", dest="dst_nozzles", type=(int),
                           metavar="<num>",
                           help="The number of outgoing nozzles per target node (1 to 10)")
        group.add_argument("--bandwidth-usage-limit", dest="usage_limit", type=(int),
                           metavar="<num>", help="The bandwidth usage limit in MiB/Sec")
        group.add_argument("--enable-compression", dest="compression", metavar="<1|0>", choices=["1", "0"],
                           help="Enable/disable compression")
        group.add_argument("--log-level", dest="log_level", metavar="<level>",
                           choices=["Error", "Warn", "Info", "Debug", "Trace"],
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
        group.add_argument('--filter-binary', choices=['1', '0'], metavar='<1|0>', default=None, dest='filter_binary',
                           help='When set to true binary documents are not replicated. When false binary documents may '
                                'be replicated')

        collection_group = self.parser.add_argument_group("Collection options")
        collection_group.add_argument('--collection-explicit-mappings', choices=['1', '0'], metavar='<1|0>',
                                      default=None, help='If explicit collection mappings is to be used. '
                                                         '(Enterprise Edition Only)')
        collection_group.add_argument('--collection-migration', choices=['1', '0'], metavar='<1|0>',
                                      default=None, help='If XDCR is to run in collection migration mode. '
                                                         '(Enterprise Edition only)')
        collection_group.add_argument('--collection-mapping-rules', type=str, default=None, metavar='<mappings>',
                                      help='The mapping rules specified as a JSON formatted string. '
                                           '(Enterprise Edition Only)')

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=False)
    def execute(self, opts):
        if not self.enterprise and opts.compression:
            _exit_if_errors(["--enable-compression can only be configured on enterprise edition"])
        if not self.enterprise and (opts.collection_migration or opts.collection_explicit_mappings is not None
                                    or opts.collection_mapping_rules is not None):
            _exit_if_errors(["[--collection-migration, --collection-explicit-mappings, --collection-mapping-rules] can"
                             " only be configured on enterprise edition"])

        if opts.compression == "0":
            opts.compression = "None"
        elif opts.compression == "1":
            opts.compression = "Auto"

        actions = sum([opts.create, opts.delete, opts.pause, opts.list, opts.resume, opts.settings, opts.get])
        if actions == 0:
            _exit_if_errors(['Must specify one of --create, --delete, --pause, --list, --resume, --settings, --get'])
        elif actions > 1:
            _exit_if_errors(['The --create, --delete, --pause, --list, --resume, --settings, --get flags may not be '
                             'specified at the same time'])
        elif opts.create:
            self._create(opts)
        elif opts.delete:
            self._delete(opts)
        elif opts.pause or opts.resume:
            self._pause_resume(opts)
        elif opts.list:
            self._list()
        elif opts.settings:
            self._settings(opts)
        elif opts.get:
            self._get(opts)

    def _get(self, opts):
        if opts.replicator_id is None:
            _exit_if_errors(["--xdcr-replicator is need to get the replicator settings"])

        settings, errors = self.rest.get_xdcr_replicator_settings(opts.replicator_id)
        _exit_if_errors(errors)
        print(json.dumps(settings, indent=4, sort_keys=True))

    def _create(self, opts):
        if opts.collection_migration == '1' and opts.collection_explicit_mappings == '1':
            _exit_if_errors(['cannot enable both collection migration and explicit mappings'])
        if opts.filter_skip and opts.filter is None:
            _exit_if_errors(["--filter-expersion is needed with the --filter-skip-restream option"])
        _, errors = self.rest.create_xdcr_replication(opts.cluster_name, opts.to_bucket, opts.from_bucket, opts.chk_int,
                                                      opts.worker_batch_size, opts.doc_batch_size, opts.fail_interval,
                                                      opts.rep_thresh, opts.src_nozzles, opts.dst_nozzles,
                                                      opts.usage_limit, opts.compression, opts.log_level,
                                                      opts.stats_interval, opts.filter, opts.priority,
                                                      opts.reset_expiry, opts.filter_del, opts.filter_exp,
                                                      opts.filter_binary, opts.collection_explicit_mappings,
                                                      opts.collection_migration, opts.collection_mapping_rules)
        _exit_if_errors(errors)

        _success("XDCR replication created")

    def _delete(self, opts):
        if opts.replicator_id is None:
            _exit_if_errors(["--xdcr-replicator is needed to delete a replication"])

        _, errors = self.rest.delete_xdcr_replicator(opts.replicator_id)
        _exit_if_errors(errors)

        _success("XDCR replication deleted")

    def _pause_resume(self, opts):
        if opts.replicator_id is None:
            _exit_if_errors(["--xdcr-replicator is needed to pause or resume a replication"])

        tasks, errors = self.rest.get_tasks()
        _exit_if_errors(errors)
        for task in tasks:
            if task["type"] == "xdcr" and task["id"] == opts.replicator_id:
                if opts.pause and task["status"] == "notRunning":
                    _exit_if_errors(["The replication is not running yet. Pause is not needed"])
                if opts.resume and task["status"] == "running":
                    _exit_if_errors(["The replication is running already. Resume is not needed"])
                break

        if opts.pause:
            _, errors = self.rest.pause_xdcr_replication(opts.replicator_id)
            _exit_if_errors(errors)
            _success("XDCR replication paused")
        elif opts.resume:
            _, errors = self.rest.resume_xdcr_replication(opts.replicator_id)
            _exit_if_errors(errors)
            _success("XDCR replication resume")

    def _list(self):
        tasks, errors = self.rest.get_tasks()
        _exit_if_errors(errors)
        for task in tasks:
            if task["type"] == "xdcr":
                print(f'stream id: {task["id"]}')
                print(f'   status: {task["status"]}')
                print(f'   source: {task["source"]}')
                print(f'   target: {task["target"]}')
                if "filterExpression" in task and task["filterExpression"] != "":
                    print(f'   filter: {task["filterExpression"]}')

    def _settings(self, opts):
        if opts.replicator_id is None:
            _exit_if_errors(["--xdcr-replicator is needed to change a replicators settings"])
        if opts.filter_skip and opts.filter is None:
            _exit_if_errors(["--filter-expersion is needed with the --filter-skip-restream option"])
        if opts.collection_migration == '1' and opts.collection_explicit_mappings == '1':
            _exit_if_errors(['cannot enable both collection migration and explicit mappings'])
        _, errors = self.rest.xdcr_replicator_settings(opts.chk_int, opts.worker_batch_size, opts.doc_batch_size,
                                                       opts.fail_interval, opts.rep_thresh, opts.src_nozzles,
                                                       opts.dst_nozzles, opts.usage_limit, opts.compression,
                                                       opts.log_level, opts.stats_interval, opts.replicator_id,
                                                       opts.filter, opts.filter_skip, opts.priority, opts.reset_expiry,
                                                       opts.filter_del, opts.filter_exp, opts.filter_binary,
                                                       opts.collection_explicit_mappings, opts.collection_migration,
                                                       opts.collection_mapping_rules)
        _exit_if_errors(errors)

        _success("XDCR replicator settings updated")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-xdcr-replicate")

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
        group.add_argument("--xdcr-hostname-external", dest="hostname_external", action="store_true",
                           help="The hostname of the remote cluster is external")
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

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        actions = sum([opts.create, opts.delete, opts.edit, opts.list])
        if actions == 0:
            _exit_if_errors(["Must specify one of --create, --delete, --edit, --list"])
        elif actions > 1:
            _exit_if_errors(["The --create, --delete, --edit, --list flags may not be specified at the same time"])
        elif opts.create or opts.edit:
            self._set(opts)
        elif opts.delete:
            self._delete(opts)
        elif opts.list:
            self._list()

    def _set(self, opts):
        cmd = "create"
        if opts.edit:
            cmd = "edit"

        if opts.name is None:
            _exit_if_errors([f'--xdcr-cluster-name is required to {cmd} a cluster connection'])
        if opts.hostname is None:
            _exit_if_errors([f'--xdcr-hostname is required to {cmd} a cluster connections'])
        if opts.username is None:
            _exit_if_errors([f'--xdcr-username is required to {cmd} a cluster connections'])
        if opts.password is None:
            _exit_if_errors([f'--xdcr-password is required to {cmd} a cluster connections'])
        if (opts.encrypt is not None or opts.encryption_type is not None) and opts.secure_connection is not None:
            _exit_if_errors(["Cannot use deprecated flags --xdcr-demand-encryption or --xdcr-encryption-type with"
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
            if opts.encryption_type is None:
                opts.encryption_type = "full"

            if opts.encryption_type == "full":
                capella = 'cloud.couchbase.com' in opts.hostname
                if opts.certificate is None and not capella:
                    _exit_if_errors(["certificate required if encryption is demanded"])

                if opts.certificate:
                    raw_cert = _exit_on_file_read_failure(opts.certificate)

        raw_user_key = None
        if opts.r_key:
            raw_user_key = _exit_on_file_read_failure(opts.r_key)
        raw_user_cert = None
        if opts.r_certificate:
            raw_user_cert = _exit_on_file_read_failure(opts.r_certificate)

        if opts.create:
            _, errors = self.rest.create_xdcr_reference(opts.name, opts.hostname, opts.hostname_external,
                                                        opts.r_username, opts.r_password, opts.encrypt,
                                                        opts.encryption_type, raw_cert, raw_user_cert, raw_user_key)
            _exit_if_errors(errors)
            _success("Cluster reference created")
        else:
            _, errors = self.rest.edit_xdcr_reference(opts.name, opts.hostname, opts.hostname_external, opts.r_username,
                                                      opts.r_password, opts.encrypt, opts.encryption_type, raw_cert,
                                                      raw_user_cert, raw_user_key)
            _exit_if_errors(errors)
            _success("Cluster reference edited")

    def _delete(self, opts):
        if opts.name is None:
            _exit_if_errors(["--xdcr-cluster-name is required to deleta a cluster connection"])

        _, errors = self.rest.delete_xdcr_reference(opts.name)
        _exit_if_errors(errors)

        _success("Cluster reference deleted")

    def _list(self):
        clusters, errors = self.rest.list_xdcr_references()
        _exit_if_errors(errors)

        for cluster in clusters:
            if not cluster["deleted"]:
                print(f'cluster name: {cluster["name"]}')
                print(f'        uuid: {cluster["uuid"]}')
                print(f'   host name: {cluster["hostname"]}')
                print(f'   user name: {cluster["username"]}')
                print(f'         uri: {cluster["uri"]}')

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-xdcr-setup")

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
                           help="Set the function deployment boundary (Deprecated)")
        group.add_argument("--name", dest="name", metavar="<name>",
                           default=False, help="The name of the function to take an action on")
        group.add_argument("--bucket", dest="bucket", metavar="<bucket>",
                           default="", help="The bucket to which the function to take an action against belongs")
        group.add_argument("--scope", dest="scope", metavar="<scope>",
                           default="", help="The scope to which the function to take an action against belongs")
        group.add_argument("--file", dest="filename", metavar="<file>",
                           default=False, help="The file to export and import function(s) to and from")
        group.add_argument("--pause", dest="pause", action="store_true", help="Pause a function")
        group.add_argument("--resume", dest="resume", action="store_true", help="Resume a function")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        # pylint: disable=protected-access
        actions = sum([opts._import, opts.export, opts.export_all, opts.delete, opts.list, opts.deploy, opts.undeploy,
                       opts.pause, opts.resume])
        if actions == 0:
            _exit_if_errors(["Must specify one of --import, --export, --export-all, --delete, --list, --deploy,"
                             " --undeploy, --pause, --resume"])
        elif actions > 1:
            _exit_if_errors(['The --import, --export, --export-all, --delete, --list, --deploy, --undeploy, --pause, '
                             '--resume flags may not be specified at the same time'])
        elif opts._import:  # pylint: disable=protected-access
            self._import(opts)
        elif opts.export:
            self._export(opts)
        elif opts.export_all:
            self._export_all(opts)
        elif opts.delete:
            self._delete(opts)
        elif opts.list:
            self._list(opts)
        elif opts.deploy:
            self._deploy_undeploy(opts, True)
        elif opts.undeploy:
            self._deploy_undeploy(opts, False)
        elif opts.pause:
            self._pause_resume(opts, True)
        elif opts.resume:
            self._pause_resume(opts, False)

    def _pause_resume(self, opts, pause):
        if not opts.name:
            _exit_if_errors([f"Flag --name is required with the {'--pause' if pause else '--resume'} flag"])
        self._check_bucket_and_scope(opts)
        _, err = self.rest.pause_resume_function(opts.name, opts.bucket, opts.scope, pause)
        self._exit_if_function_not_found(opts, err)
        _exit_if_errors(err)
        _success(f"Function was {'paused' if pause else 'resumed'}")

    def _import(self, opts):
        # Ensure users don't try to supply the bucket/scope since it's not supported for this function
        self._check_bucket_and_scope(opts, supported=False)

        if not opts.filename:
            _exit_if_errors(["--file is needed to import functions"])
        import_functions = _exit_on_file_read_failure(opts.filename)
        import_functions = json.loads(import_functions)
        _, errors = self.rest.import_functions(import_functions)
        _exit_if_errors(errors)
        _success("Events imported")

    def _export(self, opts):
        if not opts.filename:
            _exit_if_errors(["--file is needed to export a function"])

        if not opts.name:
            _exit_if_errors(["--name is needed to export a function"])

        self._check_bucket_and_scope(opts)

        functions, errors = self.rest.export_functions()
        _exit_if_errors(errors)

        # Filter the functions to only export the one requested by the user
        exported = self._filter_functions(functions, opts.bucket, opts.scope, opts.name)

        if len(exported) > 1:
            _exit_if_errors(['Unexpectedly found more than one function matching the name ' +
                             f'"{self._function_name(opts.bucket, opts.scope, opts.name)}"'])

        if len(exported) == 0:
            _exit_if_errors([self._function_not_found_message(opts)])

        _exit_on_file_write_failure(opts.filename, json.dumps(exported[-1:], separators=(',', ':')))
        _success("Function exported to: " + opts.filename)

    @classmethod
    def _function_not_found_message(cls, opts):
        function_name = cls._function_name(opts.bucket, opts.scope, opts.name)
        bucket_scope_warning = ''
        if opts.bucket == '' and opts.scope == '':
            bucket_scope_warning = ' - perhaps you need to specify --bucket and --scope'
        return f'Function "{function_name}" does not exist{bucket_scope_warning}'

    @classmethod
    def _exit_if_function_not_found(cls, opts, errors):
        if errors is None or len(errors) != 1:
            return

        err = errors[0]
        if isinstance(err, dict) and 'code' in err and err['code'] == couchbaseConstants.ERR_APP_NOT_FOUND_TS:
            _exit_if_errors([cls._function_not_found_message(opts)])

    @classmethod
    def _filter_functions(cls, functions, bucket, scope, name):
        return [fn for fn in functions if cls._should_export(fn, bucket, scope, name)]

    @classmethod
    def _should_export(cls, fn, bucket, scope, name):
        # Indicates whether this is a backwards compatible export e.g. unaware of collections
        bc = 'function_scope' not in fn or \
            fn['function_scope']['bucket'] == '*' and fn['function_scope']['scope'] == '*'

        if bucket == "" and scope == "":
            return bc and fn['appname'] == name

        return fn['function_scope']['bucket'] == bucket and \
            fn['function_scope']['scope'] == scope and \
            fn['appname'] == name

    @classmethod
    def _function_name(cls, bucket, scope, name):
        return (name if bucket == "" and scope == "" else f"{bucket}/{scope}/{name}")

    def _export_all(self, opts):
        # Ensure users don't try to supply the bucket/scope since it's not supported for this function
        self._check_bucket_and_scope(opts, supported=False)

        if not opts.filename:
            _exit_if_errors(["--file is needed to export all functions"])
        exported_functions, errors = self.rest.export_functions()
        _exit_if_errors(errors)
        _exit_on_file_write_failure(opts.filename, json.dumps(exported_functions, separators=(',', ':')))
        _success(f'All functions exported to: {opts.filename}')

    def _delete(self, opts):
        if not opts.name:
            _exit_if_errors(["--name is needed to delete a function"])
        self._check_bucket_and_scope(opts)
        _, errors = self.rest.delete_function(opts.name, opts.bucket, opts.scope)
        self._exit_if_function_not_found(opts, errors)
        _exit_if_errors(errors)
        _success("Request to delete the function was accepted")

    def _deploy_undeploy(self, opts, deploy):
        if not opts.name:
            _exit_if_errors([f"--name is needed to {'deploy' if deploy else 'undeploy'} a function"])
        if deploy:
            self._handle_boundary(opts)
        self._check_bucket_and_scope(opts)

        _, errors = self.rest.deploy_undeploy_function(opts.name, opts.bucket, opts.scope, deploy, opts.boundary)
        self._exit_if_function_not_found(opts, errors)
        # For backwards compatability we don't want to error if you try to (un)deploy an (un)deployed function
        if errors is not None and len(errors) == 1 and isinstance(errors[0], str) and \
                errors[0].endswith(f'already in {"deployed" if deploy else "undeployed"} state.'):
            _success(f'Function is already in {"deployed" if deploy else "undeployed"} state')
            return
        _exit_if_errors(errors)

        _success(f"Request to {'deploy' if deploy else 'undeploy'} the function was accepted")

    def _handle_boundary(self, opts):
        min_version, errors = self.rest.min_version()
        _exit_if_errors(errors)

        versionCheck = compare_versions(min_version, "6.6.2")
        deprecated = versionCheck != -1

        if not (deprecated or opts.boundary):
            _exit_if_errors(["--boundary is needed to deploy a function"])

        if deprecated and opts.boundary:
            opts.boundary = False  # The is the default value, it'll be ignored by the REST client
            _deprecated("The --boundary option is deprecated and will be ignored; the function definition itself now"
                        " defines the feed boundary")

    def _check_bucket_and_scope(self, opts, supported=True):
        # --bucket and --scope can only be supplied together
        # If neither bucket nor scope is supplied, the eventing service will assume collection-unaware defaults "*"
        if opts.bucket == "" and opts.scope != "" or opts.bucket != "" and opts.scope == "":
            _exit_if_errors(["You need to supply both --bucket and --scope, or neither for collection-unaware eventing "
                             "functions"])

        if opts.bucket == "" and opts.scope == "":
            return

        # sub-command does not support collection aware flags but they've been supplied
        if not supported:
            _exit_if_errors(["This operation does not support the --bucket/--scope flags"])

        min_version, errors = self.rest.min_version()
        _exit_if_errors(errors)

        result = compare_versions(min_version, "7.1.0")
        if result == -1:
            _exit_if_errors(["The --bucket/--scope flags can only be used against >= 7.1.0 clusters"])

    def _list(self, opts):
        # Ensure users don't try to supply the bucket/scope since it's not supported for this function
        self._check_bucket_and_scope(opts, supported=False)

        functions, errors = self.rest.list_functions()
        _exit_if_errors(errors)

        statuses, errors = self.rest.get_functions_status()
        _exit_if_errors(errors)

        if statuses['apps'] is not None:
            statuses = self._get_eventing_functions_paths_to_statuses_map(statuses['apps'])
            for function in functions:
                function_path = self._get_function_path_from_list_functions_endpoint(function)
                if function_path in statuses:
                    print(function_path)
                    print(f' Status: {statuses[function_path]}')
                    if 'source_scope' in function["depcfg"]:
                        print(f' Source: {function["depcfg"]["source_bucket"]}.{function["depcfg"]["source_scope"]}.'
                              f'{function["depcfg"]["source_collection"]}')
                        print(
                            f' Metadata: {function["depcfg"]["metadata_bucket"]}.{function["depcfg"]["metadata_scope"]}.'
                            f'{function["depcfg"]["metadata_collection"]}')
                    else:
                        print(f' Source Bucket: {function["depcfg"]["source_bucket"]}')
                        print(f' Metadata Bucket: {function["depcfg"]["metadata_bucket"]}')
        else:
            print('The cluster has no functions')

    def _get_eventing_functions_paths_to_statuses_map(self, apps_list):
        path_to_status_map = {}
        for function in apps_list:
            # Statuses of collection-aware functions should be mapped to their full paths (includes bucket and scope)
            path = self._get_function_path_from_functions_statuses_endpoint(function)
            path_to_status_map[path] = function['composite_status']
        return path_to_status_map

    def _get_function_path_from_list_functions_endpoint(self, function):
        """Gets full path of the given function retuned in a list from the /api/v1/functions endpoint"""
        if 'function_scope' in function and function['function_scope']['bucket'] != '*':
            return f"{function['function_scope']['bucket']}/{function['function_scope']['scope']}/{function['appname']}"
        return function['appname']

    def _get_function_path_from_functions_statuses_endpoint(self, function):
        """Gets full path of the given function retuned in a list from the /api/v1/status endpoint"""
        if 'function_scope' in function and function['function_scope']['bucket'] != '*':
            return f"{function['function_scope']['bucket']}/{function['function_scope']['scope']}/{function['name']}"
        return function['name']

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-eventing-function-setup")

    @staticmethod
    def get_description():
        return "Manage Eventing Service Functions"


class AnalyticsLinkSetup(Subcommand):
    """The analytics link setup subcommand"""

    def __init__(self):
        super(AnalyticsLinkSetup, self).__init__()
        self.parser.prog = "couchbase-cli analytics-link-setup"
        group = self.parser.add_argument_group("Analytics Service link setup options")
        group.add_argument("--create", dest="create", action="store_true",
                           default=False, help="Create a link")
        group.add_argument("--delete", dest="delete", action="store_true",
                           default=False, help="Delete a link")
        group.add_argument("--edit", dest="edit", action="store_true",
                           default=False, help="Modify a link")
        group.add_argument("--list", dest="list", action="store_true",
                           default=False, help="List all links")
        group.add_argument("--dataverse", dest="dataverse", metavar="<name>",
                           help="The dataverse of the link (Deprecated)")
        group.add_argument("--scope", dest="scope", metavar="<name>",
                           help="The analytics scope of the link in its canonical form")
        group.add_argument("--name", dest="name", metavar="<name>",
                           help="The name of the link")
        group.add_argument("--type", dest="type", metavar="<type>", choices=["couchbase", "s3", "azureblob",
                                                                             "azuredatalake", "gcs"],
                           help="The type of the link ('couchbase', 's3', 'azureblob', 'azuredatalake' or 'gcs')")

        group = self.parser.add_argument_group("Analytics Service Couchbase link setup options")
        group.add_argument("--hostname", dest="hostname", metavar="<hostname>",
                           help="The hostname of the link")
        group.add_argument("--link-username", dest="link_username", metavar="<username>",
                           help="The username of the link")
        group.add_argument("--link-password", dest="link_password", metavar="<password>",
                           help="The password of the link")
        group.add_argument("--user-certificate", dest="user_certificate", metavar="<path>",
                           help="The user certificate for authentication")
        group.add_argument("--user-key", dest="user_key", metavar="<path>",
                           help="The user key for authentication")
        group.add_argument("--user-key-passphrase", dest="user_key_passphrase", metavar="<path>",
                           help="Optional path to a JSON file containing user key passphrase settings")
        group.add_argument("--certificate", dest="certificates", metavar="<path>", action="append",
                           help="The certificate used for encryption of the link (supply one parameter for each"
                                " certificate)")
        group.add_argument("--encryption", dest="encryption", choices=["none", "full", "half"],
                           metavar="<type>",
                           help="The link encryption type ('none', 'full' or 'half')")

        group = self.parser.add_argument_group("Analytics Service S3 link setup options")
        group.add_argument("--access-key-id", dest="access_key_id", metavar="<id>",
                           help="The access key ID of the link")
        group.add_argument("--secret-access-key", dest="secret_access_key", metavar="<key>",
                           help="The secret access key of the link")
        group.add_argument("--session-token", dest="session_token", metavar="<token>",
                           help="Temporary credentials session token")
        group.add_argument("--region", dest="region", metavar="<region>",
                           help="The region of the link")
        group.add_argument("--service-endpoint", dest="service_endpoint", metavar="<url>",
                           help="The service endpoint of the link (optional)")

        group = self.parser.add_argument_group("Analytics Service Azure Blob and Azure Data Lake link setup options")
        group.add_argument("--account-name", dest="account_name", metavar="<id>",
                           help="The account name of the link")
        group.add_argument("--account-key", dest="account_key", metavar="<key>",
                           help="The account key of the link")
        group.add_argument("--shared-access-signature", dest="shared_access_signature", metavar="<token>",
                           help="The shared access signature of the link")
        group.add_argument("--managed-identity-id", dest="managed_identity_id", metavar="<id>",
                           help="The managed identity id of the link")
        group.add_argument("--client-id", dest="client_id", metavar="<id>",
                           help="The client id of the link")
        group.add_argument("--client-secret", dest="client_secret", metavar="<key>",
                           help="The client secret of the link")
        group.add_argument("--client-certificate", dest="client_certificate", metavar="<key>",
                           help="The client certificate of the link")
        group.add_argument("--client-certificate-password", dest="client_certificate_password", metavar="<key>",
                           help="The client certificate password of the link")
        group.add_argument("--tenant-id", dest="tenant_id", metavar="<id>",
                           help="The tenant id of the link")
        group.add_argument("--endpoint", dest="endpoint", metavar="<url>",
                           help="The blob endpoint of the link (required)")

        group = self.parser.add_argument_group("Analytics Service GCS link setup options")
        group.add_argument("--application-default-credentials", dest="application_default_credentials",
                           action="store_true",
                           help="The option to use application default credentials for authentication (optional)")
        group.add_argument("--json-credentials", dest="json_credentials", metavar="<key>",
                           help="The JSON credentials of the link (optional)")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        actions = sum([opts.create, opts.delete, opts.edit, opts.list])
        if actions == 0:
            _exit_if_errors(["Must specify one of --create, --delete, --edit, --list"])
        elif actions > 1:
            _exit_if_errors(["The --create, --delete, --edit, --list flags may not be specified at the same time"])
        if opts.dataverse:
            _deprecated("--dataverse is deprecated, please use --scope instead")
        if opts.dataverse and opts.scope:
            _exit_if_errors(['Only one of --dataverse and --scope is allowed'])
        if opts.create or opts.edit:
            self._set(opts)
        elif opts.delete:
            self._delete(opts)
        elif opts.list:
            self._list(opts)

    def _set(self, opts):
        cmd = "create"
        if opts.edit:
            cmd = "edit"

        if opts.dataverse is None and opts.scope is None:
            _exit_if_errors([f'--dataverse or --scope is required to {cmd} a link'])
        if opts.name is None:
            _exit_if_errors([f'--name is required to {cmd} a link'])
        if opts.create and opts.type is None:
            _exit_if_errors([f'--type is required to {cmd} a link'])

        if opts.type == 'azureblob' or opts.type == 'azuredatalake':
            self._verify_azure_options(opts)

        if opts.type == 'gcs':
            # --application-default-credentials and --json-credentials are not allowed to be passed together
            if opts.application_default_credentials and opts.json_credentials:
                _exit_if_errors(['Parameter --json-credentials is not allowed if --application-default-credentials '
                                 'is provided'])

        if opts.dataverse:
            opts.scope = opts.dataverse
        if opts.certificates:
            for index in range(len(opts.certificates)):
                opts.certificates[index] = _exit_on_file_read_failure(opts.certificates[index]).strip()
        if opts.user_key:
            opts.user_key = _exit_on_file_read_failure(opts.user_key)
        if opts.user_key_passphrase:
            opts.user_key_passphrase = _exit_on_file_read_failure(opts.user_key_passphrase)
        if opts.user_certificate:
            opts.user_certificate = _exit_on_file_read_failure(opts.user_certificate)

        if opts.create:
            _, errors = self.rest.create_analytics_link(opts)
            _exit_if_errors(errors)
            _success("Link created")
        else:
            _, errors = self.rest.edit_analytics_link(opts)
            _exit_if_errors(errors)
            _success("Link edited")

    def _delete(self, opts):
        if opts.dataverse is None and opts.scope is None:
            _exit_if_errors(['--dataverse or --scope is required to delete a link'])
        if opts.name is None:
            _exit_if_errors(['--name is required to delete a link'])
        if opts.dataverse:
            opts.scope = opts.dataverse

        _, errors = self.rest.delete_analytics_link(opts.scope, opts.name)
        _exit_if_errors(errors)
        _success("Link deleted")

    def _list(self, opts):
        if opts.dataverse:
            opts.scope = opts.dataverse
        clusters, errors = self.rest.list_analytics_links(opts.scope, opts.name, opts.type)
        _exit_if_errors(errors)
        print(json.dumps(clusters, sort_keys=True, indent=2))

    @staticmethod
    def _verify_azure_options(opts):
        # --endpoint is required
        if opts.endpoint is None:
            return ['Required parameter --endpoint not supplied']

        # --account-name and --account-key need to be provided together
        if opts.account_name and opts.account_key is None:
            return ['Parameter --account-key is required if --account-name is provided']
        if opts.account_name is None and opts.account_key:
            return ['Parameter --account-name is required if --account-key is provided']

        # No other authentication method is allowed with --account-name and --account-key
        if opts.account_name and opts.account_key:
            if opts.shared_access_signature:
                return ['Parameter --shared--access-signature is not allowed if --account-key is provided']
            if opts.managed_identity_id:
                return ['Parameter --managed-identity-id is not allowed if --account-key is provided']
            if opts.client_id:
                return ['Parameter --client-id is not allowed if --account-key is provided']
            if opts.client_secret:
                return ['Parameter --client-secret is not allowed if --account-key is provided']
            if opts.client_certificate:
                return ['Parameter --client-certificate is not allowed if --account-key is provided']
            if opts.client_certificate_password:
                return ['Parameter --client-certificate-password is not allowed if --account-key is provided']
            if opts.tenant_id:
                return ['Parameter --tenant-id is not allowed if --account-key is provided']

        # No other authentication method is allowed with --shared-access-signature
        if opts.shared_access_signature:
            if opts.managed_identity_id:
                return ['Parameter --managed-identity-id is not allowed if --shared-access-signature is provided']
            if opts.client_id:
                return ['Parameter --client-id is not allowed if --shared-access-signature is provided']
            if opts.client_secret:
                return ['Parameter --client-secret is not allowed if --shared-access-signature is provided']
            if opts.client_certificate:
                return ['Parameter --client-certificate is not allowed if --shared-access-signature is provided']
            if opts.client_certificate_password:
                return ['Parameter --client-certificate-password is not allowed if '
                        '--shared-access-signature is provided']
            if opts.tenant_id:
                return ['Parameter --tenant-id is not allowed if --shared-access-signature is provided']

        # No other authentication method is allowed with --managed-identity-id
        if opts.managed_identity_id:
            if opts.client_id:
                return ['Parameter --client-id is not allowed if --managed-identity-id is provided']
            if opts.client_secret:
                return ['Parameter --client-secret is not allowed if --managed-identity-id is provided']
            if opts.client_certificate:
                return ['Parameter --client-certificate is not allowed if --managed-identity-id is provided']
            if opts.client_certificate_password:
                return ['Parameter --client-certificate-password is not allowed if --managed-identity-id is provided']
            if opts.tenant_id:
                return ['Parameter --tenant-id is not allowed if --managed-identity-id is provided']

        # --client-secret, --client-certificate, --client-certificate-password and --tenant-id not allowed if
        # --client-id is not present
        if opts.client_id is None:
            if opts.client_secret:
                return ['Parameter --client-id is required if --client-secret is provided']
            if opts.client_certificate:
                return ['Parameter --client-id is required if --client-certificate is provided']
            if opts.client_certificate_password:
                return ['Parameter --client-id is required if --client-certificate-password is provided']
            if opts.tenant_id:
                return ['Parameter --client-id is required if --tenant-id is provided']

        # --client-secret and --client-certificate are not allowed to be passed together
        if opts.client_id:
            if opts.tenant_id is None:
                return ['Required parameter --tenant-id not supplied']
            if opts.client_secret is None and opts.client_certificate is None:
                return ['Parameter --client-secret or --client-certificate is required if --client-id is provided']
            if opts.client_secret and opts.client_certificate:
                return ['The parameters --client-secret and --client-certificate cannot be provided at the same time']
            if opts.client_secret and opts.client_certificate_password:
                return ['Parameter --client-certificate-password is not allowed if --client-secret is provided']

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-analytics-link-setup")

    @staticmethod
    def get_description():
        return "Manage Analytics Links"


class UserChangePassword(Subcommand):
    """The change password subcommand"""

    def __init__(self):
        super(UserChangePassword, self).__init__()
        self.parser.prog = "couchbase-cli user-change-password"
        group = self.parser.add_argument_group("User password change option")
        group.add_argument("--new-password", dest="new_pass", metavar="<password>", required=True,
                           help="The new password")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        if opts.new_pass is None:
            _exit_if_errors(["--new-password is required"])

        _, rv = self.rest.user_change_passsword(opts.new_pass)
        _exit_if_errors(rv)
        _success(f'Changed password for {opts.username}')

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-user-change-password")

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
        group.add_argument("--edit-collection", dest="edit_collection", metavar="<collection>", default=None,
                           help="The path to the collection to edit")
        group.add_argument("--drop-collection", dest="drop_collection", metavar="<collection>", default=None,
                           help="The path to the collection to remove")
        group.add_argument("--list-collections", dest="list_collections", metavar="<scope_list>", default=None,
                           const="", nargs='?', help="List all of the collections in the provided scopes. If no scopes "
                                                     "are provided it will print all collections")
        group.add_argument("--max-ttl", dest="max_ttl", metavar="<seconds>", type=int,
                           help="Set the maximum TTL the collection will accept")
        group.add_argument("--no-expiry", dest="no_expiry", action="store_true", default=None,
                           help="Disable maximum TTL for this collection, so documents in it never expire")
        group.add_argument("--enable-history-retention", dest="enable_history", metavar="<0|1>", choices=["0", "1"],
                           help="Enable history retention (0 or 1)")

    @rest_initialiser(cluster_init_check=True, version_check=True)
    def execute(self, opts):
        cmds = [opts.create_scope, opts.drop_scope, opts.list_scopes, opts.create_collection, opts.edit_collection,
                opts.drop_collection, opts.list_collections]
        cmd_total = sum(cmd is not None for cmd in cmds)

        args = "--create-scope, --drop-scope, --list-scopes, --create-collection, --edit-collection, " \
               "--drop-collection, or --list-collections"
        if cmd_total == 0:
            _exit_if_errors([f'Must specify one of the following: {args}'])
        elif cmd_total != 1:
            _exit_if_errors([f'Only one of the following may be specified: {args}'])

        if opts.max_ttl is not None and opts.no_expiry is not None:
            _exit_if_errors(["Only one of --max-ttl and --no-expiry may be set"])

        if (opts.max_ttl is not None or opts.no_expiry is not None) and opts.create_collection is None and opts.edit_collection is None:
            _exit_if_errors(
                ["--max-ttl/--no-expiry can only be set with --create-collection or --edit-collection"])

        if opts.enable_history is not None and opts.create_collection is None and opts.edit_collection is None:
            _exit_if_errors(["--enable-history-retention can only be set with --create-collection or "
                             "--edit-collection"])

        if opts.edit_collection is not None and (
                opts.enable_history is None and opts.max_ttl is None and opts.no_expiry is None):
            _exit_if_errors(
                ["at least one of {--enable-history-retention, --max-ttl, --no-expiry} should be set with " +
                 "--edit-collection"])

        if opts.no_expiry is not None:
            version, errors = self.rest.min_version()
            _exit_if_errors(errors)

            versionCheck = compare_versions(version, "7.6.0")
            if versionCheck == -1:
                _exit_if_errors(
                    ["--no-expiry can only be used on >= 7.6.0 clusters"])

        if opts.edit_collection is not None and opts.max_ttl is not None:
            version, errors = self.rest.min_version()
            _exit_if_errors(errors)

            if version < "7.6.0":
                _exit_if_errors(
                    ["--max-ttl can only be used with --edit-collection on >= 7.6.0 clusters"])

        if opts.no_expiry is not None:
            opts.max_ttl = -1

        if opts.create_scope:
            self._create_scope(opts)
        if opts.drop_scope:
            self._drop_scope(opts)
        if opts.list_scopes:
            self._list_scopes(opts)
        if opts.create_collection:
            self._create_collection(opts)
        if opts.edit_collection:
            self._edit_collection(opts)
        if opts.drop_collection:
            self._drop_collection(opts)
        if opts.list_collections is not None:
            self._list_collections(opts)

    def _create_scope(self, opts):
        _, errors = self.rest.create_scope(opts.bucket, opts.create_scope)
        _exit_if_errors(errors)
        _success("Scope created")

    def _drop_scope(self, opts):
        _, errors = self.rest.drop_scope(opts.bucket, opts.drop_scope)
        _exit_if_errors(errors)
        _success("Scope dropped")

    def _list_scopes(self, opts):
        manifest, errors = self.rest.get_manifest(opts.bucket)
        _exit_if_errors(errors)
        for scope in manifest['scopes']:
            print(scope['name'])

    def _create_collection(self, opts):
        scope, collection = self._get_scope_collection(opts.create_collection)
        _, errors = self.rest.create_collection(opts.bucket, scope, collection, opts.max_ttl, opts.enable_history)
        _exit_if_errors(errors)
        _success("Collection created")

    def _edit_collection(self, opts):
        scope, collection = self._get_scope_collection(opts.edit_collection)
        _, errors = self.rest.edit_collection(opts.bucket, scope, collection, opts.max_ttl, opts.enable_history)
        _exit_if_errors(errors)
        _success("Collection edited")

    def _drop_collection(self, opts):
        scope, collection = self._get_scope_collection(opts.drop_collection)
        _, errors = self.rest.drop_collection(opts.bucket, scope, collection)
        _exit_if_errors(errors)
        _success("Collection dropped")

    def _list_collections(self, opts):
        manifest, errors = self.rest.get_manifest(opts.bucket)
        _exit_if_errors(errors)

        if opts.list_collections == "":
            scope_dict = {}
        else:
            scope_dict = {scope: False for scope in opts.list_collections.split(',')}

        if opts.output == 'json':
            self._json_list_collections(manifest, scope_dict)
            return

        for scope in manifest['scopes']:
            if len(scope_dict) == 0 or scope['name'] in scope_dict:
                if len(scope_dict) > 0:
                    scope_dict[scope['name']] = True

                print(f'Scope {scope["name"]}:')
                for collection in scope['collections']:
                    print(f'    - {collection["name"]}')

        if len(scope_dict) > 0:
            for scope, found in scope_dict.items():
                if not found:
                    _warning(f'Scope "{scope}" does not exist')

    @staticmethod
    def _json_list_collections(manifest: Dict[str, Any], scope_dict: Dict[str, bool]):
        out = {}
        for scope in manifest['scopes']:
            if len(scope_dict) == 0 or scope['name'] in scope_dict:
                out[scope['name']] = [collection["name"] for collection in scope['collections']]
        print(json.dumps(out, indent=4))

    def _get_scope_collection(self, path):
        scope, collection, err = self.expand_collection_shortcut(path)
        if err is not None:
            _exit_if_errors([err])
        return scope, collection

    @staticmethod
    def expand_collection_shortcut(path):
        parts = path.split('.')
        if len(parts) != 2:
            return None, None, f'invalid collection path {path}'
        parts = ['_default' if x == '' else x for x in parts]
        return parts[0], parts[1], None

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-collection-manage")

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

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        if not (opts.enable or opts.list):
            _exit_if_errors(['--enable or --list must be provided'])
        if opts.enable and opts.list:
            _exit_if_errors(['cannot provide both --enable and --list'])

        if opts.enable:
            confirm = input('Developer preview cannot be disabled once it is enabled. '
                            'If you enter developer preview mode you will not be able to '
                            'upgrade. DO NOT USE IN PRODUCTION.\nAre you sure [y/n]: ')
            if confirm == 'y':
                _, errors = self.rest.set_dp_mode()
                _exit_if_errors(errors)
                _success("Cluster is in developer preview mode")
            elif confirm == 'n':
                _success("Developer preview mode has NOT been enabled")
            else:
                _exit_if_errors(["Unknown option provided"])

        if opts.list:
            pools, rv = self.rest.pools()
            _exit_if_errors(rv)
            if 'isDeveloperPreview' in pools and pools['isDeveloperPreview']:
                print('Cluster is in developer preview mode')
            else:
                print('Cluster is NOT in developer preview mode')

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-enable-developer-preview")

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

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        flags_used = sum([opts.set, opts.list, opts.remove])
        if flags_used != 1:
            _exit_if_errors(['Use exactly one of --set, --list or --remove'])
        if opts.set or opts.remove:
            if not opts.node:
                _exit_if_errors(['--node has to be set when using --set or --remove'])
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

            # override rest client so it uses the node to be altered
            self.rest = ClusterManager(opts.cluster, opts.username, opts.password, opts.ssl, opts.ssl_verify,
                                       opts.cacert, opts.debug)

        if opts.set:
            ports, error = self._parse_ports(opts.ports)
            _exit_if_errors(error)
            _, error = self.rest.set_alternate_address(opts.alternate_hostname, ports)
            _exit_if_errors(error)
            _success('Alternate address configuration updated')
        if opts.remove:
            _, error = self.rest.delete_alternate_address()
            _exit_if_errors(error)
            _success('Alternate address configuration deleted')
        if opts.list:
            add, error = self.rest.get_alternate_address()
            _exit_if_errors(error)
            if opts.output == 'standard':
                port_names = set()
                for node in add:
                    if 'alternateAddresses' in node:
                        if 'ports' in node['alternateAddresses']['external']:
                            for port in node['alternateAddresses']['external']['ports'].keys():
                                port_names.add(port)
                # Limiting the display of hostnames to 42, same length as a IPv6 address
                print('{:43}{:43}{}'.format('Hostname', 'Alternate Address', 'Ports (Primary/Alternate)'))
                print('{:86}'.format(' '), end='')
                port_names = sorted(port_names)
                for port in port_names:
                    column_size = len(port) + 1
                    if column_size < 11:
                        column_size = 12
                    print(f'{port:{column_size}}', end='')
                print()
                for node in add:
                    if 'alternateAddresses' in node:
                        # For cluster_run and single node clusters there is no hostname
                        if 'hostname' in node:
                            host = node["hostname"]
                        else:
                            host = 'UNKNOWN'
                        # Limiting the display of hostnames to 42 chars, same length as IPv6 address
                        if len(host) > 42:
                            host = host[:40] + '..'
                        alternateAddresses = node["alternateAddresses"]["external"]["hostname"]
                        if len(alternateAddresses) > 42:
                            alternateAddresses = alternateAddresses[:40] + '..'
                        print(f'{host:43}{alternateAddresses:43}', end='')
                        for port in port_names:
                            column_size = len(port) + 1
                            if column_size < 11:
                                column_size = 12
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
        port_tuple_list = []
        for port_value_pair in port_mappings:
            p_v = port_value_pair.split('=')
            if len(p_v) != 2:
                return None, [f'invalid port mapping: {port_value_pair}']
            try:
                int(p_v[1])
            except ValueError:
                return None, [f'invalid port mapping: {port_value_pair}']
            port_tuple_list.append((p_v[0], p_v[1]))
        return port_tuple_list, None

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
        return get_doc_page_name("couchbase-cli-setting-alternate-address")

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
        group.add_argument('--temp-dir', metavar='<path>', type=str, default=None,
                           help='This specifies the directory for temporary query data.')
        group.add_argument('--temp-dir-size', metavar='<mebibytes>', type=int, default=None,
                           help='Specify the maximum size in mebibytes for the temporary query data directory.')
        group.add_argument('--cost-based-optimizer', metavar='<1|0>', type=str, default=None,
                           help='Use cost-based optimizer (Developer Preview).')
        group.add_argument('--memory-quota', metavar='<mebibytes>', type=int, default=None,
                           help='Sets the query memory quota in MiB.')
        group.add_argument('--transaction-timeout', metavar='<duration>', type=str, default=None,
                           help='A duration string for the transaction timeout i.e (100ns, 10ms, 1s, 1m).')
        group.add_argument('--node-quota', metavar='<mebibytes>', type=int, default=None,
                           help='Sets the query node-wide quota in MiB.')
        group.add_argument('--node-quota-val-percent', metavar='<perc>', type=int, default=None,
                           help='Sets the percentage of node quota that is reserved for value memory.')
        group.add_argument('--use-replica', metavar='<unset|off|on>', type=str, default=None,
                           help='Sets whether or not a query can read from replica vBuckets.')
        group.add_argument('--num-cpus', metavar='<num>', type=int, default=None,
                           help='Number of CPUs to use for query execution.')

        access_list_group = self.parser.add_argument_group('Query curl access settings')
        access_list_group.add_argument('--curl-access', choices=['restricted', 'unrestricted'], default=None,
                                       help='Specify either unrestricted or restricted, to determine which URLs are'
                                            ' permitted to be accessed by the curl function.')
        access_list_group.add_argument('--allowed-urls', metavar='<urls>', type=str, default=None,
                                       help='Comma separated lists of URLs that are allowed to be accessed by the curl'
                                            ' function.')
        access_list_group.add_argument('--disallowed-urls', metavar='<urls>', type=str, default=None,
                                       help='Comma separated lists of URLs that are disallowed to be accessed by the'
                                            ' curl function.')

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        if sum([opts.get, opts.set]) != 1:
            _exit_if_errors(['Please provide --set or --get, both can not be provided at the same time'])

        if opts.get:
            settings, err = self.rest.get_query_settings()
            _exit_if_errors(err)
            print(json.dumps(settings))
        if opts.set:
            access_list = self._post_query_access_list(opts)
            self._post_query_settings(opts, access_list)
            _success('Updated the query settings')

    def _post_query_access_list(self, opts) -> bool:
        if opts.curl_access != 'restricted' and (opts.allowed_urls is not None or opts.disallowed_urls is not None):
            _exit_if_errors(['Can only provide --allowed-urls or --disallowed-urls with --curl-access restricted'])
        if opts.curl_access:
            allowed = opts.allowed_urls.strip().split(',') if opts.allowed_urls is not None else None
            disallowed = opts.disallowed_urls.strip().split(',') if opts.disallowed_urls is not None else None
            _, err = self.rest.post_query_curl_access_settings(opts.curl_access == 'restricted', allowed, disallowed)
            _exit_if_errors(err)
            return True
        return False

    def _post_query_settings(self, opts, access_list):
        if all(v is None for v in [opts.pipeline_batch, opts.pipeline_cap, opts.scan_cap, opts.timeout,
                                   opts.prepared_limit, opts.completed_limit, opts.completed_threshold,
                                   opts.log_level, opts.max_parallelism, opts.n1ql_feature_control, opts.temp_dir,
                                   opts.temp_dir_size, opts.cost_based_optimizer, opts.memory_quota,
                                   opts.transaction_timeout, opts.node_quota, opts.node_quota_val_percent,
                                   opts.use_replica, opts.num_cpus]):
            if access_list:
                return

            _exit_if_errors(['Please provide at least one other option with --set'])

        if opts.num_cpus is not None and opts.num_cpus <= 0:
            _exit_if_errors(['--num-cpus must be a positive integer'])

        _, err = self.rest.post_query_settings(opts.pipeline_batch, opts.pipeline_cap, opts.scan_cap, opts.timeout,
                                               opts.prepared_limit, opts.completed_limit, opts.completed_threshold,
                                               opts.log_level, opts.max_parallelism, opts.n1ql_feature_control,
                                               opts.temp_dir, opts.temp_dir_size, opts.cost_based_optimizer,
                                               opts.memory_quota, opts.transaction_timeout, opts.node_quota,
                                               opts.node_quota_val_percent, opts.use_replica, opts.num_cpus)
        _exit_if_errors(err)

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-query")

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
        group.add_argument('--ipv4only', dest='ipv4only', default=False, action="store_true",
                           help='Set IP family to IPv4 only')
        group.add_argument('--ipv6', dest='ipv6', default=False, action="store_true",
                           help='Set IP family to IPv6')
        group.add_argument('--ipv6only', dest='ipv6only', default=False, action="store_true",
                           help='Set IP family to IPv6 only')

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        flags_used = sum([opts.set, opts.get])
        if flags_used == 0:
            _exit_if_errors(['Please provide one of --set, or --get'])
        elif flags_used > 1:
            _exit_if_errors(['Please provide only one of --set, or --get'])

        if opts.get:
            self._get(self.rest)
        if opts.set:
            if sum([opts.ipv6, opts.ipv6only, opts.ipv4, opts.ipv4only]) != 1:
                _exit_if_errors(['Provided exactly one of --ipv4, --ipv4only, --ipv6 or --ipv6only together with the'
                                 ' --set option'])
            only = opts.ipv6only or opts.ipv4only
            if opts.ipv6only:
                opts.ipv6 = True
            self._set(self.rest, opts.ipv6, opts.ssl, only)

    @staticmethod
    def _set(rest, ipv6, ssl, ip_only):
        ip_fam, ip_fam_disable = ('ipv6', 'ipv4') if ipv6 else ('ipv4', 'ipv6')
        node_data, err = rest.pools('nodes')

        if err and err[0] == 'unknown pool':
            _, err = rest.enable_external_listener(ipfamily=ip_fam)
            _exit_if_errors(err)
            _, err = rest.setup_net_config(ipfamily=ip_fam, ipfamilyonly=ip_only)
            _exit_if_errors(err)
            _, err = rest.disable_unused_external_listeners(ipfamily=ip_fam_disable)
            _exit_if_errors(err)
            _success('Switched IP family of the cluster')
            return

        _exit_if_errors(err)

        ssl = ssl or force_communicate_tls(rest)

        hosts = IpFamily.get_hosts(node_data, ssl)
        for host in hosts:
            _, err = rest.enable_external_listener(host=host, ipfamily=ip_fam)
            _exit_if_errors(err)

        for h in hosts:
            _, err = rest.setup_net_config(host=h, ipfamily=ip_fam, ipfamilyonly=ip_only)
            _exit_if_errors(err)
            print(f'Switched IP family for node: {h}')

        updated_node_data, err = rest.pools('nodes')
        updated_hosts = IpFamily.get_hosts(updated_node_data, ssl)
        for h in updated_hosts:
            _, err = rest.disable_unused_external_listeners(host=h, ipfamily=ip_fam_disable)
            _exit_if_errors(err)

        _success('Switched IP family of the cluster')

    @staticmethod
    def get_hosts(node_data, ssl):
        hosts = []
        for n in node_data['nodes']:
            host = f'http://{n["hostname"]}'
            if ssl:
                addr = n["hostname"].rsplit(":", 1)[0]
                host = f'https://{addr}:{n["ports"]["httpsMgmt"]}'
            hosts.append(host)

        return hosts

    @staticmethod
    def _get(rest):
        nodes, err = rest.nodes_info()
        _exit_if_errors(err)
        ip_families = set()
        for n in nodes:
            if 'addressFamilyOnly' in n and n['addressFamilyOnly']:
                ip_families.add(n['addressFamily'] + 'only')
            else:
                ip_families.add(n['addressFamily'])

        if len(ip_families) == 1:
            ip_family = ip_families.pop()
            mode = 'UNKNOWN'
            if ip_family in ['inet', 'inet_tls']:
                mode = 'ipv4'
            elif ip_family in ['inetonly', 'inet_tlsonly']:
                mode = 'ipv4only'
            elif ip_family in ['inet6', 'inet6_tls']:
                mode = 'ipv6'
            elif ip_family in ['inet6only', 'inet6_tlsonly']:
                mode = 'ipv6only'

            print(f'Cluster using {mode}')
        else:
            print('Cluster is in mixed mode')

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-ip-family")

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

    @rest_initialiser(version_check=True)
    def execute(self, opts):
        flags_used = sum([opts.enable, opts.disable, opts.get])
        if flags_used == 0:
            _exit_if_errors(['Please provide one of --enable, --disable or --get'])
        elif flags_used > 1:
            _exit_if_errors(['Please provide only one of --enable, --disable or --get'])

        if opts.get:
            self._get(self.rest)
        elif opts.enable:
            self._change_encryption(self.rest, 'on', opts.ssl)
        elif opts.disable:
            self._change_encryption(self.rest, 'off', opts.ssl)

    @staticmethod
    def _change_encryption(rest, encryption, ssl):
        node_data, err = rest.pools('nodes')
        encryption_disable = 'off' if encryption == 'on' else 'on'

        if err and err[0] == 'unknown pool':
            _, err = rest.enable_external_listener(encryption=encryption)
            _exit_if_errors(err)
            _, err = rest.setup_net_config(encryption=encryption)
            _exit_if_errors(err)
            _, err = rest.disable_unused_external_listeners(encryption=encryption_disable)
            _exit_if_errors(err)
            _success(f'Switched node-to-node encryption {encryption}')
            return

        _exit_if_errors(err)

        ssl = ssl or force_communicate_tls(rest)

        hosts = []
        for n in node_data['nodes']:
            host = f'http://{n["hostname"]}'
            if ssl:
                addr = n["hostname"].rsplit(":", 1)[0]
                host = f'https://{addr}:{n["ports"]["httpsMgmt"]}'
            _, err = rest.enable_external_listener(host=host, encryption=encryption)
            _exit_if_errors(err)
            hosts.append(host)

        for h in hosts:
            _, err = rest.setup_net_config(host=h, encryption=encryption)
            _exit_if_errors(err)
            print(f'Turned {encryption} encryption for node: {h}')

        for h in hosts:
            _, err = rest.disable_unused_external_listeners(host=h, encryption=encryption_disable)
            _exit_if_errors(err)

        _success(f'Switched node-to-node encryption {encryption}')

    @staticmethod
    def _get(rest):
        # this will start the correct listeners in all the nodes
        nodes, err = rest.nodes_info()
        _exit_if_errors(err)
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
        return get_doc_page_name("couchbase-cli-node-to-node-encryption")

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
        group.add_argument('--moves-per-node', type=int, metavar='<num>',
                           help='Specify the number of [1-64] vBuckets to move concurrently')
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

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=False)
    def execute(self, opts):
        if sum([opts.set, opts.get, opts.cancel, opts.pending_info]) != 1:
            _exit_if_errors(['Provide either --set, --get, --cancel or --pending-info'])

        if opts.get:
            settings, err = self.rest.get_settings_rebalance()
            _exit_if_errors(err)
            if self.enterprise:
                retry_settings, err = self.rest.get_settings_rebalance_retry()
                _exit_if_errors(err)
                settings.update(retry_settings)
            if opts.output == 'json':
                print(json.dumps(settings))
            else:
                if self.enterprise:
                    print(f'Automatic rebalance retry {"enabled" if settings["enabled"] else "disabled"}')
                    print(f'Retry wait time: {settings["afterTimePeriod"]}')
                    print(f'Maximum number of retries: {settings["maxAttempts"]}')
                print(f'Maximum number of vBucket move per node: {settings["rebalanceMovesPerNode"]}')
        elif opts.set:
            if (not self.enterprise and (opts.enable is not None or opts.wait_for is not None
                                         or opts.max_attempts is not None)):
                _exit_if_errors(["Automatic rebalance retry configuration is an Enterprise Edition only feature"])
            if opts.enable == '1':
                opts.enable = 'true'
            else:
                opts.enable = 'false'

            if opts.wait_for is not None and (opts.wait_for < 5 or opts.wait_for > 3600):
                _exit_if_errors(['--wait-for must be a value between 5 and 3600'])
            if opts.max_attempts is not None and (opts.max_attempts < 1 or opts.max_attempts > 3):
                _exit_if_errors(['--max-attempts must be a value between 1 and 3'])

            if self.enterprise:
                _, err = self.rest.set_settings_rebalance_retry(opts.enable, opts.wait_for, opts.max_attempts)
                _exit_if_errors(err)

            if opts.moves_per_node is not None:
                if not 1 <= opts.moves_per_node <= 64:
                    _exit_if_errors(['--moves-per-node must be a value between 1 and 64'])
                _, err = self.rest.set_settings_rebalance(opts.moves_per_node)
                _exit_if_errors(err)

            _success('Rebalance settings updated')
        elif opts.cancel and not self.enterprise:
            _exit_if_errors(["Automatic rebalance retry configuration is an Enterprise Edition only feature"])

            if opts.rebalance_id is None:
                _exit_if_errors(['Provide the failed rebalance id using --rebalance-id <id>'])
            _, err = self.rest.cancel_rebalance_retry(opts.rebalance_id)
            _exit_if_errors(err)
            _success('Rebalance retry canceled')
        else:
            if not self.enterprise:
                _exit_if_errors(["Automatic rebalance retry configuration is an Enterprise Edition only feature"])
            rebalance_info, err = self.rest.get_rebalance_info()
            _exit_if_errors(err)
            print(json.dumps(rebalance_info))

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-rebalance")

    @staticmethod
    def get_description():
        return "Configure automatic rebalance settings"


class SettingAnalytics(Subcommand):
    """The setting-analytics subcommand"""

    def __init__(self):
        super().__init__()
        self.parser.prog = 'couchbase-cli setting-analytics'
        group = self.parser.add_argument_group('Analytics Service Settings')
        group.add_argument('--get', action='store_true', help='Retrieve the current Analytics settings')
        group.add_argument('--set', action='store_true', help='Set the Analytics settings')
        group.add_argument('--replicas', action='store', type=int, choices=[0, 1, 2, 3],
                           help='Sets the number of replicas')

    @rest_initialiser(cluster_init_check=True, version_check=True, enterprise_check=False)
    def execute(self, opts):
        if sum([opts.get, opts.set]) != 1:
            _exit_if_errors(['Please provide --set or --get, both cannot be provided at the same time'])

        if opts.get:
            settings, err = self.rest.get_analytics_settings()
            _exit_if_errors(err)
            if opts.output == 'json':
                print(json.dumps(settings))
                return
            for k, v in settings.items():
                print(f'{k}: {v}')
            return

        if opts.replicas is None:
            _exit_if_errors(['Please provide at least one other option with --set'])

        _, err = self.rest.set_analytics_settings(opts.replicas)
        _exit_if_errors(err)
        if opts.replicas:
            print('A rebalance is required for the replica change to take affect')
        _success('Analytics settings updated')

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-setting-analytics")

    @staticmethod
    def get_description():
        return "Modify Analytics settings"


class BackupService(Subcommand):
    """BackupService class is a subcommand that will contain other commands to configure the service as well as manage
    it. This approach attempts to make the interface more intuitive by keeping a hierarchical structure where the
    service can have all its options under one command instead of having multiple completely separate commands (e.g
    settings-backups, manage-backups and repository-setup-backup.)

    The idea is that the interface will look like:
    couchbase-cli backup-service [settings | plans | repositories | cloud-credentials] where each element in [] is a
    subcommand to manage those options for that part of the backup service. As such if the user is not sure of what they
    want to do they can always do couchbase-cli backup-service -h to get a top level details and then move down the
    hierarchy to a more concrete option.
    """

    def __init__(self):
        super(BackupService, self).__init__()
        self.parser.prog = "couchbase-cli backup-service"
        self.subparser = self.parser.add_subparsers(help='Sub command help', dest='sub_cmd', metavar='<subcommand>')
        self.settings_cmd = BackupServiceSettings(self.subparser)
        self.repository_cmd = BackupServiceRepository(self.subparser)
        self.plan_cmd = BackupServicePlan(self.subparser)
        self.nodeThreads_cmd = BackupServiceNodeThreadsMap(self.subparser)

    def execute(self, opts):
        if opts.sub_cmd is None or opts.sub_cmd not in ['settings', 'repository', 'plan', 'node-threads']:
            _exit_if_errors(['<subcommand> must be one of [settings, repository, plan, node-threads]'])

        if opts.sub_cmd == 'settings':
            self.settings_cmd.execute(opts)
        elif opts.sub_cmd == 'repository':
            self.repository_cmd.execute(opts)
        elif opts.sub_cmd == 'plan':
            self.plan_cmd.execute(opts)
        elif opts.sub_cmd == 'node-threads':
            self.nodeThreads_cmd.execute(opts)

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-backup-service")

    @staticmethod
    def get_description():
        return "Manage the backup service"


class BackupServiceSettings:
    """Backup service settings is a nested command and manages the backup service settings"""

    def __init__(self, subparser):
        self.rest = None
        setting_parser = subparser.add_parser('settings', help='Manage backup service settings', add_help=False,
                                              allow_abbrev=False)
        group = setting_parser.add_argument_group('Backup service settings options')
        group.add_argument('--get', action='store_true', help='Get current backup service configuration')
        group.add_argument('--set', action='store_true', help='Change the service configuration')
        group.add_argument('--history-rotation-period', dest='rotation_period', type=int, metavar='<days>',
                           help='The number of days after which the task history should be rotated')
        group.add_argument('--history-rotation-size', dest='rotation_size', type=int, metavar='<mebibytes>',
                           help='The size in MiB at which to rotate the task history')
        group.add_argument("-h", "--help", action=CBHelpAction, klass=self,
                           help="Prints the short or long help message")

    @rest_initialiser(version_check=True, enterprise_check=True, cluster_init_check=True)
    def execute(self, opts):
        if sum([opts.get, opts.set]) != 1:
            _exit_if_errors(['Must use one and only one of [--get, --set]'])

        if opts.get:
            self._get(opts)
        if opts.set:
            self._set(opts)

    def _get(self, opts):
        config, err = self.rest.get_backup_service_settings()
        _exit_if_errors(err)

        if opts.output == 'json':
            print(json.dumps(config, indent=4))
        else:
            print('-- Backup service configuration --')
            size = config['history_rotation_size'] if 'history_rotation_size' in config else 'N/A'
            period = config['history_rotation_period'] if 'history_rotation_period' in config else 'N/A'
            print(f'History rotation size: {size} MiB')
            print(
                f'History rotation period: {period} days')

    def _set(self, opts):
        if opts.rotation_period is None and opts.rotation_size is None:
            _exit_if_errors(['At least one of --history-rotation-period or --history-rotation-size is required'])

        _, err = self.rest.patch_backup_service_settings(opts.rotation_period, opts.rotation_size)
        _exit_if_errors(err)
        _success('Backup service settings patched')

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-backup-service-settings")

    @staticmethod
    def get_description():
        return 'Manage backup service settings'


class BackupServiceRepository:
    """This command manages backup services repositories.

    Things this command can do is:
    - List repositories
    - Get repository
    - Add repository
    - Archive repository
    - Import repository
    - Delete repository
    """

    def __init__(self, subparser):
        """setup the parser"""
        self.rest = None
        repository_parser = subparser.add_parser('repository', help='Manage backup repositories', add_help=False,
                                                 allow_abbrev=False)

        # action flags are mutually exclusive
        action_group = repository_parser.add_mutually_exclusive_group(required=True)
        action_group.add_argument('--list', action='store_true', help='Get all repositories')
        action_group.add_argument('--get', action='store_true', help='Get repository by id')
        action_group.add_argument('--archive', action='store_true', help='Archive a repository')
        action_group.add_argument('--add', action='store_true', help='Add a new active repository')
        action_group.add_argument('--remove', action='store_true', help='Remove an archived/imported repository')
        action_group.add_argument('-h', '--help', action=CBHelpAction, klass=self,
                                  help="Prints the short or long help message")

        # other arguments
        group = repository_parser.add_argument_group('Backup service repository configuration')
        group.add_argument('--id', metavar='<id>', help='The repository id')
        group.add_argument('--new-id', metavar='<id>', help='The new repository id')
        group.add_argument('--state', metavar='<state>', choices=['active', 'archived', 'imported'],
                           help='The repository state.')
        group.add_argument('--plan', metavar='<plan_name>', help='The plan to use as base for the repository')
        group.add_argument('--backup-archive', metavar='<archive>', help='The location to store the backups in')
        group.add_argument('--bucket-name', metavar='<name>', help='The bucket to backup')
        group.add_argument('--remove-data', action='store_true', help='Used to delete the repository data')

        # the cloud arguments are given the own group so that the short help is a bit more readable
        cloud_group = repository_parser.add_argument_group('Backup repository cloud arguments')
        cloud_group.add_argument('--cloud-credentials-name', metavar='<name>',
                                 help='The stored clouds credential name to use for the new repository')
        cloud_group.add_argument('--cloud-staging-dir', metavar='<path>', help='The path to the staging directory')
        cloud_group.add_argument('--cloud-credentials-id', metavar='<id>',
                                 help='The ID to use to communicate with the object store')
        cloud_group.add_argument('--cloud-credentials-key', metavar='<key>',
                                 help='The key to use to communicate with the object store')
        cloud_group.add_argument('--cloud-credentials-refresh-token', metavar='<token>',
                                 help='Used to refresh oauth2 tokens when accessing remote storage')
        cloud_group.add_argument('--cloud-credentials-region', metavar='<region>',
                                 help='The region for the object store')
        cloud_group.add_argument('--cloud-endpoint', metavar='<endpoint>',
                                 help='Overrides the default endpoint used to communicate with the cloud provider. '
                                      'Use for object store compatible third party solutions')
        cloud_group.add_argument('--s3-force-path-style', action='store_true',
                                 help='When using S3 or S3 compatible storage it will use the old path style.')

    @rest_initialiser(version_check=True, enterprise_check=True, cluster_init_check=True)
    def execute(self, opts):
        """Run the backup-service repository subcommand"""
        if opts.list:
            self.list_repositories(opts.state, opts.output == 'json')
        elif opts.get:
            self.get_repository(opts.id, opts.state, opts.output == 'json')
        elif opts.archive:
            self.archive_repository(opts.id, opts.new_id)
        elif opts.remove:
            self.remove_repository(opts.id, opts.state, opts.remove_data)
        elif opts.add:
            self.add_active_repository(opts.id, opts.plan, opts.backup_archive, bucket_name=opts.bucket_name,
                                       credentials_name=opts.cloud_credentials_name,
                                       credentials_id=opts.cloud_credentials_id,
                                       credentials_key=opts.cloud_credentials_key,
                                       credentials_refresh_token=opts.cloud_credentials_refresh_token,
                                       cloud_region=opts.cloud_credentials_region, staging_dir=opts.cloud_staging_dir,
                                       cloud_endpoint=opts.cloud_endpoint, s3_path_style=opts.s3_force_path_style)

    def remove_repository(self, repository_id: str, state: str, delete_repo: bool = False):
        """Removes the repository in state 'state' and with id 'repository_id'
        Args:
            repository_id (str): The repository id
            state (str): It must be either archived or imported otherwise it will return an error
            delete_repo (bool): Whether or not the backup repository should be deleted
        """
        if not repository_id:
            _exit_if_errors(['--id is required'])
        # the following is devided in two options to give better error messages depending if state is missing or if it
        # is invalid
        if not state:
            _exit_if_errors(['--state is required'])
        if state not in ['archived', 'imported']:
            _exit_if_errors([
                'can only delete archived or imported repositories to delete an active repository it needs to '
                'be archived first'])
        # can only delete repo of archived repositories
        if delete_repo and state == 'imported':
            _exit_if_errors(['cannot delete the repository for an imported repository'])

        _, errors = self.rest.delete_backup_repository(repository_id, state, delete_repo)
        _exit_if_errors(errors)
        _success('Repository was deleted')

    def add_active_repository(self, repository_id: str, plan: str, archive: str, **kwargs):
        """Adds a new active repository identified by 'repository_id' and that uses 'plan' as base.

        Args:
            repository_id (str): The ID to give to the repository. This must be unique, if it is not an error will be
                returned.
            plan (str): The name of the plan to use as base for the repository. If it does not exist the service
                will return an error.
            archive (str): The location to store the data in. It must be accessible by all nodes. To use S3 instead of
                providing a path to a filesystem directory use the syntax.
                s3://<bucket-name>/<optional_prefix>/<archive>
            **kwargs: Optional parameters [bucket_name, credentials_name, credentials_id, credentials_key, cloud_region,
                staging_dir, cloud_endpoint, s3_path_style]
        """
        if not repository_id:
            _exit_if_errors(['--id is required'])
        if not plan:
            _exit_if_errors(['--plan is required'])
        if not archive:
            _exit_if_errors(['--backup-archive is required'])

        _exit_if_errors(self.check_cloud_params(archive, **kwargs))

        add_request_body = {
            'plan': plan,
            'archive': archive,
        }

        if kwargs.get('bucket_name', False):
            add_request_body['bucket_name'] = kwargs.get('bucket_name')
        if kwargs.get('credentials_name', False):
            add_request_body['cloud_credential_name'] = kwargs.get('credentials_name')
        if kwargs.get('credentials_id', False):
            add_request_body['cloud_credentials_id'] = kwargs.get('credentials_id')
        if kwargs.get('credentials_key', False):
            add_request_body['cloud_credentials_key'] = kwargs.get('credentials_key')
        if kwargs.get('credentials_refresh_token', False):
            add_request_body['cloud_credentials_refresh_token'] = kwargs.get('credentials_refresh_token')
        if kwargs.get('cloud_region', False):
            add_request_body['cloud_region'] = kwargs.get('cloud_region')
        if kwargs.get('cloud_endpoint', False):
            add_request_body['cloud_endpoint'] = kwargs.get('cloud_endpoint')
        if kwargs.get('staging_dir', False):
            add_request_body['cloud_staging_dir'] = kwargs.get('staging_dir')
        if kwargs.get('s3_path_style', False):
            add_request_body['cloud_force_path_style'] = kwargs.get('s3_path_style')

        _, errors = self.rest.add_backup_active_repository(repository_id, add_request_body)
        _exit_if_errors(errors)
        _success('Added repository')

    @staticmethod
    def check_cloud_params(archive: str, **kwargs) -> Optional[List[str]]:
        """Checks that inside kwargs there is a valid set of parameters to add a cloud repository
        Args:
            archive (str): The archive to use for the repository.
        """
        # If not an s3 archive skip this
        if not archive.startswith('s3://'):
            return None

        creds_name = kwargs.get('credentials_name')
        region = kwargs.get('cloud_region')
        creds_id = kwargs.get('credentials_id')
        creds_key = kwargs.get('credentials_key')
        staging_dir = kwargs.get('staging_dir')

        if (creds_name and (creds_id or creds_key)) or (not creds_name and not (creds_id or creds_key)):
            return ['must provide either --cloud-credentials-name or --cloud-credentials-key and '
                    '--cloud-credentials-id']
        if not staging_dir:
            return ['--cloud-staging-dir is required']
        if not creds_name and not region:
            return ['--cloud-credentials-region is required']

        return None

    def archive_repository(self, repository_id, new_id):
        """Archive an repository. The archived repository will have the id `new_id`

        Args:
            repository_id (str): The active repository ID to be archived
            new_id (str): The id that will be given to the archived repository
        """
        if not repository_id:
            _exit_if_errors(['--id is required'])
        if not new_id:
            _exit_if_errors(['--new-id is required'])

        _, errors = self.rest.archive_backup_repository(repository_id, new_id)
        _exit_if_errors(errors)
        _success('Archived repository')

    def list_repositories(self, state=None, json_out=False):
        """List the backup repositories.

        If a repository state is given only repositories in that state will be listed. This command supports listing both in
        json and human friendly format.

        Args:
            state (str, optional): One of ['active', 'imported', 'archived']. The repository on this state will be
                retrieved.
            json_out (bool): If True the output will be JSON otherwise it will be a human friendly format.
        """
        states = ['active', 'archived', 'imported'] if state is None else [state]
        results = {}
        for get_state in states:
            repositories, errors = self.rest.get_backup_service_repositories(state=get_state)
            _exit_if_errors(errors)
            results[get_state] = repositories

        if json_out:
            print(json.dumps(results, indent=2))
        else:
            self.human_friendly_print_repositories(results)

    def get_repository(self, repository_id, state, json_out=False):
        """Retrieves one repository from the backup service

        If the repository does not exist an error will be returned

        Args:
            repository_id (str): The repository id to be retrieved
            state (str): The state of the repository to retrieve
            json_out (bool): If True the output will be JSON otherwise it will be a human friendly format.
        """
        if not repository_id:
            _exit_if_errors(['--id is required'])
        if not state:
            _exit_if_errors(['--state is required'])

        repository, errors = self.rest.get_backup_service_repository(repository_id, state)
        _exit_if_errors(errors)
        if json_out:
            print(json.dumps(repository, indent=2))
        else:
            self.human_firendly_print_repository(repository)

    @staticmethod
    def human_firendly_print_repository(repository):
        """Print the repository in a human friendly format

        Args:
            repository (obj): The backup repository information
        """

        print(f'ID: {repository["id"]}')
        print(f'State: {repository["state"]}')
        print(f'Healthy: {(not ("health" in repository and not repository["health"]["healthy"]))!s}')
        print(f'Archive: {repository["archive"]}')
        print(f'Repository: {repository["repo"]}')
        if 'bucket' in repository:
            print(f'Bucket: {repository["bucket"]["name"]}')
        if 'plan_name' in repository and repository['plan_name'] != "":
            print(f'plan: {repository["plan_name"]}')
        print(f'Creation time: {repository["creation_time"]}')

        if 'scheduled' in repository and repository['scheduled']:
            print()
            BackupServiceRepository.human_firendly_print_repository_scheduled_tasks(repository['scheduled'])

        one_off = repository['running_one_off'] if 'running_one_off' in repository else None
        running_scheduled = repository['running_tasks'] if 'running_tasks' in repository else None
        if one_off or running_scheduled:
            print()
            BackupServiceRepository.human_friendly_print_running_tasks(one_off, running_scheduled)

    @staticmethod
    def human_friendly_print_running_tasks(one_off, scheduled):
        """Prints the running task summary in a human friendly way

        Args:
            one_off (map<str, task object>): Running one off tasks
            scheduled (map<str, task object>): Running scheduled tasks
        """
        all_vals = []
        name_pad = 5
        if one_off:
            for name in one_off:
                if len(name) > name_pad:
                    name_pad = len(name)
            all_vals += one_off.values()

        if scheduled:
            for name in scheduled:
                if len(name) > name_pad:
                    name_pad = len(name)
            all_vals += scheduled.values()

        name_pad += 1

        header = f'{"Name":<{name_pad}}| Task type | Status  | Start'
        print(header)
        print('-' * (len(header) + 5))
        for task in all_vals:
            print(f'{task["name"]:<{name_pad}}| {task["type"].title():<10}| {task["status"]:<8} | {task["start"]}')

    @staticmethod
    def human_firendly_print_repository_scheduled_tasks(scheduled):
        """Print the scheduled task in a tabular format"""
        name_pad = 5
        for name in scheduled:
            if len(name) > name_pad:
                name_pad = len(name)
        name_pad += 1

        header = f'{"Name":<{name_pad}}| Task type | Next run'
        print('Scheduled tasks:')
        print(header)
        print('-' * (len(header) + 5))

        for task in scheduled.values():
            print(f'{task["name"]:<{name_pad}}| {task["task_type"].title():<10}| {task["next_run"]}')

    @staticmethod
    def human_friendly_print_repositories(repositories_map):
        """This will print the repositories in a tabular format

        Args:
            repository_map (map<state (str), repository (list of objects)>)
        """
        repository_count = 0
        id_pad = 5
        plan_pad = 7
        for repositories in repositories_map.values():
            for repository in repositories:
                repository_count += 1
                if id_pad < len(repository['id']):
                    id_pad = len(repository['id'])
                if 'plan_name' in repository and plan_pad < len(repository['plan_name']):
                    plan_pad = len(repository['plan_name'])

        if repository_count == 0:
            print('No repositories found')
            return

        # Get an extra space between the the information and the column separator
        plan_pad += 1
        id_pad += 1

        # build header
        header = f'{"ID":<{id_pad}}| {"State":<9}| {"plan":<{plan_pad}}| Healthy | Repository'
        print(header)
        print('-' * len(header))

        # print repository summary
        for _, repositories in sorted(repositories_map.items()):
            for repository in repositories:
                healthy = not ('health' in repository and not repository['health']['healthy'])
                # archived and imported repositories may not have plans so we have to replace the empty string with N/A
                plan_name = 'N/A'
                if 'plan_name' in repository and len(repository['plan_name']) != 0:
                    plan_name = repository['plan_name']

                print(f"{repository['id']:<{id_pad}}| {repository['state']:<9}| {plan_name:<{plan_pad}}| "
                      f" {healthy!s:<7}| {repository['repo']}")

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-backup-service-repository")

    @staticmethod
    def get_description():
        return 'Manage backup service repositories'


class BackupServicePlan:
    """This command manages backup services plans.

    Things this command can do is:
    - List plans
    - Add delete
    - Delete plans
    """

    def __init__(self, subparser):
        """setup the parser"""
        self.rest = None
        plan_parser = subparser.add_parser('plan', help='Manage backup plans', add_help=False,
                                           allow_abbrev=False)

        # action flags are mutually exclusive
        action_group = plan_parser.add_mutually_exclusive_group(required=True)
        action_group.add_argument('--list', action='store_true', help='List all available backup plans')
        action_group.add_argument('--get', action='store_true', help='Get a plan by name')
        action_group.add_argument('--remove', action='store_true', help='Remove a plan by name')
        action_group.add_argument('--add', action='store_true', help='Add a new plan')
        action_group.add_argument('-h', '--help', action=CBHelpAction, klass=self,
                                  help="Prints the short or long help message")

        options = plan_parser.add_argument_group('Plan options')
        options.add_argument('--name', metavar='<name>', help='Plan name')
        options.add_argument('--description', metavar='<description>', help='Optional description')
        options.add_argument('--services', metavar='<services>', help='A comma separated list of services to backup')
        options.add_argument('--task', metavar='<tasks>', nargs='+', help='JSON task definition')

    @rest_initialiser(version_check=True, enterprise_check=True, cluster_init_check=True)
    def execute(self, opts):
        """Run the backup plan managment command"""
        if opts.list:
            self.list_plans(opts.output == 'json')
        elif opts.get:
            self.get_plan(opts.name, opts.output == 'json')
        elif opts.remove:
            self.remove_plan(opts.name)
        elif opts.add:
            self.add_plan(opts.name, opts.services, opts.task, opts.description)

    def add_plan(self, name: str, services: Optional[str], tasks: Optional[List[str]], description: Optional[str]):
        """Add a new backup plan

        The validation of the inputs in the CLI is intentionally lacking as this is offloaded to the backup service.
        Args:
            name (str): The name to give the new plan. It must be unique.
            services (optional list): A list of services to backup if empty all services are backed up.
            tasks (optional list): A list of JSON strings representing the tasks to be run.
            description (optional str): A optional description string.
        """
        if not name:
            _exit_if_errors(['--name is required'])

        service_list = []
        if services:
            service_list = [service.strip() for service in services.split(',')]

        tasks_objects = []
        if tasks:
            for task_str in tasks:
                try:
                    task = json.loads(task_str)
                    tasks_objects.append(task)
                except json.decoder.JSONDecodeError as json_error:
                    _exit_if_errors([f'invalid task {json_error!s}'])

        plan = {}
        if service_list:
            plan['services'] = service_list
        if tasks_objects:
            plan['tasks'] = tasks_objects
        if description:
            plan['description'] = description

        _, errors = self.rest.add_backup_plan(name, plan)
        _exit_if_errors(errors)
        _success('Added plan')

    def remove_plan(self, name: str):
        """Removes a plan by name"""
        if not name:
            _exit_if_errors(['--name is required'])

        _, errors = self.rest.delete_backup_plan(name)
        _exit_if_errors(errors)
        _success('Plan removed')

    def get_plan(self, name: str, json_output: bool = False):
        """Gets a backup plan by name

        Args:
            name (str): The name of the plan to retrieve
            json_output (bool): Whether to print in JSON or a more human friendly way
        """
        if not name:
            _exit_if_errors(['--name is required'])

        plan, errors = self.rest.get_backup_plan(name)
        _exit_if_errors(errors)
        if json_output:
            print(json.dumps(plan, indent=2))
        else:
            self.human_print_plan(plan)

    def list_plans(self, json_output: bool = False):
        """Prints all the plans stored in the backup service

        Args:
            json_output (bool): Whether to print in JSON or a more human friendly way
        """
        plans, errors = self.rest.list_backup_plans()
        _exit_if_errors(errors)
        if json_output:
            print(json.dumps(plans, indent=2))
        else:
            self.human_print_plans(plans)

    @staticmethod
    def human_print_plan(plan: object):
        """Prints the plan in a human friendly way"""
        print(f'Name: {plan["name"]}')
        print(f'Description: {plan["description"] if "description" in plan else "N/A"}')
        print(f'Services: {BackupServicePlan.service_list_to_str(plan["services"])}')
        print(f'Default: {(plan["default"] if "deafult" in plan else False)!s}')

        # If the are no tasks return
        if not plan["tasks"]:
            return

        print()
        print('Tasks:')
        task_name_pad = 5
        schedule_pad = 10
        for task in plan['tasks']:
            if len(task['name']) > task_name_pad:
                task_name_pad = len(task['name'])

            task['schedule_str'] = BackupServicePlan.format_schedule(task['schedule'])
            if len(task['schedule_str']) > schedule_pad:
                schedule_pad = len(task['schedule_str'])

        task_name_pad += 1
        schedule_pad += 1

        header = f'{"Name":<{task_name_pad}} | {"Schedule":<{schedule_pad}} | Options'
        print(header)
        print('-' * (len(header) + 5))

        for task in plan['tasks']:
            options = BackupServicePlan.format_options(task)
            print(f'{task["name"]:<{task_name_pad}} | {task["schedule_str"]:<{schedule_pad}} | {options}')

    @staticmethod
    def format_options(task: object) -> str:
        """Format the full backup or merge options"""
        options = 'N/A'
        if task['task_type'] == 'BACKUP' and task['full_backup']:
            options = 'Full backup'
        elif task['task_type'] == 'MERGE':
            if 'merge_options' in task:
                options = (f'Merge from {task["merge_options"]["offset_start"]} to '
                           f'{task["merge_options"]["offset_end"]}')
            else:
                options = 'Merge everything'
        return options

    @staticmethod
    def format_schedule(schedule: object) -> str:
        """Format the schedule object in a string of the format <task> every <frequency>? <period> (at <time>)?"""
        task_start = f'{schedule["job_type"].lower()}'
        frequency_part = 'every'
        if schedule['frequency'] == 1:
            period = schedule["period"].lower()
            period = period if period[-1] != 's' else period[:-1]
            frequency_part += f' {period}'
        else:
            frequency_part += f' {schedule["frequency"]} {schedule["period"].lower()}'
        time_part = f' at {schedule["time"]}' if 'time' in schedule else ''
        return f'{task_start} {frequency_part}{time_part}'

    @staticmethod
    def human_print_plans(plans: List[Any]):
        """Prints a table with an overview of each plan"""
        # if plans is empty or none print no plans message
        if not plans:
            print('No plans')
            return

        name_pad = 5
        service_pad = 8
        for plan in plans:
            if len(plan['name']) > name_pad:
                name_pad = len(plan['name'])
            services_str = BackupServicePlan.service_list_to_str(plan['services'])
            if len(services_str) > service_pad:
                service_pad = len(services_str)

        name_pad += 1
        service_pad += 1
        header = f'{"Name":<{name_pad}} | # Tasks | {"Services":<{service_pad}} | Default'
        print(header)
        print('-' * (len(header) + 5))
        for plan in plans:
            task_len = len(plan['tasks']) if 'tasks' in plan and plan['tasks'] else 0
            print(f'{plan["name"]:<{name_pad}} | {task_len:<7} | '
                  f'{BackupServicePlan.service_list_to_str(plan["services"]):<{service_pad}} | '
                  f'{(plan["default"] if "default" in plan else False)!s}')

    @staticmethod
    def service_list_to_str(services: Optional[List[Any]]) -> str:
        """convert the list of services to a concise list of services"""
        if not services:
            return 'all'

        # a way to convert codenames to visible name
        convert = {'gsi': 'Indexing', 'cbas': 'Analytics', 'ft': 'Full Text Search'}
        return ', '.join([convert[service] if service in convert else service.title() for service in services])

    @staticmethod
    def get_man_page_name():
        return get_doc_page_name("couchbase-cli-backup-service-plan")

    @staticmethod
    def get_description():
        return 'Manage backup service plans'


class BackupServiceNodeThreadsMap:
    """This command manages backup services node threads map.

    Things this command can do is:
    - Get node threads map
    - Post a new node threads map
    - Patch an existing node threads map
    """

    def __init__(self, subparser):
        """setup the parser"""
        self.rest = None
        node_threads_parser = subparser.add_parser('node-threads', help='Manage backup node threads map',
                                                   add_help=False, allow_abbrev=False)

        # action flags are mutually exclusive
        action_group = node_threads_parser.add_mutually_exclusive_group(
            required=True)
        action_group.add_argument(
            '--get', action='store_true', help='Get the node threads map')
        action_group.add_argument(
            '--set', action='store_true', help='Set a new node threads map')
        action_group.add_argument(
            '--add', action='store_true', help='Add an existing node threads map')
        action_group.add_argument('-h', '--help', action=CBHelpAction, klass=self,
                                  help="Prints the short or long help message")

        options = node_threads_parser.add_argument_group(
            'Node threads map options')
        options.add_argument('--node', action='append', help='Node UUID')
        options.add_argument('--threads', action='append', type=int, help='Number of threads')

    @rest_initialiser(version_check=True, enterprise_check=True, cluster_init_check=True)
    def execute(self, opts):
        """Run the backup node threads map command"""
        if opts.get:
            self.get_node_threads_map()
        elif opts.set:
            self.post_node_threads_map(opts.node, opts.threads)
        elif opts.add:
            self.patch_node_threads_map(opts.node, opts.threads)

    def get_node_threads_map(self):
        """Get the node threads map"""
        node_threads_map, errors = self.rest.get_backup_node_threads_map()

        if errors:
            _exit_if_errors(errors)
        if not node_threads_map:
            print('No node threads map found')
            return
        self.human_print_node_threads_map(node_threads_map)

    def post_node_threads_map(self, nodes: List[str], threads: List[int]):
        """Post a new node threads map"""
        if not nodes or not threads:
            _exit_if_errors(['--node and --threads are required'])

        if len(nodes) != len(threads):
            _exit_if_errors(['--node and --threads must have the same number of arguments'])

        threads_map = dict(zip(nodes, threads))
        data = {"nodes_threads_map": threads_map}

        _, errors = self.rest.post_backup_node_threads_map(data)
        _exit_if_errors(errors)
        _success('Set node threads map')

    def patch_node_threads_map(self, nodes: List[str], threads: List[int]):
        """Patch an existing node threads map"""
        if not nodes or not threads:
            _exit_if_errors(['--node and --threads are required'])

        if len(nodes) != len(threads):
            _exit_if_errors(['--node and --threads must have the same number of arguments'])

        threads_map = dict(zip(nodes, threads))
        data = {"nodes_threads_map": threads_map}

        _, errors = self.rest.patch_backup_node_threads_map(data)
        _exit_if_errors(errors)
        _success('Updated node threads map')

    @staticmethod
    def human_print_node_threads_map(node_threads_map: Dict[str, any]):
        """Print the node threads map in a human friendly way"""
        node_pad = 5
        threads_pad = 7
        for node, threads in node_threads_map.items():
            if len(node) > node_pad:
                node_pad = len(node)
            threads_str = json.dumps(threads)
            if len(threads_str) > threads_pad:
                threads_pad = len(threads_str)

        node_pad += 1
        threads_pad += 1
        header = f'{"Node UUID":<{node_pad}} | {"Threads":<{threads_pad}}'
        print(header)
        print('-' * (len(header) + 5))
        for node, threads in node_threads_map.items():
            print(f'{node:<{node_pad}} | {threads:<{threads_pad}}')


def compare_versions(version1, version2: string) -> int:
    if version1 == "" or version1 == VERSION_UNKNOWN:
        version1 = LATEST_VERSION

    if version2 == "" or version2 == VERSION_UNKNOWN:
        version2 = LATEST_VERSION

    # Split the input strings into lists of integers
    v1_numbers = list(map(int, version1.split('.')))
    v2_numbers = list(map(int, version2.split('.')))

    # Compare each segment of the version numbers
    for v1, v2 in zip(v1_numbers, v2_numbers):
        if v1 > v2:
            return 1
        elif v1 < v2:
            return -1

    return 0
