#!/usr/bin/env python3

import copy
import logging
import optparse
import os
import random
import sqlite3
import string
import sys
import threading

from typing import Optional, Union, Tuple, List, Dict

import pump
import pump_bfd
import pump_csv
import pump_cb
import pump_gen
import pump_mc
import pump_dcp

from pump import PumpingStation


def exit_handler(err: Optional[str]):
    if err:
        sys.stderr.write(str(err) + "\n")
        sys.exit(1)
    else:
        sys.exit(0)


class Transfer:
    """Base class for 2.0 Backup/Restore/Transfer."""

    def __init__(self):
        self.name = "cbtransfer"
        self.source_alias = "source"
        self.sink_alias = "destination"
        self.usage = \
            "%prog [options] source destination\n\n" \
            "Transfer couchbase cluster data from source to destination.\n\n" \
            "Examples:\n" \
            "  %prog http://SOURCE:8091 /backups/backup-42\n" \
            "  %prog /backups/backup-42 http://DEST:8091\n" \
            "  %prog /backups/backup-42 couchbase://DEST:8091\n" \
            "  %prog http://SOURCE:8091 http://DEST:8091\n" \
            "  %prog couchstore-files:///opt/couchbase/var/lib/couchbase/data/ /backup-XXX\n" \
            "  %prog couchstore-files:///opt/couchbase/var/lib/couchbase/data/ couchbase://DEST:8091\n"

    def main(self, argv, opts_etc=None):
        if threading.currentThread().getName() == "MainThread":
            threading.currentThread().setName("mt")

        err, opts, source, sink = self.opt_parse(argv)
        if err:
            return err

        if opts_etc:
            opts.etc = opts_etc  # Used for unit tests, etc.

        process_name = f'{os.path.basename(argv[0])}-{"".join(random.sample(string.ascii_letters, 16))}'
        setattr(opts, "process_name", process_name)

        logging.info(f'{self.name}...')
        logging.info(f' source : {source}')
        logging.info(f' sink   : {sink}')
        logging.info(f' opts   : {opts.safe}')

        source_class, sink_class = self.find_handlers(opts, source, sink)
        if not source_class:
            return f'error: unknown type of source: {source}'
        if not sink_class:
            return f'error: unknown type of sink: {sink}'
        err = sink_class.check_source(opts, source_class, source, sink_class, sink)
        if err:
            return err

        try:
            pumpStation = pump.PumpingStation(opts, source_class, source,
                                              sink_class, sink)
            rv = pumpStation.run()
            self.aggregate_stats(pumpStation.cur)
            return rv
        except KeyboardInterrupt:
            return "interrupted."

    def aggregate_stats(self, cur):
        return 0

    def check_opts(self, opts):
        return None

    def opt_parse(self, argv):
        p = self.opt_parser()
        opts, rest = p.parse_args(argv[1:])
        if len(rest) != 2:
            p.print_help()
            return f'\nError: please provide both a {self.source_alias} and a {self.sink_alias}', None, None, None

        err = self.check_opts(opts)  # pylint: disable=assignment-from-none
        if err:
            return err, None, None, None

        min_thread = 1
        max_thread = 20
        if opts.threads not in list(range(min_thread, max_thread)):
            return f'\nError: option -t: value is out of range [{min_thread}, {max_thread}]', None, None, None

        if opts.username is None:
            username = os.environ.get('CB_REST_USERNAME', None)
            if username:
                opts.username = username
            else:
                return "\nError: option -u/--username is required", None, None, None
        if opts.password is None:
            password = os.environ.get('CB_REST_PASSWORD', None)
            if password:
                opts.password = password
            else:
                return "\nError: option -p/--password is required", None, None, None

        opts.extra = opt_parse_extra(opts.extra, self.opt_extra_defaults())
        opts.safe = opt_parse_helper(opts)

        return None, opts, rest[0], rest[1]

    def opt_parser(self):
        p = optparse.OptionParser(usage=self.usage)
        opt_extra_help(p, self.opt_extra_defaults(False))

        self.opt_parser_options(p)
        return p

    def opt_parser_options(self, p):
        p.add_option("-b", "--bucket-source",
                     action="store", type="string", default=None,
                     help="""Single named bucket from source cluster to transfer""")
        p.add_option("-B", "--bucket-destination",
                     action="store", type="string", default=None,
                     help="""Single named bucket on destination cluster which receives transfer.
This allows you to transfer to a bucket with a different name
as your source bucket. If you do not provide defaults to the
same name as the bucket-source""")
        self.opt_parser_options_common(p)
        p.add_option("", "--single-node",
                     action="store_true", default=False,
                     help="""Transfer from a single server node in a source cluster,
This single server node is a source node URL""")
        p.add_option("", "--source-vbucket-state",
                     action="store", type="string", default='active',
                     help="""Only transfer from source vbuckets in this state,
such as 'active' (default) or 'replica'.
Must be used with Couchbase cluster as source""")
        p.add_option("", "--destination-vbucket-state",
                     action="store", type="string", default='active',
                     help="""Only transfer to destination vbuckets in this state,
such as 'active' (default) or 'replica'.
Must be used with Couchbase cluster as source""")
        p.add_option("", "--destination-operation",
                     action="store", type="string", default=None,
                     help="""Perform this operation on transfer.
'set' will override an existing document,
'add' will not override, 'get' will load all keys transferred
from a source cluster into the caching layer at the destination""")

    def opt_parser_options_common(self, p):
        p.add_option("-i", "--id",
                     action="store", type="int", default=None,
                     help="""Transfer only items that match a vbucketID""")
        p.add_option("-k", "--key",
                     action="store", type="string", default=None,
                     help="""Transfer only items with keys that match a regexp""")
        p.add_option("", "--vbucket-list",
                     action="store", type="string", default=None,
                     help=optparse.SUPPRESS_HELP)
        p.add_option("-n", "--dry-run",
                     action="store_true", default=False,
                     help="""No actual transfer; just validate parameters, files,
                             connectivity and configurations""")
        p.add_option("-u", "--username",
                     action="store", type="string", default=None,
                     help="REST username for source cluster or server node")
        p.add_option("-p", "--password",
                     action="store", type="string", default=None,
                     help="REST password for source cluster or server node")
        p.add_option("-U", "--username-dest",
                     action="store", type="string", default=None,
                     help="REST username for destination cluster or server node")
        p.add_option("-P", "--password-dest",
                     action="store", type="string", default=None,
                     help="REST password for destination cluster or server node")
        p.add_option("-s", "--ssl",
                     action="store_true", default=False,
                     help="Transfer data with SSL enabled")
        p.add_option("", "--no-ssl-verify", default=True, action="store_false",
                     help="Skips SSL verification of certificates against the CA")
        p.add_option("", "--cacert", dest="cacert", default=None, action="store",
                           help="Verifies the cluster identity with this certificate")
        p.add_option("-t", "--threads",
                     action="store", type="int", default=4,
                     help="""Number of concurrent workers threads performing the transfer""")
        p.add_option("-v", "--verbose",
                     action="count", default=0,
                     help="verbose logging; more -v's provide more verbosity. Max is -vvv")
        p.add_option("", "--silent", action="store_true", default=False,
                     help="""Reduce logging verbosity to only include errors""")
        p.add_option("-x", "--extra",
                     action="store", type="string", default=None,
                     help="""Provide extra, uncommon config parameters;
                             comma-separated key=val(,key=val)* pairs""")
        p.add_option("-c", "--collection",
                     help=optparse.SUPPRESS_HELP)
        p.add_option("", "--force-txn", default=False, action="store_true", help=optparse.SUPPRESS_HELP)

    def opt_extra_defaults(self, add_hidden=True):
        rv = {
            "batch_max_size":  (1000,   "Transfer this # of documents per batch"),
            "batch_max_bytes": (400000, "Transfer this # of bytes per batch"),
            "cbb_max_mb":      (100000, "Split backup file on destination cluster if it exceeds MB"),
            "max_retry":       (10,     "Max number of sequential retries if transfer fails"),
            "report":          (5,      "Number batches transferred before updating progress bar in console"),
            "report_full":     (2000,   "Number batches transferred before emitting progress information in console"),
            "recv_min_bytes":  (4096,   "Amount of bytes for every TCP/IP call transferred"),
            "try_xwm":         (1,      "Transfer documents with metadata. 0 should only be used if you transfer from 1.8.x to 1.8.x"),
            "nmv_retry":       (1,      "0 or 1, where 1 retries transfer after a NOT_MY_VBUCKET message"),
            "rehash":          (0,      "For value 1, rehash the partition id's of each item; \
this is needed when transferring data between clusters with different number of partitions, \
such as when transferring data from an OSX server to a non-OSX cluster"),
            "data_only":       (0,      "For value 1, only transfer data from a backup file or cluster"),
            "design_doc_only": (0,      "For value 1, transfer design documents only from a backup file or cluster"),
            "conflict_resolve":(1,      "By default, enable conflict resolution."),
            "seqno":           (0,      "By default, start seqno from beginning."),
            "mcd_compatible":  (1,      "For value 0, display extended fields for stdout output."),
            "uncompress":      (0,      "For value 1, restore data in uncompressed mode"),
            "backoff_cap":     (10,     "Max backoff time during rebalance period"),
            "flow_control":    (1,      "For value 0, disable flow control to improve throughput"),
            "dcp_consumer_queue_length": (1000,"A DCP client needs a queue for incoming documents/messages. A large length is more efficient, but memory proportional to length*avg. doc size. Below length 150, performance degrades significantly."),
            }

        if add_hidden:
            rv["allow_recovery_vb_remap"] = (0, "Allows the vbucket list to override the vbucket map from the server.")

        return rv

    def find_handlers(self, opts, source, sink):
        return (PumpingStation.find_handler(opts, source, SOURCES),
                PumpingStation.find_handler(opts, sink, SINKS))


class Backup(Transfer):
    """Entry point for 2.0 cbbackup."""

    def __init__(self):
        self.name = "cbbackup"
        self.source_alias = "source"
        self.sink_alias = "backup_dir"

        if self._is_enterprise():
            self.usage = \
                "%prog [options] source backup_dir\n\n" \
                "Online backup of a couchbase cluster or server node.\n\n" \
                "Examples:\n" \
                "   The first backup to a given directory is a full backup, any subsequent ones are incremental.\n" \
                "       %prog -u Administrator -p password http://HOST:8091 /backup-42\n\n" \
                "   To take a differential backup after taking a full backup. \n" \
                "       %prog -u Administrator -p password couchbase://HOST:8091 /backup-43 -m diff\n\n" \
                "   To take an accumulative backup after taking a full backup. \n" \
                "       %prog -u Administrator -p password couchbase://HOST:8091 /backup-43 -m accu --single-node\n\n" \
                "Note: A full backup task is always triggered for a new sink location\n" \
                "   no matter what backup mode is specified.\n"
        else:
            self.usage = \
                "%prog [options] source backup_dir\n\n" \
                "Online backup of a couchbase cluster or server node.\n\n" \
                "Examples:\n" \
                "   Take a full backup of a cluster. \n" \
                "       %prog -u Administrator -p password http://HOST:8091 /backup-42\n\n" \
                "   Take a full backup for a single node. \n" \
                "       %prog -u Administrator -p password couchbase://HOST:8091 /backup-43 --single-node\n" \


    def opt_parser_options(self, p):
        p.add_option("-b", "--bucket-source",
                     action="store", type="string", default=None,
                     help="""single bucket from source to backup""")
        p.add_option("", "--single-node",
                     action="store_true", default=False,
                     help="""use a single server node from the source only,
                             not all server nodes from the entire cluster;
                             this single server node is defined by the source URL""")
        if self._is_enterprise():
            p.add_option("-m", "--mode",
                        action="store", type="string", default="diff",
                        help="backup mode: full, diff or accu [default:%default]")

        else:
            p.add_option("-m", "--mode",
                        action="store", type="string", default="full",
                        help=optparse.SUPPRESS_HELP)

        Transfer.opt_parser_options_common(self, p)

    def find_handlers(self, opts, source, sink):
        return PumpingStation.find_handler(opts, source, SOURCES), \
               PumpingStation.find_handler(opts, sink, SINKS)

    def check_opts(self, opts):
        mode = getattr(opts, "mode", None)
        if mode:
            if mode not in ["full", "diff", "accu"]:
                return "\nError: option mode has to be 'full', 'diff' or 'accu'"
        return None

    def _is_enterprise(self):
        try:
            import pump_bfd2
            return True
        except ImportError:
            return False


class Restore(Transfer):
    """Entry point for 2.0 cbrestore."""

    # TODO: (1) Restore - opt_parse handle 1.8 backwards compatible args.

    def __init__(self):
        self.name = "cbrestore"
        self.source_alias = "backup_dir"
        self.sink_alias = "destination"
        self.usage = \
            "%prog [options] backup_dir destination\n\n" \
            "Restores a single couchbase bucket.\n\n" \
            "Please first create the destination / bucket before restoring.\n\n" \
            "Examples:\n" \
            "  %prog /backups/backup-42 http://HOST:8091 \\\n" \
            "    --bucket-source=default --from-date=2014-01-20 --to-date=2014-03-31\n" \
            "  %prog /backups/backup-42 couchbase://HOST:8091 \\\n" \
            "    --bucket-source=default\n" \
            "  %prog /backups/backup-42 memcached://HOST:11211 \\\n" \
            "    --bucket-source=sessions --bucket-destination=sessions2"

    def opt_parser_options(self, p):
        p.add_option("-a", "--add",
                     action="store_true", default=False,
                     help="""use add instead of set to not overwrite existing
                             items in the destination""")
        p.add_option("-b", "--bucket-source",
                     action="store", type="string", default=None,
                     help="""single bucket from the backup_dir to restore;
                             if the backup_dir only contains a single bucket,
                             then that bucket will be automatically used""")
        p.add_option("-B", "--bucket-destination",
                     action="store", type="string", default=None,
                     help="""when --bucket-source is specified, overrides the
                             destination bucket name; this allows you to restore
                             to a different bucket; defaults to the same as the
                             bucket-source""")
        p.add_option("", "--from-date",
                    action="store", type="string", default=None,
                    help="""restore data from the date specified as yyyy-mm-dd. By default,
all data from the very beginning will be restored""")
        p.add_option("", "--to-date",
                    action="store", type="string", default=None,
                    help="""restore data till the date specified as yyyy-mm-dd. By default,
all data that are collected will be restored""")
        Transfer.opt_parser_options_common(self, p)

        # TODO: (1) cbrestore parameter --create-design-docs=y|n
        # TODO: (1) cbrestore parameter -d DATA, --data=DATA
        # TODO: (1) cbrestore parameter --validate-only
        # TODO: (1) cbrestore parameter -H HOST, --host=HOST
        # TODO: (1) cbrestore parameter -p PORT, --port=PORT
        # TODO: (1) cbrestore parameter option to override expiration?

    def find_handlers(self, opts, source, sink):
        return pump_bfd.BFDSource, PumpingStation.find_handler(opts, sink, SINKS)


# --------------------------------------------------

def opt_parse_helper(opts):
    logging_level = logging.WARN
    if opts.verbose >= 1:
        logging_level = logging.INFO
    if opts.verbose >= 2:
        logging_level = logging.DEBUG

    if opts.silent:
        logging_level = logging.ERROR

    logging.basicConfig(format=pump.LOGGING_FORMAT, level=logging_level)

    opts_x = copy.deepcopy(opts)
    if opts_x.username:
        opts_x.username = "<xxx>"
    if opts_x.password:
        opts_x.password = "<xxx>"
    return opts_x

def opt_parse_extra(extra, extra_defaults):
    """Convert an extra string (comma-separated key=val pairs) into
       a dict, using default values from extra_defaults dict."""
    extra_in = dict([(x[0], x[1]) for x in
                     [(kv + '=').split('=') for kv in
                      (extra or "").split(',')]])
    for k, v in extra_in.items():
        if k and not extra_defaults.get(k):
            sys.exit("error: unknown extra option: " + k)
    return dict([(k, float(extra_in.get(k, extra_defaults[k][0])))
                 for k in extra_defaults.keys()])

def opt_extra_help(parser, extra_defaults):
    extra_help = "; ".join([f'{k}={extra_defaults[k][0]} ({extra_defaults[k][1]})'
                           for k in sorted(extra_defaults.keys())])

    group = optparse.OptionGroup(parser, "Available extra config parameters (-x)",
                        extra_help)
    parser.add_option_group(group)

# --------------------------------------------------

SOURCES = [pump_bfd.BFDSource,
           pump_csv.CSVSource,
           pump_gen.GenSource,
           pump_dcp.DCPStreamSource,
           pump.StdInSource]

SINKS = [pump_bfd.BFDSink,
         pump_mc.MCSink,
         pump_cb.CBSink,
         pump_csv.CSVSink,
         pump.StdOutSink]

try:
    import pump_sfd
    SOURCES.append(pump_sfd.SFDSource)
    SINKS.append(pump_sfd.SFDSink)
except ImportError:
    pass

try:
    import pump_json
    SOURCES.append(pump_json.JSONSource)
except ImportError:
    pass

try:
    import pump_bfd2
    SINKS.insert(0, pump_bfd2.BFDSinkEx)
except ImportError:
    pass

# TODO: (1) pump_transfer - use QUIET commands
# TODO: (1) pump_transfer - verify that nth replica got the msg
# TODO: (1) pump_transfer - ability to TAP a non-active or replica vbucket / MB-4583
# TODO: (10) pump_transfer - incremental backup/restore

if __name__ == '__main__':
    sys.exit(Transfer().main(sys.argv))
