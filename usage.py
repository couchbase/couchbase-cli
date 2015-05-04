#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

class Usage:
    def getCommandSummary(self, cmd):
        if cmd == "help":
            return "show longer usage/help and examples"
        return None

def command_error(err):
    """ Print an error and terminate with exit(2) """
    print "ERROR: %s" % (err)
    sys.exit(2)
    return

def usage_header():
    """ Print the CLI usage 'header' information. """
    cluster_options = """CLUSTER:
  --cluster=HOST[:PORT] or -c HOST[:PORT]"""

    global_options = """OPTIONS:
  -u USERNAME, --user=USERNAME      admin username of the cluster
  -p PASSWORD, --password=PASSWORD  admin password of the cluster
  -o KIND, --output=KIND            KIND is json or standard
  -d, --debug
  -s, --ssl                         uses SSL for communication with secure servers"""

    command_notes = """NOTE:
  USERNAME can be set in environment variable CB_REST_USERNAME and/or
  PASSWORD can be set in environment variable CB_REST_PASSWORD instead"""

    print "couchbase-cli - command-line cluster administration tool"
    print ""
    print "%s" % (cluster_options)
    print ""
    print "%s" % (global_options)
    print ""
    print "%s" % (command_notes)
    print ""

MIN_SPACES = 2

def command_usage(cmd, command_map):
    """ For the command 'cmd', use the command_map to find the class
        which owns that command and use getCommandHelp to obtain
        detailed parameter infomation for it.
    """
    c = command_map[cmd]()
    if hasattr(c, 'getCommandHelp'):
        # None is allowed as a return
        cmd_help = c.getCommandHelp(cmd)
        if cmd_help:

            longest_key = 0
            for arg_pair in cmd_help:
                if len(arg_pair[0]) > longest_key:
                    longest_key = len(arg_pair[0])

            print "%s OPTIONS:" % (cmd)
            for arg_pair in cmd_help:
                spaces = ' ' * (MIN_SPACES + (longest_key - len(arg_pair[0])))
                print "  %s%s%s" % (arg_pair[0], spaces, arg_pair[1])
            print ""



def command_summaries(command_map):
    """ For all commands in the command_map print a formatted
    block of text showing the command and one-line summary help.
    E.g.
    command1     Create a cluster.
    command2     Delete a cluster.
    """
    summary_help = {}
    longest_cmd = 0
    for cmd in command_map:
        c = command_map[cmd]()
        if hasattr(c, 'getCommandSummary'):
            if len(cmd) > longest_cmd:
                longest_cmd = len(cmd)
            summary =  c.getCommandSummary(cmd)
            # some commands have no summary (None returned)
            if summary:
                summary_help[cmd] = summary

    for cmd in sorted(summary_help):
        spaces = ' ' * (MIN_SPACES + (longest_cmd - len(cmd)))
        print "  %s%s%s" % (cmd, spaces, summary_help[cmd])

def command_example_usage(cmd, command_map):
    """ For the command 'cmd', use the command_map to find the class
        which owns that command and use getCommandExampleHelp to obtain
        detailed parameter infomation for it.
    """
    c = command_map[cmd]()
    if hasattr(c, 'getCommandExampleHelp'):
        # None is allowed as a return
        cmd_help = c.getCommandExampleHelp(cmd)
        if cmd_help:
            print ""
            print "%s EXAMPLES:" % (cmd)
            for arg_pair in cmd_help:
                print ""
                print "  %s:" % arg_pair[0]
                print "%s" % arg_pair[1]
            print ""

def one_command_usage(cmd, command_map):
    """ Print formatted usage information for the CLI and one chosen command.
    Function terminates the process with exit(2).
    """
    usage_header()
    command_usage(cmd, command_map)
    print "usage: couchbase-cli %s CLUSTER [OPTIONS]" % (cmd)
    command_example_usage(cmd, command_map)
    sys.exit(2)

def short_usage(command_map):
    """ Print formatted usage information showing commands and one-line summaries.
    Function terminates the process with exit(2).
    """
    usage_header()
    print "usage: couchbase-cli COMMAND CLUSTER [OPTIONS]"
    print ""
    print "COMMANDs include"
    command_summaries(command_map)
    print ""
    sys.exit(2)

def all_help(command_map):
    """
        Print everything, summaries, command help and examples.
        Note: Function exits process.
    """
    usage_header()

    print """COMMAND:"""
    command_summaries(command_map)

    print ""

    for cmd in sorted(command_map):
        command_usage(cmd, command_map)

    sys.exit(2)



