"""
  membase_info.py

  Contains functions such as usage()

"""

import sys

def usage(error_msg = ""):
  print >> sys.stderr, ""
  if error_msg:
    print >> sys.stderr, "ERROR: %s" % error_msg
  print >> sys.stderr, ""

  print >> sys.stderr, "Membase Commandline Tool Set"
  print >> sys.stderr, "Command line interface to Membase cluster management via REST"
  print >> sys.stderr, ""
  print >> sys.stderr, "Usage: membase command [OPTIONS]"
  print >> sys.stderr, ""
  print >> sys.stderr, "commands:"
  print >> sys.stderr, "  server-list   Provide a list of membase servers within a given cluster"
  print >> sys.stderr, "  bucket-list   Provide a list of buckets within a given cluster"
  print >> sys.stderr, "  bucket-flush  Flush a given bucket"
  print >> sys.stderr, ""
  print >> sys.stderr, "OPTIONS:"
  print >> sys.stderr, "  -c, --cluster [=host:port]"
  print >> sys.stderr, "  -p, --password[=password]"
  print >> sys.stderr, "  -u, --user[=username]"
  print >> sys.stderr, ""
  print >> sys.stderr, "EXAMPLES:"
  print >> sys.stderr, "  membase server-list -c 192.168.0.1:8080"
  print >> sys.stderr, ""
  sys.exit(2)
