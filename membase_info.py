"""
  membase_info.py

  Contains functions such as usage()

"""

import sys

def usage(error_msg = ""):
  if error_msg:
    print >> sys.stderr, "ERROR: %s" % error_msg

  print >> sys.stderr, "Usage: membase command [OPTIONS]"
  sys.exit(2)
