#!/usr/bin/env python
"""
  membase.py

  This program is the top level source file for the Membase Command Line Tools

"""
import getopt,sys
from membase_cli_* import *


def usage():
  print >> sys.stderr, "Usage: membase command [OPTIONS]"

if __name__ == "__main__":
  try:
    optlist, args = getopt.getopt(sys.argv[2:], 'b:c:e:gdp:c:p:s:')
    cmd = sys.argv[1]
    print "command: %s" % cmd 
    optlist
  except  getopt.GetoptError, err:
    usage()
    sys.exit(2)

  commands = { 'listservers' : Listservers}

  if cmd not in commands:
    usage()

  taskrunner = commands[cmd](optlist)
  taskrunner.runCmd(cmd)


