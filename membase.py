#!/usr/bin/env python
"""
  membase.py

  This program is the top level source file for the Membase Command Line Tools

"""
import getopt,sys
from membase_cli_listservers import *
from membase_info import *



if __name__ == "__main__":
  try:
    opts, args = getopt.getopt(sys.argv[2:], 'b:c:e:gdp:c:hp:s:')
    cmd = sys.argv[1]
  except  getopt.GetoptError, err:
    usage()

  # check if usage specified
  for o, a in opts:
    if o == "-h":
      usage()

  # need to make this dynamic
  commands = { 'listservers' : Listservers}

  # make sure the command is defined
  if cmd not in commands:
    err_message= "command: '%s' not found" % cmd
    usage(err_message)

  # instantiate
  taskrunner = commands[cmd]()
  # call runCmd method
  taskrunner.runCmd(cmd, opts)


