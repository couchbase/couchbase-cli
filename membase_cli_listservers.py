"""
  listservers class

  This class implements methods that will list servers within a 
  membase cluster

"""
from membase_info import *

class Listservers:
  def __init__(self):
    """ 
      constructor
    """
    self.rest_cmd = "/

  def runCmd(self, cmd, opts):
    print "Running %s " % cmd

    # check if usage specified
    for o, a in opts:
      if o == "-s":
        print "server: %s" % a
