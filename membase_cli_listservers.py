"""
  listservers class

  This class implements methods that will list servers within a 
  membase cluster

"""
from membase_info import *
from membase_cli_rest_client import *

class Listservers:
  def __init__(self):
    """ 
      constructor
    """
    self.rest_cmd = '/pools/default'
    self.method = 'GET'
    
  def runCmd(self, cmd, opts):
    print "Running %s " % cmd

    # check if usage specified
    for o, a in opts:
      if o == "-s":
        server = a

      server, port = server.split(':')
      if not port:
        port = "8080";

      print "server: %s port: %s" % (server, port)

      rest = MembaseCliRestClient(server, port, self.method, self.rest_cmd) 

      json = rest.sendCmd(); 

