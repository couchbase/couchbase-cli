"""
  listservers class

  This class implements methods that will list servers within a 
  membase cluster

"""

import pprint
from membase_info import *
from membase_cli_rest_client import *

class Listbuckets:
  def __init__(self):
    """ 
      constructor
    """
    self.rest_cmd = '/pools/default/buckets'
    self.method = 'GET'
    
  def runCmd(self, cmd, cluster, user, password, opts):

    print "Running %s " % cmd 

    cluster, port = cluster.split(':')
    if not port:
      port = "8080";

    print "cluster: %s port: %s" % (cluster, port)

    rest = MembaseCliRestClient(cluster, port, self.method, self.rest_cmd) 

    json = rest.sendCmd(); 

    #pp = pprint.PrettyPrinter(indent=4) 
    #pp.pprint(json)

    i = 1 
    print "List of servers within the cluster %s:%s" % (cluster,port)
    for bucket in json:
       print "\t[%d]: %s" % (i,bucket['name'])
       i=i+1
