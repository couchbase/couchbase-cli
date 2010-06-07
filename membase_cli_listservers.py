"""
  listservers class

  This class implements methods that will list servers within a 
  membase cluster

"""

import pprint
from membase_info import *
from membase_cli_rest_client import *

class Listservers:
  def __init__(self):
    """ 
      constructor
    """
    self.rest_cmd = '/pools/default'
    self.method = 'GET'
    
  def runCmd(self, cmd, cluster, user, password, opts):

    print "Running %s " % cmd 

    cluster, port = cluster.split(':')
    if not port:
      port = "8080";

    print "cluster: %s port: %s" % (cluster, port)

    rest = MembaseCliRestClient(cluster, port) 

    json = rest.sendCmd(self.method, self.rest_cmd); 

    #pp = pprint.PrettyPrinter(indent=4) 
    #pp.pprint(json)

    i = 1 
    print "List of servers within the cluster %s:%s" % (cluster,port)
    for node in json['nodes']:
      print "\t[%d]: %s" % (i,node['hostname'])
      i=i+1
