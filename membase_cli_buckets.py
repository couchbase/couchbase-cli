"""
  listservers class

  This class implements methods that will list servers within a 
  membase cluster

"""

import pprint
from membase_info import *
from membase_cli_rest_client import *

rest_cmds = { 
  'listbuckets':'/pools/default/buckets',
  'bucketflush':'/pools/default/buckets/',
  'bucketdelete':'/pools/default/buckets/',
  'bucketcreate':'/pools/default/buckets/'
}
methods = { 
  'listbuckets':'GET',
  'bucketlist':'GET',
  'bucketdelete':'DELETE',
  'bucketcreate':'POST',
  'bucketflush':'POST',
  'bucketstats':'GET'
}

class Buckets:
  def __init__(self):
    """ 
      constructor
    """
    # default
    self.rest_cmd = rest_cmds['listbuckets']

    # default
    self.method = 'GET'
    
  def runCmd(self, cmd, cluster, user, password, opts):
    # default
    bucketname= ''
    cachesize= ''

    # set standard opts
    for o, a in opts:
      if o == "-b" or o == "--buckets":
        bucketname = a
      if o == "-s" or o == "--size":
        cachesize = a

    # allow user to be lazy and not specify port
    cluster, port = cluster.split(':')
    if not port:
      port = "8080";

    rest = MembaseCliRestClient(cluster, port) 
    self.rest_cmd = rest_cmds[cmd]

    # get the parameters straight
    if (cmd == 'bucketdelete' or
       cmd == 'bucketcreate' or
       cmd == 'bucketflush') :
      #if len(bucketname):
      #  rest.setParam('name', bucketname)
      if len(cachesize):
        rest.setParam('cacheSize', cachesize)
      self.rest_cmd = self.rest_cmd + bucketname
      if cmd == 'bucketflush':
        self.rest_cmd = self.rest_cmd + '/controller/doFlush'


    print "cluster: %s port: %s" % (cluster, port)
    print "REST URI:" , self.rest_cmd
    print "Running %s " % cmd 

    json = rest.sendCmd(methods[cmd], self.rest_cmd); 

    # debug
    pp = pprint.PrettyPrinter(indent=4) 
    pp.pprint(json)

    if methods[cmd] == 'GET':
      i = 1 
      print "List of servers within the cluster %s:%s" % (cluster,port)
      for bucket in json:
        print "\t[%d]: %s" % (i,bucket['name'])
        i=i+1
    else:
      print "JSON: ", json
