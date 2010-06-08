"""
  listservers class

  This class implements methods that will list servers within a 
  membase cluster

"""

import pprint
from membase_info import *
from membase_cli_rest_client import *

rest_cmds = { 
  'bucket-list'   :'/pools/default/buckets',
  'bucket-flush'  :'/pools/default/buckets/',
  'bucket-delete' :'/pools/default/buckets/',
  'bucket-create' :'/pools/default/buckets/'
}
methods = { 
  'bucket-list'   :'GET',
  'bucket-delete' :'DELETE',
  'bucket-create' :'POST',
  'bucket-flush'  :'POST',
  'bucket-stats'  :'GET'
}

class Buckets:
  def __init__(self):
    """ 
      constructor
    """
    # default
    self.rest_cmd = rest_cmds['bucket-list']

    # default
    self.method = 'GET'
    
  def runCmd(self, cmd, cluster, user, password, opts):
    # default
    bucketname= ''
    cachesize= ''
    standard_result= ''

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
    if (cmd == 'bucket-delete' or
       cmd == 'bucket-create' or
       cmd == 'bucket-flush') :
      if len(bucketname):
        rest.setParam('name', bucketname)
      if len(cachesize):
        rest.setParam('cacheSize', cachesize)
      self.rest_cmd = self.rest_cmd + bucketname
      if cmd == 'bucket-flush':
        self.rest_cmd = self.rest_cmd + '/controller/doFlush'

    response = rest.sendCmd(methods[cmd], self.rest_cmd); 

    if cmd == 'bucket-flush':
      if response.status == 204:
        data = "Success! %s flushed" % bucketname 
      else:
        data = "Error: unable to flush %s" % bucketname 
    else:
      if response.status == 200:
        data = response.read()
      else :
        print "Error! ", response.status, response.reason
        sys.exit(2)
        
    if methods[cmd] == 'GET':
      json = rest.getJson(data)   
      i = 1 
      print "List of buckets within the cluster %s:%s" % (cluster,port)
      for bucket in json:
        standard_result= standard_result + "\t[%d]: %s\n" % (i,bucket['name'])
        i=i+1
    else:
      standard_result = data
      json = rest.jsonMessage(data)   

    # debug
    #pp = pprint.PrettyPrinter(indent=4) 
    #pp.pprint(json)

    print standard_result

