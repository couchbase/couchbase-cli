"""
  membase_cli_rest_client

  Class with methods for contacting to a HTTP server, sending REST commands
  and processing the JSON response

"""

import sys, httplib, urllib, json, string
from StringIO import StringIO

class MembaseCliRestClient:

  def __init__(self, server, port, method, cmd):
    # do something here?
    self.server = server
    self.port = port
    self.cmd = cmd
    self.method = method
    self.params = {}
    self.conn = httplib.HTTPConnection(server, int(port))
    self.bootStrap()

  def setParam(self,param, value):
    self.params[param]= value

  def getParam(self,param):
    if self.params[param]:
      return self.params[param]

  def setServer(self,server):
    # set value of private server member
    self.server = server

  def getServer(self):
    # get value of private server member
    return self.server

  def bootStrap(self):
    self.conn.request('GET', '/pools')
    data = ""
    response = self.conn.getresponse()
    if response.status == 200:
      data = response.read()
    else :
      print "Error!"
      sys.exit(2)
    return

  def sendCmd(self):
    data = ""
    params = {}

    if self.method == 'POST':
      if self.params:
        params = urllib.urlencode(self.params)

    print "PARAMS: ", params
    print "REST CMD: %s %s" % (self.method,self.cmd)

    # send the request to the server
    if self.method == 'GET':
      self.conn.request(self.method, self.cmd)
    else:
      headers = { 
        'Content-type':'application/x-www-form-urlencoded',
        'Accept':'text/plain'
      }
      self.conn.request(self.method, self.cmd, params, headers)

    # obtain the response
    response = self.conn.getresponse()

    if self.cmd == 'bucketflush':
      if response.status == 204:
        data = "Success! %s flushed" % self.getparam('bucket')
      else:
        data = "Error: unable to flush %s" % self.getparam('bucket')
    else:
      if response.status == 200:
        data = response.read()
      else :
        print "Error! ", response.status, response.reason
        sys.exit(2)

    return json.loads(data)
