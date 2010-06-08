"""
  membase_cli_rest_client

  Class with methods for contacting to a HTTP server, sending REST commands
  and processing the JSON response

"""

import sys, httplib, urllib, json, string
from StringIO import StringIO

class MembaseCliRestClient:

  def __init__(self, server, port):
    # do something here?
    self.server = server
    self.port = port
    self.uri = '/pools'  
    self.method = 'GET' # default 
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

  """
  This must be prior to any other command
  """
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

  """
  sendCmd

  sends the method and URI for a REST command to be run
  against, returns the resonse object
  """
  def sendCmd(self, method, uri):
    data = ""
    params = {}
    self.method = method
    self.uri = uri

    if self.method == 'POST':
      if self.params:
        params = urllib.urlencode(self.params)

    #print "PARAMS: ", params
    #print "REST CMD: %s %s" % (self.method,self.uri)

    # send the request to the server
    if self.method == 'GET':
      self.conn.request(self.method, self.uri)
    else:
      headers = { 
        'Content-type':'application/x-www-form-urlencoded',
      }
      self.conn.request(self.method, self.uri, params, headers)

    # obtain the response
    return self.conn.getresponse()

  """
  method for returning a python object from a json response
  Used with GET
  """
  def getJson(self,data):
    return json.loads(data)

  """
  method for returning json from a python object
  Used with POST
  """
  def jsonMessage(self,data):
    return json.JSONEncoder().encode(data)


