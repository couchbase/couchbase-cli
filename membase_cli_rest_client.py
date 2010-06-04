"""
  membase_cli_rest_client

  Class with methods for contacting to a HTTP server, sending REST commands
  and processing the JSON response

"""

import sys, httplib, json, string
from StringIO import StringIO

class MembaseCliRestClient:

  def __init__(self, server, port, method, cmd):
    # do something here?
    self.server = server
    self.port = port
    self.cmd = cmd
    self.method = method
    self.url = 'http://' , self.server, ':', self.port , self.cmd
    print "URL: http://%s:%s%s" % (self.server,self.port,self.cmd)
    self.conn = httplib.HTTPConnection(server)

  def setServer(self,server):
    # set value of private server member
    self.server = server

  def getServer(self):
    # get value of private server member
    return self.server

  def sendRequest(self):
    # send the request to the server
    self.conn.request(self.method, self.url)

  def getResponse(self):
    response = self.conn.getresponse()
    data = ""

    if response.status == OK :
      data = response.read()
    else :
      print "Error!"
      sys.exit(2)
    
    return data

  def sendCmd(self):
    self.sendRequest()
    return self.getResponse()
    self.sendRequest()
    return self.getResponse()

  def processJson(data):
    return json.loads

