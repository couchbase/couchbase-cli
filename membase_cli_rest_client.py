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
    self.conn = httplib.HTTPConnection(server, int(port))
    self.bootStrap()

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
    # send the request to the server
    self.conn.request(self.method, self.cmd)
    # obtain the response
    response = self.conn.getresponse()

    if response.status == 200:
      data = response.read()
    else :
      print "Error!"
      sys.exit(2)

    return json.loads(data)


