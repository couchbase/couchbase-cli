"""
  membase_cli_rest_client

  Class with methods for contacting to a HTTP server, sending REST commands
  and processing the JSON response

"""

import sys, http.client, json, string
from StringIO import StringIO

class MembaseCliRestClient:

  def __init__(self, server, cmd):
    # do something here?
    this.server = server
    this.cmd = cmd
    

  def setServer(server):
    # set value of private server member
    this.server = server

  def getServer():
    # get value of private server member
    return this.server

  def sendRequest(cmd = this.cmd):
    # send the request to the server

  def getResponse():

  def processJson():

