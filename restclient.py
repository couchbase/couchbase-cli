#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  membase_cli_rest_client

  Class with methods for contacting to a HTTP server, sending REST commands
  and processing the JSON response

"""

import sys
import httplib
import urllib
import json
import string
from StringIO import StringIO


class RestClient:

    def __init__(self, server, port, opts= {}):

        # do something here?

        self.server = server
        self.port = port
        self.debug = opts.get('debug', False)
        self.uri = '/pools'
        self.method = 'GET'  # default
        self.params = {}
        self.conn = httplib.HTTPConnection(server, int(port))
        self.bootStrap()

    def setParam(self, param, value):
        self.params[param] = value

    def delParam(self, param):
        del self.params[param]
        print "DELPARAMS: ", self.params

    def getParam(self, param):
        if self.params[param]:
            return self.params[param]

    def setServer(self, server):

        # set value of private server member

        self.server = server

    def getServer(self):

        # get value of private server member

        return self.server

    def bootStrap(self):
        self.conn.request('GET', '/pools')
        data = ''
        response = self.conn.getresponse()
        if response.status == 200:
            data = response.read()
        else:
            print 'Error!'
            sys.exit(2)
        return

    def sendCmd(self, method, uri, params = {}):
        data = ''
        self.method = method
        self.params = params
        self.uri = uri

        if self.method == 'POST':
            encoded_params = urllib.urlencode(self.params)

        if self.debug:
            print "PARAMS: ", params
            print "REST CMD: %s %s" % (self.method,self.uri)

        # send the request to the server

        if self.method == 'GET':
            self.conn.request(self.method, self.uri)
        else:
            headers = \
                {'Content-type': 'application/x-www-form-urlencoded'}
            self.conn.request(self.method, self.uri, encoded_params, headers)

        # obtain the response

        response = self.conn.getresponse()
        if self.debug:
            print "response.status: %s" % response.status
        return response

    def getJson(self, data):
        return json.loads(data)

    def jsonMessage(self, data):
        return json.JSONEncoder().encode(data)
