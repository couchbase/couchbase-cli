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
import base64
import simplejson as json
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

    def setParam(self, param, value):
        self.params[param] = value

    def delParam(self, param):
        del self.params[param]

    def getParam(self, param):
        if self.params[param]:
            return self.params[param]

    def setServer(self, server):

        # set value of private server member

        self.server = server

    def getServer(self):

        # get value of private server member

        return self.server

    def bootStrap(self, headers):
        self.conn.request('GET', '/pools','',headers)
        data = ''
        response = self.conn.getresponse()
        if response.status == 200:
            data = response.read()
        elif response.status == 401:
            print 'Unable to log into REST API - check username and password!'
            sys.exit(2)
        else:
            print 'Error bootstrapping!'
            sys.exit(2)
        return

    def sendCmd(self,
                method,
                uri,
                user='',
                password='',
                params = {}):
        data = ''
        self.method = method
        self.params = params

        self.uri = uri
        headers = {}
        encoded_params = ''

        if user and password:
            auth = 'Basic '  + string.strip(
                        base64.encodestring(user + ':' + password))
            headers['Authorization'] = auth

        # this step must always be performed
        self.bootStrap(headers)


        # send the request to the server

        if self.method == 'POST':
            encoded_params = urllib.urlencode(self.params)
            headers['Content-type'] = 'application/x-www-form-urlencoded'

        if self.debug:
            print "METHOD: %s" % self.method
            print "PARAMS: ", params
            print "ENCODED_PARAMS: %s" % encoded_params
            print "REST CMD: %s %s" % (self.method,self.uri)

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
