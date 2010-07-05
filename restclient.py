#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  membase_cli_rest_client

  Class with methods for contacting to a HTTP server, sending REST commands
  and processing the JSON response

"""

import sys
import socket
import httplib
import urllib
import base64
import simplejson as json
import string
from StringIO import StringIO


class RestClient:

    def __init__(self, server, port, opts= {}):
        """
            __init__()
            This method is the instatiation method
            """

        # do something here?

        self.server = server
        self.port = port
        self.debug = opts.get('debug', False)
        self.uri = '/pools'
        self.method = 'GET'  # default
        self.params = {}
        self.user = ''
        self.password = ''
        self.clientConnect(server, int(port))

    def clientConnect(self, server, port):
        error_connect = "Unable to connect to %s" % self.server
        try:
            self.conn = httplib.HTTPConnection(server, port)
        except httplib.HTTPException:
            print error_connect
            sys.exit(2)
        except httplib.NotConnected:
            print error_connect
            sys.exit(2)
        except socket.error:
            print error_connect
            sys.exit(2)
        except socket.gaierror:
            print error_connect
            sys.exit(2)

    def setParam(self, param, value):
        """
            setParam()
            This method allows the caller to set parameters which are in
            turn later encoded for REST POST commands
            """

        self.params[param] = value

    def handleResponse(self,
                       method,
                       response,
                       opts={ 'success_msg':'',
                              'error_msg':''}):
        """
            handleResponse()
            This method handles the response, success or failure, and
            returns the necessary data to the caller
            """

        error = False
        if response.status == 200 or response.status == 204:
            if method == 'GET':
                data = response.read()
            else:
                data = "SUCCESS: %s" % opts['success_msg']
        elif response.status == 401:
            error = True
            data = 'ERROR: Unable to access the REST API - check username and password!'
        else:
            error = True
            data = 'ERROR: %s %s' % (opts['error_msg'], response.reason)

        if error:
            print data
            sys.exit(2)

        return data

    def bootStrap(self, headers):
        """
            bootStrap()
            This method produces the initial access required for subsequent
            REST commands to function properly
            """

        opts = {'error_msg':'Unable to bootstrap'}
        self.conn.request('GET', '/pools', '', headers)
        response = self.conn.getresponse()
        return self.handleResponse('GET', response, opts)

    def sendCmd(self,
                method,
                uri,
                user='',
                password='',
                opts = {}):
        data = ''
        headers = {}
        encoded_params = ''
        """
            sendCmd()
            This method handles accessing the REST API and returning
            either data, if a GET, or a success or error message if a POST
            """

        if user and password:
            self.user = user
            self.password = password
            auth = 'Basic '  + string.strip(
                        base64.encodestring(user + ':' + password))
            headers['Authorization'] = auth

        # this step must always be performed

        self.bootStrap(headers)

        # send the request to the server

        if method == 'POST':
            encoded_params = urllib.urlencode(self.params)
            headers['Content-type'] = 'application/x-www-form-urlencoded'
        else:
            if self.params:
                uri = uri, '?', urllib.urlencode(self.params)

        if self.debug:
            print "METHOD: %s" % method
            print "PARAMS: ", self.params
            print "ENCODED_PARAMS: %s" % encoded_params
            print "REST CMD: %s %s" % (method,uri)

        self.makeRequest(method, uri, encoded_params, headers)

        # obtain the response

        response = self.conn.getresponse()
        if self.debug:
            print "response.status: %s" % response.status
        return response

    def makeRequest(self, method, uri, encoded_params, headers):
        """
            makeRequest()
            This method handles the attempt to connect to the REST API
            on the given server
            """

        error_connect = "Unable to connect to %s" % self.server
        try:
            self.conn.request(method, uri, encoded_params, headers)
        except httplib.HTTPException:
            print error_connect
            sys.exit(2)
        except httplib.NotConnected:
            print error_connect
            sys.exit(2)
        except socket.error:
            print error_connect
            sys.exit(2)
        except socket.gaierror:
            print error_connect
            sys.exit(2)


    def getJson(self, data):
        return json.loads(data)

    def jsonMessage(self, data):
        return json.JSONEncoder().encode(data)

    def restCmd(self, method, uri, user='', password='', opts={}):
        """
            handlePostResponse - this handles the response
            from a POST according to the operation/cmd run

            """

        if method == None:
            method = 'GET'

        response = self.sendCmd(method,
                                uri,
                                user,
                                password,
                                opts)

        return self.handleResponse(method, response, opts)
