#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  Provides info about a particular server.
"""

import json
import restclient
import subprocess

import os
import sys
import string
import random
import util_cli as util

class Info:
    def __init__(self):
        self.debug = False

    def _remoteShellName(self):
        tmp = ''.join(random.choice(string.ascii_letters) for i in xrange(20))
        return 'ctl-%s@127.0.0.1' % tmp

    def runCmd(self, cmd, server, port,
               user, password, ssl, opts):
        vm = 'ns_server'
        for (o, a) in opts:
            if o == '-d' or o == '--debug':
                self.debug = True

            if o == '--vm':
                vm = a

        rest = util.restclient_factory(server, port, {'debug':self.debug}, ssl)
        opts = {'error_msg': 'server-info error'}

        data = rest.restCmd('GET', '/nodes/self',
                            user, password, opts)

        json = rest.getJson(data)

        for x in ['license', 'licenseValid', 'licenseValidUntil']:
            if x in json:
                del(json[x])

        if cmd == 'server-eshell':
            node = json['otpNode']
            cookie = json['otpCookie']

            if vm != 'ns_server':
                rest = util.restclient_factory(server, port, {'debug':self.debug}, ssl)
                rest.setPayload('ns_server:get_babysitter_cookie().')
                cookie = rest.sendCmd('POST', '/diag/eval', user, password).read()

                [short, _] = node.split('@')

                if vm == 'babysitter':
                    node = 'babysitter_of_%s@127.0.0.1' % short
                elif vm == 'couchdb':
                    node = 'couchdb_%s@127.0.0.1' % short
                else:
                    raise ValueError("unknown vm type \'%s\'" % vm)

            name = self._remoteShellName()
            p = subprocess.call([self.getErlPath(), '-name', name, '-setcookie',
                                 cookie, '-hidden', '-remsh', node])
        elif cmd == 'get-server-info':
            return json
        else:
            print json.dumps(json, sort_keys=True, indent=2)

    def getErlPath(self):
        bin = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), '..', '..', 'bin')
        cb_erl = os.path.join(bin, 'erl')
        if os.path.isfile(cb_erl):
            return cb_erl
        else:
            print "WARNING: Cannot locate Couchbase erlang. Attempting to use non-Couchbase erlang."
            return 'erl'

    def getCommandSummary(self, cmd):
        """Return one-line summary info for each supported command"""
        command_summary = {
            "server-info" : "show details on one server"}
        if cmd in command_summary:
            return command_summary[cmd]
        else:
            return None

    def getCommandExampleHelp(self, cmd):
        """ Obtain detailed example help for command
        Returns a list of command examples to illustrate how to use command
        or None if there's no example help or cmd is unknown.
        """

        if cmd == "server-info":
            return [("Server information",
"""
    couchbase-cli server-info -c 192.168.0.1:8091""")]
        else:
            return None
