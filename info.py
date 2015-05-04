#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  Provides info about a particular server.
"""

import restclient
import simplejson
import subprocess

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
        for (o, a) in opts:
            if o == '-d' or o == '--debug':
                self.debug = True

        rest = util.restclient_factory(server, port, {'debug':self.debug}, ssl)
        opts = {'error_msg': 'server-info error'}

        data = rest.restCmd('GET', '/nodes/self',
                            user, password, opts)

        json = rest.getJson(data)

        for x in ['license', 'licenseValid', 'licenseValidUntil']:
            if x in json:
                del(json[x])

        if cmd == 'get-server-info':
            return json
        elif cmd == 'server-eshell':
            name = self._remoteShellName()
            p = subprocess.call(['erl','-name',name,
                '-setcookie',json['otpCookie'],'-hidden','-remsh',json['otpNode']])
        else:
            print simplejson.dumps(json, sort_keys=True, indent=2)

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