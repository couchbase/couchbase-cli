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

class Info:
    def __init__(self):
        self.debug = False

    def _remoteShellName(self):
        tmp = ''.join(random.choice(string.ascii_letters) for i in xrange(20))
        return 'ctl-%s@127.0.0.1' % tmp

    def runCmd(self, cmd, server, port,
               user, password, opts):
        for (o, a) in opts:
            if o == '-d' or o == '--debug':
                self.debug = True

        rest = restclient.RestClient(server, port, {'debug':self.debug})
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
