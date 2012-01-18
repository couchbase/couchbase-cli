#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  Provides info about a particular server.
"""

from usage import usage

import restclient
import simplejson
import subprocess
import sys

from uuid import uuid1

class Info:
    def __init__(self):
        self.debug = False

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
        if cmd == 'server-eshell':
            name = 'ctl-%s@127.0.0.1' % str(uuid1())
            p = subprocess.call(['erl','-name',name,
                '-setcookie',json['otpCookie'],'-hidden','-remsh',json['otpNode']])
        else:
            print simplejson.dumps(json, sort_keys=True, indent=2)
