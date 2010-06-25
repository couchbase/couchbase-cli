#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
  info class

  This class implements methods that provide info about a
  particular server

"""

import pprint
from membase_info import usage
import restclient

class Info:

    def __init__(self):
        """
      constructor
    """
        # defaults
        self.debug = False
        self.verbose = False
        self.method = 'GET'
        self.info = 'all'
        self.output = 'standard'

    def runCmd(
        self,
        cmd,
        server,
        port,
        user,
        password,
        opts,
        ):

        # defaults

        standard_result = ''
        self.user = user
        self.password = password

        # set standard opts

        output= 'default'
        for (o, a) in opts:
            if o in ('-o', '--output'):
                self.output = a
            if o == '-v' or o == '--verbose':
                self.verbose = True
            if o == '-d' or o == '--debug':
                self.debug = True

        # allow user to be lazy and not specify port

        rest = restclient.RestClient(server, port, {'debug':self.debug})
        rest_uri = '/nodes/Self'

        # get the parameters straight

        response = rest.sendCmd('GET',
                                rest_uri,
                                self.user,
                                self.password)

        if response.status == 200:
            data = response.read()
        else:
            print 'Error! ', response.status, response.reason
            sys.exit(2)

        if self.output == 'json':
            print data
        else:
            json = rest.getJson(data)

            print "%s" % json['hostname']
            print "version: %s" % json['version']
            print "license: %s" % json['license']
            print "licenseValid : %s" % json['licenseValid']
            print "licenseUntil: %s" % json['licenseValidUntil']
            print "os: %s" % json['os']
            print "memoryQuota: %s" % json['memoryQuota']
            print "ports:\n\tproxy: %s\n\tdirect: %s" % \
                    (json['ports']['proxy'], json['ports']['direct'])
            storage = json['storage']
            for stype in storage:
                if storage[stype]:
                    sobj = storage[stype][0]
                    print "%s:" % stype
                    print "\tstate: %s" % sobj['state']
                    print "\tusage: %d %", sobj['diskStats']['usagePercent']
                    print "\tsize: %d", sobj['diskStats']['sizeKBytes']
                    print "\tpath: %s" % sobj['path']
                    print "\tquota: %s" % sobj['quotaMb']


