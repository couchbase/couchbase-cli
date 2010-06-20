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
        self.version = False
        self.mem = False
        self.os = False
        self.license = False
        self.storage = False
        self.ports = False

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
            if o in ('-O', '--os'):
                self.os = True
            if o in ('-P', '--ports'):
                self.ports = True
            if o in ('-l', '--license'):
                self.license = True
            if o in ('-S', '--storage'):
                self.storage = True
            if o in  ('-m', '--mem'):
                self.mem = True
            if o == '-V' or o == '--version':
                self.version = True
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
            #json = rest.getJson(data)
            #pp = pprint.PrettyPrinter(indent=4)
            #pp.pprint(json)
        else:
            json = rest.getJson(data)

            if self.verbose:
                print "server stats for %s" % json['hostname']
            if self.version:
                print "server version: %s" % json['version']
            if self.license:
                print "license: %s" % json['license']
                print "valid : %s" % json['licenseValid']
                print "valid until: %s" % json['licenseValidUntil']
            if self.os:
                print "OS: %s" % json['os']
            if self.mem:
                print "memory quota in MB: %s" % json['memoryMb']
            if self.ports:
                print "Ports:\nproxy: %s\ndirect: %s" % \
                    (json['ports']['proxy'], json['ports']['direct'])
            if self.storage:
                storage = json['storage']
                for stype in storage:
                    if len(storage[stype]):
                        sobj = storage[stype][0]
                        print "%s:" % stype
                        print "\tstate: %s" % sobj['state']
                        print "\tusage: %d %", sobj['diskStats']['usagePercent']
                        print "\tsize: %d", sobj['diskStats']['sizeKBytes']
                        print "\tpath: %s" % sobj['path']
                        print "\tquota: %s" % sobj['quotaMb']


