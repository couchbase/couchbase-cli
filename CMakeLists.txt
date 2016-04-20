PROJECT(CouchbaseClient)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# Generate the python wrappers
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/couchbase-cli)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbbackup)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbrestore)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbtransfer)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbworkloadgen)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbrecovery)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbbackupwrapper)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbrestorewrapper)
INSTALL(PROGRAMS
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/couchbase-cli
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbbackup
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbrestore
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbtransfer
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbworkloadgen
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbrecovery
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbbackupwrapper
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbrestorewrapper
        DESTINATION bin)

INSTALL(PROGRAMS
        couchbase-cli
        cbbackup
        cbrestore
        cbtransfer
        cbworkloadgen
        cbrecovery
        cbbackupwrapper
        cbrestorewrapper
        DESTINATION lib/python)

INSTALL(FILES
        buckets.py
        cb_bin_client.py
        cbsnappy.py
        cluster_manager.py
        couchbaseConstants.py
        info.py
        listservers.py
        node.py
        pump.py
        pump_bfd.py
        pump_bson.py
        pump_cb.py
        pump_csv.py
        pump_gen.py
        pump_json.py
        pump_mc.py
        pump_sfd.py
        pump_tap.py
        pump_transfer.py
        pump_dcp.py
        restclient.py
        timeout.py
        usage.py
        util_cli.py
        xdcr.py
        DESTINATION lib/python)
IF (BUILD_ENTERPRISE)
    INSTALL(FILES
            pump_bfd2.py
            DESTINATION lib/python)
ENDIF (BUILD_ENTERPRISE)

INSTALL(FILES
        simplejson/LICENSE.txt
        simplejson/__init__.py
        simplejson/decoder.py
        simplejson/encoder.py
        simplejson/scanner.py
        DESTINATION lib/python/simplejson)

INSTALL(FILES
        simplejson/LICENSE.txt
        simplejson/__init__.py
        simplejson/decoder.py
        simplejson/encoder.py
        simplejson/scanner.py
        DESTINATION lib/python/simplejson)

INSTALL(DIRECTORY requests
        DESTINATION lib/python/)
