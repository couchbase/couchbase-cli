PROJECT(CouchbaseClient)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# Generate the Version module
CONFIGURE_FILE (cb_version.cmake.py
                cb_version.py)

# Copy couchstore.py from couchstore so we can import it
CONFIGURE_FILE (../couchstore/python/couchstore.py couchstore.py COPYONLY)

# Install all executable Python scripts, with wrappers
PyWrapper (BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}"
    SCRIPTS
    couchbase-cli
    cblogredaction
    cbrestorewrapper
    cbbackup
    cbbackupwrapper
    cbrecovery
    cbrestore
    cbtransfer
    cbworkloadgen)

# Install dependent Python files
INSTALL (FILES
    cb_bin_client.py
    cb_util.py
    cbmgr.py
    cluster_manager.py
    couchbaseConstants.py
    pbar.py
    pump.py
    pump_bfd.py
    pump_cb.py
    pump_csv.py
    pump_gen.py
    pump_json.py
    pump_mc.py
    pump_sfd.py
    pump_transfer.py
    pump_dcp.py
    ${CMAKE_CURRENT_BINARY_DIR}/cb_version.py
    ${CMAKE_CURRENT_BINARY_DIR}/couchstore.py
    DESTINATION lib/python)
IF (BUILD_ENTERPRISE)
  INSTALL (FILES pump_bfd2.py DESTINATION lib/python)
ENDIF ()

ADD_SUBDIRECTORY(docs)

ADD_CUSTOM_TARGET (couchbase-cli-clean
    COMMAND git clean -dfx
    COMMENT "Cleaning couchbase-cli directory"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
IF (TARGET realclean)
    ADD_DEPENDENCIES (realclean couchbase-cli-clean)
ENDIF ()
