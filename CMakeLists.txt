PROJECT(CouchbaseClient)
CMAKE_MINIMUM_REQUIRED(VERSION 3.13)

IF (BUILD_COLUMNAR)
  SET (_cli_version "${COLUMNAR_PRODUCT_VERSION}-columnar")
ELSE ()
  SET (_cli_version "${PRODUCT_VERSION}")
ENDIF ()

# Generate the Version module
CONFIGURE_FILE (cb_version.cmake.py
                cb_version.py)

# Copy couchstore.py from couchstore so we can import it
CONFIGURE_FILE (../couchstore/python/couchstore.py couchstore.py COPYONLY)

# Set of Python files required for couchbase-cli
SET (cli_files
    cb_bin_client.py
    cbmgr.py
    cluster_manager.py
    couchbaseConstants.py
    pbar.py
    x509_adapter.py
    ${CMAKE_CURRENT_BINARY_DIR}/cb_version.py
    ${CMAKE_CURRENT_BINARY_DIR}/couchstore.py
)
# Set of other Python files
SET (py_files
    cb_util.py
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
)
IF (BUILD_ENTERPRISE)
  LIST (APPEND py_files pump_bfd2.py)
ENDIF ()

# Install couchbase-cli, including admin_tools package
PyWrapper (
    BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}"
    ADD_TO_STANDALONE_PACKAGE admin_tools
    SCRIPTS
        couchbase-cli
    EXTRA_SCRIPTS ${cli_files}
)
# Install remaining Python scripts
PyWrapper (
    BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}"
    SCRIPTS
        cblogredaction
        cbrecovery
        cbtransfer
        cbworkloadgen
    EXTRA_SCRIPTS ${py_files}
)

ADD_SUBDIRECTORY(docs)

ADD_CUSTOM_TARGET (couchbase-cli-clean
    COMMAND git clean -dfx
    COMMENT "Cleaning couchbase-cli directory"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
IF (TARGET realclean)
    ADD_DEPENDENCIES (realclean couchbase-cli-clean)
ENDIF ()
