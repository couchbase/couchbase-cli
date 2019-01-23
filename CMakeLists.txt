PROJECT(CouchbaseClient)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# Initialize venv for this directory
GET_FILENAME_COMPONENT (_snappy_libdir "${SNAPPY_LIBRARIES}" DIRECTORY)
PyVenv (
  INCLUDE_DIRS ${SNAPPY_INCLUDE_DIR}
  LIBRARY_DIRS ${_snappy_libdir}
)

# Generate the Version module
CONFIGURE_FILE (cb_version.cmake.py
                cb_version.py)

# Copy couchstore.py from couchstore so we can import it
CONFIGURE_FILE (../couchstore/python/couchstore.py couchstore.py COPYONLY)

# List of "hidden" pump modules, ie, those which are imported
# dynamically and might therefore not be detectable by PyInstaller.
SET (hidden_pumps
  pump_json
  pump_sfd
)
IF (BUILD_ENTERPRISE)
  LIST (APPEND hidden_pumps pump_bfd2)
ENDIF ()

PyInstall (TARGET couchbase-cli SCRIPT couchbase-cli INSTALL_PATH bin)
PyInstall (TARGET cblogredaction SCRIPT cblogredaction INSTALL_PATH bin)
PyInstall (TARGET cbrestorewrapper SCRIPT cbrestorewrapper INSTALL_PATH bin)

IF (APPLE)
  # Due to a bug in PyInstaller, it won't find dylib dependencies on MacOS
  # if they're in non-standard locations. So for libsnappy we have to
  # explicitly copy it into the package.
  SET (_extra_bins EXTRA_BIN "${_snappy_libdir}/libsnappy.1.dylib")
ENDIF ()

# These tools all use the pump libraries so their PyInstall()
# directives look the same
FOREACH (PUMP_TOOL cbbackup cbbackupwrapper cbrecovery cbrestore cbtransfer cbworkloadgen)
  PyInstall (TARGET ${PUMP_TOOL} SCRIPT ${PUMP_TOOL}
    INSTALL_PATH bin
    IMPORTS ${hidden_pumps}
    LIBRARY_DIRS "${_snappy_libdir}" "${CMAKE_BINARY_DIR}/couchstore"
    ${_extra_bins}
    DEPENDS couchstore
  )
ENDFOREACH ()


ADD_SUBDIRECTORY(docs)
