
SET(COUCHBASE_CLI_CMDS cblogredaction;couchbase-cli)
LIST(JOIN COUCHBASE_CLI_CMDS ", " COUCHBASE_CLI_CMDS_PRETTY)

FIND_PROGRAM(ASCIIDOCTOR "asciidoctor")

IF(ASCIIDOCTOR)
    ADD_CUSTOM_TARGET(COUCHBASE_CLI_DOCS
                      ALL
                      DEPENDS local-cbpy
                      COMMAND ${PYTHON_EXE} generate.py --build-version ${PRODUCT_VERSION}
                      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
ELSE()
    MESSAGE (WARNING "Could not find 'asciidoctor', documentation for ${COUCHBASE_CLI_CMDS_PRETTY} will not be included in the installation package")
ENDIF()

FOREACH(CMD IN LISTS COUCHBASE_CLI_CMDS)
    INSTALL(DIRECTORY generated/doc/${CMD}
            DESTINATION share/doc
            FILES_MATCHING PATTERN "*.html")
ENDFOREACH()

# Also add the couchbase-cli docs to the admin_tools package
INSTALL(
    DIRECTORY generated/doc/couchbase-cli
    DESTINATION ${admin_tools_INSTALL_PREFIX}/share/doc
    EXCLUDE_FROM_ALL COMPONENT admin_tools
)

if(NOT WIN32)
    # 1   Executable programs or shell commands
    # 2   System calls (functions provided by the kernel)
    # 3   Library calls (functions within program libraries)
    # 4   Special files (usually found in /dev)
    # 5   File formats and conventions, e.g. /etc/passwd
    # 6   Games
    # 7   Miscellaneous (including macro packages and conventions), e.g. man(7), groff(7)
    # 8   System administration commands (usually only for root)
    # 9   Kernel routines [Non standard]

    FOREACH(SECTION RANGE 1 9)
        INSTALL(DIRECTORY generated/man/man${SECTION}
                DESTINATION share/man
                FILES_MATCHING PATTERN "*.${SECTION}")
    ENDFOREACH()

    # Also add the couchbase-cli man pages to the admin_tools package, filtering out docs for other binaries not in the
    # package.
    INSTALL(
        DIRECTORY generated/man/
        DESTINATION ${admin_tools_INSTALL_PREFIX}/share/man
        EXCLUDE_FROM_ALL COMPONENT admin_tools
        FILES_MATCHING PATTERN "couchbase-cli*"
    )
ENDIF()
