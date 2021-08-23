
SET(COUCHBASE_CLI_CMDS cblogredaction;couchbase-cli)

FOREACH(CMD IN LISTS COUCHBASE_CLI_CMDS)
    INSTALL(DIRECTORY generated/doc/${CMD}
            DESTINATION share/doc
            FILES_MATCHING PATTERN "*.html")
ENDFOREACH()

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
ENDIF()
