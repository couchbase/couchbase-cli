
SET(COUCHBASE_CLI_CMDS,
    "cblogredaction"
    "couchbase-cli")

FOREACH(CMD COUCHBASE_CLI_CMDS)
    FILE(GLOB CMD_HTML_PAGES generated/doc/${CMD}/*.html)

    INSTALL(FILES ${CMD_HTML_PAGES}
            DESTINATION share/doc/${CMD})
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
        FILE(GLOB CMD_MAN_PAGES generated/man/man${SECTION}/*.${SECTION})

        # NOTE: Directories will only be created if the glob pattern above actually matched any files
        INSTALL(FILES ${CMD_MAN_PAGES}
                DESTINATION share/man/man${SECTION})
    ENDFOREACH()
ENDIF()
