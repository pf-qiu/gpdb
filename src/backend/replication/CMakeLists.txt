cmake_minimum_required(VERSION 3.12)

execute_process(COMMAND ${BISON_EXECUTABLE} "-o" "repl_gram.c" "repl_gram.y"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

execute_process(COMMAND ${FLEX_EXECUTABLE} "-orepl_scanner.c" "repl_scanner.l"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(CHILD_FILES
    walsender.c walreceiverfuncs.c walreceiver.c basebackup.c
	repl_gram.c syncrep.c
    gp_libpqwalreceiver.c gp_replication.c
    PARENT_SCOPE
)
