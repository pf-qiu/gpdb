cmake_minimum_required(VERSION 3.6)
project(psql)

include(FindPerl)
if (PERL_FOUND)
    execute_process(COMMAND ${PERL_EXECUTABLE} "${GPDB_SRC_DIR}/src/bin/psql/create_help.pl" "${GPDB_SRC_DIR}/doc/src/sgml/ref" "${GPDB_SRC_DIR}/src/bin/psql/sql_help")
else ()
    message(SEND_ERROR "Perl not found")
endif()

include(FindFLEX)
if (FLEX_FOUND)
    execute_process(COMMAND ${FLEX_EXECUTABLE} -Cfe "-o${GPDB_SRC_DIR}/src/bin/psql/psqlscan.c" "${GPDB_SRC_DIR}/src/bin/psql/psqlscan.l")
else()
    message(SEND_ERROR "Flex not found")
endif()

add_definitions("/D FRONTEND /wd4996 /wd4018 /wd4090 /wd4267 /MP")

set(SOURCE_FILES
    command.c
    common.c
    copy.c
    describe.c
    help.c
    input.c
    large_obj.c
    mainloop.c
    mbprint.c
    print.c
    prompt.c
    startup.c
    stringutils.c
    tab-complete.c
    variables.c
    ${GPDB_SRC_DIR}/src/bin/pg_dump/keywords.c
    ${GPDB_SRC_DIR}/src/backend/parser/kwlookup.c
    ${GPDB_SRC_DIR}/src/bin/pg_dump/dumputils.c
    sql_help.c
)

set(HEADER_FILES
    command.h
    common.h
    copy.h
    describe.h
    help.h
    input.h
    large_obj.h
    mainloop.h
    mbprint.h
    print.h
    prompt.h
    psqlscan.h
    settings.h
    stringutils.h
    sql_help.h
    tab-complete.h
    variables.h
    ${GPDB_SRC_DIR}/src/include/pg_config.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/bin/psql
    ${GPDB_SRC_DIR}/src/bin/pg_dump
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_executable(psql ${HEADER_FILES} ${SOURCE_FILES})
target_link_libraries(psql port libpq common ws2_32 secur32)