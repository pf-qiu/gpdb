cmake_minimum_required(VERSION 3.12)

project(replication)

# parser/gram.c
execute_process(COMMAND ${BISON_EXECUTABLE} "-o" "repl_gram.c" "repl_gram.y"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

execute_process(COMMAND ${FLEX_EXECUTABLE} "-orepl_scanner.c" "repl_scanner.l"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    walsender.c walreceiverfuncs.c walreceiver.c basebackup.c
	repl_gram.c syncrep.c
    gp_libpqwalreceiver.c gp_replication.c
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_library(replication ${SOURCE_FILES})