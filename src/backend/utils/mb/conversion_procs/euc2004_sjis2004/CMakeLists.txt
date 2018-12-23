cmake_minimum_required(VERSION 3.12)
project(euc2004_sjis2004)
set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)
include_directories(${HEADER_DIRS})
add_library(euc2004_sjis2004 SHARED euc2004_sjis2004.c)
add_dependencies(euc2004_sjis2004 postgres)
target_link_libraries(euc2004_sjis2004 port common conversion_common ${CMAKE_BINARY_DIR}/$<CONFIG>/postgres.lib)
set_target_properties(euc2004_sjis2004 PROPERTIES FOLDER conversion_procs)
install(TARGETS euc2004_sjis2004 DESTINATION lib/postgresql)
