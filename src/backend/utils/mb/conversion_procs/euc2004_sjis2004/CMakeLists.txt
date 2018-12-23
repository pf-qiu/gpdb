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
add_library(euc2004_sjis2004 SHARED euc2004_sjis2004.c ../../wchar.c ../../conv.c)
target_link_libraries(euc2004_sjis2004 port common)
install(TARGETS euc2004_sjis2004 DESTINATION lib/postgresql)
