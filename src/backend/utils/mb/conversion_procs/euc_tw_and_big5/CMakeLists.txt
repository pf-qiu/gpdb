cmake_minimum_required(VERSION 3.12)
project(euc_tw_and_big5)
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
add_library(euc_tw_and_big5 SHARED euc_tw_and_big5.c big5.c)
add_dependencies(euc_tw_and_big5 postgres)
target_link_libraries(euc_tw_and_big5 port common conversion_common ${CMAKE_BINARY_DIR}/$<CONFIG>/postgres.lib)
set_target_properties(euc_tw_and_big5 PROPERTIES FOLDER conversion_procs)
install(TARGETS euc_tw_and_big5 DESTINATION lib/postgresql)
