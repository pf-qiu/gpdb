cmake_minimum_required(VERSION 3.12)
project(latin2_and_win1250)
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
add_library(latin2_and_win1250 SHARED latin2_and_win1250.c)
add_dependencies(latin2_and_win1250 postgres)
target_link_libraries(latin2_and_win1250 port common conversion_common ${CMAKE_BINARY_DIR}/$<CONFIG>/postgres.lib)
set_target_properties(latin2_and_win1250 PROPERTIES FOLDER conversion_procs)
install(TARGETS latin2_and_win1250 DESTINATION lib/postgresql)
