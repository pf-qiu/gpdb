cmake_minimum_required(VERSION 3.12)
project(utf8_and_win)
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
add_library(utf8_and_win SHARED utf8_and_win.c)
add_dependencies(utf8_and_win postgres)
target_link_libraries(utf8_and_win port common conversion_common ${CMAKE_BINARY_DIR}/$<CONFIG>/postgres.lib)
set_target_properties(utf8_and_win PROPERTIES FOLDER conversion_procs)
install(TARGETS utf8_and_win DESTINATION lib/postgresql)
