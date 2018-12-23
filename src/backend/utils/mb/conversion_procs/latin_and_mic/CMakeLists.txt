cmake_minimum_required(VERSION 3.12)
project(latin_and_mic)
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
add_library(latin_and_mic SHARED latin_and_mic.c)
add_dependencies(latin_and_mic postgres)
target_link_libraries(latin_and_mic port common conversion_common ${CMAKE_BINARY_DIR}/$<CONFIG>/postgres.lib)
set_target_properties(latin_and_mic PROPERTIES FOLDER conversion_procs)
install(TARGETS latin_and_mic DESTINATION lib/postgresql)
