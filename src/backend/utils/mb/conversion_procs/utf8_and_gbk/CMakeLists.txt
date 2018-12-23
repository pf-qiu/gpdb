cmake_minimum_required(VERSION 3.12)
project(utf8_and_gbk)
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
add_library(utf8_and_gbk SHARED utf8_and_gbk.c ../../wchar.c ../../conv.c)
target_link_libraries(utf8_and_gbk port common)
install(TARGETS utf8_and_gbk DESTINATION lib/postgresql)
