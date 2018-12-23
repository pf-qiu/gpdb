cmake_minimum_required(VERSION 3.12)
project(euc_cn_and_mic)
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
add_library(euc_cn_and_mic SHARED euc_cn_and_mic.c ../../wchar.c ../../conv.c)
target_link_libraries(euc_cn_and_mic port common)
install(TARGETS euc_cn_and_mic DESTINATION lib/postgresql)
