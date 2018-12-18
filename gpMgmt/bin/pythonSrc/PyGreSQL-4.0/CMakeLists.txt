cmake_minimum_required(VERSION 3.12)
project(_pg)
add_definitions("/D FRONTEND /wd4996 /wd4018 /wd4090 /wd4267 /wd4244")
set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include/
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
    ${GPDB_SRC_DIR}/src/backend
    ${Python2_INCLUDE_DIRS})
include_directories(${HEADER_DIRS})
add_library (_pg SHARED pgmodule.c)
target_link_libraries(_pg libpq port common ws2_32 secur32 ${Python2_LIBRARIES})