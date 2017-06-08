cmake_minimum_required(VERSION 3.6)
project(common)

add_definitions("/D MAX_PATH=260")
add_definitions("/D FRONTEND /wd4996 /wd4018 /wd4090 /wd4244 /wd4267")

set(SOURCE_FILES
   fe_memutils.c
   relpath.c
)

set(HEADER_FILES
    ${GPDB_SRC_DIR}/src/include/common/fe_memutils.h
    ${GPDB_SRC_DIR}/src/include/common/relpath.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_library(common ${HEADER_FILES} ${SOURCE_FILES})