cmake_minimum_required(VERSION 3.6)
project(pg_controldata)

add_definitions("/D FRONTEND /wd4996 /wd4018 /wd4090 /wd4267 /MP")

set(SOURCE_FILES
    pg_controldata.c
)

set(HEADER_FILES
    ${GPDB_SRC_DIR}/src/include/pg_config.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_executable(pg_controldata ${HEADER_FILES} ${SOURCE_FILES})
target_link_libraries(pg_controldata port common ws2_32 secur32)