cmake_minimum_required(VERSION 3.6)
project(pg_basebackup)

add_definitions("/D FRONTEND /wd4996 /wd4018 /wd4090")

set(SOURCE_FILES_BASEBACKUP
    pg_basebackup.c
    receivelog.c
    streamutil.c
)

set(SOURCE_FILES_RECEIVEXLOG
    pg_receivexlog.c
    receivelog.c
    streamutil.c
)

set(HEADER_FILES
    ${GPDB_SRC_DIR}/src/include/pg_config.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_executable(pg_basebackup ${HEADER_FILES} ${SOURCE_FILES_BASEBACKUP})
target_link_libraries(pg_basebackup port libpq common ws2_32 secur32)

add_executable(pg_receivexlog ${HEADER_FILES} ${SOURCE_FILES_RECEIVEXLOG})
target_link_libraries(pg_receivexlog port libpq common ws2_32 secur32)