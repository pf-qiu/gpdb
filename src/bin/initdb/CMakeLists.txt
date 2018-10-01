cmake_minimum_required(VERSION 3.6)
project(initdb)

add_definitions("/D FRONTEND /wd4996 /wd4018 /wd4090")

set(SOURCE_FILES
    initdb.c
    findtimezone.c
    ${GPDB_SRC_DIR}/src/backend/utils/mb/encnames.c
    ${GPDB_SRC_DIR}/src/timezone/localtime.c
)

set(HEADER_FILES
    ${GPDB_SRC_DIR}/src/include/pg_config.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/timezone
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_executable(initdb ${HEADER_FILES} ${SOURCE_FILES})
target_link_libraries(initdb port common ws2_32 secur32)