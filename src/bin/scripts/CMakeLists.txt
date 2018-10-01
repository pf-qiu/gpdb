cmake_minimum_required(VERSION 3.6)
project(scripts)

add_definitions("/D FRONTEND /wd4996 /wd4018 /wd4090 /wd4267 /MP")

set(HEADER_FILES
    common.h
    ${GPDB_SRC_DIR}/src/include/pg_config.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/bin/psql
    ${GPDB_SRC_DIR}/src/bin/pg_dump
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

set(CREATEDB_FILES
    createdb.c
    common.c
)

add_executable(createdb ${HEADER_FILES} ${CREATEDB_FILES})
target_link_libraries(createdb port libpq common ws2_32 secur32)

set(CREATELANG_FILES
    createlang.c
    common.c
)

add_executable(createlang ${HEADER_FILES} ${CREATELANG_FILES})
target_link_libraries(createlang port libpq common ws2_32 secur32)

set(CREATEUSER_FILES
    createuser.c
    common.c
)

add_executable(createuser ${HEADER_FILES} ${CREATEUSER_FILES})
target_link_libraries(createuser port libpq common ws2_32 secur32)

set(DROPDB_FILES
    dropdb.c
    common.c
)

add_executable(dropdb ${HEADER_FILES} ${DROPDB_FILES})
target_link_libraries(dropdb port libpq common ws2_32 secur32)

set(DROPLANG_FILES
    droplang.c
    common.c
)

add_executable(droplang ${HEADER_FILES} ${DROPLANG_FILES})
target_link_libraries(droplang port libpq common ws2_32 secur32)

set(DROPUSER_FILES
    dropuser.c
    common.c
)

add_executable(dropuser ${HEADER_FILES} ${DROPUSER_FILES})
target_link_libraries(dropuser port libpq common ws2_32 secur32)

set(CLUSTERDB_FILES
    clusterdb.c
    common.c
)

add_executable(clusterdb ${HEADER_FILES} ${CLUSTERDB_FILES})
target_link_libraries(clusterdb port libpq common ws2_32 secur32)

set(VACUUMDB_FILES
    vacuumdb.c
    common.c
)

add_executable(vacuumdb ${HEADER_FILES} ${VACUUMDB_FILES})
target_link_libraries(vacuumdb port libpq common ws2_32 secur32)

set(REINDEX_FILES
    reindexdb.c
    common.c
)

add_executable(reindexdb ${HEADER_FILES} ${REINDEX_FILES})
target_link_libraries(reindexdb port libpq common ws2_32 secur32)

set(PG_ISREADY_FILES
    pg_isready.c
    common.c
)

add_executable(pg_isready ${HEADER_FILES} ${PG_ISREADY_FILES})
target_link_libraries(pg_isready port libpq common ws2_32 secur32)