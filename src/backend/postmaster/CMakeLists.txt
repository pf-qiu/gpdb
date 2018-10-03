cmake_minimum_required(VERSION 3.12)

project(postmaster)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    autovacuum.c bgwriter.c checkpointer.c fork_process.c pgarch.c pgstat.c
	postmaster.c syslogger.c startup.c
	perfmon.c backoff.c perfmon_segmentinfo.c
	sendalert.c alertseverity.c autostats.c walwriter.c
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_library(postmaster ${SOURCE_FILES})