cmake_minimum_required(VERSION 3.12)

project(motion)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND /wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /MP")

set(SOURCE_FILES cdbmotion.c tupchunklist.c tupser.c
	ic_common.c ic_tcp.c ic_udpifc.c htupfifo.c tupleremap.c
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

add_library(motion ${SOURCE_FILES})