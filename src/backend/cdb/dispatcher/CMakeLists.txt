cmake_minimum_required(VERSION 3.12)

project(dispatcher)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /MP")

set(SOURCE_FILES cdbconn.c cdbdisp.c cdbdisp_thread.c cdbdisp_async.c 
    cdbdispatchresult.c cdbdisp_dtx.c cdbdisp_query.c cdbgang.c 
	cdbgang_thread.c cdbgang_async.c cdbpq.c
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

add_library(dispatcher ${SOURCE_FILES})