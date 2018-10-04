cmake_minimum_required(VERSION 3.12)

project(nodes)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    nodeFuncs.c nodes.c list.c bitmapset.c tidbitmap.c
       copyfuncs.c equalfuncs.c makefuncs.c
       outfuncs.c readfuncs.c print.c read.c params.c value.c
	   outfast.c readfast.c
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_library(nodes ${SOURCE_FILES})