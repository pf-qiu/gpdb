cmake_minimum_required(VERSION 3.12)

project(port_be)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    atomics.c win32_sema.c win32_shmem.c win32_latch.c
    dynloader/win32.c
    win32/timer.c
    win32/mingwcompat.c
    win32/socket.c
    win32/crashdump.c
    win32/signal.c
    win32/security.c
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

add_library(port_be ${SOURCE_FILES})