cmake_minimum_required(VERSION 3.12)

project(libpq_be)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

configure_file("${GPDB_SRC_DIR}/src/interfaces/libpq/fe-protocol3.c" "${CMAKE_CURRENT_SOURCE_DIR}/fe-protocol3.c" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/interfaces/libpq/fe-connect.c" "${CMAKE_CURRENT_SOURCE_DIR}/fe-connect.c" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/interfaces/libpq/fe-exec.c" "${CMAKE_CURRENT_SOURCE_DIR}/fe-exec.c" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/interfaces/libpq/pqexpbuffer.c" "${CMAKE_CURRENT_SOURCE_DIR}/pqexpbuffer.c" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/interfaces/libpq/fe-auth.c" "${CMAKE_CURRENT_SOURCE_DIR}/fe-auth.c" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/interfaces/libpq/fe-misc.c" "${CMAKE_CURRENT_SOURCE_DIR}/fe-misc.c" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/interfaces/libpq/fe-protocol2.c" "${CMAKE_CURRENT_SOURCE_DIR}/fe-protocol2.c" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/interfaces/libpq/fe-secure.c" "${CMAKE_CURRENT_SOURCE_DIR}/fe-secure.c" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/port/getpeereid.c" "${CMAKE_CURRENT_SOURCE_DIR}/getpeereid.c" COPYONLY)

set(SOURCE_FILES
    be-fsstubs.c be-secure.c auth.c crypt.c hba.c ip.c md5.c pqcomm.c
    pqformat.c pqsignal.c sha2.c pg_sha2.c fe-protocol3.c fe-connect.c
    fe-exec.c pqexpbuffer.c fe-auth.c fe-misc.c fe-protocol2.c fe-secure.c
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

add_library(libpq_be ${SOURCE_FILES})