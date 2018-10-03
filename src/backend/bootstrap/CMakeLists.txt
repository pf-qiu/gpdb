cmake_minimum_required(VERSION 3.12)

execute_process(COMMAND ${FLEX_EXECUTABLE}  "-obootscanner.c" "bootscanner.l"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

execute_process(COMMAND ${BISON_EXECUTABLE}  "-obootparse.c" "bootparse.y"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

project(bootstrap)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    bootstrap.c
    bootparse.c
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

add_library(bootstrap ${SOURCE_FILES})