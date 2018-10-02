cmake_minimum_required(VERSION 3.6)

add_subdirectory(utils)
#add_subdirectory(access)
#add_subdirectory(bootstrap)
#add_subdirectory(catalog)
add_subdirectory(cdb)
#add_subdirectory(commands)
add_subdirectory(executor)
#add_subdirectory(foreign)
#add_subdirectory(fts)
#add_subdirectory(gpopt)
#add_subdirectory(lib)
#add_subdirectory(libpq)
#add_subdirectory(main)
#add_subdirectory(nodes)
#add_subdirectory(optimizer)
#add_subdirectory(parser)
#add_subdirectory(po)
#add_subdirectory(port)
#add_subdirectory(postmaster)
#add_subdirectory(regex)
#add_subdirectory(replication)
#add_subdirectory(rewrite)
#add_subdirectory(snowball)
#add_subdirectory(storage)
#add_subdirectory(tcop)
#add_subdirectory(tsearch)

#[[
project(backend)

add_definitions("/D MAX_PATH=260")
add_definitions("/D FRONTEND /D ENABLE_THREAD_SAFETY /wd4996 /wd4018 /wd4090 /wd4244 /wd4267")
add_definitions("/MP")

set(SOURCE_FILES
)

set(HEADER_FILES
    ${GPDB_SRC_DIR}/src/include/pg_config.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_library(port ${HEADER_FILES} ${SOURCE_FILES})
]]