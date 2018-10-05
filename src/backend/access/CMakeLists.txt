cmake_minimum_required(VERSION 3.12)
project(access)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SUBDIRS common gin gist hash heap index nbtree rmgrdesc spgist
    transam external bitmap appendonly aocs)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
    list(TRANSFORM CHILD_FILES PREPEND "${SUBDIR}/")
    set(ACCESS_FILES ${ACCESS_FILES} ${CHILD_FILES})    
endforeach(SUBDIR)

set(SOURCE_FILES
    ${ACCESS_FILES}
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

add_library(access ${SOURCE_FILES})

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCE_FILES})