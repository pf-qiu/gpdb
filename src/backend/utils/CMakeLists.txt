cmake_minimum_required(VERSION 3.12)

configure_file("probes.h.win32" "${CMAKE_CURRENT_SOURCE_DIR}/probes.h" COPYONLY)

set(CATALOGDIR ${GPDB_SRC_DIR}/src/backend/catalog)

configure_file("${CATALOGDIR}/Catalog.pm" "${CMAKE_CURRENT_SOURCE_DIR}/pg_proc_combined.h" COPYONLY)

file(READ "${GPDB_SRC_DIR}/src/include/catalog/pg_proc.h" FILE_CONTENT)
file(APPEND "${CMAKE_CURRENT_SOURCE_DIR}/pg_proc_combined.h" ${FILE_CONTENT})

file(READ "${GPDB_SRC_DIR}/src/include/catalog/pg_proc_gp.h" FILE_CONTENT)
file(APPEND "${CMAKE_CURRENT_SOURCE_DIR}/pg_proc_combined.h" ${FILE_CONTENT})

# fmgrtab.c
execute_process(COMMAND ${PERL_EXECUTABLE} "-I" "${CATALOGDIR}" "Gen_fmgrtab.pl" "pg_proc_combined.h"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
# errcodes.h
execute_process(COMMAND ${PERL_EXECUTABLE} "generate-errcodes.pl" "errcodes.txt"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_FILE "errcodes.h")

project(utils)

set(MMGR_FILES
    aset.c mcxt.c memaccounting.c mpool.c portalmem.c memprot.c vmem_tracker.c
    redzone_handler.c runaway_cleaner.c idle_tracker.c event_version.c ext_alloc.c
    psprintf.c)
list(TRANSFORM MMGR_FILES PREPEND "mmgr/")

set(SUBDIRS adt cache datumstream error fmgr gdd hash init mb misc mmgr resowner
    resgroup resscheduler sort time gpmon gp workfile_manager resource_manager
    hyperloglog)
set(UTILS_FILES)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
    list(TRANSFORM CHILD_FILES PREPEND "${SUBDIR}/")
    set(UTILS_FILES ${UTILS_FILES} ${CHILD_FILES})    
endforeach(SUBDIR)

set(SOURCE_FILES
    fmgrtab.c
    session_state.c
    ${UTILS_FILES})

set(CHILD_FILES ${UTILS_FILES} PARENT_SCOPE)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND /D _USE_MATH_DEFINES")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

include_directories(${HEADER_DIRS})

add_library(utils ${SOURCE_FILES})

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCE_FILES})