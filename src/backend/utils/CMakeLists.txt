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

set(SUBDIRS adt cache datumstream error fmgr gdd hash init mb misc mmgr resowner
    resgroup resscheduler sort time gpmon gp workfile_manager resource_manager
    hyperloglog)
set(UTILS_FILES)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
    list(TRANSFORM CHILD_FILES PREPEND "${SUBDIR}/")
    set(UTILS_FILES ${UTILS_FILES} ${CHILD_FILES})    
endforeach(SUBDIR)

set(CHILD_FILES
    fmgrtab.c
    session_state.c
    ${UTILS_FILES}
    PARENT_SCOPE
)