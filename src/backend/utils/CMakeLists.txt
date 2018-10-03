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
