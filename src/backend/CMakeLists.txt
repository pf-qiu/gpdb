cmake_minimum_required(VERSION 3.12)


set(CATALOGDIR ${GPDB_SRC_DIR}/src/backend/catalog)

# utils/pg_proc_combined.h, catalog/pg_proc_combined.h
configure_file("${CATALOGDIR}/Catalog.pm" "${CMAKE_CURRENT_SOURCE_DIR}/utils/pg_proc_combined.h" COPYONLY)

file(READ "${GPDB_SRC_DIR}/src/include/catalog/pg_proc.h" FILE_CONTENT)
file(APPEND "${CMAKE_CURRENT_SOURCE_DIR}/utils/pg_proc_combined.h" ${FILE_CONTENT})
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/catalog/pg_proc_combined.h" ${FILE_CONTENT})

file(READ "${GPDB_SRC_DIR}/src/include/catalog/pg_proc_gp.h" FILE_CONTENT)
file(APPEND "${CMAKE_CURRENT_SOURCE_DIR}/utils/pg_proc_combined.h" ${FILE_CONTENT})
file(APPEND "${CMAKE_CURRENT_SOURCE_DIR}/catalog/pg_proc_combined.h" ${FILE_CONTENT})

# utils/fmgrtab.c
execute_process(COMMAND ${PERL_EXECUTABLE} "-I" "${CATALOGDIR}" "Gen_fmgrtab.pl" "pg_proc_combined.h"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/utils)
# utils/errcodes.h
execute_process(COMMAND ${PERL_EXECUTABLE} "generate-errcodes.pl" "errcodes.txt"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/utils
    OUTPUT_FILE "utils/errcodes.h")

# parser/gram.c
execute_process(COMMAND ${BISON_EXECUTABLE} "-d" "-o" "gram.c" "gram.y"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/parser)

# parser/scan.c
# Latest flex on Windows is 2.5.4, which doesn't recognize syntax.
# Use a pre-generated scan.c.include
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/parser/scan.c.in" "${CMAKE_CURRENT_SOURCE_DIR}/parser/scan.c" COPYONLY)

add_subdirectory(access)
add_subdirectory(bootstrap)
add_subdirectory(catalog)
add_subdirectory(cdb)
add_subdirectory(commands)
add_subdirectory(executor)
add_subdirectory(foreign)
add_subdirectory(fts)
add_subdirectory(lib)
add_subdirectory(libpq)
add_subdirectory(nodes)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(port)
add_subdirectory(postmaster)
#add_subdirectory(regex)
#add_subdirectory(replication)
#add_subdirectory(rewrite)
#add_subdirectory(snowball)
#add_subdirectory(storage)
#add_subdirectory(tcop)
#add_subdirectory(tsearch)
add_subdirectory(utils)

project(backend)

add_definitions("/D FRONTEND /D ENABLE_THREAD_SAFETY /wd4996 /wd4018 /wd4090 /wd4244 /wd4267")
add_definitions("/MP")

set(SOURCE_FILES
    main/main.c
    pthread.c
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_executable(postgres ${HEADER_FILES} ${SOURCE_FILES})
target_link_libraries(postgres port ws2_32 secur32)
