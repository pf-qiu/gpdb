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

project(backend)
add_definitions("/D BUILDING_DLL")
add_definitions("/D ENABLE_THREAD_SAFETY")
add_definitions("/D EXEC_BACKEND")
add_definitions("/D USE_CURL")
add_definitions("/D _USE_MATH_DEFINES")
add_definitions("/D WIN32_STACK_RLIMIT=4194304")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SUBDIRS access bootstrap catalog parser commands executor foreign
    fts lib libpq main nodes optimizer port postmaster regex
    replication rewrite storage tcop tsearch utils
    cdb)
set(BACKEND_FILES)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
    list(TRANSFORM CHILD_FILES PREPEND "${SUBDIR}/")
    set(BACKEND_FILES ${BACKEND_FILES} ${CHILD_FILES})    
endforeach(SUBDIR)

set(SOURCE_FILES
    ${BACKEND_FILES}
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_executable(postgres ${HEADER_FILES} ${SOURCE_FILES})
target_link_libraries(postgres
    libpq port timezone common
    ws2_32 secur32 ${ZLIB_LIBRARIES} ${CURL_LIBRARIES})

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCE_FILES})
