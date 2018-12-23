cmake_minimum_required(VERSION 3.12)
project(dict_snowball)

add_subdirectory(libstemmer)
list(TRANSFORM CHILD_FILES PREPEND "libstemmer/")
set(SOURCE_FILES ${CHILD_FILES})

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/snowball
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})
add_library(dict_snowball SHARED ${HEADER_FILES} ${SOURCE_FILES})
add_dependencies(dict_snowball postgres)
target_link_libraries(dict_snowball ${CMAKE_BINARY_DIR}/$<CONFIG>/postgres.lib)

configure_file("snowball_create.sql.in" "snowball_create.sql" COPYONLY)

install(TARGETS dict_snowball DESTINATION lib/postgresql)

install(FILES snowball_create.sql
    DESTINATION share/postgresql)