cmake_minimum_required(VERSION 3.12)

set (GPDB_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR})

configure_file("${GPDB_SRC_DIR}/src/include/pg_config.h.win32" "${GPDB_SRC_DIR}/src/include/pg_config.h" COPYONLY)
configure_file("${GPDB_SRC_DIR}/src/include/pg_config_ext.h.win32" "${GPDB_SRC_DIR}/src/include/pg_config_ext.h" COPYONLY)

configure_file("${GPDB_SRC_DIR}/src/include/port/win32.h" "${GPDB_SRC_DIR}/src/include/pg_config_os.h" COPYONLY)

# () specify a subgroup to capature, it matches the PG_MAJOR_VERSION from configure.in.
# Extract version number from subgroup, into $CMAKE_MATCH_1
# Append definition to final pg_config.h
set(VERSION_PATTERN "PG_PACKAGE_VERSION=([0-9\.]*)")
set(GPDB_VERSION_PATTERN "\\\[Greenplum Database\\\], \\\[([^\]]*)\\\]")
file(STRINGS "${GPDB_SRC_DIR}/configure.in" CONFIGURE_FILE)
string(REGEX MATCH "${VERSION_PATTERN}" PG_MAJORVERSION_STRING ${CONFIGURE_FILE})
set(PG_MAJORVERSION ${CMAKE_MATCH_1})
file(APPEND "${GPDB_SRC_DIR}/src/include/pg_config.h" "\n#define PG_MAJORVERSION \"${PG_MAJORVERSION}\"\n" )
message("PG_MAJORVERSION: " "${PG_MAJORVERSION}")

string(REGEX MATCH "${GPDB_VERSION_PATTERN}" GP_VERSION_STRING ${CONFIGURE_FILE})
set(GP_VERSION ${CMAKE_MATCH_1})
file(APPEND "${GPDB_SRC_DIR}/src/include/pg_config.h" "\n#define GP_VERSION \"${GP_VERSION}\"\n" )
message("GP_VERSION: " "${GP_VERSION}")

string(REGEX MATCH "[^\.]*" GP_MAJORVERSION ${GP_VERSION})
file(APPEND "${GPDB_SRC_DIR}/src/include/pg_config.h" "\n#define GP_MAJORVERSION \"${GP_MAJORVERSION}\"\n" )
message("GP_MAJORVERSION: " "${GP_MAJORVERSION}")

# pg_config_paths.h is shared by both port and libpq
file (WRITE "${GPDB_SRC_DIR}/src/port/pg_config_paths.h"
    "#define PGBINDIR \"${GPDB_SRC_DIR}/bin\""
    "\n#define PGSHAREDIR \"${GPDB_SRC_DIR}/share/postgresql\""
    "\n#define SYSCONFDIR \"${GPDB_SRC_DIR}/etc/postgresql\""
    "\n#define INCLUDEDIR \"${GPDB_SRC_DIR}/include\""
    "\n#define PKGINCLUDEDIR \"${GPDB_SRC_DIR}/include/postgresql\""
    "\n#define INCLUDEDIRSERVER \"${GPDB_SRC_DIR}/include/postgresql/server\""
    "\n#define LIBDIR \"${GPDB_SRC_DIR}/lib\""
    "\n#define PKGLIBDIR \"${GPDB_SRC_DIR}/lib/postgresql\""
    "\n#define LOCALEDIR \"${GPDB_SRC_DIR}/share/locale\""
    "\n#define DOCDIR \"${GPDB_SRC_DIR}/doc/postgresql\""
    "\n#define HTMLDIR \"${GPDB_SRC_DIR}/doc/postgresql\""
    "\n#define MANDIR \"${GPDB_SRC_DIR}/share/man\""
)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(ZLIB REQUIRED)
if (ZLIB_FOUND)
    include_directories(${ZLIB_INCLUDE_DIRS})
else ()
    message(FATAL_ERROR "zlib not found")
endif(ZLIB_FOUND)

find_package(CURL REQUIRED)
if (CURL_FOUND)
    include_directories(${CURL_INCLUDE_DIRS})
else ()
    message(FATAL_ERROR "curl not found")
endif(CURL_FOUND)

include(FindPerl)
if (NOT PERL_FOUND)
    message(FATAL_ERROR "Perl not found")
endif()

include(FindFLEX)
if (NOT FLEX_FOUND)
    message(FATAL_ERROR "Flex not found")
endif()

include(FindBISON)
if (NOT BISON_FOUND)
    message(FATAL_ERROR "bison not found")
endif()

add_subdirectory(src/common)
add_subdirectory(src/port)
add_subdirectory(src/timezone)
add_subdirectory(src/interfaces/libpq)
add_subdirectory(src/bin)
add_subdirectory(src/backend)