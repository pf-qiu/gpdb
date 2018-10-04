cmake_minimum_required(VERSION 3.12)

project(tsearch)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    ts_locale.c ts_parse.c wparser.c wparser_def.c dict.c
	dict_simple.c dict_synonym.c dict_thesaurus.c
	dict_ispell.c regis.c spell.c
	to_tsany.c ts_selfuncs.c ts_typanalyze.c ts_utils.c
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_library(tsearch ${SOURCE_FILES})