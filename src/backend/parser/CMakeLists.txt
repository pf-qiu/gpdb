cmake_minimum_required(VERSION 3.12)

project(parser)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    analyze.c gram.c scan.c keywords.c kwlookup.c parser.c
    parse_agg.c parse_clause.c parse_coerce.c parse_collate.c parse_cte.c
    parse_expr.c parse_func.c parse_node.c parse_oper.c parse_param.c
    parse_relation.c parse_target.c parse_type.c parse_utilcmd.c scansup.c
    parse_partition.c
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

add_library(parser ${SOURCE_FILES})