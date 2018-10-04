cmake_minimum_required(VERSION 3.12)

project(optimizer)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D EXEC_BACKEND /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    prep/prepjointree.c
    prep/preptlist.c
    prep/prepqual.c
    prep/prepunion.c
    util/relnode.c
    util/pathnode.c
    util/joininfo.c
    util/tlist.c
    util/placeholder.c
    util/walkers.c
    util/plancat.c
    util/predtest.c
    util/clauses.c
    util/restrictinfo.c
    util/var.c
    plan/planmain.c
    plan/planshare.c
    plan/transform.c
    plan/initsplan.c
    plan/planagg.c
    plan/planpartition.c
    plan/analyzejoins.c
    plan/createplan.c
    plan/subselect.c
    plan/plangroupext.c
    plan/planner.c
    plan/setrefs.c
    path/orindxpath.c
    path/costsize.c
    path/tidpath.c
    path/indxpath.c
    path/joinrels.c
    path/pathkeys.c
    path/clausesel.c
    path/equivclass.c
    path/allpaths.c
    path/joinpath.c
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

add_library(optimizer ${SOURCE_FILES})