cmake_minimum_required(VERSION 3.12)

project(commands)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
aggregatecmds.c alter.c analyze.c analyzeutils.c async.c cluster.c comment.c
	collationcmds.c constraint.c conversioncmds.c copy.c createas.c
	dbcommands.c define.c discard.c dropcmds.c
	event_trigger.c explain.c extension.c foreigncmds.c functioncmds.c
	indexcmds.c lockcmds.c matview.c operatorcmds.c opclasscmds.c
	portalcmds.c prepare.c proclang.c
	schemacmds.c seclabel.c sequence.c tablecmds.c tablespace.c trigger.c
	tsearchcmds.c typecmds.c user.c vacuum.c vacuumlazy.c
	variable.c view.c
    analyzefuncs.c extprotocolcmds.c exttablecmds.c queue.c
    resgroupcmds.c
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

add_library(commands ${SOURCE_FILES})