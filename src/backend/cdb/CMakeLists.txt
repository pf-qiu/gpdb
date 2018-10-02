cmake_minimum_required(VERSION 3.6)

find_package( ZLIB REQUIRED )
if ( ZLIB_FOUND )
    include_directories( ${ZLIB_INCLUDE_DIRS} )
else ()
    message(SEND_ERROR "zlib not found")
endif( ZLIB_FOUND )

add_subdirectory(dispatcher)

project(cdb)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /MP")

set(SOURCE_FILES
cdbappendonlystorageformat.c
       cdbappendonlystorageread.c cdbappendonlystoragewrite.c
	   cdbbufferedappend.c cdbbufferedread.c
	   cdbcat.c cdbcopy.c
	   cdbdistributedsnapshot.c
	   cdbdistributedxid.c cdbdistributedxacts.c
	   cdbdoublylinked.c
	   cdbdtxcontextinfo.c
	   cdbfts.c
	   cdbgroup.c
	   cdbhash.c cdbheap.c
	   cdbllize.c cdblocaldistribxact.c
	   cdbappendonlyxlog.c
	   cdbmutate.c
	   cdboidsync.c
	   cdbpartindex.c
	   cdbpartition.c
	   cdbpath.c cdbpathlocus.c cdbpathtoplan.c
	   cdbpgdatabase.c
	   cdbplan.c cdbpullup.c
	   cdbrelsize.c
	   cdbsetop.c cdbsreh.c cdbsrlz.c cdbsubplan.c cdbsubselect.c
	   cdbtargeteddispatch.c cdbthreadlog.c
	   cdbtimer.c
	   cdbtm.c cdbtmutils.c
	   cdbutil.c
	   cdbvars.c cdbvarblock.c
	   partitionselection.c
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

add_library(cdb ${SOURCE_FILES})