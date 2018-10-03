cmake_minimum_required(VERSION 3.12)

find_package( CURL REQUIRED )
if ( CURL_FOUND )
    include_directories( ${CURL_INCLUDE_DIRS} )
else ()
    message(SEND_ERROR "curl not found")
endif( CURL_FOUND )

project(access)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    transam/twophase_rmgr.c
    transam/clog.c
    transam/xlogutils.c
    transam/xlogarchive.c
    transam/gp_distributed_log.c
    transam/rmgr.c
    transam/distributedlog.c
    transam/multixact.c
    transam/timeline.c
    transam/xact.c
    transam/xlogreader.c
    transam/xlogfuncs.c
    transam/varsup.c
    transam/slru.c
    transam/twophase.c
    transam/xlogloc.c
    transam/transam.c
    transam/xlog.c
    transam/subtrans.c
    transam/gp_transaction_log.c
    hash/hashinsert.c
    hash/hashscan.c
    hash/hashfunc.c
    hash/hashsort.c
    hash/hashutil.c
    hash/hashsearch.c
    hash/hash.c
    hash/hashovfl.c
    hash/hashpage.c
    bitmap/bitmapsearch.c
    bitmap/bitmappages.c
    bitmap/bitmap.c
    bitmap/bitmapinsert.c
    bitmap/bitmapxlog.c
    bitmap/bitmapattutil.c
    bitmap/bitmaputil.c
    aocs/aocs_compaction.c
    aocs/aocsam.c
    aocs/aocssegfiles.c
    rmgrdesc/xactdesc.c
    rmgrdesc/tblspcdesc.c
    rmgrdesc/bitmapdesc.c
    rmgrdesc/xlogdesc.c
    rmgrdesc/heapdesc.c
    rmgrdesc/dbasedesc.c
    rmgrdesc/spgdesc.c
    rmgrdesc/gistdesc.c
    rmgrdesc/seqdesc.c
    rmgrdesc/gindesc.c
    rmgrdesc/appendonlydesc.c
    rmgrdesc/distributedlogdesc.c
    rmgrdesc/mxactdesc.c
    rmgrdesc/smgrdesc.c
    rmgrdesc/standbydesc.c
    rmgrdesc/hashdesc.c
    rmgrdesc/relmapdesc.c
    rmgrdesc/nbtdesc.c
    rmgrdesc/clogdesc.c
    appendonly/appendonlyam.c
    appendonly/appendonly_visimap_store.c
    appendonly/appendonly_visimap.c
    appendonly/aomd.c
    appendonly/aosegfiles.c
    appendonly/appendonly_visimap_entry.c
    appendonly/appendonly_visimap_udf.c
    appendonly/appendonlywriter.c
    appendonly/appendonlyblockdirectory.c
    appendonly/appendonly_compaction.c
    appendonly/appendonlytid.c
    nbtree/nbtsort.c
    nbtree/nbtutils.c
    nbtree/nbtree.c
    nbtree/nbtxlog.c
    nbtree/nbtinsert.c
    nbtree/nbtcompare.c
    nbtree/nbtsearch.c
    nbtree/nbtpage.c
    common/tupconvert.c
    common/indextuple.c
    common/bufmask.c
    common/memtuple.c
    common/reloptions.c
    common/reloptions_gp.c
    common/printtup.c
    common/tupdesc.c
    common/heaptuple.c
    common/scankey.c
    heap/rewriteheap.c
    heap/syncscan.c
    heap/hio.c
    heap/pruneheap.c
    heap/tuptoaster.c
    heap/heapam.c
    heap/visibilitymap.c
    index/indexam.c
    index/genam.c
    external/url.c
    external/fileam.c
    external/url_curl.c
    external/url_execute_win32.c
    external/url_file.c
    external/url_custom.c
    gist/gistxlog.c
    gist/gistget.c
    gist/gistvacuum.c
    gist/gistproc.c
    gist/gist.c
    gist/gistbuild.c
    gist/gistscan.c
    gist/gistsplit.c
    gist/gistutil.c
    gist/gistbuildbuffers.c
    spgist/spgtextproc.c
    spgist/spgxlog.c
    spgist/spgdoinsert.c
    spgist/spgkdtreeproc.c
    spgist/spgvacuum.c
    spgist/spginsert.c
    spgist/spgquadtreeproc.c
    spgist/spgscan.c
    spgist/spgutils.c
    gin/ginxlog.c
    gin/ginbtree.c
    gin/ginvacuum.c
    gin/ginbulk.c
    gin/ginfast.c
    gin/ginget.c
    gin/ginentrypage.c
    gin/ginscan.c
    gin/gininsert.c
    gin/ginutil.c
    gin/ginarrayproc.c
    gin/gindatapage.c
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

add_library(access ${SOURCE_FILES})