cmake_minimum_required(VERSION 3.12)

project(storage)

add_definitions("/D BUILDING_DLL /D ENABLE_THREAD_SAFETY /D USE_CURL")
add_definitions("/wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /wd4715")
add_definitions("/MP")

set(SOURCE_FILES
    buffer/bufmgr.c
    buffer/buf_table.c
    buffer/buf_init.c
    buffer/freelist.c
    buffer/localbuf.c
    file/gp_compress.c
    file/compress_zlib.c
    file/buffile.c
    file/compress_nothing.c
    file/fd.c
    file/copydir.c
    file/bfz.c
    file/reinit.c
    freespace/fsmpage.c
    freespace/freespace.c
    freespace/indexfsm.c
    ipc/shmem.c
    ipc/procarray.c
    ipc/ipci.c
    ipc/standby.c
    ipc/sinvaladt.c
    ipc/sinval.c
    ipc/shmqueue.c
    ipc/pmsignal.c
    ipc/ipc.c
    ipc/procsignal.c
    large_object/inv_api.c
    lmgr/lmgr.c
    lmgr/predicate.c
    lmgr/s_lock.c
    lmgr/lwlock.c
    lmgr/proc.c
    lmgr/deadlock.c
    lmgr/spin.c
    lmgr/lock.c
    page/bufpage.c
    page/checksum.c
    page/itemptr.c
    smgr/smgr.c
    smgr/smgrtype.c
    smgr/md.c
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

add_library(storage ${SOURCE_FILES})