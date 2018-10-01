cmake_minimum_required(VERSION 3.6)

project(executor)

add_definitions("/D BUILDING_DLL /wd4996 /wd4018 /wd4090 /wd4244 /wd4267 /MP")

set(SOURCE_FILES
execAmi.c execCurrent.c execGrouping.c execJunk.c execMain.c
       execProcnode.c execQual.c execScan.c execTuples.c
       execUtils.c functions.c instrument.c nodeAppend.c nodeAgg.c
       nodeBitmapAnd.c nodeBitmapOr.c
       nodeBitmapHeapscan.c nodeBitmapIndexscan.c nodeHash.c
       nodeHashjoin.c nodeIndexscan.c nodeIndexonlyscan.c
       nodeLimit.c nodeLockRows.c
       nodeMaterial.c nodeMergeAppend.c nodeMergejoin.c nodeModifyTable.c
       nodeNestloop.c nodeFunctionscan.c nodeRecursiveunion.c nodeResult.c
       nodeSeqscan.c nodeSetOp.c nodeSort.c nodeUnique.c
       nodeValuesscan.c nodeCtescan.c nodeWorktablescan.c
       nodeSubplan.c nodeSubqueryscan.c nodeTidscan.c
       nodeForeignscan.c nodeWindowAgg.c tstoreReceiver.c spi.c

       nodeBitmapTableScan.c
       nodeExternalscan.c
       nodeDynamicIndexscan.c
       nodeDynamicBitmapIndexscan.c
       nodeMotion.c
       nodeRepeat.c
       nodeShareInputScan.c
       nodeTableFunction.c
       nodeTableScan.c nodeDynamicTableScan.c
       nodeSequence.c
       nodeRowTrigger.c
       nodeAssertOp.c
       nodeSplitUpdate.c
       nodeDML.c
       execDML.c
       nodePartitionSelector.c
       execBitmapTableScan.c execBitmapHeapScan.c execBitmapAOScan.c
       execDynamicScan.c
       execHHashagg.c execGpmon.c execWorkfile.c execHeapScan.c execAOScan.c
       execAOCSScan.c nodeBitmapAppendOnlyscan.c
)

set(HEADER_FILES
    ${GPDB_SRC_DIR}/src/include/pg_config.h
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

add_library(executor ${HEADER_FILES} ${SOURCE_FILES})