cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
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
    PARENT_SCOPE
)