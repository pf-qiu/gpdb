cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    clog.c transam.c varsup.c xact.c rmgr.c slru.c subtrans.c multixact.c
    timeline.c twophase.c twophase_rmgr.c xlog.c xlogarchive.c xlogfuncs.c
    xlogreader.c xlogutils.c
    distributedlog.c gp_transaction_log.c gp_distributed_log.c xlogloc.c
    PARENT_SCOPE)
