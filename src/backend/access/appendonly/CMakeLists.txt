cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    appendonlyam.c aosegfiles.c aomd.c appendonlywriter.c appendonlytid.c
    appendonlyblockdirectory.c appendonly_visimap.c
    appendonly_visimap_entry.c appendonly_visimap_store.c
    appendonly_compaction.c appendonly_visimap_udf.c
    PARENT_SCOPE)
