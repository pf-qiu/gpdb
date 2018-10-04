cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES 
    aset.c mcxt.c memaccounting.c mpool.c portalmem.c memprot.c vmem_tracker.c
    redzone_handler.c runaway_cleaner.c idle_tracker.c event_version.c
    ext_alloc.c psprintf.c
    PARENT_SCOPE)
