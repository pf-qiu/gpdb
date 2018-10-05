cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    spgutils.c spginsert.c spgscan.c spgvacuum.c
    spgdoinsert.c spgxlog.c
    spgtextproc.c spgquadtreeproc.c spgkdtreeproc.c
    PARENT_SCOPE)
