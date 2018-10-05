cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    lmgr.c lock.c proc.c deadlock.c lwlock.c spin.c s_lock.c predicate.c
    PARENT_SCOPE)
