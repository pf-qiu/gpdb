cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    atomics.c win32_sema.c win32_shmem.c win32_latch.c
    dynloader/win32.c
    win32/timer.c
    win32/mingwcompat.c
    win32/socket.c
    win32/crashdump.c
    win32/signal.c
    win32/security.c
    win32/pthread.c
    PARENT_SCOPE
)