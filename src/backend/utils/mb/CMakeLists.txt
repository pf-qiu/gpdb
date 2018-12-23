cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES 
    encnames.c conv.c mbutils.c wchar.c wstrcmp.c wstrncmp.c
    PARENT_SCOPE)

add_subdirectory(conversion_procs)