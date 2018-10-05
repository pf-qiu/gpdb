cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    fd.c buffile.c copydir.c reinit.c
    bfz.c compress_nothing.c compress_zlib.c gp_compress.c
    PARENT_SCOPE)
