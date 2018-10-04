cmake_minimum_required(VERSION 3.12)

execute_process(COMMAND ${FLEX_EXECUTABLE} "-oguc-file.c" "guc-file.l"
    RESULT_VARIABLE ret
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
if (NOT ret EQUAL 0)
    message(FATAL_ERROR "failed to gernerate guc-file.c")
endif()

set(CHILD_FILES 
    guc.c help_config.c pg_rusage.c ps_status.c rbtree.c
    faultinjector.c testutils.c uriparser.c
    bitstream.c bitmap_compression.c guc_gp.c zlib_wrapper.c backend_cancel.c
    superuser.c timeout.c tzparser.c
    fstream/fstream.c fstream/gfile.c
    PARENT_SCOPE)
