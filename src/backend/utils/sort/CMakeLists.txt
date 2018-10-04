cmake_minimum_required(VERSION 3.12)

execute_process(COMMAND ${PERL_EXECUTABLE} "gen_qsort_tuple.pl"
    OUTPUT_FILE "${CMAKE_CURRENT_SOURCE_DIR}/qsort_tuple.c"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(CHILD_FILES 
    logtape.c sortsupport.c tuplesort.c tuplestore.c tuplestorenew.c
    tuplesort_mk.c tuplesort_mkheap.c tuplesort_mkqsort.c
    PARENT_SCOPE)
