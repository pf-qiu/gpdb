cmake_minimum_required(VERSION 3.12)

execute_process(COMMAND ${FLEX_EXECUTABLE} "-obootscanner.c" "bootscanner.l"
    RESULT_VARIABLE ret
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
if (NOT ret EQUAL 0)
    message(FATAL_ERROR "failed to gernerate botscanner.c")
endif()
execute_process(COMMAND ${BISON_EXECUTABLE}  "-o" "bootparse.c" "bootparse.y"
    RESULT_VARIABLE ret
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
if (NOT ret EQUAL 0)
    message(FATAL_ERROR "failed to generate bootparse.c")
endif()

set(CHILD_FILES
    bootstrap.c
    bootparse.c
    PARENT_SCOPE
)