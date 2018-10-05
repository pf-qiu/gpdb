cmake_minimum_required(VERSION 3.12)

set(SUBDIRS path plan prep util)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
    list(TRANSFORM CHILD_FILES PREPEND "${SUBDIR}/")
    set(OPTIMIZER_FILES ${OPTIMIZER_FILES} ${CHILD_FILES})    
endforeach(SUBDIR)

set(CHILD_FILES ${OPTIMIZER_FILES} PARENT_SCOPE)
