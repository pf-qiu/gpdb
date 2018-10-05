cmake_minimum_required(VERSION 3.12)

set(SUBDIRS common gin gist hash heap index nbtree rmgrdesc spgist
    transam external bitmap appendonly aocs)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
    list(TRANSFORM CHILD_FILES PREPEND "${SUBDIR}/")
    set(ACCESS_FILES ${ACCESS_FILES} ${CHILD_FILES})    
endforeach(SUBDIR)

set(CHILD_FILES ${ACCESS_FILES} PARENT_SCOPE)
