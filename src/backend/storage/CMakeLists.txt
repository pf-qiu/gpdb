cmake_minimum_required(VERSION 3.12)

set(SUBDIRS buffer file freespace ipc large_object lmgr page smgr)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
    list(TRANSFORM CHILD_FILES PREPEND "${SUBDIR}/")
    set(STORAGE_FILES ${STORAGE_FILES} ${CHILD_FILES})    
endforeach(SUBDIR)

set(CHILD_FILES ${STORAGE_FILES} PARENT_SCOPE)
