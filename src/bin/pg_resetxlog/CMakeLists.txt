cmake_minimum_required(VERSION 3.12)
project(pg_resetxlog)

add_definitions("/D FRONTEND /D ENABLE_THREAD_SAFETY /wd4996 /wd4018 /wd4090 /wd4267 /MP")

set(SOURCE_FILES
    pg_resetxlog.c
)

set(HEADER_FILES
    ${GPDB_SRC_DIR}/src/include/pg_config.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_executable(pg_resetxlog ${HEADER_FILES} ${SOURCE_FILES})
target_link_libraries(pg_resetxlog port common ws2_32 secur32)

set_target_properties(pg_resetxlog PROPERTIES FOLDER bin)
install(TARGETS pg_resetxlog DESTINATION bin)