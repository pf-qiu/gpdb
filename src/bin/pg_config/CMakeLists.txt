cmake_minimum_required(VERSION 3.12)
project(pg_config)

add_definitions("/D FRONTEND /D ENABLE_THREAD_SAFETY /wd4996 /wd4018 /wd4090 /wd4267 /MP")

set(SOURCE_FILES
    pg_config.c
)

set(HEADER_FILES
    ${GPDB_SRC_DIR}/src/include/pg_config.h
)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)

include_directories(${HEADER_DIRS})

add_executable(pg_config ${HEADER_FILES} ${SOURCE_FILES})
target_link_libraries(pg_config port common ws2_32 secur32)
install(TARGETS pg_config DESTINATION bin)