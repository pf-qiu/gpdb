cmake_minimum_required(VERSION 3.12)

project(conversion_procs)
configure_file("conversion_create.sql.in" "${CMAKE_CURRENT_SOURCE_DIR}/conversion_create.sql" COPYONLY)

set(HEADER_DIRS
    ${GPDB_SRC_DIR}/src/backend
    ${GPDB_SRC_DIR}/src/port
    ${GPDB_SRC_DIR}/src/interfaces/libpq
    ${GPDB_SRC_DIR}/src/include
    ${GPDB_SRC_DIR}/src/include/port
    ${GPDB_SRC_DIR}/src/include/port/win32
    ${GPDB_SRC_DIR}/src/include/port/win32_msvc
)
include_directories(${HEADER_DIRS})
add_library(conversion_common STATIC ../wchar.c ../conv.c)
set_target_properties(conversion_common PROPERTIES FOLDER conversion_procs)
set(SUBDIRS
    ascii_and_mic
    cyrillic_and_mic
    euc2004_sjis2004
    euc_cn_and_mic
    euc_jp_and_sjis
    euc_kr_and_mic
    euc_tw_and_big5
    latin2_and_win1250
    latin_and_mic
    utf8_and_ascii
    utf8_and_big5
    utf8_and_cyrillic
    utf8_and_euc2004
    utf8_and_euc_cn
    utf8_and_euc_jp
    utf8_and_euc_kr
    utf8_and_euc_tw
    utf8_and_gb18030
    utf8_and_gbk
    utf8_and_iso8859
    utf8_and_iso8859_1
    utf8_and_johab
    utf8_and_sjis
    utf8_and_sjis2004
    utf8_and_uhc
    utf8_and_win)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
endforeach(SUBDIR)

install(FILES conversion_create.sql DESTINATION share/postgresql)