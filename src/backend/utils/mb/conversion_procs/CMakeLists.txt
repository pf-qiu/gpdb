cmake_minimum_required(VERSION 3.12)

configure_file("conversion_create.sql.in" "${CMAKE_CURRENT_SOURCE_DIR}/conversion_create.sql" COPYONLY)

install(FILES conversion_create.sql DESTINATION share/postgresql)