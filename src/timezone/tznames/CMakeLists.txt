cmake_minimum_required(VERSION 3.12)

install(FILES Africa.txt America.txt Antarctica.txt Asia.txt
	Atlantic.txt Australia.txt Etc.txt Europe.txt Indian.txt Pacific.txt
    DESTINATION share/postgresql)

install(FILES Default Australia India
    DESTINATION share/postgresql)