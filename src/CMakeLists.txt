cmake_minimum_required(VERSION 2.7)

#add_subdirectory(query)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-Wall -Wextra -Werror -pedantic -Wno-unused-result -Wno-unused-parameter")

set(SOURCE_FILES
        db/Database.cpp db/Database.h
        db/Table.cpp db/Table.h

        utils/formatter.h utils/uexception.h

        query/Query.cpp query/Query.h
        query/QueryParser.cpp query/QueryParser.h query/QueryResult.h
        query/QueryBuilders.cpp query/QueryBuilders.h

        query/management/LoadTableQuery.cpp query/management/LoadTableQuery.h
        query/management/DropTableQuery.cpp query/management/DropTableQuery.h
        query/management/DumpTableQuery.cpp query/management/DumpTableQuery.h
        query/management/ListTableQuery.cpp query/management/ListTableQuery.h
        query/management/QuitQuery.cpp query/management/QuitQuery.h
        query/management/PrintTableQuery.cpp query/management/PrintTableQuery.h

        query/data/InsertQuery.cpp query/data/InsertQuery.h
        query/data/UpdateQuery.cpp query/data/UpdateQuery.h
        )
add_executable(lemondb main.cpp ${SOURCE_FILES})
target_link_libraries(lemondb pthread)

