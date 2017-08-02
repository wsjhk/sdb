cmake_minimum_required(VERSION 3.5)
project(sdb)

add_compile_options(-std=c++1z)

add_subdirectory(src/db)
add_subdirectory(src/parser)

aux_source_directory(. DIR_SRCS)
add_executable(sdb ${DIR_SRCS})
target_link_libraries(sdb db parser)
target_link_libraries(sdb -lboost_system -lboost_filesystem)
