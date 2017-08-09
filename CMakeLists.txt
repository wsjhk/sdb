cmake_minimum_required(VERSION 3.5)
project(sdb)

add_compile_options(-std=c++1z)

add_subdirectory(src/db)
add_subdirectory(src/sql)

aux_source_directory(. DIR_SRCS)
add_executable(sdb ${DIR_SRCS})
target_link_libraries(sdb db sql -lstdc++fs)
