cmake_minimum_required(VERSION 3.5)
project(sdb)

add_compile_options(-std=c++1z)
# add_compile_options(-fconcepts)

# add_subdirectory(src/db)
# add_subdirectory(src/sql)

# aux_source_directory(. DIR_SRCS)
# add_executable(sdb ${DIR_SRCS})
# target_link_libraries(sdb db sql -lstdc++fs)

# === test ===
enable_testing()
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

file(GLOB DB_TEST_SOURCES_FILES test/db/*.cpp)
file(GLOB DB_SOURCES_FILES src/db/io.cpp src/db/cache.cpp src/db/block_alloc.cpp)

add_executable(sdb_test test/Main.cpp ${DB_TEST_SOURCES_FILES} ${DB_SOURCES_FILES})
target_link_libraries(sdb_test ${GTEST_BOTH_LIBRARIES} -lstdc++fs)
