cmake_minimum_required(VERSION 3.16)
project(kvstore CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O2")
set(CMAKE_CXX_FLAGS_DEBUG   "-g -fsanitize=address,undefined")

include_directories(include)

add_library(kvstore_lib
    src/bloom_filter.cpp
    src/wal.cpp
    src/sstable.cpp
    src/block_cache.cpp
    src/kvstore.cpp
)
target_include_directories(kvstore_lib PUBLIC include)

add_executable(kvstore src/main.cpp)
target_link_libraries(kvstore kvstore_lib)

add_executable(bench bench/bench.cpp)
target_link_libraries(bench kvstore_lib)

find_package(GTest QUIET)
if(GTest_FOUND)
  enable_testing()
  add_executable(run_tests tests/tests.cpp)
  target_link_libraries(run_tests kvstore_lib GTest::gtest_main)
  add_test(NAME all_tests COMMAND run_tests)
  message(STATUS "GoogleTest found - tests enabled")
else()
  message(STATUS "GoogleTest not found - apt install libgtest-dev")
endif()
