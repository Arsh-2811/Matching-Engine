cmake_minimum_required(VERSION 3.20)

project(matching_engine)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

enable_testing()

add_library(core
    src/add.cpp
)

target_compile_options(core PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

target_include_directories(core PUBLIC include)

# -------------------------------
# GoogleTest (download automatically)
# -------------------------------

include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/heads/main.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(googletest)

add_executable(unit_tests
    tests/test_add.cpp
)

target_link_libraries(unit_tests
    core
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(unit_tests)