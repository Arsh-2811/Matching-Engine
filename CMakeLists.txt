cmake_minimum_required(VERSION 3.20)

project(matching_engine)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

enable_testing()

file(GLOB CORE_SOURCES CONFIGURE_DEPENDS src/*.cpp)

add_library(core
    ${CORE_SOURCES}
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

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS tests/*.cpp)

add_executable(unit_tests
    ${TEST_SOURCES}
)

target_link_libraries(unit_tests
    core
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(unit_tests)