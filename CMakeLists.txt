cmake_minimum_required(VERSION 3.20)

project(matching_engine)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

enable_testing()

add_library(core
    src/core.cpp
    src/network/worker.cpp
)

add_executable(exchange_app src/main.cpp)
add_executable(load_gen tests/load_generator.cpp)

target_compile_options(core PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    $<$<CONFIG:Release>:-O3 -march=native -flto>
)
target_link_options(core PRIVATE
    $<$<CONFIG:Release>:-flto>
)

target_include_directories(core PUBLIC include)

target_compile_options(exchange_app PRIVATE $<$<CONFIG:Release>:-O3 -march=native -flto>)
target_compile_options(load_gen     PRIVATE $<$<CONFIG:Release>:-O3 -march=native -flto>)
target_link_options(exchange_app    PRIVATE $<$<CONFIG:Release>:-flto>)
target_link_options(load_gen        PRIVATE $<$<CONFIG:Release>:-flto>)

target_link_libraries(exchange_app core)
target_link_libraries(load_gen core)

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
list(FILTER TEST_SOURCES EXCLUDE REGEX ".*load_generator.cpp$")

add_executable(unit_tests
    ${TEST_SOURCES}
)

target_link_libraries(unit_tests
    core
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(unit_tests)