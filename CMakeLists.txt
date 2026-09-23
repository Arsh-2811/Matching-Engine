cmake_minimum_required(VERSION 3.20)

project(matching_engine)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# PGO profile data directory — must be the same absolute path for PGOGen and PGOUse.
# Guaranteed by reusing the same -B build directory for both phases.
set(PGO_PROFILE_DIR "${CMAKE_BINARY_DIR}/pgo-profiles")

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
    $<$<CONFIG:PGOGen>:-O3 -march=native -flto -fprofile-generate=${PGO_PROFILE_DIR} -fprofile-update=atomic>
    $<$<CONFIG:PGOUse>:-O3 -march=native -flto -fprofile-use=${PGO_PROFILE_DIR} -fprofile-correction -Wno-error=coverage-mismatch>
)
target_link_options(core PRIVATE
    $<$<CONFIG:Release>:-flto>
    $<$<CONFIG:PGOGen>:-flto>
    $<$<CONFIG:PGOUse>:-flto>
)

target_include_directories(core PUBLIC include)

target_compile_options(exchange_app PRIVATE
    $<$<CONFIG:Release>:-O3 -march=native -flto>
    $<$<CONFIG:PGOGen>:-O3 -march=native -flto -fprofile-generate=${PGO_PROFILE_DIR} -fprofile-update=atomic>
    $<$<CONFIG:PGOUse>:-O3 -march=native -flto -fprofile-use=${PGO_PROFILE_DIR} -fprofile-correction -Wno-error=coverage-mismatch>
)
target_compile_options(load_gen PRIVATE
    $<$<CONFIG:Release>:-O3 -march=native -flto>
    $<$<CONFIG:PGOGen>:-O3 -march=native -flto -fprofile-generate=${PGO_PROFILE_DIR} -fprofile-update=atomic>
    $<$<CONFIG:PGOUse>:-O3 -march=native -flto -fprofile-use=${PGO_PROFILE_DIR} -fprofile-correction -Wno-error=coverage-mismatch>
)
target_link_options(exchange_app PRIVATE
    $<$<CONFIG:Release>:-flto>
    $<$<CONFIG:PGOGen>:-flto -fprofile-generate=${PGO_PROFILE_DIR}>
    $<$<CONFIG:PGOUse>:-flto>
)
target_link_options(load_gen PRIVATE
    $<$<CONFIG:Release>:-flto>
    $<$<CONFIG:PGOGen>:-flto -fprofile-generate=${PGO_PROFILE_DIR}>
    $<$<CONFIG:PGOUse>:-flto>
)

target_link_libraries(exchange_app core)
target_link_libraries(load_gen core)

# GoogleTest disabled — network download not available in this environment