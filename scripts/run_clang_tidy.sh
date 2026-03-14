#!/usr/bin/env bash
set -e

clang-tidy \
-p build \
-header-filter='src/|include/' \
src/*.cpp tests/*.cpp \
--quiet