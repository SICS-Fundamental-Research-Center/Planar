# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Finds liblz4.
#
# This module defines:
# LZ4_FOUND
# LZ4_INCLUDE_DIR
# LZ4_LIBRARY
#
if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(SNAPPY_ROOT_DIR "/usr")
endif()

find_library(SNAPPY_LIBRARY snappy
        PATHS ${SNAPPY_ROOT_DIR}/include)

find_path(SNAPPY_INCLUDE_DIR snappy.h
        PATHS ${SNAPPY_ROOT_DIR}/lib)

find_package_handle_standard_args(Snappy DEFAULT_MSG
        SNAPPY_LIBRARY
        SNAPPY_INCLUDE_DIR)

mark_as_advanced(
        SNAPPY_LIBRARY
        SNAPPY_INCLUDE_DIR)

if (SNAPPY_FOUND)
    set(SNAPPY_LIBRARIES ${SNAPPY_LIBRARY})
    set(SNAPPY_INCLUDE_DIRS ${SNAPPY_INCLUDE_DIR})
    message(STATUS "Found snappy (include: ${SNAPPY_INCLUDE_DIRS}, library: ${SNAPPY_LIBRARIES})")
endif ()
