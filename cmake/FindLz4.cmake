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

#find_path(LZ4_INCLUDE_DIR NAMES lz4.h)
#
#find_library(LZ4_LIBRARY_DEBUG NAMES lz4d)
#find_library(LZ4_LIBRARY_RELEASE NAMES lz4)
#
#include(SelectLibraryConfigurations)
#SELECT_LIBRARY_CONFIGURATIONS(LZ4)
#
#include(FindPackageHandleStandardArgs)
#FIND_PACKAGE_HANDLE_STANDARD_ARGS(
#        LZ4 DEFAULT_MSG
#        LZ4_LIBRARY LZ4_INCLUDE_DIR
#)
#
#if (LZ4_FOUND)
#    message(STATUS "Found LZ4: ${LZ4_LIBRARY}")
#endif()
#
#mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY)
#
# 设置LZ4的默认路径

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(LZ4_ROOT_DIR "/usr")
endif()

# 查找LZ4库
find_path(LZ4_INCLUDE_DIR lz4.h
        PATHS ${LZ4_ROOT_DIR}/include
        PATH_SUFFIXES lz4
        )

find_library(LZ4_LIBRARY NAMES lz4
        PATHS ${LZ4_ROOT_DIR}/lib
        )

# 检查是否找到LZ4库和头文件
if(LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
    set(LZ4_FOUND TRUE)
else()
    set(LZ4_FOUND FALSE)
endif()

# 导出LZ4的相关变量
if(LZ4_FOUND)
    set(LZ4_INCLUDE_DIRS ${LZ4_INCLUDE_DIR})
    set(LZ4_LIBRARIES ${LZ4_LIBRARY})
    message(STATUS "Found LZ4: ${LZ4_LIBRARIES}")
endif()

# 导出LZ4的相关变量到全局变量
#if(NOT LZ4_FIND_QUIETLY)
#    set(LZ4_INCLUDE_DIRS ${LZ4_INCLUDE_DIRS} CACHE PATH "LZ4 include directories")
#    set(LZ4_LIBRARIES ${LZ4_LIBRARIES} CACHE FILEPATH "LZ4 libraries")
#endif()
