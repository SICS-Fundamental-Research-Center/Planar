# This file is used to find folly library in CMake script, modified from the
# code from
#
#   https://github.com/BVLC/caffe/blob/master/cmake/Modules/FindGlog.cmake
#
# which is licensed under the 2-Clause BSD License.
#
# - Try to find folly
#
# The following variables are optionally searched for defaults
#  FOLLY_ROOT_DIR:            Base directory where all JEMALLOC components are found
#
# The following are set after configuration is done:
#  FOLLY_FOUND
#  FOLLY_INCLUDE_DIRS
#  FOLLY_LIBRARIES

find_package(fmt REQUIRED)
find_package(Glog REQUIRED)
find_package(DoubleConversion REQUIRED)

include(FindPackageHandleStandardArgs)

set(FOLLY_ROOT_DIR "" CACHE PATH "Folder contains libfolly")

# We are testing only a couple of files in the include directories
find_path(FOLLY_INCLUDE_DIR folly PATHS ${FOLLY_ROOT_DIR}/include)
find_library(FOLLY_LIBRARY folly PATHS  ${FOLLY_ROOT_DIR}/lib)

find_package_handle_standard_args(FOLLY DEFAULT_MSG FOLLY_INCLUDE_DIR FOLLY_LIBRARY)


if(FOLLY_FOUND)
    set(FOLLY_INCLUDE_DIRS ${FOLLY_INCLUDE_DIR})
    set(FOLLY_LIBRARIES
            ${FOLLY_LIBRARY}
            ${LIBFMT_LIBRARY}
            ${GLOG_LIBRARIES}
            ${DoubleConversion_LIBRARIES}
    )
    message(STATUS "Found folly (include: ${FOLLY_INCLUDE_DIRS}, library: ${FOLLY_LIBRARIES})")
    mark_as_advanced(FOLLY_LIBRARY_DEBUG FOLLY_LIBRARY_RELEASE
            FOLLY_LIBRARY FOLLY_INCLUDE_DIR FOLLY_ROOT_DIR)
endif()