cmake_minimum_required(VERSION 3.12)
project(graph_systems_tools)

#######################
# Libraries
#######################
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/common
        ${PROJECT_ROOT_DIR}/core
)

# find gflag
find_package(gflags REQUIRED)

#######################
# Tests
#######################
if (TEST)
    add_subdirectory(tests)
endif ()

#######################
# Artifacts
#######################
add_subdirectory(graph_convert)
