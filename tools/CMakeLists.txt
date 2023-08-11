cmake_minimum_required(VERSION 3.12)
project(graph_systems_tools)

#######################
# Libraries
#######################
include_directories(
        ${PROJECT_ROOT_DIR}/core
)

file(GLOB TOOLS_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/common/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/util/*.cpp
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
add_subdirectory(graph_converter)
add_subdirectory(graph_partitioner)