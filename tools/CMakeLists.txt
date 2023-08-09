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
        )

file(GLOB TOOLS_HEADERS
        ${CMAKE_CURRENT_SOURCE_DIR}/common/*.h
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
add_subdirectory(graph_partition)