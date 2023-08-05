cmake_minimum_required(VERSION 3.12)
project(graph_systems_tools)

#######################
# Libraries
#######################
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/tools_common
        ${PROJECT_ROOT_DIR}/core
)

file(GLOB TOOLS_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/tools_common/*.cpp
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
