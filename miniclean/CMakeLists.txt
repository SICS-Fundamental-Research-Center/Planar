cmake_minimum_required(VERSION 3.12)
project(graph_systems_miniclean)

# Directories
set(LIBRARY_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/miniclean) 

#######################
# Libraries
#######################
include_directories(
        ${PROJECT_ROOT_DIR}
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
file(GLOB GRAPH_MINICLEAN_SOURCES
        "*.cpp"
        "graphs/*.cpp"
        "components/*.cpp"
        "io/*.cpp"
        )
message(STATUS "CORE source files: ${GRAPH_MINICLEAN_SOURCES}")
list(FILTER GRAPH_MINICLEAN_SOURCES EXCLUDE REGEX ".*_test\\.cpp$")

file(GLOB GRAPH_MINICLEAN_HEADERS
        "*.h"
        "graphs/*.h"
        "components/*.h"
        "io/*.h"
        )
message(STATUS "CORE header files: ${GRAPH_MINICLEAN_HEADERS}")

add_subdirectory(apps)

#######################
# Generate libgraph_core
#######################
add_library(graph_systems_miniclean ${GRAPH_MINICLEAN_SOURCES} ${GRAPH_MINICLEAN_HEADERS})
target_link_libraries(graph_systems_miniclean
        graph_systems_core
        yaml-cpp
        gflags
        ${FOLLY_LIBRARIES}
        ${LIBFMT_LIBRARY}
)