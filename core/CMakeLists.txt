project(graph_systems_core)

# Directories
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin)

#######################
# Libraries
#######################
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
)

#######################
# Artifacts
#######################

file(GLOB GRAPH_CORE_SOURCES
        "*.cpp"
        "apis/*.cpp"
        "apps/*.cpp"
        "common/*.cpp"
        "common/multithreading/*.cpp"
        "components/*.cpp"
        "data_structures/*.cpp"
        "data_structures/graph/*.cpp"
        "io/*.cpp"
        "update_stores/*.cpp"
        "scheduler/*.cpp"
        "util/*.cpp"
        )
list(FILTER GRAPH_CORE_SOURCES EXCLUDE REGEX ".*_test\\.cpp$")
message(STATUS "CORE source files: ${GRAPH_CORE_SOURCES}")

file(GLOB GRAPH_CORE_HEADERS
        "*.h"
        "apis/*.h"
        "apps/*.h"
        "common/*.h"
        "common/multithreading/*.h"
        "components/*.h"
        "data_structures/*.h"
        "data_structures/graph/*.h"
        "io/*.h"
        "update_stores/*.h"
        "scheduler/*.h"
        "util/*.h"
        )

#######################
# Tests
#######################
if (TEST)
    add_subdirectory(tests)
endif ()

#######################
# Generate libgraph_core
#######################
add_library(graph_systems_core ${GRAPH_CORE_SOURCES} ${GRAPH_CORE_HEADERS})

target_link_libraries(graph_systems_core
        ${FOLLY_LIBRARIES}
        )
