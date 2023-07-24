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
        "common/*.cpp"
        "common/multithreading/*.cpp"
        "data_structures/*.cpp"
        "util/*.cpp"
        "io/*.cpp"
        )

file(GLOB GRAPH_CORE_HEADERS
        "*.h"
        "common/*.h"
        "common/multithreading/*.h"
        "data_structures/*.h"
        "util/*.h"
        "io/*.h"
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
