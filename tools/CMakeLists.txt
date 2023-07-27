project(tools)

# Directories
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/tools)

#######################
# Libraries
#######################
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# find gflag
find_package(gflags REQUIRED)

#######################
# Artifacts
#######################
file(GLOB GRAPH_CORE_SOURCES
        "*.cpp"
        "graph_convert/*.cpp"
        )

list(FILTER GRAPH_CORE_SOURCES EXCLUDE REGEX ".*_test\\.cpp$")
message(STATUS "CORE source files: ${GRAPH_CORE_SOURCES}")

file(GLOB GRAPH_CORE_HEADERS
        "*.h"
        "graph_convert/*.h"
        )

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

file(GLOB appfiles "${CMAKE_CURRENT_SOURCE_DIR}/graph_convert/*.cpp")
foreach (appfile ${appfiles})
    message(STATUS "Found tool: ${appfile}")
    get_filename_component(app ${appfile} NAME_WE)
    add_executable("${app}_exec" ${appfile})
    target_link_libraries("${app}_exec" graph_systems_core yaml-cpp)
endforeach ()
