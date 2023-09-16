cmake_minimum_required(VERSION 3.12)
project(graph_systems_tools_graph_convert)

# Directories
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/tools/)

#######################
# Artifacts
#######################
file(GLOB appfiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach (appfile ${appfiles})
    message(STATUS "Found tool: ${appfile}")
    get_filename_component(app ${appfile} NAME_WE)
    add_executable("${app}_exec" ${appfile} ${TOOLS_SOURCES})
    target_link_libraries("${app}_exec"
            graph_systems_core
            yaml-cpp
            gflags
            ${FOLLY_LIBRARIES}
            ${TBB_LIBRARIES}
    )
endforeach ()
