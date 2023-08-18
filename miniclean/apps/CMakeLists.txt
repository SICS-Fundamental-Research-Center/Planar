cmake_minimum_required(VERSION 3.12)
project(graph_system_miniclean_apps)

# Directories
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/miniclean/apps)

#######################
# Artifacts
#######################
file(GLOB appfiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach (appfile ${appfiles})
    message(STATUS "Found tool: ${appfile}")
    get_filename_component(app ${appfile} NAME_WE)
    add_executable("${app}_exec" ${appfile})
    target_link_libraries("${app}_exec"
            graph_systems_core
            graph_systems_miniclean 
            yaml-cpp
            gflags
    )
endforeach ()