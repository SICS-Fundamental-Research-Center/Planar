cmake_minimum_required(VERSION 3.12)
project(pram_on_nvme)

#########################
# Libraries
#########################
include_directories(
        ${PROJECT_ROOT_DIR}
        ${PROJECT_ROOT_DIR}/core
)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/planar)

#########################
# Artifacts
#########################
file(GLOB PLANAR_APP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

foreach (PLANAR_APP_SOURCE ${PLANAR_APP_SOURCES})
    message(STATUS "planar app source: ${PLANAR_APP_SOURCE}")
    get_filename_component(PLANAR_APP_NAME ${PLANAR_APP_SOURCE} NAME_WE)
    add_executable("${PLANAR_APP_NAME}_exec" ${PLANAR_APP_SOURCE})
    target_link_libraries("${PLANAR_APP_NAME}_exec"
            graph_systems_core
            yaml-cpp
            gflags
            ${FOLLY_LIBRARIES}
            )
endforeach ()
