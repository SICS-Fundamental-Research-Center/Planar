cmake_minimum_required(VERSION 3.12)
project(pram_on_nvme)

#########################
# Libraries
#########################
include_directories(
        ${PROJECT_ROOT_DIR}
        ${PROJECT_ROOT_DIR}/core
)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/nvme)

#########################
# Artifacts
#########################
file(GLOB NVME_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/apps/*.cpp"
        )

list(FILTER NVME_SOURCES EXCLUDE REGEX ".*_test\\.cpp$")
message(STATUS "NVMe CORE source files: ${NVME_SOURCES}")

file(GLOB NVME_HEADERS
        "*.h"
        "apps/*.h"
        )

foreach (NVME_SOURCE ${NVME_SOURCES})
    message(STATUS "nvme app source: ${NVME_SOURCE}")
    get_filename_component(NVME_NAME ${NVME_SOURCE} NAME_WE)
    add_executable("${NVME_NAME}_exec" ${NVME_SOURCE} ${NVME_HEADERS})
    target_link_libraries("${NVME_NAME}_exec"
            graph_systems_core
            yaml-cpp
            gflags
            ${FOLLY_LIBRARIES}
            )
endforeach ()
