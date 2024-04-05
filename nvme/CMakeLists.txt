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
        "${CMAKE_CURRENT_SOURCE_DIR}/apis/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/apps/*.cpp"
        "${CMAME_CURRENT_SOURCE_DIR}/common/multithreading/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/components/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/io/*cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/update_stores/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/scheduler/*.cpp"
        )

list(FILTER NVME_SOURCES EXCLUDE REGEX ".*_test\\.cpp$")
message(STATUS "NVMe CORE source files: ${NVME_SOURCES}")

file(GLOB NVME_HEADERS
        "${CMAKE_CURRENT_SOURCE_DIR}/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/apis/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/apps/*.h"
        "${CMAME_CURRENT_SOURCE_DIR}/common/*.h"
        "${CMAME_CURRENT_SOURCE_DIR}/common/multithreading/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/components/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/io/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/update_stores/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/scheduler/*.h"
        )

file(GLOB APP_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/run/*cpp"
        )
list(FILTER APP_SOURCES EXCLUDE REGEX ".*_test\\.cpp$")

foreach (APP_SOURCE ${APP_SOURCES})
    message(STATUS "nvme app source: ${APP_SOURCE}")
    get_filename_component(APP_NAME ${APP_SOURCE} NAME_WE)
    add_executable("${APP_NAME}_exec" ${APP_SOURCE} ${NVME_HEADERS} ${NVME_SOURCES})
    target_link_libraries("${APP_NAME}_exec"
            graph_systems_core
            yaml-cpp
            gflags
            ${FOLLY_LIBRARIES}
            )
endforeach ()

#######################
# Tests
#######################
if (TEST)
    add_subdirectory(tests)
endif ()

#######################
# Generate libgraph_core
#######################
add_library(nvme_core ${NVME_SOURCES} ${NVME_HEADERS})

target_link_libraries(nvme_core
        yaml-cpp
        gflags
        ${FOLLY_LIBRARIES}
        )