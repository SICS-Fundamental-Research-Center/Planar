cmake_minimum_required(VERSION 3.12)
project(planar_apps)

#########################
# Libraries
#########################
include_directories(
        ${PROJECT_ROOT_DIR}
        ${PROJECT_ROOT_DIR}/core
)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/compress)

#########################
# Artifacts
#########################
file(GLOB COMPRESS_APP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

find_package(LibLZMA REQUIRED)

find_package(ZLIB REQUIRED)

find_package(Lz4 REQUIRED)

find_package(Snappy REQUIRED)

find_package(Boost COMPONENTS chrono thread REQUIRED)

foreach (COMPRESS_APP_SOURCE ${COMPRESS_APP_SOURCES})
    message(STATUS "Compress try: ${COMPRESS_APP_SOURCE}")
    get_filename_component(COMPRESS_APP_NAME ${COMPRESS_APP_SOURCE} NAME_WE)
    add_executable("${COMPRESS_APP_NAME}_exec" ${COMPRESS_APP_SOURCE})
    target_link_libraries("${COMPRESS_APP_NAME}_exec"
            graph_systems_core
            yaml-cpp
            gflags
            ${FOLLY_LIBRARIES}
            LibLZMA::LibLZMA
            ZLIB::ZLIB
            ${LZ4_LIBRARIES}
            ${SNAPPY_LIBRARIES}
            ${Boost_LIBRARIES}
            )
endforeach ()
