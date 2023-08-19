cmake_minimum_required(VERSION 3.12)
project(graph_systems_tools_tests)

include_directories(SYSTEM ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
include_directories(SYSTEM ${gmock_SOURCE_DIR}/include ${gmock_SOURCE_DIR})

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/tests/)

####################
# Build Unit Tests #
####################
file(GLOB testfiles
        "${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp"
        )

foreach (testfile ${testfiles})
    get_filename_component(filename ${testfile} NAME_WE)
    string(REPLACE "_test"
            "" testname
            ${filename})
    string(TOUPPER ${testname} TESTNAME)
    add_executable(${filename} "${testfile}")
    target_link_libraries(${filename}
            graph_systems_core
            gtest
            gtest_main
            ${FOLLY_LIBRARIES}
            yaml-cpp::yaml-cpp
            )
    add_test(NAME "${TESTNAME}" COMMAND "${filename}")
endforeach ()
