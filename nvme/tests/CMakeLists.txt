project(graph-systems_nvme_tests)

include_directories(SYSTEM ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
include_directories(SYSTEM ${gmock_SOURCE_DIR}/include ${gmock_SOURCE_DIR})

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_ROOT_DIR}/bin/tests/)

####################
# Build Unit Tests #
####################
file(GLOB testfiles
        "${PROJECT_ROOT_DIR}/nvme/apis/*_test.cpp"
        "${PROJECT_ROOT_DIR}/nvme/apps/*_test.cpp"
        "${PROJECT_ROOT_DIR}/nvme/io/*_test.cpp"
        "${PROJECT_ROOT_DIR}/nvme/scheduler/*_test.cpp"
        "${PROJECT_ROOT_DIR}/nvme/tests/*_test.cpp"
        "${PROJECT_ROOT_DIR}/nvme/update_stores/*_test.cpp"
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
            gflags
            )
    add_test(NAME "${TESTNAME}" COMMAND "${filename}")
    target_compile_definitions(${filename} PUBLIC TEST_DATA_DIR= "${PROJECT_ROOT_DIR}/testfile")
    #    set_tests_properties(${TESTNAME} PROPERTIES ENVIRONMENT "DATA_DIR=${PROJECT_ROOT_DIR}/testfile")
endforeach ()

