cmake_minimum_required(VERSION 3.20)

project(miniserve_ai_core VERSION 0.1.0 LANGUAGES CXX)

option(MINISERVE_BUILD_PYTHON "Build the pybind11 extension module" ON)
option(MINISERVE_BUILD_TESTS "Build native C++ tests" ON)

add_library(miniserve_queue STATIC
    core/bounded_queue.cpp
)

target_include_directories(miniserve_queue
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/core
)

target_compile_features(miniserve_queue PUBLIC cxx_std_17)
set_target_properties(miniserve_queue PROPERTIES POSITION_INDEPENDENT_CODE ON)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(miniserve_queue PRIVATE -Wall -Wextra -Wpedantic)
elseif(MSVC)
    target_compile_options(miniserve_queue PRIVATE /W4)
endif()

if(MINISERVE_BUILD_PYTHON)
    find_package(Python 3.10 REQUIRED COMPONENTS Interpreter Development.Module)

    execute_process(
        COMMAND "${Python_EXECUTABLE}" -m pybind11 --cmakedir
        RESULT_VARIABLE PYBIND11_CMAKEDIR_RESULT
        OUTPUT_VARIABLE PYBIND11_CMAKEDIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )

    if(NOT PYBIND11_CMAKEDIR_RESULT EQUAL 0)
        message(FATAL_ERROR
            "pybind11 is not installed for ${Python_EXECUTABLE}. "
            "Activate the project virtual environment and run: "
            "python -m pip install -r requirements.txt"
        )
    endif()

    list(APPEND CMAKE_PREFIX_PATH "${PYBIND11_CMAKEDIR}")
    find_package(pybind11 CONFIG REQUIRED)

    pybind11_add_module(miniserve_core MODULE
        core/bindings.cpp
    )
    target_link_libraries(miniserve_core PRIVATE miniserve_queue)
    target_compile_features(miniserve_core PRIVATE cxx_std_17)

    set_target_properties(miniserve_core PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/python"
    )
endif()

if(MINISERVE_BUILD_TESTS)
    include(CTest)
    find_package(Threads REQUIRED)

    add_executable(test_bounded_queue
        tests/cpp/test_bounded_queue.cpp
    )
    target_link_libraries(test_bounded_queue PRIVATE miniserve_queue Threads::Threads)
    target_compile_features(test_bounded_queue PRIVATE cxx_std_17)

    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(test_bounded_queue PRIVATE -Wall -Wextra -Wpedantic)
    elseif(MSVC)
        target_compile_options(test_bounded_queue PRIVATE /W4)
    endif()

    add_test(NAME bounded_queue_native COMMAND test_bounded_queue)
endif()
