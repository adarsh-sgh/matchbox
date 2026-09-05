cmake_minimum_required(VERSION 3.16)
project(matchbox CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

option(MATCHBOX_ASAN "Build with AddressSanitizer + UBSan" OFF)

find_package(Threads REQUIRED)

add_library(matchbox STATIC
  src/book.cpp
  src/engine.cpp
  src/net.cpp
  src/poller.cpp
  src/publisher.cpp
  src/server.cpp)
target_include_directories(matchbox PUBLIC include)
target_compile_options(matchbox PUBLIC -Wall -Wextra -Wpedantic)
target_link_libraries(matchbox PUBLIC Threads::Threads)

if(MATCHBOX_ASAN)
  target_compile_options(matchbox PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(matchbox PUBLIC -fsanitize=address,undefined)
endif()

add_executable(matchbox_server src/main_server.cpp)
add_executable(matchbox_client src/main_client.cpp)
add_executable(matchbox_bench src/main_bench.cpp)
foreach(t matchbox_server matchbox_client matchbox_bench)
  target_link_libraries(${t} PRIVATE matchbox)
endforeach()

enable_testing()
add_executable(matchbox_tests
  tests/test_main.cpp
  tests/test_book.cpp
  tests/test_engine.cpp
  tests/test_protocol.cpp
  tests/test_spsc.cpp
  tests/test_e2e.cpp)
target_link_libraries(matchbox_tests PRIVATE matchbox)
add_test(NAME matchbox_tests COMMAND matchbox_tests)
