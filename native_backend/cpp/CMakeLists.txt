cmake_minimum_required(VERSION 3.20)
project(kubesizer_native_simulator LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
option(ENABLE_SANITIZERS "Enable address and undefined-behavior sanitizers" OFF)

find_path(JANSSON_INCLUDE_DIR jansson.h HINTS "$ENV{KUBESIZER_NATIVE_PREFIX}/include" REQUIRED)
find_library(JANSSON_LIBRARY jansson HINTS "$ENV{KUBESIZER_NATIVE_PREFIX}/lib" REQUIRED)

add_executable(kubesizer-simulator src/main.cpp)
target_include_directories(kubesizer-simulator PRIVATE "${JANSSON_INCLUDE_DIR}")
target_link_libraries(kubesizer-simulator PRIVATE "${JANSSON_LIBRARY}")
target_compile_options(kubesizer-simulator PRIVATE -Wall -Wextra -Wpedantic)

if(ENABLE_SANITIZERS)
  target_compile_options(kubesizer-simulator PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(kubesizer-simulator PRIVATE -fsanitize=address,undefined)
endif()
