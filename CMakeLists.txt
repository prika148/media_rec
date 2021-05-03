cmake_minimum_required(VERSION 2.8)
project(media_rec)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -g")
set(SRC_LIST main.cpp)

add_executable(${PROJECT_NAME} ${SRC_LIST})

TARGET_LINK_LIBRARIES(${PROJECT_NAME} pthread)
