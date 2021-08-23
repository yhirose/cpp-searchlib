cmake_minimum_required(VERSION 3.14)
project(scope)

add_executable(scope main.cpp)

target_include_directories(scope PRIVATE ../include ../src)
