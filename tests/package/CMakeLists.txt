cmake_minimum_required(VERSION 3.25)
project(chhttp_package_consumer LANGUAGES CXX)

find_package(chhttp CONFIG REQUIRED)
add_executable(chhttp_package_consumer main.cpp)
target_link_libraries(chhttp_package_consumer PRIVATE chhttp::chhttp)
target_compile_features(chhttp_package_consumer PRIVATE cxx_std_20)

