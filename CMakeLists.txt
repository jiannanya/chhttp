cmake_minimum_required(VERSION 3.25)
project(chhttp VERSION 0.2.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(CHHTTP_BUILD_TESTS "Build the chhttp test suite" ON)
option(CHHTTP_BUILD_EXAMPLES "Build chhttp examples" ON)
option(CHHTTP_ENABLE_TLS "Enable TLS through OpenSSL" ON)
option(CHHTTP_ENABLE_COMPRESSION "Enable gzip/deflate, Brotli and Zstd" ON)

find_package(libuv CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

if(CHHTTP_ENABLE_COMPRESSION)
  find_package(ZLIB REQUIRED)
  find_package(unofficial-brotli CONFIG REQUIRED)
  find_package(zstd CONFIG REQUIRED)
endif()

add_library(chhttp STATIC
  src/common.cpp
  src/runtime.cpp
  src/protocol.cpp
  src/server.cpp
  src/client.cpp
  src/sse.cpp
)
add_library(chhttp::chhttp ALIAS chhttp)

target_compile_features(chhttp PUBLIC cxx_std_20)
target_include_directories(chhttp
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(chhttp PUBLIC
  $<IF:$<TARGET_EXISTS:libuv::uv_a>,libuv::uv_a,libuv::uv>
  OpenSSL::Crypto
)
target_compile_definitions(chhttp PUBLIC CHHTTP_HAS_CRYPTO=1)

if(CHHTTP_ENABLE_TLS)
  target_compile_definitions(chhttp PUBLIC CHHTTP_HAS_TLS=1)
  target_link_libraries(chhttp PUBLIC OpenSSL::SSL)
endif()

if(CHHTTP_ENABLE_COMPRESSION)
  target_compile_definitions(chhttp PUBLIC CHHTTP_HAS_COMPRESSION=1)
  target_link_libraries(chhttp PRIVATE
    ZLIB::ZLIB
    unofficial::brotli::brotlidec
    unofficial::brotli::brotlienc
    zstd::libzstd
  )
endif()

if(WIN32)
  target_compile_definitions(chhttp PUBLIC _WIN32_WINNT=0x0A00)
  target_compile_definitions(chhttp PRIVATE WIN32_LEAN_AND_MEAN NOMINMAX)
  target_link_libraries(chhttp PUBLIC ws2_32 crypt32)
endif()

if(MSVC)
  target_compile_options(chhttp PRIVATE /W4 /permissive- /utf-8 /Zc:preprocessor)
else()
  target_compile_options(chhttp PRIVATE -Wall -Wextra -Wpedantic)
endif()

set_target_properties(chhttp PROPERTIES
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR}
  EXPORT_NAME chhttp
)

if(CHHTTP_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(CHHTTP_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

install(TARGETS chhttp EXPORT chhttpTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT chhttpTargets
  FILE chhttpTargets.cmake
  NAMESPACE chhttp::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/chhttp
)
configure_package_config_file(cmake/chhttpConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/chhttpConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/chhttp
)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/chhttpConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/chhttpConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/chhttpConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/chhttp
)
