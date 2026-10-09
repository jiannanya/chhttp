cmake_minimum_required(VERSION 3.25)
project(chhttp VERSION 0.4.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(CHHTTP_BUILD_TESTS "Build the chhttp test suite" ON)
option(CHHTTP_BUILD_EXAMPLES "Build chhttp examples" ON)
option(CHHTTP_BUILD_BENCHMARKS "Build repeatable performance/memory benchmarks" OFF)
option(CHHTTP_ENABLE_TLS "Enable TLS through OpenSSL" ON)
option(CHHTTP_ENABLE_COMPRESSION "Enable gzip/deflate, Brotli and Zstd" ON)
option(CHHTTP_ENABLE_LTO "Compile the static library with link-time optimization" OFF)

find_package(libuv CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

if(CHHTTP_ENABLE_COMPRESSION)
  find_package(ZLIB REQUIRED)
  find_package(zstd CONFIG REQUIRED)
  # vcpkg ships an "unofficial-brotli" config, but many system package managers
  # (Homebrew, Debian, Fedora) only install plain headers and libraries. Fall
  # back to pkg-config or a manual search so the default configuration builds
  # everywhere instead of requiring a vcpkg-specific package.
  find_package(unofficial-brotli CONFIG QUIET)
  if(NOT unofficial-brotli_FOUND)
    if(PkgConfig_FOUND OR NOT DEFINED CMAKE_DISABLE_FIND_PACKAGE_PkgConfig)
      find_package(PkgConfig QUIET)
    endif()
    if(PkgConfig_FOUND)
      pkg_check_modules(BROTLIENC IMPORTED_TARGET libbrotlienc)
      pkg_check_modules(BROTLIDEC IMPORTED_TARGET libbrotlidec)
      pkg_check_modules(BROTLICOMMON IMPORTED_TARGET libbrotlicommon)
    endif()
    if(BROTLIENC_FOUND AND BROTLIDEC_FOUND)
      add_library(unofficial::brotli::brotlienc ALIAS PkgConfig::BROTLIENC)
      add_library(unofficial::brotli::brotlidec ALIAS PkgConfig::BROTLIDEC)
    else()
      find_path(BROTLI_INCLUDE_DIR brotli/encode.h)
      find_library(BROTLIENC_LIBRARY NAMES brotlienc)
      find_library(BROTLIDEC_LIBRARY NAMES brotlidec)
      find_library(BROTLICOMMON_LIBRARY NAMES brotlicommon)
      if(BROTLI_INCLUDE_DIR AND BROTLIENC_LIBRARY AND BROTLIDEC_LIBRARY)
        add_library(chhttp_brotlienc UNKNOWN IMPORTED)
        set_target_properties(chhttp_brotlienc PROPERTIES
          IMPORTED_LOCATION "${BROTLIENC_LIBRARY}"
          INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
        add_library(chhttp_brotlidec UNKNOWN IMPORTED)
        set_target_properties(chhttp_brotlidec PROPERTIES
          IMPORTED_LOCATION "${BROTLIDEC_LIBRARY}"
          INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
        # Brotli's decoder links against its common library on most platforms.
        if(BROTLICOMMON_LIBRARY AND NOT APPLE)
          set_target_properties(chhttp_brotlidec PROPERTIES
            INTERFACE_LINK_LIBRARIES "${BROTLICOMMON_LIBRARY}")
        endif()
        add_library(unofficial::brotli::brotlienc ALIAS chhttp_brotlienc)
        add_library(unofficial::brotli::brotlidec ALIAS chhttp_brotlidec)
      else()
        message(FATAL_ERROR
          "Brotli was not found. Install it (for example `brew install brotli` or "
          "`apt install libbrotli-dev`), set CHHTTP_ENABLE_COMPRESSION=OFF, or point "
          "CMAKE_PREFIX_PATH at a vcpkg toolchain that provides unofficial-brotli.")
      endif()
    endif()
  endif()
endif()

add_library(chhttp STATIC
  src/common.cpp
  src/runtime.cpp
  src/protocol.cpp
  src/streaming_body.cpp
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

# Portable LTO: CMake maps this to MSVC /GL+/LTCG and GCC/Clang -flto, so no
# architecture- or compiler-specific flags are needed.
if(CHHTTP_ENABLE_LTO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT CHHTTP_IPO_SUPPORTED OUTPUT CHHTTP_IPO_ERROR)
  if(CHHTTP_IPO_SUPPORTED)
    set_property(TARGET chhttp PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
  else()
    message(WARNING "Link-time optimization was requested but is unsupported: ${CHHTTP_IPO_ERROR}")
  endif()
endif()

if(CHHTTP_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(CHHTTP_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(CHHTTP_BUILD_BENCHMARKS)
  add_executable(chhttp_benchmarks benchmarks/benchmark.cpp)
  target_link_libraries(chhttp_benchmarks PRIVATE chhttp::chhttp)
  if(WIN32)
    target_link_libraries(chhttp_benchmarks PRIVATE psapi)
    target_compile_definitions(chhttp_benchmarks PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
  endif()
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
