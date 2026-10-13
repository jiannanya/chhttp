# Shared by the source build and installed package. Keep stable imported target
# names so an installed archive can use a different discovery method than its
# producer (vcpkg, pkg-config or plain system libraries).
set(chhttp_BROTLI_FOUND FALSE)
if(TARGET unofficial::brotli::brotlienc AND TARGET unofficial::brotli::brotlidec)
  set(chhttp_BROTLI_FOUND TRUE)
  return()
endif()

find_package(unofficial-brotli CONFIG QUIET)
if(TARGET unofficial::brotli::brotlienc AND TARGET unofficial::brotli::brotlidec)
  set(chhttp_BROTLI_FOUND TRUE)
  return()
endif()

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(CHHTTP_BROTLIENC QUIET IMPORTED_TARGET libbrotlienc)
  pkg_check_modules(CHHTTP_BROTLIDEC QUIET IMPORTED_TARGET libbrotlidec)
endif()
if(TARGET PkgConfig::CHHTTP_BROTLIENC AND TARGET PkgConfig::CHHTTP_BROTLIDEC)
  foreach(component IN ITEMS enc dec)
    if(NOT TARGET unofficial::brotli::brotli${component})
      string(TOUPPER "${component}" upper_component)
      add_library(unofficial::brotli::brotli${component} INTERFACE IMPORTED)
      set_target_properties(unofficial::brotli::brotli${component} PROPERTIES
        INTERFACE_LINK_LIBRARIES "PkgConfig::CHHTTP_BROTLI${upper_component}")
    endif()
  endforeach()
  set(chhttp_BROTLI_FOUND TRUE)
  return()
endif()

find_path(BROTLI_INCLUDE_DIR brotli/encode.h)
find_library(BROTLIENC_LIBRARY NAMES brotlienc)
find_library(BROTLIDEC_LIBRARY NAMES brotlidec)
find_library(BROTLICOMMON_LIBRARY NAMES brotlicommon)
if(BROTLI_INCLUDE_DIR AND BROTLIENC_LIBRARY AND BROTLIDEC_LIBRARY)
  foreach(component IN ITEMS enc dec)
    if(NOT TARGET unofficial::brotli::brotli${component})
      string(TOUPPER "${component}" upper_component)
      add_library(unofficial::brotli::brotli${component} UNKNOWN IMPORTED)
      set_target_properties(unofficial::brotli::brotli${component} PROPERTIES
        IMPORTED_LOCATION "${BROTLI${upper_component}_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
      if(BROTLICOMMON_LIBRARY)
        set_target_properties(unofficial::brotli::brotli${component} PROPERTIES
          INTERFACE_LINK_LIBRARIES "${BROTLICOMMON_LIBRARY}")
      endif()
    endif()
  endforeach()
  set(chhttp_BROTLI_FOUND TRUE)
endif()
