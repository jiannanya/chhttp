add_executable(chhttp_tests test_main.cpp)
target_link_libraries(chhttp_tests PRIVATE chhttp::chhttp)
target_compile_features(chhttp_tests PRIVATE cxx_std_20)
if(MSVC)
  target_compile_options(chhttp_tests PRIVATE /W4 /permissive- /utf-8)
else()
  target_compile_options(chhttp_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
add_test(NAME chhttp.functional COMMAND chhttp_tests --functional)
add_test(NAME chhttp.stress COMMAND chhttp_tests --stress)
add_test(NAME chhttp.property COMMAND chhttp_tests --property)
set_tests_properties(chhttp.property PROPERTIES LABELS "property" RUN_SERIAL TRUE TIMEOUT 180)
set_tests_properties(chhttp.functional PROPERTIES
  LABELS "functional"
  RUN_SERIAL TRUE
  TIMEOUT 180
)
set_tests_properties(chhttp.stress PROPERTIES
  LABELS "stress"
  PROCESSORS 4
  RUN_SERIAL TRUE
  TIMEOUT 300
)

if(WIN32 AND CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND
   CMAKE_CXX_FLAGS MATCHES "-shared-libasan")
  execute_process(COMMAND "${CMAKE_CXX_COMPILER}" --print-runtime-dir
    OUTPUT_VARIABLE CHHTTP_ASAN_RUNTIME_DIR OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY)
  set_tests_properties(chhttp.functional chhttp.stress chhttp.property PROPERTIES
    ENVIRONMENT_MODIFICATION "PATH=path_list_prepend:${CHHTTP_ASAN_RUNTIME_DIR}")
endif()
