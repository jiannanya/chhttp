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
