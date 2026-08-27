add_executable(chhttp_tests test_main.cpp)
target_link_libraries(chhttp_tests PRIVATE chhttp::chhttp)
target_compile_features(chhttp_tests PRIVATE cxx_std_20)
if(MSVC)
  target_compile_options(chhttp_tests PRIVATE /W4 /permissive- /utf-8)
endif()
add_test(NAME chhttp.all COMMAND chhttp_tests)
set_tests_properties(chhttp.all PROPERTIES TIMEOUT 120)

