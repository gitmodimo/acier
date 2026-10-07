# SPDX-License-Identifier: Apache-2.0

find_package(Threads REQUIRED)

add_executable(acier_smoke smoke.cc)
target_link_libraries(acier_smoke PRIVATE Acier::acier)
add_test(NAME acier.smoke COMMAND acier_smoke)

add_executable(acier_core_test core_test.cc)
target_link_libraries(acier_core_test PRIVATE Acier::acier)
add_test(NAME acier_core_test COMMAND acier_core_test)
set_tests_properties(acier_core_test PROPERTIES TIMEOUT 60)

add_executable(acier_dataset_test dataset_test.cc)
target_link_libraries(acier_dataset_test PRIVATE Acier::acier Threads::Threads)
add_test(NAME acier_dataset_test COMMAND acier_dataset_test)
set_tests_properties(acier_dataset_test PROPERTIES TIMEOUT 60)

foreach(component join pipeline compute stl generator)
  add_executable(acier_${component}_test ${component}_test.cc)
  target_link_libraries(acier_${component}_test PRIVATE Acier::acier Threads::Threads)
  add_test(NAME acier.${component} COMMAND acier_${component}_test)
  set_tests_properties(acier.${component} PROPERTIES TIMEOUT 60)
endforeach()

include(headers.cmake)
