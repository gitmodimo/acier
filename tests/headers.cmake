# SPDX-License-Identifier: Apache-2.0
# Compile each common public header alone, and all headers in both orders.
# Optional format wrappers require separate Arrow format features and are omitted.
file(GLOB_RECURSE acier_test_headers CONFIGURE_DEPENDS
  RELATIVE "${PROJECT_SOURCE_DIR}/include" "${PROJECT_SOURCE_DIR}/include/acier/*.h")
list(FILTER acier_test_headers EXCLUDE REGEX "acier/dataset/file_(csv|json|orc|parquet)\\.h$")
set(acier_header_sources)
set(acier_header_all "")
set(acier_header_reverse "")
foreach(header IN LISTS acier_test_headers)
  string(MAKE_C_IDENTIFIER "${header}" header_id)
  set(source "${CMAKE_CURRENT_BINARY_DIR}/headers/${header_id}.cc")
  file(GENERATE OUTPUT "${source}" CONTENT "#include <${header}>\n")
  list(APPEND acier_header_sources "${source}")
  string(APPEND acier_header_all "#include <${header}>\n")
  set(acier_header_reverse "#include <${header}>\n${acier_header_reverse}")
endforeach()
foreach(order all reverse)
  set(source "${CMAKE_CURRENT_BINARY_DIR}/headers/${order}.cc")
  file(GENERATE OUTPUT "${source}" CONTENT "${acier_header_${order}}")
  list(APPEND acier_header_sources "${source}")
endforeach()
add_library(acier_header_checks OBJECT ${acier_header_sources})
target_link_libraries(acier_header_checks PRIVATE Acier::acier)
