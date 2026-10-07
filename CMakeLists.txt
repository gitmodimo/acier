# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)
project(Acier VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_package(Arrow CONFIG REQUIRED)
find_package(ArrowAcero CONFIG REQUIRED)
find_package(ArrowDataset CONFIG REQUIRED)
if(ARROW_VERSION VERSION_LESS 21.0.0)
  message(FATAL_ERROR "Acier requires Arrow 21.0.0 or newer")
endif()

set(ACIER_ARROW_LINKAGE AUTO CACHE STRING "Arrow linkage: AUTO, SHARED, or STATIC")
set_property(CACHE ACIER_ARROW_LINKAGE PROPERTY STRINGS AUTO SHARED STATIC)
if(NOT ACIER_ARROW_LINKAGE MATCHES "^(AUTO|SHARED|STATIC)$")
  message(FATAL_ERROR "ACIER_ARROW_LINKAGE must be AUTO, SHARED, or STATIC")
endif()

set(acier_arrow_linkage "")
foreach(linkage SHARED STATIC)
  string(TOLOWER "${linkage}" suffix)
  if((ACIER_ARROW_LINKAGE STREQUAL "AUTO" OR ACIER_ARROW_LINKAGE STREQUAL "${linkage}")
     AND TARGET Arrow::arrow_${suffix}
     AND TARGET ArrowAcero::arrow_acero_${suffix}
     AND TARGET ArrowDataset::arrow_dataset_${suffix})
    set(acier_arrow_linkage "${suffix}")
    break()
  endif()
endforeach()
if(NOT acier_arrow_linkage)
  message(FATAL_ERROR "Arrow, Acero, and Dataset must all provide the requested linkage")
endif()

option(BUILD_SHARED_LIBS "Build Acier as a shared library" OFF)
add_library(acier
  src/initialize.cc
  src/core/source_node.cc
  src/core/filter_node.cc
  src/core/pivot_node.cc
  src/dataset/write_node.cc
  src/dataset/writer.cc
  src/join/asof_join_node.cc
  src/join/sorted_merge_node.cc
  src/join/register.cc
  src/pipeline/backpressure.cc
  src/pipeline/accumulation_queue.cc
  src/pipeline/pipe_node.cc
  src/pipeline/aggregate_internal.cc
  src/pipeline/scalar_aggregate_node.cc
  src/pipeline/groupby_aggregate_node.cc
  src/pipeline/register.cc
  src/compute/api_aggregate.cc
  src/compute/aggregate_tdigest.cc
  src/compute/tdigest_internal.cc)
set_target_properties(acier PROPERTIES POSITION_INDEPENDENT_CODE ON
  VERSION ${PROJECT_VERSION} SOVERSION 0)
if(BUILD_SHARED_LIBS)
  target_compile_definitions(acier PRIVATE ACIER_EXPORTING PUBLIC ACIER_SHARED)
endif()
target_include_directories(acier PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
add_library(Acier::acier ALIAS acier)
target_compile_features(acier PUBLIC cxx_std_17)
target_include_directories(acier PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(acier PUBLIC
  Arrow::arrow_${acier_arrow_linkage}
  ArrowAcero::arrow_acero_${acier_arrow_linkage}
  ArrowDataset::arrow_dataset_${acier_arrow_linkage})

option(ACIER_BUILD_TESTS "Build Acier integration tests" OFF)
if(ACIER_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

set(acier_cmake_dir "${CMAKE_INSTALL_LIBDIR}/cmake/Acier")
configure_package_config_file(cmake/AcierConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/AcierConfig.cmake"
  INSTALL_DESTINATION "${acier_cmake_dir}")
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/AcierConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}" COMPATIBILITY SameMajorVersion)
install(TARGETS acier EXPORT AcierTargets
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(EXPORT AcierTargets NAMESPACE Acier:: DESTINATION "${acier_cmake_dir}")
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/AcierConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/AcierConfigVersion.cmake"
  DESTINATION "${acier_cmake_dir}")
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(FILES LICENSE.txt NOTICE.txt DESTINATION "${CMAKE_INSTALL_DATADIR}/acier")
