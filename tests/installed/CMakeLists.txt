# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)
project(AcierInstalledConsumer LANGUAGES CXX)
find_package(Acier CONFIG REQUIRED)
add_executable(acier_installed_smoke ../smoke.cc)
target_link_libraries(acier_installed_smoke PRIVATE Acier::acier)
enable_testing()
add_test(NAME acier.installed COMMAND acier_installed_smoke)
