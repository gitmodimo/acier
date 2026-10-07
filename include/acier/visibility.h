// SPDX-License-Identifier: Apache-2.0
#pragma once

#if defined(_WIN32) && defined(ACIER_SHARED)
#  if defined(ACIER_EXPORTING)
#    define ACIER_EXPORT __declspec(dllexport)
#  else
#    define ACIER_EXPORT __declspec(dllimport)
#  endif
#else
#  define ACIER_EXPORT
#endif
