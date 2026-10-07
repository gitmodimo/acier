// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cassert>
#define ACIER_DCHECK(x) assert(x)
#define ACIER_DCHECK_EQ(a, b) assert((a) == (b))
#define ACIER_DCHECK_NE(a, b) assert((a) != (b))
#define ACIER_DCHECK_LT(a, b) assert((a) < (b))
#define ACIER_DCHECK_LE(a, b) assert((a) <= (b))
#define ACIER_DCHECK_GT(a, b) assert((a) > (b))
#define ACIER_DCHECK_GE(a, b) assert((a) >= (b))
