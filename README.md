# Acier

Acier is a C++ execution and Dataset library built on Apache Arrow. It is licensed
under Apache-2.0; see [LICENSE.txt](LICENSE.txt) and [NOTICE.txt](NOTICE.txt).

## API

Include `<acier/api.h>` for execution plans, `<acier/dataset/api.h>` for datasets,
and `<acier/compute/api.h>` for the TDigest functions.

`acier::ExecPlan`, `acier::ExecNode`, `acier::Declaration`, and the ordinary Acero
options retain their Arrow type identities. Dataset data types remain available
under `acier::dataset`. Arrays, schemas, buffers, expressions, status, results,
and general Compute operations remain in their Arrow namespaces.

Call `acier::Initialize()` before constructing plans with Acier factories or
functions. Initialization is thread-safe and repeatable. Acier registers the
following factories alongside Arrow's original factories:

| Factory | Options |
| --- | --- |
| `acier_source` | `acier::SourceNodeOptions` |
| `acier_table_source` | `acier::TableSourceNodeOptions` |
| `acier_record_batch_source` | `acier::RecordBatchSourceNodeOptions` |
| `acier_exec_batch_source` | `acier::ExecBatchSourceNodeOptions` |
| `acier_array_vector_source` | `acier::ArrayVectorSourceNodeOptions` |
| `acier_record_batch_reader_source` | `acier::RecordBatchReaderSourceNodeOptions` |
| `acier_filter` | `acier::FilterNodeOptions` |
| `acier_pivot_longer` | `acier::PivotLongerNodeOptions` |
| `acier_asofjoin` | `acier::AsofJoinNodeOptions` |
| `acier_sorted_merge` | `acier::SortedMergeNodeOptions` / `acier::OrderByNodeOptions` |
| `acier_pipe_source` | `acier::PipeSourceNodeOptions` |
| `acier_pipe_sink`, `acier_pipe_tee` | `acier::PipeSinkNodeOptions` |
| `acier_aggregate` | `acier::AggregateNodeOptions` |
| `acier_write`, `acier_tee` | `acier::dataset::WriteNodeOptions` |

Use the option type listed for each factory. For example, `acier_filter` requires
`acier::FilterNodeOptions`, while Arrow's `filter` requires
`arrow::acero::FilterNodeOptions`. Likewise, `acier_write` and `acier_tee` require
`acier::dataset::WriteNodeOptions`, while Arrow's `write` and `tee` use
`arrow::dataset::WriteNodeOptions`. Option contracts, supported types, units and
defaults are documented
in the public headers. The AsofJoin output adds nullable right-side fields.

`acier::compute` provides `TDigest`, `TDigestMap`, `TDigestReduce`,
`TDigestQuantile`, `TDigestQuantileElementWise`, and `ApproximateMedian`, registered
as `acier_tdigest`, `acier_tdigest_map`, `acier_tdigest_reduce`,
`acier_tdigest_quantile`, `acier_tdigest_quantile_element_wise`, and
`acier_approximate_median`. `acier::compute::TDigestOptions`,
`acier::compute::TDigestMapOptions`, `acier::compute::TDigestReduceOptions` and
`acier::compute::TDigestQuantileOptions` have distinct registry identities. Existing
Arrow functions are preserved.

Additional APIs include `acier::Pipe`, backpressure controls and queues,
`acier::PushGenerator` in `<acier/util/async_generator.h>`, and conversion helpers
under `acier::stl` in `<acier/stl.h>`.

`RegisterNodeFactories(registry)` registers execution factories in a supplied
registry. `acier::compute::Initialize(registry)` registers Compute extensions in
a supplied FunctionRegistry. These custom-registry calls return errors for
existing names, may leave partial registration on error, and require caller
synchronization. Keep the registries alive while using them.

[tests/smoke.cc](tests/smoke.cc) is a complete Dataset scan/filter/aggregation
example. Additional permanent regression tests are in [tests/](tests/).

## Build and install

Requires CMake 3.20+, a C++17 compiler (or the newer standard required by Arrow),
and matching installed Arrow, Acero and Dataset packages. Arrow must report
version 21 or newer. Acero and Dataset also require Arrow Compute. Acier does
not require Substrait. Optional CSV, JSON, ORC and Parquet wrappers require the
corresponding Arrow build features.

```sh
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/arrow/install \
  -DCMAKE_INSTALL_PREFIX=/path/to/acier/install -DACIER_BUILD_TESTS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
cmake --install build
```

`BUILD_SHARED_LIBS=ON` builds shared Acier; the default is static.
`ACIER_ARROW_LINKAGE` accepts `AUTO` (default), `SHARED` or `STATIC`. `AUTO`
prefers shared Arrow, falling back to static when all required shared targets
are unavailable. All components must provide the selected linkage. The installed
Acier package retains this selection.

Rebuild Acier when changing Arrow. Installed Acier checks the Arrow version used
at compilation, but a version string alone does not establish compatibility
between different builds. Use matching headers and libraries from
one Arrow build.

## Use from CMake

```cmake
find_package(Acier CONFIG REQUIRED)
target_link_libraries(my_target PRIVATE Acier::acier)
```

Set `CMAKE_PREFIX_PATH` to include both installations. Alternatively,
`add_subdirectory(/path/to/acier acier-build)` provides the same target.

## Developer documentation

See [Developing Acier](docs/README.md) for the source layout, regression tests,
CI checks and contribution guidelines. The [component comparisons](docs/README.md#component-comparisons)
itemize differences from upstream Arrow and Acero.
