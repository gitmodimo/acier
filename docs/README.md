# Developing Acier

Acier extends Apache Arrow with execution nodes, pipelines, Dataset writing,
TDigest functions and utility APIs. The public interfaces are under `include/acier`;
implementations are under `src`, and regression tests are under `tests`.

## Building and testing

See the [build instructions](../README.md#build-and-install) for a local build.
Enable `ACIER_BUILD_TESTS` to build the regression suites and compile the common public
headers independently and together in both include orders. Optional CSV, JSON, ORC
and Parquet wrappers are excluded from these header checks. Run all configured
tests with:

```sh
ctest --test-dir build --output-on-failure
```

The suites cover execution nodes, Dataset operations, joins, pipelines, Compute
functions, STL conversion and asynchronous generators. The smoke test exercises
a complete execution plan.

## Continuous integration

`ci/arrow.env` pins the upstream Arrow revision used by CI. To reproduce the
release checks, build that revision and run the test driver:

```sh
source ci/arrow.env
git clone https://github.com/apache/arrow.git build-ci/arrow-source
git -C build-ci/arrow-source checkout --detach "$ARROW_REVISION"
bash ci/build-arrow.sh build-ci/arrow-source build-ci/arrow-build build-ci/arrow-install release
bash ci/test.sh build-ci/arrow-install build-ci/acier release
```

The release driver builds and tests both shared and static configurations,
installs Acier, and builds and runs a separate consumer of the installed CMake
package. The `sanitizers` mode runs a shared Debug configuration with AddressSanitizer
and UndefinedBehaviorSanitizer; use it for both scripts with a compatible compiler.
`CMAKE_BUILD_PARALLEL_LEVEL` controls build and test concurrency.

## Component comparisons

The component pages itemize Acier additions and differences against upstream
Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.
This is a fixed source comparison, not a claim about every later Arrow release.
Dataset, Compute and utility pages compare the corresponding Arrow components;
execution-node pages compare Acero. The CI dependency remains the separate pin
in `ci/arrow.env`. Each page links the implementation and relevant test coverage.

| Component | Comparison |
| --- | --- |
| [Ordered and segmented aggregation](components/aggregate.md) | Threaded ordered/segmented execution |
| [AsofJoin](components/asofjoin.md) | Intervals, schema, batching and scheduling |
| [Backpressure composition](components/backpressure.md) | Pause/stop composition |
| [Continuous integration](components/ci.md) | Pinned upstream build and test coverage |
| [Concurrent queues](components/concurrent_queue.md) | Public helpers and callback handling |
| [Dataset Tee](components/dataset_tee.md) | Schema, metadata and combined pressure |
| [Dataset writing](components/dataset_write.md) | Queue threshold and writer cancellation |
| [Non-null filter](components/filter.md) | Non-null selection and schema refinement |
| [Package and dependencies](components/package.md) | Installed dependency and export boundaries |
| [Pipes](components/pipes.md) | Same-plan connections and fan-out |
| [PivotLonger scalar features](components/pivot_longer.md) | Shared scalar features; option and error differences |
| [Platform and tracing boundaries](components/platform_support.md) | Platform and tracing boundaries |
| [PushGenerator backpressure](components/push_generator.md) | Watermark-aware generation |
| [RecordBatchReader source ordering](components/reader_source.md) | Selectable ordering |
| [Registration and coexistence](components/registration.md) | Separate names and option identities |
| [Scheduler integration](components/scheduler.md) | Reuse of Arrow scheduling |
| [Sequencing and scheduled processing](components/sequencing.md) | Scheduled processing with backpressure |
| [SortedMerge](components/sorted_merge.md) | Validation, batching and scheduling |
| [Source pause and stop lifecycle](components/source_lifecycle.md) | Atomic stop/pause handling |
| [STL and optional conversion](components/stl_conversion.md) | Optional extraction and nested child nullability |
| [TDigest Compute extension](components/tdigest.md) | Weighted digest functions and schemas |

## Contributions

Keep API documentation focused on observable contracts, including supported types,
units, ordering, nullability and lifetime requirements. Add focused regression
tests for changes to these contracts. Format C++ sources with the repository's
`.clang-format` rules. Preserve Apache Arrow attribution and license notices in
derived code, and identify modifications where applicable.
