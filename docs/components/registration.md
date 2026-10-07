# Registration and coexistence

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/exec_plan.h](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/exec_plan.h).

## Additions to upstream registries

- `acier::Initialize()` initializes Arrow Compute/Dataset and Acier factories with
  a retained status and thread-safe one-time execution. No static constructor
  mutates the registries.
- Acier factory/function names use `acier_`, leaving Arrow registrations intact.
  `acier::FilterNodeOptions` and `arrow::acero::FilterNodeOptions`, for example,
  are separate types. In contrast, `acier::AggregateNodeOptions` aliases
  `arrow::acero::AggregateNodeOptions`.
- `RegisterNodeFactories(registry)` and `acier::compute::Initialize(registry)`
  register into caller-supplied registries. Duplicate names return errors and
  registration may be partial on failure; these calls do not provide rollback.
- Callers retain registry ownership and must serialize custom registration.
- Tests cover repeated initialization, custom registries, duplicate/null errors,
  option identities and original-factory coexistence.

## Implementation and coverage

- Acier: [src/initialize.cc](../../src/initialize.cc), [src/join/register.cc](../../src/join/register.cc), [src/pipeline/register.cc](../../src/pipeline/register.cc), [src/compute/aggregate_tdigest.cc](../../src/compute/aggregate_tdigest.cc).
- Regression coverage: [tests/core_test.cc](../../tests/core_test.cc), [tests/compute_test.cc](../../tests/compute_test.cc).
