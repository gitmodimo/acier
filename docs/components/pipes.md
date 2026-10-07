# Pipes

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.

## Additions to upstream Acero

- The comparison baseline has no corresponding public Pipe API or pipe factories.
  Acier supplies `Pipe`, `PipeSource`, `acier_pipe_source`, `acier_pipe_sink` and
  `acier_pipe_tee` for named, same-plan connections and fan-out.
- Connections check schema and ordering. One endpoint can receive synchronously;
  other deliveries are scheduled through the plan.
- Pause-on-any and stop-on-all are the defaults. `acier::PipeSinkNodeOptions` allows
  the alternative policies. Stopping a consumer releases its pause; repeated stop
  requests do not issue duplicate stop callbacks.
- A stopped Tee branch does not stop still-active siblings until the selected
  stop policy is satisfied.

## Boundaries and coverage

- Discovery connects Acier sources during initialization. These are same-plan
  connections, not cross-plan transport or a bounded message broker.
- Pipes and endpoints must outlive scheduled plan work.
- Tests cover fan-out row counts, one branch stopping early, pause/resume,
  both stop policies and serial/threaded execution. See also
  [backpressure composition](backpressure.md).

## Implementation and coverage

- Acier: [include/acier/pipe_node.h](../../include/acier/pipe_node.h), [include/acier/pipeline_options.h](../../include/acier/pipeline_options.h), [src/pipeline/pipe_node.cc](../../src/pipeline/pipe_node.cc).
- Regression coverage: [tests/pipeline_test.cc](../../tests/pipeline_test.cc).
