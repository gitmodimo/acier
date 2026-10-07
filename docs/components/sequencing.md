# Sequencing and scheduled processing

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/accumulation_queue.h](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/accumulation_queue.h).

## Differences from upstream

- Acier's owned `SerialSequencingQueue::Processor::MakeBackpressureWrapper`
  adds scheduled processing, queue watermarks, stop/error handling and executor
  selection. The upstream public processor API does not provide this wrapper.
- `requires_io=false` selects CPU work; `true` selects I/O work. Queue draining
  is serialized. Pending scheduled batches are cleared on stop or processing error,
  and queue pressure is released; an active `Process` call may still finish.
- Watermark callbacks run outside the queue mutex and allow synchronous reentry.

## Shared behavior and boundaries

- Ordinary index sequencing delegates to Arrow's installed serial sequencer.
  `AccumulationQueue` and `SequencingQueue` remain Arrow aliases.
- Indices must occur exactly once, consecutively from zero. A missing index holds
  later batches; this helper does not add completion-time gap detection.
- Watermarks count already-sequenced scheduled batches, excluding the missing-index
  backlog. Processor lifetime must cover all scheduled work.
- Tests cover contiguous results, CPU/I/O executor identity and processing errors
  in serial/threaded plans.

## Implementation and coverage

- Acier: [include/acier/accumulation_queue.h](../../include/acier/accumulation_queue.h), [src/pipeline/accumulation_queue.cc](../../src/pipeline/accumulation_queue.cc).
- Regression coverage: [tests/pipeline_test.cc](../../tests/pipeline_test.cc).
