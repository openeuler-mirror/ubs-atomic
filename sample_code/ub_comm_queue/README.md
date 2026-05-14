# ub_comm_queue samples

## Half-write recovery demo

`half_write_recovery_test.cpp` verifies the case where a producer reserves a remote slot by advancing `tail_`, then exits before committing `ready_seq`.

Build the library with the test-only injection hook enabled:

```bash
cmake -S . -B build-half-write \
  -DCMAKE_BUILD_TYPE=Release \
  -DUB_COMM_QUEUE_ENABLE_HALF_WRITE_INJECT=ON
cmake --build build-half-write --target ub_comm_queue -j
```

Build the demo against the injected library and normal ubsmem dependencies used by the other queue samples.

Run the one-shot driver:

```bash
./half_write_recovery_test --role D -l 2
```

The driver starts:

1. `B`: receiver node.
2. `I`: injector producer. It sets `UB_COMM_QUEUE_INJECT_HALF_WRITE_EXIT=1`, reserves a remote slot, logs `half_write_inject_exit`, and exits with code `86` before writing `ready_seq`.
3. `A`: normal producer. It sends the next valid message.

Expected logs:

- Injector prints `[half_write_inject] ... exit_code=86`.
- Receiver status first shows the queue has occupied entries.
- Queue internals log `Skipped stale reserved ring entry`.
- Receiver prints `GOOD received`.
- Driver prints `PASS: half-write recovery verified`.

If `TYPE_POISON` is dispatched, the injection hook was not compiled in or did not fire.
