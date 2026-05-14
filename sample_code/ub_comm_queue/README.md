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

### Process-exit injection

Run the process-exit driver:

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

### Thread-hang poison injection

For a two-node environment where killing an entire producer process is inconvenient, use the threaded poison mode:

```bash
./half_write_recovery_test --role H -l 2 -n 8
```

The threaded driver starts:

1. `B`: receiver node. It expects `-n` good messages and periodically prints `ub_comm_queue_get_status`.
2. `T`: producer node A. Inside one process it starts:
   - one poison sender thread with `TYPE_POISON`;
   - `n` normal sender threads with `TYPE_GOOD`.

The poison sender sets:

```bash
UB_COMM_QUEUE_INJECT_HALF_WRITE_MSG_TYPE=131
UB_COMM_QUEUE_INJECT_HALF_WRITE_ACTION=hang
```

When that thread reserves a remote slot, the queue hook hangs it before any data copy or `ready_seq` commit. Other producer threads in the same process keep sending normal messages, so receiver B should observe:

- the poison message is never dispatched;
- status shows queued entries behind the half-written head slot;
- queue internals log `Skipped stale reserved ring entry` after `HALF_WRITE_TIMEOUT_US`;
- all `TYPE_GOOD` messages arrive after the skip;
- driver prints `PASS: threaded half-write recovery verified`.

This mode validates the important behavior without relying on a real producer process crash: the head slot is half-written, later slots are complete and accumulated, and the receiver recovers using its local timeout observation.

Manual two-shell run:

```bash
# shell 1
./half_write_recovery_test --role B -l 2 -n 8

# shell 2
./half_write_recovery_test --role T -l 2 -n 8
```
