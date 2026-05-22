# ub_comm_queue reliability EER demo

`reliability_eer_demo.cpp` covers the queue reliability cases without poison message types.

The demo creates two logical nodes in one communication queue group, one business ring, and prints every major step.

Covered cases:

- `UBCQ_2N_SEND_EER_001`: a producer-side thread reserves the consumer ring tail and exits before `ready_seq`; other producer threads send normal messages concurrently; the consumer skips the half-written head slot and continues receiving; a new sender thread verifies post-recovery sending.
- `UBCQ_2N_RCV_EER_001`: consumer heartbeat interval 100 ms, producer polling 100 ms, timeout 1500 ms; producer sends continuously, detects consumer heartbeat stop, exits its send loop, then sends normally after consumer heartbeat recovery.
- `UBCQ_2N_RCV_EER_002`: producer queries default heartbeat config without setting it; after consumer heartbeat fault, producer detects timeout with the default 1000 ms window.
- `UBCQ_IF_RCV_EER_001`: query local heartbeat config with `request == NULL && effective != NULL`.
- `UBCQ_IF_RCV_EER_002`: set local heartbeat config with `request != NULL && effective == NULL`.
- `UBCQ_IF_RCV_EER_003`: set and query local heartbeat config with `request != NULL && effective != NULL`.

Example build command, adjusted to your local build output and dependency paths:

```bash
g++ -std=c++17 -O2 -pthread \
  -I../../include -I../../src/common -I../../src/ub_comm_queue \
  reliability_eer_demo.cpp \
  ../../src/ub_comm_queue/MPSCRingBuffer.cpp \
  ../../src/ub_comm_queue/UBShmTransport.cpp \
  ../../src/ub_comm_queue/ub_dist_comm_queue.cpp \
  ../../src/ub_comm_queue/ub_atomic_log_print.cpp \
  -o reliability_eer_demo
```

Run:

```bash
./reliability_eer_demo
```
