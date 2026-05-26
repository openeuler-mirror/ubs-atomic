/*
 * Reliability EER demo for ub_comm_queue.
 *
 * This sample intentionally uses public headers only. It does not include
 * MPSCRingBuffer.h/UBShmTransport.h and does not access private members.
 *
 * Note about half-write validation:
 * The public API cannot deterministically pause a producer after reserving a
 * ring slot and before publishing ready state. To validate a true half-write
 * slot, run role A under continuous send pressure and kill/stop that process
 * from outside during the send loop. The consumer side will report whether
 * normal messages continue after the recovery timeout.
 */

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

namespace {

constexpr uint8_t NODE_A = 0;
constexpr uint8_t NODE_B = 1;
constexpr uint8_t MAX_NODES = 2;

constexpr unsigned long SHM_MAP_MB = 1024;
constexpr size_t INIT_REGION_SIZE = 1024 * 1024;
constexpr size_t RING_REGION_SIZE = 64 * 1024 * 1024;
constexpr uint32_t RING_CAPACITY = 1024;
constexpr uint32_t MAX_MSG_SIZE = 256;
constexpr uint8_t PRIORITY_EER = 1;

constexpr uint8_t TYPE_EER_DATA = 150;
constexpr uint8_t TYPE_EER_ACK = 151;

constexpr uint32_t CASE_SEND_EER = 1;
constexpr uint32_t CASE_RCV_EER = 2;
constexpr uint32_t CASE_RECOVERY_PROBE = 3;

#pragma pack(push, 1)
struct EerBody {
    uint32_t case_id;
    uint32_t seq;
    uint64_t send_ns;
    char padding[32];
};
#pragma pack(pop)

static_assert(sizeof(EerBody) <= MAX_MSG_SIZE - sizeof(message_header_t), "EerBody body too large");

struct Context {
    ub_shm_comm_t *handle = nullptr;
    uint8_t self_node = NODE_A;
    uint8_t peer_node = NODE_B;
    std::atomic<uint32_t> recv_count{0};
    std::atomic<uint32_t> ack_count{0};
    std::atomic<uint32_t> recovery_ack_count{0};
};

char g_role = 'A';
char g_sender_shm_name[64] = "shm_node0_export";
char g_receiver_shm_name[64] = "shm_node1_export";
int g_wait_peer_timeout_s = 15;
int g_log_level = LOG_LEVEL_ERROR;
uint32_t g_messages = 512;
uint32_t g_threads = 4;
uint32_t g_send_interval_us = 1000;
uint32_t g_fault_after_ms = 0;
uint32_t g_resume_after_ms = 0;
pid_t g_peer_pid = -1;
std::string g_case = "service";
Context g_ctx;

int my_stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    (void)func;
    const char *level_str = "UNKNOWN";
    switch (level) {
        case LOG_LEVEL_DEBUG:
            level_str = "DEBUG";
            break;
        case LOG_LEVEL_INFO:
            level_str = "INFO";
            break;
        case LOG_LEVEL_WARN:
            level_str = "WARN";
            break;
        case LOG_LEVEL_ERROR:
            level_str = "ERROR";
            break;
        case LOG_LEVEL_CRITICAL:
            level_str = "CRITICAL";
            break;
        default:
            break;
    }
    fprintf(stdout, "[%s:%u] [%s] %s\n", file, line, level_str, message);
    return 0;
}

int64_t now_ns()
{
    return (int64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void cpu_relax()
{
#if defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    asm volatile("" ::: "memory");
#endif
}

void wait_enter(const char *message)
{
    printf("\n%s\n", message);
    printf("确认操作完成后按 Enter 继续...");
    fflush(stdout);

    int ch = getchar();
    while (ch != '\n' && ch != EOF) {
        ch = getchar();
    }
}

bool is_case(const char *name)
{
    return g_case == name || g_case == "all";
}

int init_ub_shm()
{
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_init_attributes failed, ret=%d\n", ret);
        return -1;
    }
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_initialize failed, ret=%d\n", ret);
        return -1;
    }
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_lookup_regions failed, ret=%d\n", ret);
        return -1;
    }
    return 0;
}

int map_ub_shm(const char *shm_name, void *&addr)
{
    const unsigned long length = SHM_MAP_MB * 1024UL * 1024UL;
    int ret = ubsmem_shmem_map(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_name, 0, &addr);
    if (ret != 0) {
        fprintf(stderr, "Failed to map shared memory '%s', ret=%d\n", shm_name, ret);
        return -1;
    }
    return 0;
}

bool wait_peer_ready(ub_shm_comm_t *handle, uint8_t peer_node, int timeout_s)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        if (ub_comm_queue_check_ready(handle, peer_node)) {
            return true;
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

int init_handle(ub_shm_comm_t &handle)
{
    if (init_ub_shm() != 0) {
        return -1;
    }

    void *sender_shm_base = nullptr;
    if (map_ub_shm(g_sender_shm_name, sender_shm_base) != 0) {
        return -1;
    }

    void *receiver_shm_base = nullptr;
    if (map_ub_shm(g_receiver_shm_name, receiver_shm_base) != 0) {
        return -1;
    }

    void *init_area = sender_shm_base;
    void *ring_area_a = static_cast<char *>(sender_shm_base) + INIT_REGION_SIZE;
    void *ring_area_b = receiver_shm_base;

    ub_ring_desc_t ring_descs[1];
    ring_descs[0] = {RING_CAPACITY, MAX_MSG_SIZE, PRIORITY_EER};

    ub_comm_conf_t conf{};
    conf.cpu_id = -1;
    conf.max_nodes = MAX_NODES;
    conf.current_node_id = (g_role == 'A') ? NODE_A : NODE_B;
    conf.num_rings = 1;
    conf.ring_descs = ring_descs;

    ub_shm_area_t init_shm_area{};
    init_shm_area.ptr = init_area;
    init_shm_area.size = INIT_REGION_SIZE;

    ub_ring_region_info_t ring_info[2];
    ring_info[0].node_id = NODE_A;
    ring_info[0].region.ptr = ring_area_a;
    ring_info[0].region.size = RING_REGION_SIZE;
    ring_info[1].node_id = NODE_B;
    ring_info[1].region.ptr = ring_area_b;
    ring_info[1].region.size = RING_REGION_SIZE;

    ub_ring_region_map_t ring_map{ring_info, 2};
    if (ub_comm_queue_init(&handle, &init_shm_area, &ring_map, &conf) != 0) {
        fprintf(stderr, "ub_comm_queue_init failed\n");
        return -1;
    }

    g_ctx.handle = &handle;
    g_ctx.self_node = (g_role == 'A') ? NODE_A : NODE_B;
    g_ctx.peer_node = (g_role == 'A') ? NODE_B : NODE_A;
    return 0;
}

bool send_message(uint8_t dest_node, uint8_t msg_type, uint32_t case_id, uint32_t seq)
{
    EerBody body{};
    body.case_id = case_id;
    body.seq = seq;
    body.send_ns = static_cast<uint64_t>(now_ns());

    message_t msg{};
    msg.header.dest_node_id = dest_node;
    msg.header.src_node_id = g_ctx.self_node;
    msg.header.msg_type = msg_type;
    msg.header.priority = PRIORITY_EER;
    msg.header.body_length = sizeof(body);
    msg.body = reinterpret_cast<char *>(&body);

    int ret = ub_comm_queue_send(g_ctx.handle, &msg);
    if (ret >= 0) {
        return true;
    }
    fprintf(stderr, "[send] failed ret=%d case=%u seq=%u dest=%u\n", ret, case_id, seq, dest_node);
    return false;
}

bool send_with_retry(uint8_t dest_node, uint8_t msg_type, uint32_t case_id, uint32_t seq, int timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (send_message(dest_node, msg_type, case_id, seq)) {
            return true;
        }
        cpu_relax();
        std::this_thread::sleep_for(1ms);
    }
    return false;
}

bool wait_counter(const std::atomic<uint32_t> &counter, uint32_t expected, int timeout_ms, const char *label)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        uint32_t actual = counter.load(std::memory_order_acquire);
        if (actual >= expected) {
            printf("[%s] reached expected=%u actual=%u\n", label, expected, actual);
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    fprintf(stderr, "[%s] timeout expected=%u actual=%u\n", label, expected,
            counter.load(std::memory_order_acquire));
    return false;
}

void on_ack(const message_t *msg, void *ctx)
{
    auto *test_ctx = reinterpret_cast<Context *>(ctx);
    if (msg->header.body_length != sizeof(EerBody)) {
        return;
    }
    const auto *body = reinterpret_cast<const EerBody *>(msg->body);
    test_ctx->ack_count.fetch_add(1, std::memory_order_release);
    if (body->case_id == CASE_RECOVERY_PROBE) {
        test_ctx->recovery_ack_count.fetch_add(1, std::memory_order_release);
    }
}

void on_data(const message_t *msg, void *ctx)
{
    auto *test_ctx = reinterpret_cast<Context *>(ctx);
    if (msg->header.body_length != sizeof(EerBody)) {
        return;
    }
    const auto *body = reinterpret_cast<const EerBody *>(msg->body);
    uint32_t count = test_ctx->recv_count.fetch_add(1, std::memory_order_acq_rel) + 1;
    if ((count <= 10) || (count % 100 == 0)) {
        printf("[recv] type=%u src=%u case=%u seq=%u total=%u\n", msg->header.msg_type,
               msg->header.src_node_id, body->case_id, body->seq, count);
    }
    (void)send_with_retry(msg->header.src_node_id, TYPE_EER_ACK, body->case_id, body->seq, 3000);
}

bool register_callbacks()
{
    if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_EER_DATA, UB_FUNC_SYNC, on_data, &g_ctx) != 0) {
        return false;
    }
    if (ub_comm_queue_register_process_func(g_ctx.handle, TYPE_EER_ACK, UB_FUNC_SYNC, on_ack, &g_ctx) != 0) {
        return false;
    }
    return true;
}

bool run_interface_cases()
{
    printf("[UBCQ_IF_RCV_EER_001/002/003] public heartbeat config API\n");

    ub_comm_queue_heartbeat_config_t effective{};
    if (ub_comm_queue_config_heartbeat(g_ctx.handle, nullptr, &effective) != 0) {
        fprintf(stderr, "[UBCQ_IF_RCV_EER_001] query default failed\n");
        return false;
    }
    printf("[UBCQ_IF_RCV_EER_001] interval=%u check=%u timeout=%u\n",
           effective.heartbeat_interval_ms, effective.check_interval_ms, effective.timeout_ms);

    ub_comm_queue_heartbeat_config_t request1{100, 100, 1500};
    if (ub_comm_queue_config_heartbeat(g_ctx.handle, &request1, nullptr) != 0) {
        fprintf(stderr, "[UBCQ_IF_RCV_EER_002] set-only failed\n");
        return false;
    }
    printf("[UBCQ_IF_RCV_EER_002] set-only ok\n");

    ub_comm_queue_heartbeat_config_t request2{100, 100, 1500};
    effective = {};
    if (ub_comm_queue_config_heartbeat(g_ctx.handle, &request2, &effective) != 0) {
        fprintf(stderr, "[UBCQ_IF_RCV_EER_003] set-query failed\n");
        return false;
    }
    printf("[UBCQ_IF_RCV_EER_003] interval=%u check=%u timeout=%u\n",
           effective.heartbeat_interval_ms, effective.check_interval_ms, effective.timeout_ms);
    return effective.heartbeat_interval_ms == request2.heartbeat_interval_ms &&
           effective.check_interval_ms == request2.check_interval_ms &&
           effective.timeout_ms == request2.timeout_ms;
}

void maybe_drive_peer_fault()
{
    if (g_peer_pid <= 0 || g_fault_after_ms == 0) {
        return;
    }

    std::thread([pid = g_peer_pid]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(g_fault_after_ms));
        printf("[fault] send SIGSTOP to peer pid=%d\n", static_cast<int>(pid));
        if (kill(pid, SIGSTOP) != 0) {
            fprintf(stderr, "[fault] SIGSTOP failed errno=%d\n", errno);
            return;
        }
        if (g_resume_after_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(g_resume_after_ms));
            printf("[fault] send SIGCONT to peer pid=%d\n", static_cast<int>(pid));
            if (kill(pid, SIGCONT) != 0) {
                fprintf(stderr, "[fault] SIGCONT failed errno=%d\n", errno);
            }
        }
    }).detach();
}

void prompt_peer_pause(const char *case_id)
{
    if (g_peer_pid > 0 && g_fault_after_ms > 0) {
        maybe_drive_peer_fault();
        printf("[%s] will auto SIGSTOP peer pid=%d after %u ms\n", case_id, static_cast<int>(g_peer_pid),
               g_fault_after_ms);
        return;
    }

    char message[512];
    snprintf(message, sizeof(message),
             "[%s] 手动故障注入点：请现在暂停接收端 B。\n"
             "  在 B 节点执行：kill -STOP <B端pid>\n"
             "  B 端启动时会打印 pid；如果是跨节点，请在 B 节点终端操作。\n"
             "  A 端按 Enter 后会开始轮询发送，并等待心跳超时被感知。",
             case_id);
    wait_enter(message);
}

void prompt_peer_resume(const char *case_id)
{
    if (g_peer_pid > 0 && g_resume_after_ms > 0) {
        printf("[%s] auto SIGCONT was scheduled for peer pid=%d after %u ms\n", case_id, static_cast<int>(g_peer_pid),
               g_resume_after_ms);
        return;
    }

    char message[512];
    snprintf(message, sizeof(message),
             "[%s] 已感知 B 端不可用。请现在恢复接收端 B。\n"
             "  在 B 节点执行：kill -CONT <B端pid>\n"
             "  如果 B 是被 kill 掉的，请重新启动 B：./reliability_eer_demo --role B --case service ...\n"
             "  A 端按 Enter 后会等待 B ready 并发送恢复探测消息。",
             case_id);
    wait_enter(message);
}

bool run_send_eer_case()
{
    printf("[UBCQ_2N_SEND_EER_001] start public-API send pressure\n");
    printf("[UBCQ_2N_SEND_EER_001] A pid=%d\n", static_cast<int>(getpid()));
    printf("[UBCQ_2N_SEND_EER_001] public API cannot force a half-written slot at an exact instruction.\n");
    printf("[UBCQ_2N_SEND_EER_001] For destructive validation, kill/stop A during the send loop, then run "
           "`--case probe` after B recovers.\n");
    wait_enter("[UBCQ_2N_SEND_EER_001] 即将启动生产端并发发送。请确认 B 端正在运行，然后按 Enter 开始。");

    const uint32_t expected = g_threads * g_messages;
    const uint32_t ack_before = g_ctx.ack_count.load(std::memory_order_acquire);
    std::vector<std::thread> workers;
    workers.reserve(g_threads);

    for (uint32_t t = 0; t < g_threads; ++t) {
        workers.emplace_back([t]() {
            for (uint32_t i = 0; i < g_messages; ++i) {
                const uint32_t seq = t * g_messages + i;
                (void)send_with_retry(g_ctx.peer_node, TYPE_EER_DATA, CASE_SEND_EER, seq, 3000);
                if (g_send_interval_us > 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(g_send_interval_us));
                }
            }
            printf("[UBCQ_2N_SEND_EER_001] sender thread %u exited after normal sends\n", t);
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }

    if (!wait_counter(g_ctx.ack_count, ack_before + expected, 15000, "UBCQ_2N_SEND_EER_001 ack")) {
        return false;
    }

    const uint32_t recovery_before = g_ctx.recovery_ack_count.load(std::memory_order_acquire);
    if (!send_with_retry(g_ctx.peer_node, TYPE_EER_DATA, CASE_RECOVERY_PROBE, 0, 3000)) {
        return false;
    }
    return wait_counter(g_ctx.recovery_ack_count, recovery_before + 1, 3000, "post-recovery ack");
}

bool run_recovery_probe_case()
{
    printf("[UBCQ_RECOVERY_PROBE] send one recovery probe message\n");
    if (!wait_peer_ready(g_ctx.handle, g_ctx.peer_node, g_wait_peer_timeout_s)) {
        fprintf(stderr, "[UBCQ_RECOVERY_PROBE] peer is not ready within %d seconds\n", g_wait_peer_timeout_s);
        return false;
    }

    const uint32_t recovery_before = g_ctx.recovery_ack_count.load(std::memory_order_acquire);
    if (!send_with_retry(g_ctx.peer_node, TYPE_EER_DATA, CASE_RECOVERY_PROBE, 0, 3000)) {
        return false;
    }
    return wait_counter(g_ctx.recovery_ack_count, recovery_before + 1, 5000, "recovery probe ack");
}

bool run_receiver_eer_case(bool query_default)
{
    const char *case_id = query_default ? "UBCQ_2N_RCV_EER_002" : "UBCQ_2N_RCV_EER_001";
    printf("[%s] start heartbeat detection case\n", case_id);

    ub_comm_queue_heartbeat_config_t effective{};
    if (query_default) {
        if (ub_comm_queue_config_heartbeat(g_ctx.handle, nullptr, &effective) != 0) {
            return false;
        }
    } else {
        ub_comm_queue_heartbeat_config_t request{100, 100, 1500};
        if (ub_comm_queue_config_heartbeat(g_ctx.handle, &request, &effective) != 0) {
            return false;
        }
    }
    printf("[%s] local heartbeat interval=%u check=%u timeout=%u\n", case_id,
           effective.heartbeat_interval_ms, effective.check_interval_ms, effective.timeout_ms);

    prompt_peer_pause(case_id);

    const auto detect_deadline = std::chrono::steady_clock::now() + 30s;
    uint32_t seq = 0;
    bool detected = false;
    while (std::chrono::steady_clock::now() < detect_deadline) {
        EerBody body{};
        body.case_id = CASE_RCV_EER;
        body.seq = seq++;
        body.send_ns = static_cast<uint64_t>(now_ns());

        message_t msg{};
        msg.header.dest_node_id = g_ctx.peer_node;
        msg.header.src_node_id = g_ctx.self_node;
        msg.header.msg_type = TYPE_EER_DATA;
        msg.header.priority = PRIORITY_EER;
        msg.header.body_length = sizeof(body);
        msg.body = reinterpret_cast<char *>(&body);

        int ret = ub_comm_queue_send(g_ctx.handle, &msg);
        if (ret < 0) {
            if (!ub_comm_queue_check_ready(g_ctx.handle, g_ctx.peer_node)) {
                printf("[%s] producer detected peer unavailable ret=%d after seq=%u\n", case_id, ret, seq);
                detected = true;
                break;
            }
            std::this_thread::sleep_for(20ms);
            continue;
        }
        std::this_thread::sleep_for(20ms);
    }

    if (!detected) {
        fprintf(stderr, "[%s] did not detect peer timeout. Stop role B or pass --peer-pid with --fault-after-ms.\n",
                case_id);
        return false;
    }

    prompt_peer_resume(case_id);
    printf("[%s] waiting peer recovery\n", case_id);
    if (!wait_peer_ready(g_ctx.handle, g_ctx.peer_node, g_wait_peer_timeout_s)) {
        fprintf(stderr, "[%s] peer did not recover within %d seconds\n", case_id, g_wait_peer_timeout_s);
        return false;
    }

    const uint32_t recovery_before = g_ctx.recovery_ack_count.load(std::memory_order_acquire);
    if (!send_with_retry(g_ctx.peer_node, TYPE_EER_DATA, CASE_RECOVERY_PROBE, seq, 3000)) {
        return false;
    }
    return wait_counter(g_ctx.recovery_ack_count, recovery_before + 1, 5000, "receiver recovery ack");
}

int run_role_a()
{
    if (g_case != "if" && !wait_peer_ready(g_ctx.handle, g_ctx.peer_node, g_wait_peer_timeout_s)) {
        fprintf(stderr, "Peer node is not ready within %d seconds\n", g_wait_peer_timeout_s);
        return 1;
    }

    bool ok = true;
    if (is_case("send")) {
        ok = run_send_eer_case() && ok;
    }
    if (is_case("rcv-default")) {
        ok = run_receiver_eer_case(true) && ok;
    }
    if (is_case("rcv-normal")) {
        ok = run_receiver_eer_case(false) && ok;
    }
    if (is_case("if")) {
        ok = run_interface_cases() && ok;
    }
    if (g_case == "probe") {
        ok = run_recovery_probe_case() && ok;
    }

    if (ok) {
        printf("PASS: role A reliability EER case '%s' finished\n", g_case.c_str());
        return 0;
    }
    fprintf(stderr, "FAIL: role A reliability EER case '%s' failed\n", g_case.c_str());
    return 2;
}

void maybe_self_stop()
{
    if (g_fault_after_ms == 0 || g_peer_pid > 0) {
        return;
    }

    std::thread([]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(g_fault_after_ms));
        printf("[consumer] pid=%d self SIGSTOP now. Resume externally with: kill -CONT %d\n",
               static_cast<int>(getpid()), static_cast<int>(getpid()));
        raise(SIGSTOP);
        printf("[consumer] resumed after SIGCONT\n");
    }).detach();
}

int run_role_b()
{
    printf("Role B is ready. pid=%d\n", static_cast<int>(getpid()));
    printf("For heartbeat fault injection: kill -STOP %d; later kill -CONT %d\n",
           static_cast<int>(getpid()), static_cast<int>(getpid()));
    maybe_self_stop();

    while (true) {
        std::this_thread::sleep_for(1s);
        printf("[receiver] total_data=%u total_ack=%u\n",
               g_ctx.recv_count.load(std::memory_order_acquire),
               g_ctx.ack_count.load(std::memory_order_acquire));
    }
    return 0;
}

void print_help(const char *prog)
{
    printf("Usage: %s --role A|B [options]\n", prog);
    printf("  --role/-o             process role. A runs validation, B runs receiver service\n");
    printf("  --case <name>         role A: send|rcv-normal|rcv-default|if|probe|all\n");
    printf("                        role B: service, default service\n");
    printf("  -s <name>             sender shared memory name, default shm_node0_export\n");
    printf("  -r <name>             receiver shared memory name, default shm_node1_export\n");
    printf("  -n <count>            messages per sender thread, default 512\n");
    printf("  -t <threads>          sender thread count, default 4\n");
    printf("  -i <us>               per-message interval in us, default 1000\n");
    printf("  -w <seconds>          wait peer/recovery timeout, default 15\n");
    printf("  -l <level>            log level, default ERROR\n");
    printf("  --fault-after-ms <n>  role A sends SIGSTOP to --peer-pid after n ms; role B self-stops if no --peer-pid\n");
    printf("  --resume-after-ms <n> role A sends SIGCONT to --peer-pid after n ms\n");
    printf("  --peer-pid <pid>      peer process pid for same-host SIGSTOP/SIGCONT automation\n");
}

bool parse_args(int argc, char **argv)
{
    enum {
        OPT_ROLE = 'o',
        OPT_FAULT_AFTER = 1000,
        OPT_RESUME_AFTER,
        OPT_PEER_PID,
        OPT_CASE,
    };

    static struct option long_options[] = {
        {"role", required_argument, nullptr, OPT_ROLE},
        {"case", required_argument, nullptr, OPT_CASE},
        {"fault-after-ms", required_argument, nullptr, OPT_FAULT_AFTER},
        {"resume-after-ms", required_argument, nullptr, OPT_RESUME_AFTER},
        {"peer-pid", required_argument, nullptr, OPT_PEER_PID},
        {nullptr, 0, nullptr, 0},
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "o:s:r:n:t:i:w:l:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case OPT_ROLE:
                g_role = optarg[0];
                break;
            case OPT_CASE:
                g_case = optarg;
                break;
            case 's':
                strncpy(g_sender_shm_name, optarg, sizeof(g_sender_shm_name) - 1);
                break;
            case 'r':
                strncpy(g_receiver_shm_name, optarg, sizeof(g_receiver_shm_name) - 1);
                break;
            case 'n':
                g_messages = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                break;
            case 't':
                g_threads = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                break;
            case 'i':
                g_send_interval_us = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                break;
            case 'w':
                g_wait_peer_timeout_s = atoi(optarg);
                break;
            case 'l':
                g_log_level = atoi(optarg);
                break;
            case OPT_FAULT_AFTER:
                g_fault_after_ms = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                break;
            case OPT_RESUME_AFTER:
                g_resume_after_ms = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                break;
            case OPT_PEER_PID:
                g_peer_pid = static_cast<pid_t>(strtol(optarg, nullptr, 10));
                break;
            case 'h':
                print_help(argv[0]);
                return false;
            default:
                print_help(argv[0]);
                return false;
        }
    }

    if (g_role != 'A' && g_role != 'B') {
        fprintf(stderr, "Invalid role '%c', expected A or B\n", g_role);
        return false;
    }
    if (g_messages == 0 || g_threads == 0) {
        fprintf(stderr, "messages and threads must be greater than 0\n");
        return false;
    }
    if (g_case != "send" && g_case != "rcv-normal" && g_case != "rcv-default" && g_case != "if" &&
        g_case != "probe" && g_case != "all" && g_case != "service") {
        fprintf(stderr, "Invalid case '%s'\n", g_case.c_str());
        print_help(argv[0]);
        return false;
    }
    if (g_role == 'A' && g_case == "service") {
        fprintf(stderr, "role A must specify --case send|rcv-normal|rcv-default|if|probe|all\n");
        return false;
    }
    if (g_role == 'B' && g_case != "service") {
        fprintf(stderr, "role B only supports --case service\n");
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char **argv)
{
    if (!parse_args(argc, argv)) {
        return 1;
    }

    ub_atomic_set_log_level(g_log_level);
    ub_atomic_register_log_func(my_stdout_logger);

    ub_shm_comm_t handle = nullptr;
    if (init_handle(handle) != 0) {
        return 1;
    }

    if (!register_callbacks()) {
        fprintf(stderr, "register_callbacks failed\n");
        ub_comm_queue_deinit(&handle);
        return 1;
    }

    const int ret = (g_role == 'A') ? run_role_a() : run_role_b();
    ub_comm_queue_deinit(&handle);
    return ret;
}
