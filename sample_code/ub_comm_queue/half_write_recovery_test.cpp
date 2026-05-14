#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <vector>
#include <thread>

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
constexpr uint8_t PRIORITY_TEST = 1;
constexpr uint8_t TYPE_POISON = 131;
constexpr uint8_t TYPE_GOOD = 132;
constexpr int INJECT_EXIT_CODE = 86;

#pragma pack(push, 1)
struct DemoBody {
    uint32_t seq;
    uint32_t magic;
    uint64_t send_ns;
    char text[48];
};
#pragma pack(pop)

static_assert(sizeof(DemoBody) <= MAX_MSG_SIZE - sizeof(message_header_t), "DemoBody too large");

char g_role = 'D';
char g_sender_shm_name[64] = "shm_node0_export";
char g_receiver_shm_name[64] = "shm_node1_export";
int g_log_level = LOG_LEVEL_WARN;
int g_wait_peer_timeout_s = 15;
uint32_t g_expected_good = 8;
const char *g_prog_path = nullptr;
ub_shm_comm_t *g_handle = nullptr;
std::atomic<uint32_t> g_good_received{0};
std::atomic<uint32_t> g_poison_received{0};

int stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
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

uint64_t now_ns()
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

void cpu_relax()
{
#if defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    asm volatile("" ::: "memory");
#endif
}

int init_ub_shm()
{
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_init_attributes failed\n");
        return -1;
    }
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_initialize failed\n");
        return -1;
    }
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "ubsmem_lookup_regions failed\n");
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

int init_handle(ub_shm_comm_t &handle, uint8_t self_node)
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
    ring_descs[0] = {RING_CAPACITY, MAX_MSG_SIZE, PRIORITY_TEST};

    ub_comm_conf_t conf{};
    conf.cpu_id = -1;
    conf.max_nodes = MAX_NODES;
    conf.current_node_id = self_node;
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
        fprintf(stderr, "ub_comm_queue_init failed, self_node=%u\n", self_node);
        return -1;
    }
    g_handle = &handle;
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

bool send_body(ub_shm_comm_t *handle, uint8_t msg_type, uint32_t seq)
{
    DemoBody body{};
    body.seq = seq;
    body.magic = 0x48414c46U;
    body.send_ns = now_ns();
    snprintf(body.text, sizeof(body.text), "%s", msg_type == TYPE_POISON ? "poison-half-write" : "good-after-poison");

    message_t msg{};
    msg.header.dest_node_id = NODE_B;
    msg.header.src_node_id = NODE_A;
    msg.header.msg_type = msg_type;
    msg.header.priority = PRIORITY_TEST;
    msg.header.body_length = sizeof(body);
    msg.body = reinterpret_cast<char *>(&body);

    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < deadline) {
        int ret = ub_comm_queue_send(handle, &msg);
        if (ret >= 0) {
            printf("[sender] send success type=%u seq=%u ret=%d\n", msg_type, seq, ret);
            return true;
        }
        printf("[sender] send retry type=%u seq=%u ret=%d\n", msg_type, seq, ret);
        cpu_relax();
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

void on_good(const message_t *msg, void *ctx)
{
    (void)ctx;
    if (msg->header.body_length != sizeof(DemoBody)) {
        fprintf(stderr, "[receiver] bad good body length=%u\n", msg->header.body_length);
        return;
    }
    const auto *body = reinterpret_cast<const DemoBody *>(msg->body);
    printf("[receiver] GOOD received seq=%u magic=0x%x text=%s latency_ms=%.3f\n",
           body->seq, body->magic, body->text, (now_ns() - body->send_ns) / 1000000.0);
    g_good_received.fetch_add(1, std::memory_order_release);
}

void on_poison(const message_t *msg, void *ctx)
{
    (void)ctx;
    (void)msg;
    fprintf(stderr, "[receiver] ERROR: poison message was dispatched; injection probably not enabled\n");
    g_poison_received.fetch_add(1, std::memory_order_release);
}

void print_status_loop(ub_shm_comm_t *handle, std::atomic<bool> &stop)
{
    while (!stop.load(std::memory_order_relaxed)) {
        ub_comm_queue_status_t status{};
        int ret = ub_comm_queue_get_status(handle, NODE_B, PRIORITY_TEST, &status);
        if (ret == 0) {
            printf("[receiver-status] used=%" PRIu64 " free=%" PRIu64 " total=%" PRIu64 " state=%d max_depth=%" PRIu64 "\n",
                   status.used, status.free, status.total, static_cast<int>(status.state), status.max_depth);
        } else {
            printf("[receiver-status] get_status ret=%d\n", ret);
        }
        std::this_thread::sleep_for(500ms);
    }
}

int run_receiver()
{
    ub_shm_comm_t handle = nullptr;
    if (init_handle(handle, NODE_B) != 0) {
        return 1;
    }
    if (ub_comm_queue_register_process_func(&handle, TYPE_GOOD, UB_FUNC_SYNC, on_good, nullptr) != 0 ||
        ub_comm_queue_register_process_func(&handle, TYPE_POISON, UB_FUNC_SYNC, on_poison, nullptr) != 0) {
        fprintf(stderr, "[receiver] callback register failed\n");
        ub_comm_queue_deinit(&handle);
        return 2;
    }

    printf("[receiver] node B ready; waiting for poison slot to be skipped and %u good messages to arrive\n",
           g_expected_good);
    std::atomic<bool> stop_status{false};
    std::thread status_thread(print_status_loop, &handle, std::ref(stop_status));

    const auto deadline = std::chrono::steady_clock::now() + 25s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (g_poison_received.load(std::memory_order_acquire) != 0) {
            stop_status.store(true, std::memory_order_release);
            status_thread.join();
            ub_comm_queue_deinit(&handle);
            return 3;
        }
        if (g_good_received.load(std::memory_order_acquire) >= g_expected_good) {
            stop_status.store(true, std::memory_order_release);
            status_thread.join();
            printf("[receiver] PASS: %u good messages arrived after half-write recovery\n",
                   g_good_received.load(std::memory_order_acquire));
            ub_comm_queue_deinit(&handle);
            return 0;
        }
        std::this_thread::sleep_for(20ms);
    }

    stop_status.store(true, std::memory_order_release);
    status_thread.join();
    fprintf(stderr, "[receiver] TIMEOUT: good=%u poison=%u\n",
            g_good_received.load(std::memory_order_acquire), g_poison_received.load(std::memory_order_acquire));
    ub_comm_queue_deinit(&handle);
    return 4;
}

int run_sender(bool inject)
{
    ub_shm_comm_t handle = nullptr;
    if (init_handle(handle, NODE_A) != 0) {
        return 1;
    }
    if (!wait_peer_ready(&handle, NODE_B, g_wait_peer_timeout_s)) {
        fprintf(stderr, "[sender] peer B not ready\n");
        ub_comm_queue_deinit(&handle);
        return 2;
    }

    if (inject) {
        printf("[injector] enabling UB_COMM_QUEUE_INJECT_HALF_WRITE_EXIT=1; process should exit with %d\n",
               INJECT_EXIT_CODE);
        setenv("UB_COMM_QUEUE_INJECT_HALF_WRITE_EXIT", "1", 1);
        unsetenv("UB_COMM_QUEUE_INJECT_HALF_WRITE_ACTION");
        unsetenv("UB_COMM_QUEUE_INJECT_HALF_WRITE_MSG_TYPE");
        (void)send_body(&handle, TYPE_POISON, 1);
        fprintf(stderr, "[injector] ERROR: send returned; rebuild lib with UB_COMM_QUEUE_ENABLE_HALF_WRITE_INJECT\n");
        ub_comm_queue_deinit(&handle);
        return 3;
    }

    std::this_thread::sleep_for(500ms);
    bool ok = send_body(&handle, TYPE_GOOD, 2);
    ub_comm_queue_deinit(&handle);
    return ok ? 0 : 4;
}

int run_thread_poison_sender()
{
    ub_shm_comm_t handle = nullptr;
    if (init_handle(handle, NODE_A) != 0) {
        return 1;
    }
    if (!wait_peer_ready(&handle, NODE_B, g_wait_peer_timeout_s)) {
        fprintf(stderr, "[thread-poison] peer B not ready\n");
        ub_comm_queue_deinit(&handle);
        return 2;
    }

    char poison_type[16];
    snprintf(poison_type, sizeof(poison_type), "%u", TYPE_POISON);
    setenv("UB_COMM_QUEUE_INJECT_HALF_WRITE_MSG_TYPE", poison_type, 1);
    setenv("UB_COMM_QUEUE_INJECT_HALF_WRITE_ACTION", "hang", 1);
    unsetenv("UB_COMM_QUEUE_INJECT_HALF_WRITE_EXIT");

    printf("[thread-poison] start poison thread: msg_type=%u action=hang\n", TYPE_POISON);
    std::thread poison_thread([&handle]() {
        (void)send_body(&handle, TYPE_POISON, 1);
        fprintf(stderr, "[thread-poison] ERROR: poison send returned; injection did not hang\n");
    });
    poison_thread.detach();

    std::this_thread::sleep_for(1s);
    printf("[thread-poison] start %u normal sender threads while poison thread is stuck\n", g_expected_good);

    std::atomic<uint32_t> ok_count{0};
    std::vector<std::thread> workers;
    workers.reserve(g_expected_good);
    for (uint32_t i = 0; i < g_expected_good; ++i) {
        workers.emplace_back([&handle, &ok_count, i]() {
            if (send_body(&handle, TYPE_GOOD, 100 + i)) {
                ok_count.fetch_add(1, std::memory_order_release);
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }

    printf("[thread-poison] normal sends completed ok=%u/%u; exiting process will kill detached poison thread\n",
           ok_count.load(std::memory_order_acquire), g_expected_good);
    ub_comm_queue_deinit(&handle);
    return ok_count.load(std::memory_order_acquire) == g_expected_good ? 0 : 4;
}

pid_t spawn_role(char role)
{
    pid_t pid = fork();
    if (pid != 0) {
        return pid;
    }

    char role_arg[2] = {role, '\0'};
    char log_arg[16];
    char wait_arg[16];
    char expected_arg[16];
    snprintf(log_arg, sizeof(log_arg), "%d", g_log_level);
    snprintf(wait_arg, sizeof(wait_arg), "%d", g_wait_peer_timeout_s);
    snprintf(expected_arg, sizeof(expected_arg), "%u", g_expected_good);
    execl(g_prog_path, g_prog_path, "--role", role_arg, "-s", g_sender_shm_name, "-r", g_receiver_shm_name,
          "-l", log_arg, "-w", wait_arg, "-n", expected_arg, nullptr);
    fprintf(stderr, "[driver] execl role=%c failed errno=%d\n", role, errno);
    _exit(127);
}

int wait_child(pid_t pid, const char *name)
{
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        fprintf(stderr, "[driver] waitpid(%s) failed errno=%d\n", name, errno);
        return -1;
    }
    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        printf("[driver] %s exited code=%d\n", name, code);
        return code;
    }
    if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        printf("[driver] %s killed by signal=%d\n", name, sig);
        return 128 + sig;
    }
    return -1;
}

int run_driver()
{
    g_expected_good = 1;
    printf("[driver] start receiver B\n");
    pid_t receiver = spawn_role('B');
    std::this_thread::sleep_for(2s);

    printf("[driver] start injector I\n");
    pid_t injector = spawn_role('I');
    int injector_code = wait_child(injector, "injector");
    if (injector_code != INJECT_EXIT_CODE) {
        fprintf(stderr, "[driver] injector did not exit with %d; got %d\n", INJECT_EXIT_CODE, injector_code);
        kill(receiver, SIGTERM);
        (void)wait_child(receiver, "receiver");
        return 2;
    }

    printf("[driver] start normal sender A\n");
    pid_t sender = spawn_role('A');
    int sender_code = wait_child(sender, "sender");
    int receiver_code = wait_child(receiver, "receiver");

    if (sender_code == 0 && receiver_code == 0) {
        printf("[driver] PASS: half-write recovery verified\n");
        return 0;
    }
    fprintf(stderr, "[driver] FAIL: sender=%d receiver=%d\n", sender_code, receiver_code);
    return 3;
}

int run_thread_driver()
{
    printf("[thread-driver] start receiver B\n");
    pid_t receiver = spawn_role('B');
    std::this_thread::sleep_for(2s);

    printf("[thread-driver] start threaded poison sender T\n");
    pid_t sender = spawn_role('T');
    int sender_code = wait_child(sender, "thread-poison-sender");
    int receiver_code = wait_child(receiver, "receiver");

    if (sender_code == 0 && receiver_code == 0) {
        printf("[thread-driver] PASS: threaded half-write recovery verified\n");
        return 0;
    }
    fprintf(stderr, "[thread-driver] FAIL: sender=%d receiver=%d\n", sender_code, receiver_code);
    return 3;
}

void print_help(const char *prog)
{
    printf("Usage: %s --role D|H|B|I|A|T [-s sender_shm] [-r receiver_shm] [-l log_level] "
           "[-w wait_peer_timeout_s] [-n expected_good]\n", prog);
    printf("  D: one-shot driver. Starts B, then I, then A and checks exit codes.\n");
    printf("  H: threaded poison driver. Starts B, then T in the same two-node environment.\n");
    printf("  B: receiver node. Waits for a normal message after a half-written head slot.\n");
    printf("  I: injector node. Sends one message and exits after tail reservation when injection is enabled.\n");
    printf("  A: normal sender node. Sends the message that should pass after recovery.\n");
    printf("  T: same-process threaded poison sender. One thread hangs after tail reservation; other threads send good messages.\n");
    printf("Build the queue library with -DUB_COMM_QUEUE_ENABLE_HALF_WRITE_INJECT for role I/driver tests.\n");
}

bool parse_args(int argc, char **argv)
{
    static struct option long_options[] = {
        {"role", required_argument, nullptr, 'o'},
        {nullptr, 0, nullptr, 0},
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "o:s:r:l:w:n:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'o':
                g_role = optarg[0];
                break;
            case 's':
                strncpy(g_sender_shm_name, optarg, sizeof(g_sender_shm_name) - 1);
                break;
            case 'r':
                strncpy(g_receiver_shm_name, optarg, sizeof(g_receiver_shm_name) - 1);
                break;
            case 'l':
                g_log_level = atoi(optarg);
                break;
            case 'w':
                g_wait_peer_timeout_s = atoi(optarg);
                break;
            case 'n':
                g_expected_good = static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
                if (g_expected_good == 0) {
                    g_expected_good = 1;
                }
                break;
            case 'h':
                print_help(argv[0]);
                return false;
            default:
                print_help(argv[0]);
                return false;
        }
    }

    if (g_role != 'D' && g_role != 'H' && g_role != 'B' && g_role != 'I' && g_role != 'A' && g_role != 'T') {
        fprintf(stderr, "invalid role '%c'\n", g_role);
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char **argv)
{
    setvbuf(stdout, nullptr, _IONBF, 0);
    g_prog_path = argv[0];
    if (!parse_args(argc, argv)) {
        return 1;
    }

    ub_atomic_set_log_level(g_log_level);
    ub_atomic_register_log_func(stdout_logger);

    if (g_role == 'D') {
        return run_driver();
    }
    if (g_role == 'H') {
        return run_thread_driver();
    }
    if (g_role == 'B') {
        return run_receiver();
    }
    if (g_role == 'I') {
        return run_sender(true);
    }
    if (g_role == 'T') {
        return run_thread_poison_sender();
    }
    return run_sender(false);
}
