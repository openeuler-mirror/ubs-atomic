/*
 * demo_interactive.cpp - UB Comm Queue 交互式功能演示
 *
 * Usage:
 *   ./demo_interactive --role A [options]   (交互式菜单，主控端)
 *   ./demo_interactive --role B [options]   (自动回显服务器)
 *
 * Options:
 *   --role A|B         运行角色 (必选)
 *   --cpu-id <N>       绑定 CPU ID (A 默认 4, B 默认 200)
 *   --msg-size <bytes> 消息总长度含消息头 (支持: 64, 4096, 8192，默认 64)
 *   -s <shm_name>      发送端共享内存名
 *   -r <shm_name>      接收端共享内存名
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <string>
#include <mutex>
#include <map>
#include <getopt.h>
#include <algorithm>
#include <cinttypes>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <iostream>
#include <sstream>
#include <cstring>

#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"

using namespace std::chrono_literals;

// ===================== 演示消息类型 =====================

enum DemoMsgType : uint8_t {
    MSG_SELF_TEST     = 10,  // 自发自收测试
    MSG_ECHO_REQ      = 20,  // 回显请求
    MSG_ECHO_RESP     = 21,  // 回显响应
    MSG_CUSTOM_DEMO   = 30,  // 回调注册演示
    MSG_CONCURRENT    = 40,  // 并发测试
    MSG_FLOW_CTRL     = 50,  // 流控测试
};

// ===================== 消息体 =====================

static const size_t   RING_CAPACITY = 1024;
static const size_t   HEADER_SIZE   = sizeof(message_header_t);
static const uint32_t DEMO_BODY_SIZE = 48;

#pragma pack(push, 1)
struct DemoMsg {
    uint32_t seq;
    int64_t  send_time_ns;
    char     text[36];
};
#pragma pack(pop)
static_assert(sizeof(DemoMsg) == DEMO_BODY_SIZE, "DemoMsg must be 48 bytes");

// ===================== 全局状态 =====================

enum Role { ROLE_A, ROLE_B };

static Role     g_role         = ROLE_A;
static bool     g_role_set     = false;
static int      g_cpu_id       = -1;
static size_t   g_msg_size     = 64;
static uint64_t NodeA          = 0;
static uint64_t NodeB          = 1;
static char     kSenderShmName[64]   = "shm_node0_export";
static char     kReceiverShmName[64] = "shm_node1_export";
static unsigned long g_shm_size_mb   = 1024;
static ub_shm_comm_t g_handle        = nullptr;
static ub_shm_comm_t* g_handlep      = &g_handle;

// 回调统计
struct CallbackRecord {
    uint32_t seq;
    int64_t  recv_time_ns;
    int64_t  send_time_ns;
    char     text[36];
};

static std::atomic<uint64_t> g_self_recv_count{0};
static CallbackRecord        g_self_last_record{};
static std::atomic<bool>     g_self_received{false};

static std::atomic<uint64_t> g_echo_resp_count{0};
static CallbackRecord        g_echo_last_record{};
static std::atomic<bool>     g_echo_received{false};

static std::atomic<uint64_t> g_custom_recv_count{0};
static CallbackRecord        g_custom_last_record{};
static std::atomic<bool>     g_custom_received{false};
static bool                  g_custom_async = false;

static std::atomic<uint64_t> g_concurrent_recv_count{0};
static std::atomic<uint64_t> g_concurrent_send_count{0};
static std::vector<int64_t>  g_concurrent_rtts;
static std::mutex            g_concurrent_mtx;

static std::atomic<uint64_t> g_flow_recv_count{0};

// B 角色统计
static std::atomic<uint64_t> g_b_echo_sent{0};
static std::atomic<uint64_t> g_b_custom_sent{0};
static std::atomic<uint64_t> g_b_concurrent_resp{0};

// ===================== 日志 =====================

static int my_stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    const char *level_str = "UNKNOWN";
    switch (level) {
        case 0: level_str = "DEBUG"; break;
        case 1: level_str = "INFO"; break;
        case 2: level_str = "WARN"; break;
        case 3: level_str = "ERROR"; break;
        case 4: level_str = "CRITICAL"; break;
        default: break;
    }
    time_t now = time(0);
    char *time_str = ctime(&now);
    time_str[strlen(time_str) - 1] = '\0';
    fprintf(stdout, "[%s] [%s:%u] [%s] %s\n", time_str, file, line, level_str, message);
    return 0;
}

// ===================== 工具函数 =====================

static inline int64_t get_ns() {
    return (int64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

static inline uint32_t body_length() {
    return static_cast<uint32_t>(g_msg_size - HEADER_SIZE);
}

#if defined(__aarch64__)
static inline void cpu_relax() { asm volatile("yield" ::: "memory"); }
#else
static inline void cpu_relax() { asm volatile("" ::: "memory"); }
#endif

static void fill_demo_msg(DemoMsg& msg, uint32_t seq, const char* text) {
    msg.seq = seq;
    msg.send_time_ns = get_ns();
    strncpy(msg.text, text, sizeof(msg.text) - 1);
    msg.text[sizeof(msg.text) - 1] = '\0';
}

static void send_message(uint8_t msg_type, uint8_t dest, const DemoMsg& body_data) {
    const uint32_t blen = body_length();
    std::vector<char> body_buf(blen, 0);
    memcpy(body_buf.data(), &body_data, sizeof(DemoMsg));

    message_t msg{};
    msg.header.src_node_id  = (g_role == ROLE_A) ? NodeA : NodeB;
    msg.header.dest_node_id = dest;
    msg.header.msg_type     = msg_type;
    msg.header.priority     = 1;
    msg.header.body_length  = blen;
    msg.body                = body_buf.data();

    for (int retry = 0; retry < 100000; ++retry) {
        int ret = ub_comm_queue_send(g_handlep, &msg);
        if (ret >= 0) return;
        if (ret == UB_COMM_ERR_RING_FULL) { cpu_relax(); continue; }
        fprintf(stderr, "  [发送失败] ret=%d\n", ret);
        return;
    }
    fprintf(stderr, "  [发送超时] 队列持续满\n");
}

static const char* queue_state_str(ub_comm_queue_state_t s) {
    switch (s) {
        case UB_COMM_QUEUE_IDLE:      return "IDLE";
        case UB_COMM_QUEUE_NORMAL:    return "NORMAL";
        case UB_COMM_QUEUE_CONGESTED: return "CONGESTED";
        case UB_COMM_QUEUE_FULL:      return "FULL";
        default:                      return "UNKNOWN";
    }
}

static void print_queue_status(uint8_t node_id) {
    ub_comm_queue_status_t status{};
    int ret = ub_comm_queue_get_status(g_handlep, node_id, 1, &status);
    if (ret != 0) {
        printf("  [错误] 获取状态失败, ret=%d\n", ret);
        return;
    }
    printf("  队列状态 [Node %u, Priority 1]:\n", node_id);
    printf("    state               : %s\n", queue_state_str(status.state));
    printf("    used / total        : %lu / %lu\n", status.used, status.total);
    printf("    free                : %lu\n", status.free);
    printf("    congestion_threshold: %lu\n", status.congestion_threshold);
    printf("    max_depth           : %lu\n", status.max_depth);
}

// ===================== A 角色：回调函数 =====================

static void on_self_test(const message_t *msg, void *ctx) {
    if (msg->header.body_length < sizeof(DemoMsg)) return;
    const DemoMsg* dm = reinterpret_cast<const DemoMsg*>(msg->body);
    g_self_last_record.seq         = dm->seq;
    g_self_last_record.send_time_ns = dm->send_time_ns;
    g_self_last_record.recv_time_ns = get_ns();
    strncpy(g_self_last_record.text, dm->text, sizeof(g_self_last_record.text) - 1);
    g_self_recv_count.fetch_add(1, std::memory_order_relaxed);
    g_self_received.store(true, std::memory_order_release);
}

static void on_echo_resp(const message_t *msg, void *ctx) {
    if (msg->header.body_length < sizeof(DemoMsg)) return;
    const DemoMsg* dm = reinterpret_cast<const DemoMsg*>(msg->body);
    g_echo_last_record.seq         = dm->seq;
    g_echo_last_record.send_time_ns = dm->send_time_ns;
    g_echo_last_record.recv_time_ns = get_ns();
    strncpy(g_echo_last_record.text, dm->text, sizeof(g_echo_last_record.text) - 1);
    g_echo_resp_count.fetch_add(1, std::memory_order_relaxed);
    g_echo_received.store(true, std::memory_order_release);
}

static void on_custom_demo(const message_t *msg, void *ctx) {
    if (msg->header.body_length < sizeof(DemoMsg)) return;
    const DemoMsg* dm = reinterpret_cast<const DemoMsg*>(msg->body);
    g_custom_last_record.seq         = dm->seq;
    g_custom_last_record.send_time_ns = dm->send_time_ns;
    g_custom_last_record.recv_time_ns = get_ns();
    strncpy(g_custom_last_record.text, dm->text, sizeof(g_custom_last_record.text) - 1);
    g_custom_recv_count.fetch_add(1, std::memory_order_relaxed);
    g_custom_received.store(true, std::memory_order_release);
}

static void on_concurrent_resp(const message_t *msg, void *ctx) {
    if (msg->header.body_length < sizeof(DemoMsg)) return;
    const DemoMsg* dm = reinterpret_cast<const DemoMsg*>(msg->body);
    int64_t rtt = get_ns() - dm->send_time_ns;
    {
        std::lock_guard<std::mutex> lock(g_concurrent_mtx);
        g_concurrent_rtts.push_back(rtt);
    }
    g_concurrent_recv_count.fetch_add(1, std::memory_order_relaxed);
}

// ===================== B 角色：回调函数 =====================

static void on_b_echo_req(const message_t *msg, void *ctx) {
    if (msg->header.body_length < sizeof(DemoMsg)) return;
    DemoMsg dm;
    memcpy(&dm, msg->body, sizeof(DemoMsg));
    // 标记为 B 处理后回显
    strncat(dm.text, "<-B", sizeof(dm.text) - strlen(dm.text) - 1);
    send_message(MSG_ECHO_RESP, NodeA, dm);
    g_b_echo_sent.fetch_add(1, std::memory_order_relaxed);
    printf("  [B] 回显请求 seq=%u text='%s'\n", dm.seq, dm.text);
}

static void on_b_custom(const message_t *msg, void *ctx) {
    if (msg->header.body_length < sizeof(DemoMsg)) return;
    DemoMsg dm;
    memcpy(&dm, msg->body, sizeof(DemoMsg));
    g_b_custom_sent.fetch_add(1, std::memory_order_relaxed);
    printf("  [B] 收到自定义消息 seq=%u text='%s' (异步=%s)\n",
           dm.seq, dm.text, g_custom_async ? "是" : "否");
}

static void on_b_concurrent(const message_t *msg, void *ctx) {
    if (msg->header.body_length < sizeof(DemoMsg)) return;
    DemoMsg dm;
    memcpy(&dm, msg->body, sizeof(DemoMsg));
    send_message(MSG_CONCURRENT, NodeA, dm);
    g_b_concurrent_resp.fetch_add(1, std::memory_order_relaxed);
}

// ===================== A 角色：演示函数 =====================

static void demo_self_send() {
    printf("\n");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║          1. 自发自收演示                          ║\n");
    printf("╚══════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("  原理: 节点 A 向自己发送消息 (dest_node_id = self),\n");
    printf("        消息进入本地环回队列，由分发线程取出并触发回调。\n");
    printf("        这验证了本地环回路径 (Local Loopback)。\n");
    printf("\n");

    g_self_received.store(false, std::memory_order_relaxed);

    DemoMsg body;
    fill_demo_msg(body, 1, "Hello Self!");

    printf("  >> 正在向自己发送消息...\n");
    int64_t t0 = get_ns();
    send_message(MSG_SELF_TEST, NodeA, body);
    int64_t send_cost = get_ns() - t0;
    printf("  >> 发送完成, send_cost=%ld ns\n", send_cost);

    // 等待回调
    printf("  >> 等待回调触发...\n");
    for (int i = 0; i < 5000; ++i) {
        if (g_self_received.load(std::memory_order_acquire)) break;
        std::this_thread::sleep_for(1ms);
    }

    if (g_self_received.load(std::memory_order_relaxed)) {
        int64_t rtt = g_self_last_record.recv_time_ns - g_self_last_record.send_time_ns;
        printf("  >> 回调已触发!\n");
        printf("     seq          : %u\n", g_self_last_record.seq);
        printf("     text         : %s\n", g_self_last_record.text);
        printf("     RTT (自发自收): %ld ns (%.3f us)\n", rtt, rtt / 1000.0);
        printf("     总计接收     : %lu 条\n", g_self_recv_count.load());
    } else {
        printf("  >> [超时] 未收到自发自收消息 (5s)\n");
    }
    printf("\n");
}

static void demo_peer_send() {
    printf("\n");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║          2. 发送给对端演示                        ║\n");
    printf("╚══════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("  原理: 节点 A 向节点 B 发送 MSG_ECHO_REQ,\n");
    printf("        B 的回调收到后回送 MSG_ECHO_RESP,\n");
    printf("        A 的回调收到响应，计算跨节点 RTT。\n");
    printf("\n");

    if (!ub_comm_queue_check_ready(g_handlep, NodeB)) {
        printf("  [警告] 节点 B 尚未就绪，结果可能异常\n");
    }

    g_echo_received.store(false, std::memory_order_relaxed);

    DemoMsg body;
    fill_demo_msg(body, 1, "Hello B!");

    printf("  >> 正在向 Node B 发送回显请求...\n");
    int64_t t0 = get_ns();
    send_message(MSG_ECHO_REQ, NodeB, body);

    // 等待响应
    printf("  >> 等待 B 的回显响应...\n");
    for (int i = 0; i < 5000; ++i) {
        if (g_echo_received.load(std::memory_order_acquire)) break;
        std::this_thread::sleep_for(1ms);
    }

    if (g_echo_received.load(std::memory_order_relaxed)) {
        int64_t rtt = g_echo_last_record.recv_time_ns - g_echo_last_record.send_time_ns;
        printf("  >> 收到回显响应!\n");
        printf("     seq   : %u\n", g_echo_last_record.seq);
        printf("     text  : %s\n", g_echo_last_record.text);
        printf("     RTT   : %ld ns (%.3f us)\n", rtt, rtt / 1000.0);
        printf("     总计  : %lu 条响应\n", g_echo_resp_count.load());
    } else {
        printf("  >> [超时] 未收到回显响应 (5s)\n");
        printf("     请确认 Node B 已启动 (--role B)\n");
    }
    printf("\n");
}

static void demo_register_callback() {
    printf("\n");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║          3. 注册消息回调演示                      ║\n");
    printf("╚══════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("  原理: 通过 ub_comm_queue_register_process_func\n");
    printf("        注册回调，支持同步 (UB_FUNC_SYNC) 和\n");
    printf("        异步 (UB_FUNC_ASYNC) 两种模式。\n");
    printf("        同步回调在分发线程中直接执行，\n");
    printf("        异步回调在线程池中执行。\n");
    printf("\n");

    // 第一步：发送但无回调
    printf("  [步骤1] 取消注册 MSG_CUSTOM_DEMO 回调，然后发送...\n");
    // 注意：API 不支持取消注册，但可以发送一个没有回调的类型
    printf("  >> 发送未注册回调的消息类型 (MSG_FLOW_CTRL=50)...\n");
    {
        DemoMsg body;
        fill_demo_msg(body, 1, "No callback");
        send_message(MSG_FLOW_CTRL, NodeA, body);
        printf("  >> 消息已发送，但没有回调处理它 (消息被丢弃)\n");
    }

    // 第二步：注册同步回调
    printf("\n  [步骤2] 注册同步回调 (UB_FUNC_SYNC)...\n");
    g_custom_received.store(false, std::memory_order_relaxed);
    int ret = ub_comm_queue_register_process_func(g_handlep, MSG_CUSTOM_DEMO,
                                                   UB_FUNC_SYNC, on_custom_demo, nullptr);
    if (ret != 0) {
        printf("  [错误] 注册失败, ret=%d\n", ret);
        return;
    }
    printf("  >> 同步回调已注册\n");

    {
        DemoMsg body;
        fill_demo_msg(body, 2, "Sync msg");
        printf("  >> 发送 MSG_CUSTOM_DEMO...\n");
        send_message(MSG_CUSTOM_DEMO, NodeA, body);
    }

    for (int i = 0; i < 3000; ++i) {
        if (g_custom_received.load(std::memory_order_acquire)) break;
        std::this_thread::sleep_for(1ms);
    }

    if (g_custom_received.load(std::memory_order_relaxed)) {
        printf("  >> 同步回调被触发! text='%s'\n", g_custom_last_record.text);
    } else {
        printf("  >> [超时] 同步回调未被触发\n");
    }

    // 第三步：注册异步回调
    printf("\n  [步骤3] 重新注册为异步回调 (UB_FUNC_ASYNC)...\n");
    g_custom_received.store(false, std::memory_order_relaxed);
    g_custom_async = true;
    ret = ub_comm_queue_register_process_func(g_handlep, MSG_CUSTOM_DEMO,
                                               UB_FUNC_ASYNC, on_custom_demo, nullptr);
    if (ret != 0) {
        printf("  [错误] 注册失败, ret=%d\n", ret);
        return;
    }
    printf("  >> 异步回调已注册 (由线程池执行)\n");

    {
        DemoMsg body;
        fill_demo_msg(body, 3, "Async msg");
        printf("  >> 发送 MSG_CUSTOM_DEMO...\n");
        send_message(MSG_CUSTOM_DEMO, NodeA, body);
    }

    for (int i = 0; i < 3000; ++i) {
        if (g_custom_received.load(std::memory_order_acquire)) break;
        std::this_thread::sleep_for(1ms);
    }

    if (g_custom_received.load(std::memory_order_relaxed)) {
        printf("  >> 异步回调被触发! text='%s'\n", g_custom_last_record.text);
        printf("     (异步回调由线程池执行，可能稍有延迟)\n");
    } else {
        printf("  >> [超时] 异步回调未被触发\n");
    }

    // 恢复同步回调
    g_custom_async = false;
    ub_comm_queue_register_process_func(g_handlep, MSG_CUSTOM_DEMO,
                                         UB_FUNC_SYNC, on_custom_demo, nullptr);
    printf("\n  >> 已恢复为同步回调\n");
    printf("\n");
}

static void demo_concurrent_send() {
    printf("\n");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║          4. 并发发送演示                          ║\n");
    printf("╚══════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("  原理: 启动多个线程同时向对端发送消息,\n");
    printf("        对端回显后统计吞吐量和延迟分布。\n");
    printf("\n");

    int thread_num = 4;
    uint64_t msg_per_thread = 500;
    printf("  参数: threads=%d  msg_per_thread=%lu\n", thread_num, msg_per_thread);
    printf("  总消息数: %lu\n", thread_num * msg_per_thread);

    if (!ub_comm_queue_check_ready(g_handlep, NodeB)) {
        printf("  [警告] 节点 B 尚未就绪\n");
    }

    g_concurrent_recv_count.store(0);
    g_concurrent_send_count.store(0);
    {
        std::lock_guard<std::mutex> lock(g_concurrent_mtx);
        g_concurrent_rtts.clear();
    }

    const uint64_t total = thread_num * msg_per_thread;
    const auto start_time = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < thread_num; ++t) {
        threads.emplace_back([t, msg_per_thread]() {
            const uint32_t blen = body_length();
            for (uint64_t i = 0; i < msg_per_thread; ++i) {
                DemoMsg body;
                fill_demo_msg(body, t * msg_per_thread + i, "concurrent");

                std::vector<char> buf(blen, 0);
                memcpy(buf.data(), &body, sizeof(DemoMsg));

                message_t msg{};
                msg.header.src_node_id  = NodeA;
                msg.header.dest_node_id = NodeB;
                msg.header.msg_type     = MSG_CONCURRENT;
                msg.header.priority     = 1;
                msg.header.body_length  = blen;
                msg.body                = buf.data();

                for (;;) {
                    int ret = ub_comm_queue_send(g_handlep, &msg);
                    if (ret >= 0) break;
                    cpu_relax();
                }
                g_concurrent_send_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& th : threads) th.join();

    // 等待所有响应
    printf("  >> 发送完成，等待响应...\n");
    for (int i = 0; i < 10000; ++i) {
        if (g_concurrent_recv_count.load() >= total) break;
        std::this_thread::sleep_for(1ms);
    }

    const auto end_time = std::chrono::steady_clock::now();
    const double elapsed_s = std::chrono::duration<double>(end_time - start_time).count();
    const uint64_t recv_count = g_concurrent_recv_count.load();

    printf("\n  ===== 并发测试结果 =====\n");
    printf("  发送     : %lu 条\n", g_concurrent_send_count.load());
    printf("  接收     : %lu 条\n", recv_count);
    printf("  耗时     : %.3f s\n", elapsed_s);
    if (recv_count > 0) {
        printf("  吞吐量   : %.0f msg/s\n", recv_count / elapsed_s);

        std::vector<int64_t> rtts;
        {
            std::lock_guard<std::mutex> lock(g_concurrent_mtx);
            rtts = g_concurrent_rtts;
        }
        if (!rtts.empty()) {
            std::sort(rtts.begin(), rtts.end());
            double sum = 0;
            for (auto v : rtts) sum += (double)v;
            printf("  RTT 统计:\n");
            printf("    avg = %.2f us\n", (sum / rtts.size()) / 1000.0);
            printf("    p50 = %.2f us\n", rtts[rtts.size() * 50 / 100] / 1000.0);
            printf("    p99 = %.2f us\n", rtts[rtts.size() * 99 / 100] / 1000.0);
            printf("    max = %.2f us\n", (double)rtts.back() / 1000.0);
        }
    }
    printf("\n");
}

static void demo_queue_status() {
    printf("\n");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║          5. 队列状态查询                          ║\n");
    printf("╚══════════════════════════════════════════════════╝\n");
    printf("\n");

    printf("  --- 本地队列 (Node A) ---\n");
    print_queue_status(NodeA);

    printf("\n  --- 对端队列 (Node B) ---\n");
    print_queue_status(NodeB);

    printf("\n  --- 节点就绪状态 ---\n");
    printf("    Node A ready: %s\n", ub_comm_queue_check_ready(g_handlep, NodeA) ? "是" : "否");
    printf("    Node B ready: %s\n", ub_comm_queue_check_ready(g_handlep, NodeB) ? "是" : "否");
    printf("\n");
}

static void demo_flow_control() {
    printf("\n");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║          6. 流控演示                              ║\n");
    printf("╚══════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("  原理: 通过 ub_comm_queue_set_congestion_threshold\n");
    printf("        设置拥塞百分比阈值。阈值作用于本地节点的环，\n");
    printf("        当环使用量超过阈值时，send 返回 CONGESTED，\n");
    printf("        但消息仍然发送成功。当环满时返回错误码。\n");
    printf("\n");

    // 1. 显示当前状态
    printf("  [步骤1] 当前本地队列状态:\n");
    print_queue_status(NodeA);

    // 2. 设置低拥塞阈值
    printf("\n  [步骤2] 设置本地环拥塞阈值为 20%%...\n");
    printf("  (20%% of %zu = 约 %zu 条消息触发拥塞)\n",
           RING_CAPACITY, RING_CAPACITY * 20 / 100);
    int ret = ub_comm_queue_set_congestion_threshold(g_handlep, 1, 20);
    if (ret != 0) {
        printf("  [错误] 设置失败, ret=%d\n", ret);
    } else {
        printf("  >> 设置成功\n");
    }

    // 3. 自发自收突发，观察返回值
    // 使用自发自收路径，阈值作用在本地环上
    printf("\n  [步骤3] 自发自收突发发送 (观察返回值):\n");
    const uint32_t blen = body_length();
    int congested_count = 0;
    int ok_count = 0;
    int full_count = 0;
    const int burst_size = 300;

    for (int i = 0; i < burst_size; ++i) {
        DemoMsg body;
        fill_demo_msg(body, i, "flow-ctrl");

        std::vector<char> buf(blen, 0);
        memcpy(buf.data(), &body, sizeof(DemoMsg));

        message_t msg{};
        msg.header.src_node_id  = NodeA;
        msg.header.dest_node_id = NodeA;  // 自发自收，阈值作用在本地环
        msg.header.msg_type     = MSG_FLOW_CTRL;
        msg.header.priority     = 1;
        msg.header.body_length  = blen;
        msg.body                = buf.data();

        int send_ret = ub_comm_queue_send(g_handlep, &msg);
        if (send_ret == UB_COMM_OK) {
            ok_count++;
        } else if (send_ret == UB_COMM_SEND_CONGESTED) {
            congested_count++;
        } else {
            full_count++;
            if (full_count >= 3) break;
        }
    }

    printf("    发送总数                : %d\n", ok_count + congested_count + full_count);
    printf("    UB_COMM_OK             : %d 次\n", ok_count);
    printf("    UB_COMM_SEND_CONGESTED : %d 次\n", congested_count);
    printf("    发送失败 (FULL/ERROR)  : %d 次\n", full_count);

    printf("\n  [步骤4] 发送后本地队列状态:\n");
    print_queue_status(NodeA);

    // 4. 恢复默认阈值
    printf("\n  [步骤5] 恢复拥塞阈值为 80%%...\n");
    ub_comm_queue_set_congestion_threshold(g_handlep, 1, 80);
    printf("  >> 已恢复\n");

    printf("\n  流控要点:\n");
    printf("    - UB_COMM_OK (0):             发送成功，队列正常\n");
    printf("    - UB_COMM_SEND_CONGESTED (1): 发送成功，但队列已超阈值\n");
    printf("    - UB_COMM_ERR_RING_FULL:      队列满，发送失败\n");
    printf("    - 拥塞阈值作用于本地环 (自发自收路径)\n");
    printf("    - 远端生产者通过共享元数据观察远端环的拥塞状态\n");
    printf("\n");
}

static void demo_heartbeat() {
    printf("\n");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║          7. 心跳配置演示                          ║\n");
    printf("╚══════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("  原理: 每个节点的消费者线程定期更新心跳序列号,\n");
    printf("        生产者线程定期检查远端心跳。如果远端心跳\n");
    printf("        在 timeout_ms 内未更新，则认为远端不可达。\n");
    printf("\n");

    // 查询当前配置
    ub_comm_queue_heartbeat_config_t effective{};
    int ret = ub_comm_queue_config_heartbeat(g_handlep, nullptr, &effective);
    if (ret != 0) {
        printf("  [错误] 查询心跳配置失败, ret=%d\n", ret);
        return;
    }

    printf("  当前心跳配置:\n");
    printf("    heartbeat_interval_ms    : %u ms (本地消费者更新间隔)\n", effective.heartbeat_interval_ms);
    printf("    check_interval_ms        : %u ms (本地生产者检查间隔)\n", effective.check_interval_ms);
    printf("    timeout_ms               : %u ms (远端超时判定)\n", effective.timeout_ms);

    // 修改配置
    printf("\n  修改心跳配置: interval=200ms, check=200ms, timeout=2000ms\n");
    ub_comm_queue_heartbeat_config_t request{};
    request.heartbeat_interval_ms = 200;
    request.check_interval_ms     = 200;
    request.timeout_ms            = 2000;
    ret = ub_comm_queue_config_heartbeat(g_handlep, &request, &effective);
    if (ret != 0) {
        printf("  [错误] 修改失败, ret=%d\n", ret);
    } else {
        printf("  >> 修改成功\n");
        printf("  生效配置:\n");
        printf("    heartbeat_interval_ms    : %u ms\n", effective.heartbeat_interval_ms);
        printf("    check_interval_ms        : %u ms\n", effective.check_interval_ms);
        printf("    timeout_ms               : %u ms\n", effective.timeout_ms);
    }

    // 恢复默认
    printf("\n  恢复默认配置...\n");
    request.heartbeat_interval_ms = 100;
    request.check_interval_ms     = 100;
    request.timeout_ms            = 1000;
    ub_comm_queue_config_heartbeat(g_handlep, &request, &effective);
    printf("  >> 已恢复\n");
    printf("\n");
}

// ===================== 交互式菜单 =====================

static void print_menu() {
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════╗\n");
    printf("║         UB Comm Queue 交互式功能演示                     ║\n");
    printf("║         节点: A (Node 0)    消息大小: %zu bytes         ║\n", g_msg_size);
    printf("╠══════════════════════════════════════════════════════════╣\n");
    printf("║  1. 自发自收       (Local Loopback)                     ║\n");
    printf("║  2. 发送给对端     (Peer Echo RTT)                      ║\n");
    printf("║  3. 注册消息回调   (Sync / Async Callback)              ║\n");
    printf("║  4. 并发发送       (Multi-thread Throughput)            ║\n");
    printf("║  5. 队列状态查询   (Queue Status)                       ║\n");
    printf("║  6. 流控演示       (Congestion Threshold)               ║\n");
    printf("║  7. 心跳配置       (Heartbeat Config)                   ║\n");
    printf("║  0. 退出                                                ║\n");
    printf("╚══════════════════════════════════════════════════════════╝\n");
    printf("  请选择 > ");
}

static void run_interactive() {
    // 注册 A 角色回调
    ub_comm_queue_register_process_func(g_handlep, MSG_SELF_TEST,   UB_FUNC_SYNC, on_self_test,       nullptr);
    ub_comm_queue_register_process_func(g_handlep, MSG_ECHO_RESP,   UB_FUNC_SYNC, on_echo_resp,       nullptr);
    ub_comm_queue_register_process_func(g_handlep, MSG_CUSTOM_DEMO, UB_FUNC_SYNC, on_custom_demo,     nullptr);
    ub_comm_queue_register_process_func(g_handlep, MSG_CONCURRENT,  UB_FUNC_SYNC, on_concurrent_resp, nullptr);
    // MSG_FLOW_CTRL 无需回调，分发线程直接消费即可 (用于流控演示)

    printf("\n  交互式 Demo 已就绪。Node A 的回调已注册。\n");
    printf("  请确保 Node B 也在运行 (./demo_interactive --role B)\n");

    while (true) {
        print_menu();
        std::string input;
        std::getline(std::cin, input);

        int choice = 0;
        try { choice = std::stoi(input); } catch (...) { continue; }

        switch (choice) {
            case 1: demo_self_send(); break;
            case 2: demo_peer_send(); break;
            case 3: demo_register_callback(); break;
            case 4: demo_concurrent_send(); break;
            case 5: demo_queue_status(); break;
            case 6: demo_flow_control(); break;
            case 7: demo_heartbeat(); break;
            case 0:
                printf("  退出 Demo.\n");
                return;
            default:
                printf("  无效选择，请重新输入.\n");
                break;
        }
    }
}

// ===================== B 角色：自动回显服务器 =====================

static void run_echo_server() {
    // 注册 B 角色回调
    ub_comm_queue_register_process_func(g_handlep, MSG_ECHO_REQ,    UB_FUNC_SYNC, on_b_echo_req,     nullptr);
    ub_comm_queue_register_process_func(g_handlep, MSG_CUSTOM_DEMO, UB_FUNC_SYNC, on_b_custom,       nullptr);
    ub_comm_queue_register_process_func(g_handlep, MSG_CONCURRENT,  UB_FUNC_SYNC, on_b_concurrent,   nullptr);
    ub_comm_queue_register_process_func(g_handlep, MSG_FLOW_CTRL,   UB_FUNC_SYNC, [](const message_t*, void*) {
        g_flow_recv_count.fetch_add(1, std::memory_order_relaxed);
    }, nullptr);

    printf("  B 角色回显服务器已就绪.\n");
    printf("  注册回调: MSG_ECHO_REQ, MSG_CUSTOM_DEMO, MSG_CONCURRENT, MSG_FLOW_CTRL\n");
    printf("  等待消息中... (Ctrl+C 退出)\n\n");

    while (true) {
        std::this_thread::sleep_for(2s);
        auto echo = g_b_echo_sent.load();
        auto custom = g_b_custom_sent.load();
        auto conc = g_b_concurrent_resp.load();
        auto flow = g_flow_recv_count.load();
        if (echo + custom + conc + flow > 0) {
            printf("  [B 统计] echo=%lu custom=%lu concurrent_resp=%lu flow_ctrl=%lu\n",
                   echo, custom, conc, flow);
        }
    }
}

// ===================== 公共函数 =====================

static int init_ub_shm() {
    ubsmem_options_t opts{};
    int ret = ubsmem_init_attributes(&opts);
    if (ret != UBSM_OK) { fprintf(stderr, "Failed to init ubsmem attributes!\n"); return -1; }
    ret = ubsmem_initialize(&opts);
    if (ret != UBSM_OK) { fprintf(stderr, "Failed to init ubsmem!\n"); return -1; }
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) { fprintf(stderr, "Failed to lookup regions!\n"); return -1; }
    return 0;
}

static int map_ub_shm(const char *shm_name, void *&addr) {
    const unsigned long length = g_shm_size_mb * 1024UL * 1024UL;
    int ret = ubsmem_shmem_map(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_name, 0, &addr);
    if (ret != 0) { fprintf(stderr, "Failed to map '%s'! ret=%d\n", shm_name, ret); return -1; }
    fprintf(stdout, "Mapped '%s' at %p\n", shm_name, addr);
    return 0;
}

static void print_help(const char* prog) {
    printf("Usage: %s --role A|B [options]\n", prog);
    printf("\n");
    printf("Required:\n");
    printf("  --role A|B         运行角色 (A=交互菜单, B=自动回显服务器)\n");
    printf("\n");
    printf("Common options:\n");
    printf("  --cpu-id <N>       绑定 CPU ID (A 默认 4, B 默认 200)\n");
    printf("  --msg-size <bytes> 消息总长度含消息头 (支持: 64, 4096, 8192，默认 64)\n");
    printf("  -s <shm_name>      发送端共享内存名 (默认 shm_node0_export)\n");
    printf("  -r <shm_name>      接收端共享内存名 (默认 shm_node1_export)\n");
    printf("  -h                 显示帮助\n");
}

// ===================== main =====================

int main(int argc, char* argv[]) {
    ub_atomic_set_log_level(LOG_LEVEL_ERROR);
    ub_atomic_register_log_func(my_stdout_logger);

    static struct option long_options[] = {
        {"role",     required_argument, 0, 'R'},
        {"cpu-id",   required_argument, 0, 'C'},
        {"msg-size", required_argument, 0, 'M'},
        {"help",     no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    int long_index = 0;
    while ((opt = getopt_long(argc, argv, "s:r:h", long_options, &long_index)) != -1) {
        switch (opt) {
            case 'R':
                if (strcmp(optarg, "A") == 0) g_role = ROLE_A;
                else if (strcmp(optarg, "B") == 0) g_role = ROLE_B;
                else { fprintf(stderr, "Invalid role '%s'. Must be A or B.\n", optarg); return -1; }
                g_role_set = true;
                break;
            case 'C': g_cpu_id = atoi(optarg); break;
            case 'M': g_msg_size = strtoull(optarg, nullptr, 10); break;
            case 's': strncpy(kSenderShmName, optarg, sizeof(kSenderShmName) - 1); break;
            case 'r': strncpy(kReceiverShmName, optarg, sizeof(kReceiverShmName) - 1); break;
            case 'h': print_help(argv[0]); return 0;
            default:  print_help(argv[0]); return -1;
        }
    }

    if (!g_role_set) {
        fprintf(stderr, "Error: --role A|B is required.\n");
        print_help(argv[0]);
        return -1;
    }

    if (g_msg_size != 64 && g_msg_size != 4096 && g_msg_size != 8192) {
        fprintf(stderr, "Invalid --msg-size %zu. Supported: 64, 4096, 8192\n", g_msg_size);
        return -1;
    }

    if (g_cpu_id < 0) g_cpu_id = (g_role == ROLE_A) ? 4 : 200;
    if (g_role == ROLE_B) g_shm_size_mb = 128;

    const char* role_str = (g_role == ROLE_A) ? "A (交互菜单)" : "B (回显服务器)";
    printf("===== Demo Role %s | cpu_id=%d msg_size=%zu =====\n", role_str, g_cpu_id, g_msg_size);

    // 初始化共享内存
    if (init_ub_shm() != 0) return -1;

    void *sender_shm_base = nullptr;
    if (map_ub_shm(kSenderShmName, sender_shm_base) != 0) return -1;
    void *receiver_shm_base = nullptr;
    if (map_ub_shm(kReceiverShmName, receiver_shm_base) != 0) return -1;

    // 初始化通信队列
    const size_t init_size = 1024 * 1024;
    const size_t min_ring_size = 1900800;
    const size_t ring_size = std::max(min_ring_size, RING_CAPACITY * g_msg_size * 2);

    void* init_area   = sender_shm_base;
    void* ring_area_A = (char*)sender_shm_base + init_size;
    void* ring_area_B = (char*)receiver_shm_base;

    ub_ring_desc_t ring_desc = {static_cast<uint32_t>(RING_CAPACITY),
                                static_cast<uint32_t>(g_msg_size), 1};
    ub_comm_conf_t conf = {
        .cpu_id = g_cpu_id,
        .max_nodes = 2,
        .current_node_id = (g_role == ROLE_A) ? NodeA : NodeB,
        .num_rings = 1,
        .ring_descs = &ring_desc
    };

    ub_shm_area_t init_shm_area;
    init_shm_area.ptr  = init_area;
    init_shm_area.size = init_size;

    ub_ring_region_info_t ring_info[2];
    ring_info[0].node_id = NodeA;
    ring_info[0].region.ptr  = ring_area_A;
    ring_info[0].region.size = ring_size;
    ring_info[1].node_id = NodeB;
    ring_info[1].region.ptr  = ring_area_B;
    ring_info[1].region.size = ring_size;

    ub_ring_region_map_t ring_map{ ring_info, 2 };

    if (ub_comm_queue_init(g_handlep, &init_shm_area, &ring_map, &conf) != 0) {
        fprintf(stderr, "ub_comm_queue_init failed\n");
        return -1;
    }

    printf("  通信队列初始化成功.\n");

    // 等待对端就绪
    uint8_t peer_id = (g_role == ROLE_A) ? NodeB : NodeA;
    printf("  等待对端 (Node %u) 就绪", peer_id);
    for (int i = 0; i < 30; ++i) {
        if (ub_comm_queue_check_ready(g_handlep, peer_id)) {
            printf(" 就绪!\n");
            break;
        }
        printf(".");
        fflush(stdout);
        std::this_thread::sleep_for(1s);
    }
    if (!ub_comm_queue_check_ready(g_handlep, peer_id)) {
        printf(" 未就绪 (可继续，部分功能受限)\n");
    }

    // 角色分支
    if (g_role == ROLE_A) {
        run_interactive();
    } else {
        run_echo_server();
    }

    ub_comm_queue_deinit(g_handlep);
    return 0;
}
