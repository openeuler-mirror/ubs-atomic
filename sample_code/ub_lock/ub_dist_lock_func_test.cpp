#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <new>
#include <numeric>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>

#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "ub_dist_comm_queue.h"
#include "ubs_mem.h"
#include "ubs_mem_def.h"
//
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include "ub_dist_lock.h"
// POSIX
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>

static uint64_t *g_shm_data_ptr = NULL;
static void *g_shm_base_ptr = NULL;
static ubsmem_options_t g_ubsm_opts;

#define LOCK_SLOT_NUM 2
#define ORIGIN_LOCK_SLOT 0
#define REBUILD_LOCK_SLOT 1
#define SHM_NAME "shm_ub_lock"
#define SHM_TOTAL_SIZE (1024 * 1024 * 1024)

static const char *kQueryResultFile = "test.txt";
static const uint16_t kQueryExchangePort = 39091;

static inline ub_rw_lock_t *lock_at(uint8_t *base, size_t stride, int slot)
{
    return reinterpret_cast<ub_rw_lock_t *>(base + (size_t)slot * stride);
}

static inline int slot_index_for_group(bool use_rebuild_group)
{
    return use_rebuild_group ? REBUILD_LOCK_SLOT : ORIGIN_LOCK_SLOT;
}

static inline int flip_slot_index(int active_slot)
{
    return active_slot == ORIGIN_LOCK_SLOT ? REBUILD_LOCK_SLOT : ORIGIN_LOCK_SLOT;
}

static inline std::string trim(std::string s)
{
    auto not_space = [](unsigned char c) {
        return !std::isspace(c);
    };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
    s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
    return s;
}

struct DwConfig {
    std::string self; // NodeA/NodeB/NodeC/NodeD
    int master = 0;
    int nodes = 0;                                    // 2/4
    std::string lock_shm;                             // shm_ub_lock
    std::unordered_map<std::string, std::string> shm; // key: NodeA.. value: shm name
    std::unordered_map<std::string, std::string> ip;  // key: NodeA.. value: fixed ip
};

static bool load_config(const std::string &path, DwConfig &cfg, std::string &err)
{
    std::ifstream in(path);
    if (!in) {
        err = "cannot open config: " + path;
        return false;
    }

    std::string line;
    int lineno = 0;
    while (std::getline(in, line)) {
        lineno++;
        auto pos_hash = line.find('#');
        if (pos_hash != std::string::npos)
            line = line.substr(0, pos_hash);

        line = trim(line);
        if (line.empty())
            continue;

        auto pos_eq = line.find('=');
        if (pos_eq == std::string::npos) {
            err = "bad line (no '=') at " + std::to_string(lineno) + ": " + line;
            return false;
        }

        std::string key = trim(line.substr(0, pos_eq));
        std::string val = trim(line.substr(pos_eq + 1));

        if (key == "self")
            cfg.self = val;
        else if (key == "nodes")
            cfg.nodes = std::stoi(val);
        else if (key == "lock_shm")
            cfg.lock_shm = val;
        else if (key.rfind("shm.", 0) == 0) {
            // key like shm.NodeA
            std::string node = key.substr(4);
            cfg.shm[node] = val;
        } else if (key.rfind("ip.", 0) == 0) {
            std::string node = key.substr(3);
            cfg.ip[node] = val;
        } else {
            //  key
        }
    }

    if (cfg.self.empty()) {
        err = "missing key: self";
        return false;
    }
    if (cfg.nodes != 2 && cfg.nodes != 4) {
        err = "nodes must be 2 or 4";
        return false;
    }
    if (cfg.lock_shm.empty()) {
        err = "missing key: lock_shm";
        return false;
    }

    std::vector<std::string> need = (cfg.nodes == 2) ? std::vector<std::string>{"NodeA", "NodeB"} :
                                                       std::vector<std::string>{"NodeA", "NodeB", "NodeC", "NodeD"};

    for (auto &n : need) {
        if (cfg.shm.find(n) == cfg.shm.end()) {
            err = "missing key: shm." + n;
            return false;
        }
    }
    if (std::find(need.begin(), need.end(), cfg.self) == need.end()) {
        err = "self must be one of configured nodes";
        return false;
    }
    return true;
}

int my_stdout_logger(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    const char *level_str = "UNKNOWN";
    switch (level) {
        case 0:
            level_str = "DEBUG";
            break;
        case 1:
            level_str = "INFO";
            break;
        case 2:
            level_str = "WARN";
            break;
        case 3:
            level_str = "ERROR";
            break;
        case 4:
            level_str = "CRITICAL";
            break;
        default:
            break;
    }

    time_t now = time(0);
    char *time_str = ctime(&now);
    time_str[strlen(time_str) - 1] = '\0';

    if (fprintf(stdout, "[%s] [%s:%u] [%s] %s\n", time_str, file, line, level_str, message) < 0) {
        clearerr(stdout);
    }

    return 0;
}

struct Job {
    uint64_t id;
    std::string op; // lock/query/rebuild command
    uint32_t recover_process_id = 0;
};

static bool parse_recover_target(const std::string &token, uint32_t &process_id)
{
    char *end = nullptr;
    errno = 0;
    unsigned long value = std::strtoul(token.c_str(), &end, 10);
    if (errno != 0 || end == token.c_str() || *end != '\0' || value > UINT32_MAX)
        return false;

    process_id = static_cast<uint32_t>(value);
    return true;
}

static bool parse_line(const std::string &line, std::string &op, uint32_t &recover_process_id)
{
    std::istringstream iss(line);
    if (!(iss >> op))
        return false;

    if (op == "recover") {
        std::string target;
        if (!(iss >> target))
            return false;
        std::string extra;
        if (iss >> extra)
            return false;
        return parse_recover_target(target, recover_process_id);
    }

    std::string extra;
    if (iss >> extra)
        return false;
    return true;
}

inline uint64_t ub_get_tid_u64()
{
    static thread_local const uint64_t tid =
        static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return tid;
}

static ub_location_t make_location(uint8_t node_id)
{
    ub_location_t loc{};
    loc.node_id = node_id;
    loc.tid = static_cast<int32_t>(syscall(SYS_gettid));
    return loc;
}

template <typename T>
constexpr T align_up(T value, std::size_t alignment)
{
    if (alignment == 0) {
        return value;
    }
    return (value + alignment - 1) / alignment * alignment;
}

int init_ub_shm()
{
    ubsmem_options_t ubsm_shmem_opts;
    int ret;

    // Initialize ubsmem attributes
    ret = ubsmem_init_attributes(&ubsm_shmem_opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "Failed to initialize ubsmem attributes!\n");
        return -1;
    }

    // Initialize ubsmem
    ret = ubsmem_initialize(&ubsm_shmem_opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "Failed to initialize ubsmem!\n");
        return -1;
    }

    // Look up node information
    ubsmem_regions_t regions = {0};
    ret = ubsmem_lookup_regions(&regions);
    if (ret != UBSM_OK) {
        fprintf(stderr, "Failed to look up node information!\n");
        return -1;
    }
    return 0;
}

int map_ub_shm(const char *shm_name, void *&addr)
{
    unsigned long length = 1024 * 1024 * 1024;
    int ret;

    ret = ubsmem_shmem_map(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_name, 0, &addr);
    if (ret != 0) {
        fprintf(stderr, "Failed to map shared memory! ret=%d\n", ret);
        return -1;
    }
    if (fprintf(stdout, "addr: %p\n", addr) < 0) {
        clearerr(stdout);
    }

    return 0;
}

static int node_name_to_id(const std::string &self)
{
    if (self == "NodeA")
        return 0;
    if (self == "NodeB")
        return 1;
    if (self == "NodeC")
        return 2;
    if (self == "NodeD")
        return 3;
    return -1;
}

static bool ensure_parent_dir(const std::string &path, std::string &err)
{
    auto pos = path.find_last_of('/');
    if (pos == std::string::npos) {
        return true;
    }
    std::string dir = path.substr(0, pos);
    if (dir.empty()) {
        return true;
    }
    if (mkdir(dir.c_str(), 0755) == 0 || errno == EEXIST) {
        return true;
    }
    err = "mkdir failed for " + dir + ", errno=" + std::to_string(errno);
    return false;
}

static bool write_string_to_file(const std::string &path, const std::string &content, std::string &err)
{
    if (!ensure_parent_dir(path, err)) {
        return false;
    }
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out) {
        err = "cannot open output file: " + path;
        return false;
    }
    out << content;
    if (!out.good()) {
        err = "write output file failed: " + path;
        return false;
    }
    return true;
}

static bool read_file_to_string(const std::string &path, std::string &content, std::string &err)
{
    std::ifstream in(path);
    if (!in) {
        err = "cannot open file: " + path;
        return false;
    }
    std::ostringstream oss;
    oss << in.rdbuf();
    content = oss.str();
    return true;
}

static std::string serialize_query_result_line(const ub_lock_query_result_t &result)
{
    std::ostringstream oss;
    oss << static_cast<unsigned>(result.node_id) << ' ' << static_cast<int>(result.held_mode) << ' '
        << result.holder_tid << ' ' << result.recursive_count << ' ' << static_cast<int>(result.has_shared_ref) << ' '
        << static_cast<int>(result.reserve_mode) << '\n';
    return oss.str();
}

static bool parse_query_result_line(const std::string &line, ub_lock_query_result_t &result)
{
    std::istringstream iss(line);
    unsigned node_id = 0;
    int held_mode = 0;
    int has_shared_ref = 0;
    int reserve_mode = 0;
    result = {};
    if (!(iss >> node_id >> held_mode >> result.holder_tid >> result.recursive_count >> has_shared_ref >>
          reserve_mode)) {
        return false;
    }
    result.node_id = static_cast<uint8_t>(node_id);
    result.held_mode = static_cast<ub_lock_mode_t>(held_mode);
    result.has_shared_ref = (has_shared_ref != 0);
    result.reserve_mode = static_cast<ub_lock_mode_t>(reserve_mode);
    return true;
}

static bool send_all(int fd, const void *buf, size_t len)
{
    const char *ptr = static_cast<const char *>(buf);
    while (len > 0) {
        ssize_t sent = send(fd, ptr, len, 0);
        if (sent <= 0) {
            return false;
        }
        ptr += sent;
        len -= static_cast<size_t>(sent);
    }
    return true;
}

static bool recv_all(int fd, void *buf, size_t len)
{
    char *ptr = static_cast<char *>(buf);
    while (len > 0) {
        ssize_t received = recv(fd, ptr, len, 0);
        if (received <= 0) {
            return false;
        }
        ptr += received;
        len -= static_cast<size_t>(received);
    }
    return true;
}

static bool send_string_payload(int fd, const std::string &payload)
{
    uint32_t len = static_cast<uint32_t>(payload.size());
    uint32_t net_len = htonl(len);
    return send_all(fd, &net_len, sizeof(net_len)) && (len == 0 || send_all(fd, payload.data(), len));
}

static bool recv_string_payload(int fd, std::string &payload)
{
    uint32_t net_len = 0;
    if (!recv_all(fd, &net_len, sizeof(net_len))) {
        return false;
    }
    uint32_t len = ntohl(net_len);
    payload.assign(len, '\0');
    return len == 0 || recv_all(fd, payload.data(), len);
}

static std::string peer_node_name(const DwConfig &cfg)
{
    if (cfg.nodes != 2) {
        return "";
    }
    return cfg.self == "NodeA" ? "NodeB" : "NodeA";
}

static bool exchange_query_payload_with_peer(const DwConfig &cfg, const std::string &local_payload,
                                             std::string &remote_payload, std::string &err)
{
    if (cfg.nodes != 2) {
        err = "query/rebuild sample exchange currently supports 2 nodes only";
        return false;
    }

    const std::string peer_name = peer_node_name(cfg);
    auto peer_it = cfg.ip.find(peer_name);
    if (peer_it == cfg.ip.end() || peer_it->second.empty()) {
        err = "missing fixed ip config for " + peer_name + ", please set ip." + peer_name + " in dw_lock.conf";
        return false;
    }

    if (cfg.self == "NodeA") {
        int server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) {
            err = "socket create failed, errno=" + std::to_string(errno);
            return false;
        }
        int opt = 1;
        (void)setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(kQueryExchangePort);
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
            err = "bind failed, errno=" + std::to_string(errno);
            close(server_fd);
            return false;
        }
        if (listen(server_fd, 1) != 0) {
            err = "listen failed, errno=" + std::to_string(errno);
            close(server_fd);
            return false;
        }

        int conn_fd = accept(server_fd, nullptr, nullptr);
        if (conn_fd < 0) {
            err = "accept failed, errno=" + std::to_string(errno);
            close(server_fd);
            return false;
        }

        const bool ok = recv_string_payload(conn_fd, remote_payload) && send_string_payload(conn_fd, local_payload);
        if (!ok) {
            err = "exchange payload failed on server side";
        }
        close(conn_fd);
        close(server_fd);
        return ok;
    }

    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) {
        err = "socket create failed, errno=" + std::to_string(errno);
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(kQueryExchangePort);
    if (inet_pton(AF_INET, peer_it->second.c_str(), &addr.sin_addr) != 1) {
        err = "invalid peer ip for " + peer_name + ": " + peer_it->second;
        close(client_fd);
        return false;
    }

    bool connected = false;
    for (int retry = 0; retry < 50; ++retry) {
        if (connect(client_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    if (!connected) {
        err = "connect to peer failed: " + peer_it->second + ":" + std::to_string(kQueryExchangePort);
        close(client_fd);
        return false;
    }

    const bool ok = send_string_payload(client_fd, local_payload) && recv_string_payload(client_fd, remote_payload);
    if (!ok) {
        err = "exchange payload failed on client side";
    }
    close(client_fd);
    return ok;
}

static bool build_rebuild_results_from_payloads(const std::string &local_payload, const std::string &remote_payload,
                                                std::vector<ub_lock_query_result_t> &results, std::string &merged_text,
                                                std::string &err)
{
    std::vector<ub_lock_query_result_t> parsed;
    std::set<uint8_t> seen_nodes;
    std::vector<std::string> lines;

    auto consume_payload = [&](const std::string &payload) -> bool {
        std::istringstream iss(payload);
        std::string line;
        while (std::getline(iss, line)) {
            line = trim(line);
            if (line.empty()) {
                continue;
            }
            ub_lock_query_result_t result{};
            if (!parse_query_result_line(line, result)) {
                err = "parse query result line failed: " + line;
                return false;
            }
            if (!seen_nodes.insert(result.node_id).second) {
                err = "duplicate node_id in query results: " + std::to_string(result.node_id);
                return false;
            }
            parsed.push_back(result);
            lines.push_back(line);
        }
        return true;
    };

    if (!consume_payload(local_payload) || !consume_payload(remote_payload)) {
        return false;
    }

    std::sort(parsed.begin(), parsed.end(), [](const ub_lock_query_result_t &lhs, const ub_lock_query_result_t &rhs) {
        return lhs.node_id < rhs.node_id;
    });
    std::sort(lines.begin(), lines.end());

    std::ostringstream oss;
    for (const auto &line : lines) {
        oss << line << '\n';
    }
    merged_text = oss.str();
    results = std::move(parsed);
    return true;
}

int dw_queue_init(const DwConfig &cfg)
{
    const int self_id = node_name_to_id(cfg.self);
    if (self_id < 0 || self_id >= cfg.nodes) {
        std::cerr << "[ERR] bad self: " << cfg.self << "\n";
        return -1;
    }

    // ====== ======
    const size_t kPerHandlerInitRegionSize = 1024 * 1024; // 1KB
    const size_t RING_SIZE = 1376640;

    const size_t unaligned_handler_size = kPerHandlerInitRegionSize + RING_SIZE;
    const size_t HANDLER_SIZE = align_up(unaligned_handler_size, 64);
    std::cout << "Unaligned handler size: " << unaligned_handler_size << ", Aligned handler size: " << HANDLER_SIZE
              << "\n";

    const size_t INIT_OFFSET = 0;
    const size_t RING_OFFSET = kPerHandlerInitRegionSize;

    if (HANDLER_SIZE > SHM_TOTAL_SIZE) {
        std::cerr << "Error: Required size (" << HANDLER_SIZE << ") exceeds SHM_TOTAL_SIZE (" << SHM_TOTAL_SIZE
                  << ")\n";
        return -1;
    }

    // ====== init ub shm ======
    if (init_ub_shm() != 0) {
        std::cerr << " \n";
        return -1;
    }
    std::cout << " \n";

    // ======  nodes  NodeA..NodeD  node_id -> shm======
    // cfg.shm  unordered_map NodeA/B/C/D
    std::array<std::string, 4> shm_name{};
    shm_name[0] = cfg.shm.at("NodeA");
    shm_name[1] = cfg.shm.at("NodeB");
    if (cfg.nodes == 4) {
        shm_name[2] = cfg.shm.at("NodeC");
        shm_name[3] = cfg.shm.at("NodeD");
    }

    // ====== map  shm ======
    std::array<void *, 4> shm_base{};
    for (int nid = 0; nid < cfg.nodes; nid++) {
        void *base = nullptr;
        if (map_ub_shm(shm_name[nid].c_str(), base) != 0) {
            std::cerr << " " << cfg.self << " : node_id=" << nid << ", shm=" << shm_name[nid] << "\n";
            return -1;
        }
        shm_base[nid] = base;
        std::cout << cfg.self << " : node_id=" << nid << ", shm=" << shm_name[nid] << ", base=" << base << "\n";
    }

    // ====== init  NodeA (node_id=0)  shm ======
    void *init_region = (char *)shm_base[0] + INIT_OFFSET;

    // ====== ring  node  ring  shm ======
    std::vector<ub_ring_region_info_t> ring_infos;
    ring_infos.resize(cfg.nodes);
    for (int nid = 0; nid < cfg.nodes; nid++) {
        ring_infos[nid].node_id = (uint8_t)nid;
        ring_infos[nid].region.ptr = (char *)shm_base[nid] + RING_OFFSET;
        ring_infos[nid].region.size = RING_SIZE;

        std::cout << cfg.self << " ring_region: node_id=" << nid << ", addr=" << ring_infos[nid].region.ptr
                  << ", size=" << ring_infos[nid].region.size << "\n";
    }

    ub_ring_region_map_t ring_regions;
    ring_regions.entries = ring_infos.data();
    ring_regions.count = (uint32_t)ring_infos.size();

    // ====== ring desc + confcurrent_node_id = self_id======
    ub_ring_desc_t ring_descs[1];
    ring_descs[0].ring_capacity = 1024;
    ring_descs[0].max_msg_size = 512;
    ring_descs[0].priority = 1;

    ub_comm_conf_t conf;
    conf.max_nodes = (uint8_t)cfg.nodes;
    conf.cpu_id = -1;
    conf.current_node_id = (uint8_t)self_id;
    conf.num_rings = 1;
    conf.ring_descs = ring_descs;

    ub_shm_area_t init_area;
    init_area.ptr = init_region;
    init_area.size = kPerHandlerInitRegionSize;

    ub_shm_comm_t handle = nullptr;
    auto ret = ub_comm_queue_init(&handle, &init_area, &ring_regions, &conf);
    if (ret != 0) {
        std::cerr << "[ERROR] \n";
        return 1;
    }

    std::cout << "...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return 0;
}

static int map_ubsmem_shm(const char *path)
{
    int ret = UBSM_OK;
    int wait_sec = 0;
    void *shm_addr = NULL;

    // Step 1: ubsmem
    ret = ubsmem_init_attributes(&g_ubsm_opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[Error] ubsmem_init_attributes failed (ret: %d)\n", ret);
        return -1;
    }

    ret = ubsmem_initialize(&g_ubsm_opts);
    if (ret != UBSM_OK) {
        fprintf(stderr, "[Error] ubsmem_initialize failed (ret: %d)\n", ret);
        return -1;
    }

    // Step 2: 128MBubsmem
    printf("[Info] Single mode: Map 128MB ubsmem shm %s (size: %lu bytes)\n", path, SHM_TOTAL_SIZE);

    ret = ubsmem_shmem_map(NULL,
                           SHM_TOTAL_SIZE, // 128MB
                           PROT_READ | PROT_WRITE,
                           MAP_SHARED, // ubsmemflagMAP_ANONYMOUS
                           path, 0, &shm_addr);
    if (ret != 0) {
        fprintf(stderr, "[Error] Single mode map 128MB shm failed (ret: %d)\n", ret);
        ubsmem_finalize();
        return -1;
    }
    printf("[Info] Single mode map 128MB shm success, base addr: %p\n", shm_addr);

    // Step 3: uint64_t
    uintptr_t shm_uintptr = (uintptr_t)shm_addr;
    // 8uint64_t
    if (shm_uintptr % alignof(uint64_t) != 0) {
        shm_uintptr += (alignof(uint64_t) - (shm_uintptr % alignof(uint64_t)));
        printf("[Info] Adjust shm addr to align with uint64_t: %p  %p\n", shm_addr, (void *)shm_uintptr);
    }
    g_shm_data_ptr = (uint64_t *)shm_uintptr; //
    g_shm_base_ptr = shm_addr;                // 128MBunmap

    printf("[Info] First aligned uint64_t addr: %p (offset: %lu bytes from base)\n", g_shm_data_ptr,
           (uint64_t)(g_shm_data_ptr - (uint64_t *)g_shm_base_ptr));

    return 0;
}

int main(int argc, char *argv[])
{
    ub_atomic_set_log_level(LOG_LEVEL_INFO);
    ub_atomic_register_log_func(my_stdout_logger);

    //    std::string conf_path = (argc >= 2) ? argv[1] : "dw_lock.conf";
    std::string conf_path = "dw_lock.conf";
    DwConfig cfg;
    std::string err;
    if (!load_config(conf_path, cfg, err)) {
        std::cerr << "[ERROR] load_config failed: " << err << "\n";
        return 2;
    }
    dw_queue_init(cfg);

    std::string lock_shm = cfg.lock_shm;
    if (map_ubsmem_shm(lock_shm.c_str()) != 0) {
        std::cerr << "map_ubsmem_shm failed, shm=" << lock_shm << "\n";
        return -1;
    }

    uint8_t *lock_base_addr = (uint8_t *)(g_shm_data_ptr);
    const size_t stride = align_up(static_cast<size_t>(UB_RW_LOCK_SIZE), 64);
    if ((size_t)LOCK_SLOT_NUM * stride > SHM_TOTAL_SIZE) {
        std::cerr << "[A][ERR] lock shm too small for lock slots\n";
        ubsmem_shmem_unmap(lock_base_addr, SHM_TOTAL_SIZE);
        return 6;
    }

    int g_node_id = node_name_to_id(cfg.self);
    ub_location_t creator{};
    creator.node_id = g_node_id;
    creator.tid = ub_get_tid_u64();

    ub_lock_config_t config{};
    config.lease_time = 60000;
    config.heartbeat_timeout = 500;

    ub_lock_policy_t policy{};
    policy.timeout_ts = 10000;
    policy.allow_delay_release = false;
    policy.recursive = false;
    bool start_from_rebuild_group = false;

    for (int i = 1; i < argc; i++) {
        std::string arg(argv[i]);

        if (arg == "delay=1" || arg == "delay=true") {
            policy.allow_delay_release = true;
        } else if (arg == "delay=0" || arg == "delay=false") {
            policy.allow_delay_release = false;
        } else if (arg == "recursive=1" || arg == "recursive=true") {
            policy.recursive = true;
        } else if (arg == "recursive=0" || arg == "recursive=false") {
            policy.recursive = false;
        } else if (arg == "slot=origin" || arg == "base=origin") {
            start_from_rebuild_group = false;
        } else if (arg == "slot=rebuild" || arg == "base=rebuild") {
            start_from_rebuild_group = true;
        } else {
            std::cout << "[WARN] unknown arg: " << arg << "\n";
        }
    }

    std::cout << "========== Lock Policy ==========\n";
    std::cout << "allow_delay_release = " << policy.allow_delay_release << "\n";
    std::cout << "recursive           = " << policy.recursive << "\n";
    std::cout << "startup_slot_group  = " << (start_from_rebuild_group ? "rebuild" : "origin") << "\n";
    std::cout << "=================================\n";

    const int startup_slot = slot_index_for_group(start_from_rebuild_group);
    ub_rw_lock_t *startup_lock = lock_at(lock_base_addr, stride, startup_slot);
    ub_rw_lock_create(startup_lock, &config, &creator);
    std::cout << "[A] lock_create startup_slot=" << startup_slot << ", addr=" << startup_lock << "\n";

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> job_seq{1};
    std::atomic<int> active_lock_slot{startup_slot};

    int worker_n = 4; //
    std::vector<std::deque<Job>> queues(worker_n);
    std::vector<std::mutex> q_mtx(worker_n);
    std::vector<std::condition_variable> q_cv(worker_n);

    //  owner =  worker
    std::atomic<int> cur_owner{0};

    //  worker  queued / running
    std::vector<std::atomic<int>> queued(worker_n);
    std::vector<std::atomic<int>> running(worker_n);
    for (int i = 0; i < worker_n; i++) {
        queued[i].store(0);
        running[i].store(0);
    }

    auto worker = [&](int wid) {
        ub_location_t loc = make_location((uint8_t)g_node_id); //   wid  owner

        while (!stop.load(std::memory_order_relaxed)) {
            Job jb{};
            {
                std::unique_lock<std::mutex> lk(q_mtx[wid]);
                q_cv[wid].wait(lk, [&] { return stop.load() || !queues[wid].empty(); });
                if (stop.load() && queues[wid].empty())
                    break;

                jb = queues[wid].front();
                queues[wid].pop_front();
                queued[wid].fetch_sub(1, std::memory_order_relaxed);
                running[wid].fetch_add(1, std::memory_order_relaxed);
            }

            int ret = -1;
            const int active_slot = active_lock_slot.load(std::memory_order_acquire);
            ub_rw_lock_t *lkptr = lock_at(lock_base_addr, stride, active_slot);
            const int target_slot = flip_slot_index(active_slot);
            ub_rw_lock_t *new_lkptr = lock_at(lock_base_addr, stride, target_slot);
            if (jb.op == "s+")
                ret = ub_rw_lock_s_lock(lkptr, &policy, &loc);
            else if (jb.op == "s-")
                ret = ub_rw_lock_s_unlock(lkptr, &policy, &loc);
            else if (jb.op == "sx+")
                ret = ub_rw_lock_sx_lock(lkptr, &policy, &loc);
            else if (jb.op == "sx-")
                ret = ub_rw_lock_sx_unlock(lkptr, &policy, &loc);
            else if (jb.op == "x+")
                ret = ub_rw_lock_x_lock(lkptr, &policy, &loc);
            else if (jb.op == "x-")
                ret = ub_rw_lock_x_unlock(lkptr, &policy, &loc);
            else if (jb.op == "recover")
                ret = ub_rw_lock_recover(lkptr, jb.recover_process_id, &loc);
            else if (jb.op == "query") {
                ub_lock_query_result_t result{};
                ret = ub_rw_lock_query_holder(lkptr, &loc, &result);
                if (ret == UB_LOCK_SUCCESS) {
                    std::string err_msg;
                    std::string content = serialize_query_result_line(result);
                    if (!write_string_to_file(kQueryResultFile, content, err_msg)) {
                        std::cout << "[QUERY] write file failed: " << err_msg << "\n";
                        ret = UB_LOCK_ERROR;
                    } else {
                        std::cout << "[QUERY] write result to " << kQueryResultFile << ": " << trim(content) << "\n";
                    }
                }
            } else if (jb.op == "rebuild") {
                std::string err_msg;
                std::string file_content;
                if (!read_file_to_string(kQueryResultFile, file_content, err_msg)) {
                    std::cout << "[REBUILD] read file failed: " << err_msg << "\n";
                    ret = UB_LOCK_ERROR;
                } else {
                    std::vector<ub_lock_query_result_t> results;
                    std::string merged_text;
                    if (!build_rebuild_results_from_payloads(file_content, "", results, merged_text, err_msg)) {
                        std::cout << "[REBUILD] parse merged file failed: " << err_msg << "\n";
                        ret = UB_LOCK_ERROR;
                    } else {
                        if (loc.node_id == 0) {
                            std::memset(new_lkptr, 0, UB_RW_LOCK_SIZE);
                        }
                        ub_lock_rebuild_info_t rebuild_info{};
                        rebuild_info.query_results = results.data();
                        rebuild_info.query_result_count = static_cast<uint32_t>(results.size());
                        ret = ub_rw_lock_rebuild(lkptr, new_lkptr, &rebuild_info, &loc);
                        if (ret == UB_LOCK_SUCCESS) {
                            active_lock_slot.store(target_slot, std::memory_order_release);
                        }
                    }
                }
            } else if (jb.op == "queryrebuild") {
                if (cfg.nodes != 2) {
                    std::cout << "[QUERYREBUILD] sample currently supports 2 nodes only\n";
                    ret = UB_LOCK_ERROR;
                } else {
                    if (loc.node_id == 0) {
                        std::memset(new_lkptr, 0, UB_RW_LOCK_SIZE);
                    }
                    ub_lock_query_result_t result{};
                    ret = ub_rw_lock_query_holder(lkptr, &loc, &result);
                    if (ret == UB_LOCK_SUCCESS) {
                        std::string err_msg;
                        const std::string local_payload = serialize_query_result_line(result);
                        if (!write_string_to_file(kQueryResultFile, local_payload, err_msg)) {
                            std::cout << "[QUERYREBUILD] write local file failed: " << err_msg << "\n";
                            ret = UB_LOCK_ERROR;
                        } else {
                            std::string remote_payload;
                            if (!exchange_query_payload_with_peer(cfg, local_payload, remote_payload, err_msg)) {
                                std::cout << "[QUERYREBUILD] exchange with peer failed: " << err_msg << "\n";
                                ret = UB_LOCK_ERROR;
                            } else {
                                std::vector<ub_lock_query_result_t> results;
                                std::string merged_text;
                                if (!build_rebuild_results_from_payloads(local_payload, remote_payload, results,
                                                                         merged_text, err_msg)) {
                                    std::cout << "[QUERYREBUILD] build rebuild info failed: " << err_msg << "\n";
                                    ret = UB_LOCK_ERROR;
                                } else {
                                    if (!write_string_to_file(kQueryResultFile, merged_text, err_msg)) {
                                        std::cout << "[QUERYREBUILD] write merged file failed: " << err_msg << "\n";
                                        ret = UB_LOCK_ERROR;
                                    } else {
                                        ub_lock_rebuild_info_t rebuild_info{};
                                        rebuild_info.query_results = results.data();
                                        rebuild_info.query_result_count = static_cast<uint32_t>(results.size());
                                        ret = ub_rw_lock_rebuild(lkptr, new_lkptr, &rebuild_info, &loc);
                                        if (ret == UB_LOCK_SUCCESS) {
                                            active_lock_slot.store(target_slot, std::memory_order_release);
                                            std::cout << "[QUERYREBUILD] merged results saved to " << kQueryResultFile
                                                      << "\n";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            std::cout << "[JOB " << jb.id << "]"
                      << " owner=" << wid << " " << jb.op;
            if (jb.op == "recover") {
                std::cout << " " << jb.recover_process_id;
            }
            std::cout << " => "
                      << (ret == UB_LOCK_SUCCESS ? "SUCCESS" : "FAIL")
                      // << ", lock_word=" << lkptr->lock_word
                      << ", node_id=" << (int)loc.node_id << ", tid=" << loc.tid << ", lkptr=" << (void *)lkptr
                      << ", allow_delay_release=" << policy.allow_delay_release << ", recursive=" << policy.recursive
                      << "\n";

            running[wid].fetch_sub(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> workers;
    workers.reserve(worker_n);
    for (int i = 0; i < worker_n; i++)
        workers.emplace_back(worker, i);

    std::cout << "Commands:\n"
              << "  x+ | x- | s+ | s- | sx+ | sx-\n"
              << "  recover <node_id>\n"
              << "  query | rebuild | queryrebuild\n"
              << "  owner 0..3\n"
              << "  jobs\n"
              << "  q / quit\n";

    std::string line;
    while (std::cin.good()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::cout << "ub_lock_cli> ";
        if (!std::getline(std::cin, line))
            break;

        if (line.empty())
            continue;

        std::string op;
        uint32_t recover_process_id = 0;
        if (line.rfind("owner", 0) == 0) { // starts_with "owner"
            std::istringstream iss(line);
            std::string tmp;
            int k = -1;
            iss >> tmp >> k;
            if (k < 0 || k >= worker_n) {
                std::cout << "Bad owner. Use: owner 0.." << (worker_n - 1) << "\n";
                continue;
            }
            cur_owner.store(k, std::memory_order_relaxed);
            std::cout << "[OWNER] now=" << k << "\n";
            continue;
        }

        if (!parse_line(line, op, recover_process_id)) {
            std::cout << "Bad input. Example: x+ or recover 1\n";
            continue;
        }

        if (op == "help") {
            std::cout << "Example: x+ or recover 1\n";
            continue;
        }
        if (op == "jobs") {
            int cur = cur_owner.load(std::memory_order_relaxed);
            std::cout << "cur_owner=" << cur << "\n";
            for (int i = 0; i < worker_n; i++) {
                std::cout << "  owner " << i << ": queued=" << queued[i].load() << ", running=" << running[i].load()
                          << "\n";
            }
            continue;
        }
        if (op == "q" || op == "quit") {
            break;
        }

        Job jb;
        jb.id = job_seq.fetch_add(1, std::memory_order_relaxed);
        jb.op = op;
        jb.recover_process_id = recover_process_id;

        int wid = cur_owner.load(std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lk(q_mtx[wid]);
            queues[wid].push_back(jb);
            queued[wid].fetch_add(1, std::memory_order_relaxed);
        }
        q_cv[wid].notify_one();

        std::cout << "[ENQUEUE " << jb.id << "] owner=" << wid << " " << jb.op;
        if (jb.op == "recover") {
            std::cout << " " << jb.recover_process_id;
        }
        std::cout << "\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    //  worker
    stop.store(true);
    for (int i = 0; i < worker_n; i++)
        q_cv[i].notify_all();
    for (auto &t : workers)
        t.join();

    int active_slot = active_lock_slot.load(std::memory_order_acquire);
    ub_rw_lock_free(lock_at(lock_base_addr, stride, active_slot), &creator);
    std::cout << "[A] lock_free active_slot=" << active_slot << "\n";
    ubsmem_shmem_unmap(g_shm_base_ptr, SHM_TOTAL_SIZE);
    return 0;
}
