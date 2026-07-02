#include "ubs_mem.h"
#include "ubs_mem_def.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace {

constexpr const char *kConfigPath = "./ubsm_region.conf";
constexpr unsigned long kBytesPerMb = 1024UL * 1024UL;
constexpr int kShmPermission = 0777;
constexpr size_t kShmNameMax = 256;

std::array<char, MAX_REGION_NAME_DESC_LENGTH> g_region_name{};
std::array<char, kShmNameMax> g_shm_name{};

struct RegionConfig {
    size_t request_size_mb = 0;
    std::vector<std::string> hosts;
};

template <size_t N>
bool fill_api_buffer(const std::string &value, std::array<char, N> &buffer, const char *name)
{
    if (value.empty() || value.size() >= N) {
        std::cerr << name << " is empty or too long: " << value << "\n";
        return false;
    }
    buffer.fill('\0');
    std::memcpy(buffer.data(), value.c_str(), value.size());
    return true;
}

std::string trim(const std::string &value)
{
    auto begin = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) { return std::isspace(ch); });
    auto end = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) { return std::isspace(ch); }).base();
    if (begin >= end) {
        return {};
    }
    return std::string(begin, end);
}

bool parse_size_mb(const std::string &value, size_t &size_mb)
{
    char *end = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (errno != 0 || end == value.c_str() || *end != '\0' || parsed == 0) {
        return false;
    }
    if (parsed > static_cast<unsigned long long>(std::numeric_limits<size_t>::max())) {
        return false;
    }
    size_mb = static_cast<size_t>(parsed);
    return true;
}

bool parse_hosts_list(const std::string &value, std::vector<std::string> &hosts)
{
    std::stringstream ss(value);
    std::string token;
    hosts.clear();

    while (std::getline(ss, token, ',')) {
        std::string host = trim(token);
        if (host.empty()) {
            continue;
        }
        if (hosts.size() >= MAX_REGION_NODE_NUM) {
            std::cerr << "too many hosts, max=" << MAX_REGION_NODE_NUM << "\n";
            return false;
        }
        if (host.size() >= MAX_HOST_NAME_DESC_LENGTH) {
            std::cerr << "host name is too long: " << host << "\n";
            return false;
        }
        hosts.push_back(host);
    }

    return !hosts.empty();
}

int load_region_config(const std::string &path, RegionConfig &cfg)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "open config failed: " << path << ", errno=" << errno << "(" << std::strerror(errno) << ")\n";
        return -1;
    }

    bool got_size = false;
    bool got_hosts = false;
    std::string line;

    while (std::getline(file, line)) {
        std::string item = trim(line);
        if (item.empty() || item.front() == '#') {
            continue;
        }

        const size_t eq_pos = item.find('=');
        if (eq_pos == std::string::npos) {
            continue;
        }

        const std::string key = trim(item.substr(0, eq_pos));
        const std::string val = trim(item.substr(eq_pos + 1));

        if (key == "request_size_mb") {
            if (!parse_size_mb(val, cfg.request_size_mb)) {
                std::cerr << "invalid request_size_mb: " << val << "\n";
                return -2;
            }
            got_size = true;
        } else if (key == "hosts") {
            if (!parse_hosts_list(val, cfg.hosts)) {
                std::cerr << "invalid hosts list: " << val << "\n";
                return -3;
            }
            got_hosts = true;
        }
    }

    if (!got_size) {
        std::cerr << "missing request_size_mb in config\n";
        return -4;
    }
    if (!got_hosts) {
        std::cerr << "missing hosts in config\n";
        return -5;
    }
    return 0;
}

bool has_host(const RegionConfig &cfg, const std::string &target_hostname)
{
    return std::find(cfg.hosts.begin(), cfg.hosts.end(), target_hostname) != cfg.hosts.end();
}

void print_configured_hosts(const RegionConfig &cfg)
{
    std::cout << "Configured hosts:\n";
    for (const auto &host : cfg.hosts) {
        std::cout << "  - " << host << "\n";
    }
}

int copy_host_name(const std::string &host, ubsmem_region_attributes_t &attr, size_t index)
{
    int written = std::snprintf(attr.hosts[index].host_name, sizeof(attr.hosts[index].host_name), "%s", host.c_str());
    if (written < 0 || static_cast<size_t>(written) >= sizeof(attr.hosts[index].host_name)) {
        std::cerr << "host name copy failed: " << host << "\n";
        return -1;
    }
    return 0;
}

int build_region_attr_from_config(const RegionConfig &cfg, const std::string &target_hostname,
                                  ubsmem_region_attributes_t &attr)
{
    std::memset(&attr, 0, sizeof(attr));
    if (cfg.hosts.empty() || target_hostname.empty() || !has_host(cfg, target_hostname)) {
        return -1;
    }

    attr.host_num = static_cast<int>(cfg.hosts.size());
    for (size_t i = 0; i < cfg.hosts.size(); ++i) {
        if (copy_host_name(cfg.hosts[i], attr, i) != 0) {
            return -1;
        }
        attr.hosts[i].affinity = (cfg.hosts[i] == target_hostname);
    }
    return 0;
}

int init_ubsm()
{
    ubsmem_options_t options;
    std::memset(&options, 0, sizeof(options));

    int ret = ubsmem_init_attributes(&options);
    if (ret != UBSM_OK) {
        std::cerr << "Failed to initialize ubsmem attributes! ret=" << ret << "\n";
        return -1;
    }

    ret = ubsmem_initialize(&options);
    if (ret != UBSM_OK) {
        std::cerr << "Failed to initialize ubsmem! ret=" << ret << "\n";
        return -1;
    }
    return 0;
}

int prepare_region_name(const RegionConfig &cfg, const std::string &target_hostname)
{
    if (!has_host(cfg, target_hostname)) {
        std::cout << "ERROR: target_hostname '" << target_hostname << "' not in hosts list\n";
        print_configured_hosts(cfg);
        return -1;
    }

    const std::string region_name = target_hostname + "_affinity";
    if (!fill_api_buffer(region_name, g_region_name, "region name")) {
        return -1;
    }
    std::cout << "\nThe region_name that we need is " << g_region_name.data() << ".\n";
    return 0;
}

int create_shared_region(const RegionConfig &cfg, const std::string &target_hostname)
{
    int ret = prepare_region_name(cfg, target_hostname);
    if (ret != 0) {
        return ret;
    }

    ubsmem_region_desc_t desc;
    std::memset(&desc, 0, sizeof(desc));
    ret = ubsmem_lookup_region(g_region_name.data(), &desc);
    if (ret == 0) {
        return 0;
    }

    std::cout << "ubsmem_lookup_region failed: ret=" << ret << ", name=" << g_region_name.data()
              << ", now attempt to create it\n";

    ubsmem_region_attributes_t attr;
    ret = build_region_attr_from_config(cfg, target_hostname, attr);
    if (ret != 0) {
        std::cerr << "build_region_attr_from_config failed: ret=" << ret << "\n";
        return ret;
    }

    size_t region_size = 0;
    ret = ubsmem_create_region(g_region_name.data(), region_size, &attr);
    if (ret != 0) {
        std::cerr << "ubsmem_create_region failed: ret=" << ret << ", name=" << g_region_name.data()
                  << ", size=" << region_size << "\n";
        return ret;
    }

    std::cout << "======ubsmem_create_region successfully: name=" << g_region_name.data() << ", size=" << region_size
              << "======\n";
    return 0;
}

int create_shared_memory(size_t request_size_mb)
{
    if (request_size_mb > std::numeric_limits<unsigned long>::max() / kBytesPerMb) {
        std::cerr << "request_size_mb is too large: " << request_size_mb << "\n";
        return -1;
    }

    unsigned long length = static_cast<unsigned long>(request_size_mb) * kBytesPerMb;
    int flags = UBSM_FLAG_ONLY_IMPORT_NONCACHE | UBSM_FLAG_WR_DELAY_COMP;
    int ret = ubsmem_shmem_allocate(g_region_name.data(), g_shm_name.data(), length, kShmPermission, flags);
    if (ret != 0) {
        std::cerr << "Failed to allocate shared memory! ret=" << ret << "\n";
        return -1;
    }
    return 0;
}

int delete_shared_memory()
{
    int ret = ubsmem_shmem_deallocate(g_shm_name.data());
    if (ret != 0) {
        std::cerr << "Failed to deallocate shared memory! ret=" << ret << "\n";
        return -1;
    }
    return 0;
}

void print_usage(const char *program)
{
    std::cerr << "Usage: " << program << " <create|delete> <expected_export_hostname> <shared_memory_name>\n"
              << "Examples:\n"
              << "  Create shared memory: " << program << " create computer01 shm_ub_lock\n"
              << "  Delete shared memory: " << program << " delete computer01 shm_ub_lock\n";
}

} // namespace

int main(int argc, char *argv[])
{
    if (argc != 4) {
        std::cerr << "Error: Incorrect number of parameters!\n";
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    const std::string operation = argv[1];
    const std::string target_hostname = argv[2];
    const std::string shm_name = argv[3];

    if (operation != "create" && operation != "delete") {
        std::cerr << "Error: Invalid operation type! Only 'create' or 'delete' is supported\n";
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }
    if (!fill_api_buffer(shm_name, g_shm_name, "shared memory name")) {
        return EXIT_FAILURE;
    }

    RegionConfig cfg;
    int ret = load_region_config(kConfigPath, cfg);
    if (ret != 0) {
        std::cerr << "load_region_config failed: ret=" << ret << "\n";
        return EXIT_FAILURE;
    }

    ret = init_ubsm();
    if (ret != 0) {
        std::cerr << "init_ubsm failed!\n";
        return EXIT_FAILURE;
    }

    ret = create_shared_region(cfg, target_hostname);
    if (ret != 0) {
        std::cerr << "create_shared_region failed! ret=" << ret << "\n";
        return EXIT_FAILURE;
    }

    if (operation == "delete") {
        ret = delete_shared_memory();
        if (ret == 0) {
            std::cout << "Shared memory deleted successfully!\n";
            return EXIT_SUCCESS;
        }
        std::cerr << "Failed to delete shared memory!\n";
        return EXIT_FAILURE;
    }

    std::cout << "Start creating shared memory: name=" << g_shm_name.data() << ", size=" << cfg.request_size_mb
              << "MB, region=" << g_region_name.data() << "\n";
    ret = create_shared_memory(cfg.request_size_mb);
    if (ret == 0) {
        std::cout << "Shared memory created successfully!\n";
        return EXIT_SUCCESS;
    }
    std::cerr << "Failed to create shared memory!\n";
    return EXIT_FAILURE;
}
