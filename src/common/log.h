/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
*/

#ifndef LOG_H
#define LOG_H
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

// 日志等级枚举
enum class LogLevel
{
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL
};

inline LogLevel GetLogMinLevel()
{
    // 局部静态原子变量：懒加载（首次调用初始化）+ 单例（全程唯一）+ 多线程安全（C++11后静态变量初始化是原子的）
    static std::atomic<LogLevel> log_min_level = []() -> LogLevel {
        // 步骤1：读取环境变量SO_LOG_LEVEL，无则返回默认ERROR
        const char *env_level = getenv("UB_ATOMIC_LOG_LEVEL");
        if (env_level == nullptr) {
            return LogLevel::ERROR;
        }

        // 步骤2：解析环境变量值——支持字符串（DEBUG/INFO/WARN/ERROR/FATAL）和数字（0/1/2/3/4）
        if (strcmp(env_level, "DEBUG") == 0 || strcmp(env_level, "0") == 0) {
            return LogLevel::DEBUG;
        } else if (strcmp(env_level, "INFO") == 0 || strcmp(env_level, "1") == 0) {
            return LogLevel::INFO;
        } else if (strcmp(env_level, "WARN") == 0 || strcmp(env_level, "2") == 0) {
            return LogLevel::WARN;
        } else if (strcmp(env_level, "ERROR") == 0 || strcmp(env_level, "3") == 0) {
            return LogLevel::ERROR;
        } else if (strcmp(env_level, "FATAL") == 0 || strcmp(env_level, "4") == 0) {
            return LogLevel::FATAL;
        } else if (strcmp(env_level, "NONE") == 0 || strcmp(env_level, "5") == 0) {
            // 扩展：NONE/5表示全关闭日志（所有级别都不输出）
            return static_cast<LogLevel>(5);
        } else {
            // 解析失败，默认ERROR
            return LogLevel::ERROR;
        }
    }();

    // 步骤3：返回阈值，使用宽松内存序，原子读开销接近普通变量
    return log_min_level.load(std::memory_order_relaxed);
}

inline bool IsLogLevelEnable(LogLevel level)
{
    return static_cast<int>(level) >= static_cast<int>(GetLogMinLevel());
}

// 将日志等级转换为字符串
inline const char *LogLevelToString(LogLevel level)
{
    switch (level) {
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::WARN:
            return "WARN";
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::FATAL:
            return "FATAL";
        default:
            return "UNKNOWN";
    }
}

// 自动获取格式化时间（精确到纳秒）
inline std::string GetCurrentTime()
{
    // 1. 获取当前系统时间（精确到纳秒）
    auto now = std::chrono::system_clock::now();
    // 2. 计算从epoch到现在的总纳秒数
    auto ns_total = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    // 3. 拆分出秒数（用于格式化日期）和剩余纳秒数（0~999999999）
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(ns_total);
    auto ns_remain = ns_total - secs;

    // 4. 格式化秒级日期时间（年/月/日/时/分/秒）
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm local_tm;
    localtime_r(&t, &local_tm); // Linux/macOS线程安全；Windows替换为localtime_s

    // 5. 拼接纳秒级时间字符串（缓冲区扩容到40字节，纳秒占9位）
    char time_buf[40]{};
    int ret = std::snprintf(time_buf, sizeof(time_buf), "%04d-%02d-%02d %02d:%02d:%02d.%09d",
                            local_tm.tm_year + 1900,              // 年（tm_year是从1900开始的偏移）
                            local_tm.tm_mon + 1,                  // 月（tm_mon从0开始）
                            local_tm.tm_mday,                     // 日
                            local_tm.tm_hour,                     // 时
                            local_tm.tm_min,                      // 分
                            local_tm.tm_sec,                      // 秒
                            static_cast<int>(ns_remain.count())); // 纳秒（9位）
    if (ret < 0) {
        return "0000-00-00 00:00:00.000000000";
    }
    time_buf[sizeof(time_buf) - 1] = '\0';
    return time_buf;
}

// 自动获取进程ID
inline std::string GetPid()
{
    return std::to_string(getpid()); // Windows替换为GetProcessId(GetCurrentProcess())
}

// 自动获取线程ID
inline std::string GetTid()
{
    std::ostringstream oss;
    oss << std::this_thread::get_id(); // Windows也可改用GetCurrentThreadId()
    return oss.str();
}

// 核心日志流类：支持流式输出，析构时自动输出日志
class LogStream {
public:
    // 构造函数：自动接收上下文信息
    LogStream(LogLevel level, const char *file, int line, const char *func)
        : level_(level),
          file_(file),
          line_(line),
          func_(func)
    {
    }

    // 析构函数：自动拼接所有信息并输出
    ~LogStream()
    {
        if (!IsLogLevelEnable(level_))
            return;
        // 拼接日志头部（时间、进程、线程、等级、文件、行号、函数）
        std::string header = "[" + GetCurrentTime() + "][" + LogLevelToString(level_) + "][" + GetPid() + "][" +
                             GetTid() + "][" + func_ + ":" + std::to_string(line_) + "] ";

        // 输出完整日志（ss.str()是日志体内容）
        std::cout << header << ss_.str() << std::endl;

        // FATAL级别退出程序
        if (level_ == LogLevel::FATAL) {
            exit(EXIT_FAILURE);
        }
    }

    // 重载uint8_t，
    LogStream &operator<<(uint8_t value)
    {
        if (!IsLogLevelEnable(level_))
            return *this;
        ss_ << static_cast<uint32_t>(value);
        return *this;
    }

    // 重载int8_t，
    LogStream &operator<<(int8_t value)
    {
        if (!IsLogLevelEnable(level_))
            return *this;
        ss_ << static_cast<int32_t>(value);
        return *this;
    }

    // 重载<<运算符，支持任意类型的流式输入
    template <typename T>
    LogStream &operator<<(const T &value)
    {
        if (!IsLogLevelEnable(level_))
            return *this;
        ss_ << value;
        return *this;
    }

    // 兼容std::endl等操纵符
    LogStream &operator<<(std::ostream &(*manip)(std::ostream &))
    {
        if (!IsLogLevelEnable(level_))
            return *this;
        manip(ss_);
        return *this;
    }

private:
    LogLevel level_;        // 日志等级
    const char *file_;      // 文件名
    int line_;              // 行号
    const char *func_;      // 函数名
    std::ostringstream ss_; // 日志体内容缓冲区
};

// 对外暴露的流式日志宏（核心）
// 用法：LOG_INFO << "hello " << name << " age: " << 18 << endl;
#define LOG(level) LogStream(level, __FILE__, __LINE__, __FUNCTION__)

// 各等级的简化宏
#define LOG_DEBUG LOG(LogLevel::DEBUG)
#define LOG_INFO LOG(LogLevel::INFO)
#define LOG_WARN LOG(LogLevel::WARN)
#define LOG_ERROR LOG(LogLevel::ERROR)
#define LOG_FATAL LOG(LogLevel::FATAL)

#endif
