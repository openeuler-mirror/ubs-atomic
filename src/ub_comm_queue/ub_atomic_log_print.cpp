#include "ub_atomic_log_print.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <atomic>

static std::atomic<int> g_log_level_threshold{LOG_LEVEL_DEFAULT};

ub_atomic_log_func g_logger_func = NULL;
const char *g_log_level_str[LOG_LEVEL_NUM] = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"};

void register_print_func(ub_atomic_log_func func)
{
    g_logger_func = (func != NULL) ? func : log_no_print;
}

// 空的日志处理函数，当未注册任何函数时使用
int log_no_print(int level, const char *file, const char *func, uint32_t line, const char *message)
{
    (void)level;
    (void)file;
    (void)func;
    (void)line;
    (void)message;
    return 0;
}

int get_log_level_threshold(void)
{
    return g_log_level_threshold.load(std::memory_order_relaxed);
}

int set_log_level_threshold(int level)
{
    if (level >= LOG_LEVEL_DEBUG && level <= LOG_LEVEL_CRITICAL) {
        g_log_level_threshold.store(level, std::memory_order_relaxed);
        return 0;
    }

    return -1;
}

// 内部日志打印函数，接收格式化字符串和可变参数
void log_print(int level, const char *file, const char *func, uint32_t line, const char *message, ...)
{
    if (!g_logger_func) {
        g_logger_func = log_no_print;
    }

    if (level < 0 || level >= LOG_LEVEL_NUM || level < get_log_level_threshold()) {
        return;
    }

    // 使用可变参数列表构建最终的消息字符串
    va_list args;
    va_start(args, message);

    char formatted_message[MAX_LOG_MESSAGE_LENGTH]{};
    int written_len = vsnprintf(formatted_message, sizeof(formatted_message), message, args);

    if (written_len < 0) {
        const char *error_msg = "Error formatting log message.";
        g_logger_func(LOG_LEVEL_ERROR, file, func, line, error_msg);
        va_end(args);
        return;
    }

    va_end(args);

    // 调用用户注册的日志处理函数
    g_logger_func(level, file, func, line, formatted_message);
}