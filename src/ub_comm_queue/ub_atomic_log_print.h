/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

 * rmrs is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef UB_ATOMIC_LOG_PRINT_H
#define UB_ATOMIC_LOG_PRINT_H

#include <stdint.h>
#include <cstdlib>
#include <cstring>
#include "ub_dist_comm_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

#define LOG_LEVEL_NONE 5                  // 用于过滤，高于此级别的日志不输出
#define LOG_LEVEL_NUM 5                   // 日志级别数量
#define LOG_LEVEL_DEFAULT LOG_LEVEL_ERROR // 默认日志级别
#define LOG_LEVEL_INVALID -1              // 日志级别无效
#define MAX_LOG_MESSAGE_LENGTH 2048       // 缓冲区大小

#ifndef FILENAME
#define FILENAME (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#endif

int get_log_level_threshold(void);
int set_log_level_threshold(int level);

#define ATOMIC_LOG(level, msg, ...)                                                 \
    do {                                                                            \
        if ((level) >= get_log_level_threshold()) {                                 \
            log_print((level), FILENAME, __func__, __LINE__, (msg), ##__VA_ARGS__); \
        }                                                                           \
    } while (0)

// 全局的日志处理函数指针，初始指向一个空函数
extern ub_atomic_log_func g_logger_func;

// 日志级别对应的字符串数组
extern const char *g_log_level_str[LOG_LEVEL_NUM];

void register_print_func(ub_atomic_log_func func);

int log_no_print(int level, const char *file, const char *func, uint32_t line, const char *message);

void log_print(int level, const char *file, const char *func, uint32_t line, const char *message, ...);

#ifdef __cplusplus
}
#endif

#endif // UB_ATOMIC_LOG_PRINT_H