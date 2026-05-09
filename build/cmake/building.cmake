if(NOT DEFINED CMAKE_BUILD_TYPE OR x${CMAKE_BUILD_TYPE} STREQUAL "x")
    set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
endif()

set(SECURE_BUILD_FLAGS
        -fstack-protector-strong    # 防止栈溢出攻击
        -fPIE                       # 生成位置无关可执行文件
        -fPIC                       # 生成位置无关代码
)

set(BASE_BUILD_FLAGS
        -Wall                       # 启用大多数警告
        -Wextra                     # 启用额外警告
#        -Wpedantic                 # 启用严格警告
        -Wconversion                # 类型转换警告
        -Wshadow=local              # 变量隐藏警告
        -Wfloat-equal               # 浮点数相等比较警告
        -fsigned-char               # char 默认为 signed char
        -fno-common                 # 全局变量同名警告
)

set(DEBUG_FLAGS ${BASE_BUILD_FLAGS} ${SECURE_BUILD_FLAGS}
        -O0                         # 不进行优化
        -ggdb3                      # 生成最详细的调试信息
        -fno-omit-frame-pointer     # 保留帧指针，便于调试时的堆栈跟踪
)
set(RELEASE_FLAGS ${BASE_BUILD_FLAGS} ${SECURE_BUILD_FLAGS}
        -D_FORTIFY_SOURCE=2         # 启用源代码强化（fortification）级别 2
)

# 检测内存错误
option(ASAN_BUILD "OFF")
if(ASAN_BUILD STREQUAL "ON")
    add_link_options(-fsanitize=address)
    message(STATUS "AddressSanitizer (ASAN) is enabled.")
endif()

# 检测内存泄漏
option(LSAN_BUILD "OFF")
if(LSAN_BUILD STREQUAL "ON")
    add_link_options(-fsanitize=leak)
    message(STATUS "LeakSanitizer (LSAN) is enabled.")
endif()

# 检测多线程问题
option(TSAN_BUILD "OFF")
if(TSAN_BUILD STREQUAL "ON")
    add_link_options(-fsanitize=thread)
    message(STATUS "ThreadSanitizer (TSAN) is enabled.")
endif()

# 检测未定义行为
option(UBSAN_BUILD "OFF")
if(UBSAN_BUILD STREQUAL "ON")
    add_link_options(-fsanitize=undefined)
    message(STATUS "UndefinedBehaviorSanitizer (UBSAN) is enabled.")
endif()