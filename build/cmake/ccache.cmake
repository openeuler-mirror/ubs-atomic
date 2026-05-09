# 查找 ccache 可执行文件
find_program(CCACHE_PROGRAM ccache)

# 如果找到了 ccache，则配置 CMake 使用 ccache
if(CCACHE_PROGRAM)
    # 设置 CCache 作为 C 编译器的前缀
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_PROGRAM})

    # 设置 CCache 作为 C++ 编译器的前缀
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
endif()