# 收集目录下的源文件
file(GLOB_RECURSE UBS_ATOMIC_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ub_comm_queue/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ub_lock/*.cpp)

# 创建共享库
add_library(ubs_atomic SHARED
    ${UBS_ATOMIC_SRCS}
)

# 设置 libubs-atomic.so 的输出目录
set_target_properties(ubs_atomic PROPERTIES
    OUTPUT_NAME ubs-atomic
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    POSITION_INDEPENDENT_CODE ON  # 确保生成的共享库支持 PIE
)

# 为 libubs-atomic.so 设置 include 目录
target_include_directories(ubs_atomic PUBLIC
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/common
)

if (UB_COMM_QUEUE_ENABLE_HALF_WRITE_INJECT)
    target_compile_definitions(ubs_atomic PRIVATE UB_COMM_QUEUE_ENABLE_HALF_WRITE_INJECT)
endif ()

set(BOUNDSCHECK_LIB /usr/lib64/libboundscheck.so)

# 链接库到可执行文件
target_link_libraries(ubs_atomic PRIVATE
    ${BOUNDSCHECK_LIB}
    ub_comm_queue
    ub_lock
)

# 链接库到可执行文件
# 添加子目录
add_subdirectory(ub_comm_queue)
add_subdirectory(ub_lock)
