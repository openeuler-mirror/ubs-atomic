# 收集目录下的源文件
file(GLOB_RECURSE TURBO_TDSQL_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/ub_comm_queue/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ub_lock/*.cpp)

# 创建共享库
add_library(ubturbo_tdsql SHARED 
    ${TURBO_TDSQL_SRCS}
)

# 设置 libubturbo_tdsql.so 的输出目录
set_target_properties(ubturbo_tdsql PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    POSITION_INDEPENDENT_CODE ON  # 确保生成的共享库支持 PIE
)

# 为 libubturbo_tdsql.so 设置 include 目录
target_include_directories(ubturbo_tdsql PUBLIC
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/common
)

set(BOUNDSCHECK_LIB /usr/lib64/libboundscheck.so)

# 链接库到可执行文件
target_link_libraries(ubturbo_tdsql PRIVATE
    ${BOUNDSCHECK_LIB}
    ub_comm_queue
    ub_lock
)

# 链接库到可执行文件
# 添加子目录
add_subdirectory(ub_comm_queue)
add_subdirectory(ub_lock)