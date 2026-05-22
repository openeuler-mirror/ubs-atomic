file(GLOB_RECURSE UB_COMM_QUEUE_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(ub_comm_queue STATIC ${UB_COMM_QUEUE_SRCS})

target_include_directories(ub_comm_queue PUBLIC
    ${PROJECT_SOURCE_DIR}/include/
    ${PROJECT_SOURCE_DIR}/src/common
)
