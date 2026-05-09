file(GLOB_RECURSE UB_LOCK_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(ub_lock STATIC ${UB_LOCK_SRCS})

target_include_directories(ub_lock PUBLIC
    ${PROJECT_SOURCE_DIR}/include/ub_comm_queue
    ${PROJECT_SOURCE_DIR}/src/common
    ${PROJECT_SOURCE_DIR}/src/ub_comm_queue
)
