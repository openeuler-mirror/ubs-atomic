cmake_minimum_required(VERSION 3.13)
project(ubs_atomic_ut  VERSION 1.0 LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
message(STATUS, "in test dir...")

add_compile_options(-Wno-pmf-conversions)

set(UBS_ATOMIC_ROOT_DIR ${CMAKE_CURRENT_LIST_DIR}/../)
add_compile_options(-fPIC)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(3rdparty)

set(3RDPARTY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../3rdparty)
set(BOUNDSCHECK_LIB /usr/lib64/libboundscheck.so)

# 全局作用域的头文件定义，不依赖具体模块
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/include              # 外部共享头文件
        ${CMAKE_BINARY_DIR}/include     # 自动生成头文件
)

# 添加测试cpp文件，这里按照模块划分
# aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/testcase TESTCASE_DEMO_SRC)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/testcase/ub_lock TESTCASE_UBLOCK_SRC)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/testcase/ub_comm_queue TESTCASE_UB_COMM_QUEUE_SRC)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/testcase/ub_dist_tx_res TESTCASE_UB_DIST_TX_RES_SRC)

set(TESTCASE_SRC 
        # ${TESTCASE_DEMO_SRC}
        ${TESTCASE_UBLOCK_SRC}
        ${TESTCASE_UB_COMM_QUEUE_SRC}
        ${TESTCASE_UB_DIST_TX_RES_SRC})

# 需要的代码源文件写这里，按照模块划分


# file(GLOB UBS_ATOMIC_DEMO_FILES ${UBS_ATOMIC_ROOT_DIR}/src/*.cpp)
file(GLOB UBS_ATOMIC_UBLOCK_FILES ${UBS_ATOMIC_ROOT_DIR}/src/ub_lock/*.cpp)
file(GLOB UBS_ATOMIC_UBCOMM_QUEUE_FILES ${UBS_ATOMIC_ROOT_DIR}/src/ub_comm_queue/*.cpp)
file(GLOB UBS_ATOMIC_UB_DIST_TX_RES_FILES ${UBS_ATOMIC_ROOT_DIR}/src/ub_dist_tx_res/*.cpp)

set(UBS_ATOMIC_SRC 
        # ${UBS_ATOMIC_DEMO_FILES}
        ${UBS_ATOMIC_UBLOCK_FILES}
        ${UBS_ATOMIC_UBCOMM_QUEUE_FILES}
        ${UBS_ATOMIC_UB_DIST_TX_RES_FILES})

# MOCK文件加这里，目前将mock目录下递归添加所有cpp，应该只用加文件，不用修改这里
file(GLOB_RECURSE TESTCASE_MOCK_SRC ${CMAKE_CURRENT_SOURCE_DIR}/mock/*.cpp)

# 添加可执行文件
set(TEST_SRC_UBS_ATOMIC ${UBS_ATOMIC_SRC} ${TESTCASE_SRC} ${TESTCASE_MOCK_SRC})
add_executable(ubs_atomic_ut ${TEST_SRC_UBS_ATOMIC})

target_compile_options(ubs_atomic_ut PRIVATE
		-O0 -g -fprofile-arcs -ftest-coverage)

target_link_options(ubs_atomic_ut PRIVATE
		-lgcov --coverage)

TARGET_MOCKCPP(ubs_atomic_ut)
TARGET_GTEST(ubs_atomic_ut)

# 需要的头文件写这里面
target_include_directories(ubs_atomic_ut BEFORE PUBLIC
	${UBS_ATOMIC_ROOT_DIR}/include
	${UBS_ATOMIC_ROOT_DIR}/src/common
	${UBS_ATOMIC_ROOT_DIR}/src/ub_comm_queue
	${UBS_ATOMIC_ROOT_DIR}/src/ub_lock)

# 传到源码、测试代码、打桩代码中的宏
target_compile_definitions(ubs_atomic_ut PUBLIC DT_CONFIG
)

target_link_libraries(ubs_atomic_ut
      -Wl,--start-group
      pthread dl
      -Wl,--end-group
)

find_library(BOUNDSCHECK_LIB boundscheck)
if(BOUNDSCHECK_LIB)
    target_link_libraries(ubs_atomic_ut ${BOUNDSCHECK_LIB})
else()
    message(FATAL_ERROR "Not found boundscheck")
endif()

# print compile and link flags
get_target_property(COMPILE_FLAGS ubs_atomic_ut COMPILE_OPTIONS)
get_target_property(LINK_FLAGS ubs_atomic_ut LINK_OPTIONS)
message(STATUS "Compiler id: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "Compile flags: ${COMPILE_FLAGS}")
message(STATUS "Link flags: ${LINK_FLAGS}")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")