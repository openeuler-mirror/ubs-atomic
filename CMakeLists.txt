cmake_minimum_required(VERSION 3.22)

set(UBS_ATOMIC_VERSION "1.0.0" CACHE STRING "UBS atomic package version")
set(UBS_ATOMIC_PACKAGE_RELEASE "1" CACHE STRING "UBS atomic RPM package release")
project(ubs-atomic VERSION ${UBS_ATOMIC_VERSION} LANGUAGES C CXX)


# 默认使用 C11
set(CMAKE_C_STANDARD_DEFAULT 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS NO)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# 默认使用 C++17
set(CMAKE_CXX_STANDARD_DEFAULT 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS NO)

message(STATUS "CMAKE_CXX_STANDARD: c++${CMAKE_CXX_STANDARD}")

set(SRC_DIR ${PROJECT_SOURCE_DIR}/src)
set(3RDPARTY_DIR ${PROJECT_SOURCE_DIR}/3rdparty)

# 安全编译选项控制
set(UBS_ATOMIC_COMMON_FLAGS "")
set(SECURE_LD_FLAGS "-Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now -pie -Wl,--build-id=none")
set(SECURE_C_FLAGS "-fstack-protector-strong")
# LSE 原子指令集编译选项。仅 ARM64 编译器支持，x86 本地编译时不能传入。
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64|ARM64)$")
    set(MARCH_FLAGS "-march=armv8-a+lse")
else()
    set(MARCH_FLAGS "")
endif()
set(COMMON_FLAGS "-fno-omit-frame-pointer ${SECURE_C_FLAGS} ${SECURE_LD_FLAGS} ${UBS_ATOMIC_COMMON_FLAGS} ${MARCH_FLAGS}")

if (CMAKE_BUILD_TYPE MATCHES "^[Dd]ebug")
    set(DEBUG_FLAGS "-O0 -ggdb3 -fPIE")
    set(CMAKE_CXX_FLAGS_DEBUG "${COMMON_FLAGS} ${DEBUG_FLAGS} -lrt")
else()
    set(RELEASE_FLAGS "-O2 -D_FORTIFY_SOURCE=2 -DNDEBUG=1 -fPIE -fPIC -s")
    set(CMAKE_CXX_FLAGS_RELEASE "${COMMON_FLAGS} ${RELEASE_FLAGS} -lrt -lpthread")
endif()


# 设置可执行文件和库的输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# 禁用 RPATH
set(CMAKE_SKIP_RPATH ON)

include(GNUInstallDirs)
set(CMAKE_INSTALL_LIBDIR lib64 CACHE PATH "Install directory for libraries" FORCE)

message(DEBUG "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
message(DEBUG "CMAKE_INSTALL_BINDIR: ${CMAKE_INSTALL_BINDIR}")
message(DEBUG "CMAKE_INSTALL_LIBDIR: ${CMAKE_INSTALL_LIBDIR}")
message(DEBUG "CMAKE_INSTALL_INCLUDEDIR: ${CMAKE_INSTALL_INCLUDEDIR}")

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/build/cmake" ${CMAKE_MODULE_PATH})
include(building)
include(ccache)
include(utils)

# 设置所有共享库的输出目录
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
# 设置所有可执行的输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

message(STATUS "current platform: ${CMAKE_SYSTEM_NAME}")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(DEBUG "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(DEBUG "CXX_FLAGS_DEBUG: ${CXX_FLAGS_DEBUG}")
message(DEBUG "CXX_FLAGS_RELEASE: ${CXX_FLAGS_RELEASE}")
message(DEBUG "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(DEBUG "CMAKE_CXX_FLAGS_RELWITHDEBINFO: ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
message(DEBUG "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")
message(DEBUG "CMAKE_CXX_FLAGS_MINSIZEREL: ${CMAKE_CXX_FLAGS_MINSIZEREL}")

# 加上构建过程日志的颜色输出
option (FORCE_COLORED_OUTPUT "Always produce ANSI-colored output (GNU/Clang only)." OFF)
option(UB_COMM_QUEUE_ENABLE_HALF_WRITE_INJECT "Enable ub_comm_queue half-write fault injection hooks for demos/tests" OFF)
if (${FORCE_COLORED_OUTPUT})
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fcolor-diagnostics)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=always)
    endif ()
else ()
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=auto)
    endif ()
endif ()

# coverage option
option(ENABLE_COVERAGE "Use gcov" OFF)
message(DEBUG ENABLE_COVERAGE=${ENABLE_COVERAGE})
if (ENABLE_COVERAGE)
    setup_coverage()
endif ()

# 全局作用域的头文件定义，不依赖具体模块
include_directories(
        ${PROJECT_SOURCE_DIR}/include   # 外部共享头文件
        ${SRC_DIR}/include              # 内部共享头文件
        ${CMAKE_BINARY_DIR}/include     # 自动生成头文件
)

add_subdirectory(src)

option(BUILD_TESTS "Build tests" OFF)
if (BUILD_TESTS)
    add_subdirectory(test)
endif ()

# ------------------------------
# 安装配置（组件化）
# ------------------------------
install(TARGETS ubs_atomic
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING PATTERN "*.h")

set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_NAME "ubs-atomic")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_FILE_NAME "ubs-atomic-${PROJECT_VERSION}-${UBS_ATOMIC_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_RPM_PACKAGE_NAME "ubs-atomic")
set(CPACK_RPM_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_RPM_PACKAGE_RELEASE "${UBS_ATOMIC_PACKAGE_RELEASE}")

set(CPACK_RPM_PACKAGE_ARCHITECTURE "${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_RPM_FILE_NAME "${CPACK_PACKAGE_FILE_NAME}.rpm")
set(CPACK_RPM_PACKAGE_LICENSE "Mulan PSL v2")
set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
set(CPACK_RPM_PACKAGE_SUMMARY "UBS atomic shared-memory primitives")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "${CPACK_RPM_PACKAGE_SUMMARY}")
include(CPack)
