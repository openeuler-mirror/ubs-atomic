#!/bin/bash

# ***********************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
# script for build and package RackAgent
# ***********************************************************************

###
### build.sh --- build project
###
### Usage:
###     build.sh <target> [-T <type>] [-D] [-C] [-t <target>]
###
### Options:
###     <target>                Build target used by Make
###     -h | --help             Show help message
###     -D | --debug            Build debug version
###     -C | --coverage         Generate coverage report files
###     -H | --http-server      Start http server for coverage report
###     -c | --clean            Clean build directory
###     -S | --source-compiling Source compiling for 3rdparty
###     -j | --jobs             Specifying running jobs for make
###     -v | --verbose          Verbose output
###     -V | --deploy-version   Release version for deploy
###     -t | --target           Specifying build target, default is `all`
###                             Supported targets:
###                                 `all`       build all target in source code
###                                 `3rdparty`  build 3rdparty libs
###                                 `test`      build all tests in test/ directory
###     -T | --type             Build type
###                             Supported targets:
###                                 `Debug`           Provides detailed debugging information with no optimizations
###                                 `Release`         Optimizes for performance with no debugging information
###                                 `RelWithDebInfo`  Optimizes for performance while retaining debugging information
###                                 `MinSizeRel`      Optimizes for the smallest binary size
###     --asan                  Enable AddressSanitizer (ASAN)
###     --lsan                  Enable LeakSanitizer (LSAN)
###     --tsan                  Enable ThreadSanitizer (TSAN)
###     --ubsan                 Enable UndefinedBehaviorSanitizer (UBSAN)

# 函数内命令（后台命令）失败时，立即退出函数
set -o errtrace
# 脚本内命令（前台命令）失败时，立即退出脚本
set -o errexit

# 获取当前脚本的绝对路径
SCRIPT_DIR="$(readlink -f "$0")"

# 脚本错误处理
trap_error() {
    echo "================================================================="
    echo "Error occurred in script at line: $SCRIPT_DIR:${BASH_LINENO[0]}"
    echo "Error occurred in command: $BASH_COMMAND"
    echo "Call stack:"
    for i in "${!FUNCNAME[@]}"; do
        # 只打印非空行号
        if [[ "${BASH_LINENO[i]}" != "0" ]]; then
            echo "  ${FUNCNAME[i + 1]} at line ${BASH_LINENO[i]}"
        fi
    done
    echo "================================================================="
    exit 1
}

# 脚本出错时捕获错误，并执行 trap_error 处理错误
trap 'trap_error $LINENO' ERR
# 脚本退出时，恢复到脚本的执行目录
trap 'cd $PROJECT_ROOT_DIR; echo "Script exited with status $?"; exit' EXIT

# 定义构建类型与构建目录的映射
declare -A BUILD_DIRS=(
    ["Debug"]="dist/debug"
    ["Release"]="dist/release"
    ["RelWithDebInfo"]="relwithdebinfo"
    ["MinSizeRel"]="minsizerel"
)

# 获取项目根目录（目前为构建脚本所在目录）
PROJECT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

output_dir=${PROJECT_ROOT_DIR}/output

build_target="all"
build_type="Release"
build_dir="dist"
std="17" # CXX 语言版本
generator="Unix Makefiles"
package_version="1.0.0"
package_release="1"

enable_coverage="OFF"
enable_test="OFF"
skip_run_tests="OFF"
force_colored_output="OFF" # 强制启用ANSI颜色输出

# 判断是否在流水线构建
build_in_ci=false
if [[ $is_build_project == 'true' ]]; then
    echo "Current build in ci env."
    build_in_ci=true
fi

# 设置默认的并发数为 CPU 线程数
jobs=$(nproc)

# 非流水线构建
if [[ $build_in_ci != true ]]; then
    # 本地构建减 2 防止 100% CPU 占用
    jobs=$((jobs > 2 ? jobs - 2 : jobs))
    if command -v ninja >/dev/null 2>&1; then
        generator="Ninja"
        echo "Ninja is installed. Generator set to: $generator"
    else
        echo "Ninja is not installed."
        echo "You can install Ninja using the following command:"
        echo "For openEuler: sudo dnf install ninja-build"
        echo "For Windows: You can download it from https://github.com/ninja-build/ninja/releases"
    fi
fi

FAILURE='[\033[1;31mFAILED\033[0;39m]'

# 日志打印辅助函数
function log_info() {
    LOG_FILE=${build_dir}/build.log
    if [ $# -lt 1 ]; then
        return
    fi
    echo "$(date +"%F %T") [INFO] $*" >>"$LOG_FILE"
    echo "$(date +"%F %T") [INFO] $*"
}

function echo_failure() {
    echo -e "UBTurbo project : $FAILURE"
}

# 解析 help 信息并打印，help 信息放在文件头
# 注意：脚本内其他地方不要以 ### 开头进行注释
function help() {
    sed -rn 's/^### ?//;T;p;' "$0"
}

# 脚本参数解析
function parse_args() {
    trans_params=()
    local trans_flag=false
    while [[ $# -gt 0 ]]; do
        case "$1" in
        -h | --help)
            help
            exit
            ;;
        -D | --debug)
            build_type='Debug'
            shift
            ;;
        -T | --type)
            if [[ $# -gt 1 && "$2" != "-"* ]]; then
                build_type="$2"
                shift 2
            else
                log_info "Error: Argument required after -T | --type."
                exit 1
            fi
            ;;
        -t | --target)
            if [[ $# -gt 1 && "$2" != "-"* ]]; then
                build_target="$2"
                shift 2
            else
                log_info "Error: Argument required after -t|--target."
                exit 1
            fi
            ;;
        -C | --coverage)
            enable_coverage='ON'
            shift
            ;;
        -c | --clean)
            enable_clean='ON'
            shift
            ;;
        -j | --jobs)
            if [[ $# -gt 1 && "$2" != "-"* ]]; then
                jobs="$2"
                shift 2
            else
                log_info "Error: Argument required after -j|--jobs."
                exit 1
            fi
            ;;
        --std)
            if [[ $# -gt 1 && "$2" != "-"* ]]; then
                std="$2"
                shift 2
            else
                log_info "Error: Argument required after --std, like --std 14."
                exit 1
            fi
            ;;
        -v | --verbose)
            export VERBOSE=1
            shift
            ;;
        -V | --deploy-version)
            if [[ $# -gt 1 && "$2" != "-"* ]]; then
                deploy_version="$2"
                if [[ "$deploy_version" =~ ^([0-9]+(\.[0-9]+){2})-([0-9]+)$ ]]; then
                    package_version="${BASH_REMATCH[1]}"
                    package_release="${BASH_REMATCH[3]}"
                elif [[ "$deploy_version" =~ ^[0-9]+$ ]]; then
                    package_release="$deploy_version"
                else
                    log_info "Error: Invalid deploy version '$deploy_version'. Use 1.0.0-1 or release number like 1."
                    exit 1
                fi
                shift 2
            else
                log_info "Error: Argument required after -V | --deploy-version."
                exit 1
            fi
            ;;
        --skip-run-tests)
            skip_run_tests='ON'
            shift
            ;;
        --asan)
            enable_asan='ON'
            shift
            ;;
        --lsan)
            enable_lsan='ON'
            shift
            ;;
        --tsan)
            enable_tsan='ON'
            shift
            ;;
        --ubsan)
            enable_ubsan='ON'
            shift
            ;;
        --ninja)
            generator='Ninja'
            shift
            ;;
        --make)
            generator='Unix Makefiles'
            shift
            ;;
        --)
            trans_flag=true
            shift
            ;;
        *)
            if $trans_flag; then
                trans_params+=("$1")
            else
                # 否则，可能是未识别的选项或默认的目标（取决于具体需求）
                [ "$1" != "" ] && build_target="$1"
            fi
            shift
            ;;
        esac
    done
}

function clean() {
    local target_dirs=()
    case $1 in
    "3rdparty")
        target_dirs+=("${PROJECT_ROOT_DIR}/deps")
        ;;
    "package")
        target_dirs+=("${build_dir}")
        target_dirs+=("${output_dir}")
        ;;
    "package_all_in_one")
        target_dirs+=("${build_dir}")
        target_dirs+=("${output_dir}")
        ;;
    *)
        target_dirs+=("${build_dir}")
        ;;
    esac

    for target_dir in "${target_dirs[@]}"; do
        if [ ! -d "$target_dir" ]; then
            echo "Warning: Directory '$target_dir' does not exist."
        else
            rm -rf "$target_dir"
            echo "Directory '$target_dir' has been cleaned."
        fi
    done

    if [[ ! -d "${build_dir}" ]]; then
        mkdir -p "${build_dir}"
        echo "Directory ${build_dir} has been recreated."
    fi
}

# 执行 CMake 构建
function build_cmake() {
    # 启用测试
    if [[ $build_target == 'test' || $build_target == 'ut' || $build_target =~ _ut$ ]]; then
        enable_test='ON'
        build_type='Debug'
        CURRENT_PATH=$(cd "$(dirname "$0")"; pwd)
        echo "${CURRENT_PATH:?}"
        cd "${CURRENT_PATH:?}"
        sh ./test/run_dt.sh
        exit 0
    fi

    # 根据构建类型选择不同构建目录
    build_dir="$PROJECT_ROOT_DIR/${BUILD_DIRS[$build_type]}"
    if [ -z "$build_dir" ]; then
        echo "Error: Invalid build type '$build_type'"
        exit 1
    fi

    # 清理目录
    if [[ $build_target == 'package' || $enable_clean == 'ON' ]]; then
        clean "$build_target"
    fi

    # 确保构建目录已创建
    [ ! -d "${build_dir}" ] && mkdir -p "${build_dir}"

    log_info "***** start build_cmake *****"

    log_info "building target ${build_target}."
    if [[ $generator == 'Ninja' ]]; then
        force_colored_output='ON'
    fi

    # CMake 配置
    cmake -S . -B "${build_dir}" -G "${generator}" "${trans_params[@]}" \
        -DCMAKE_BUILD_TYPE=${build_type} \
        -DCMAKE_CXX_STANDARD="${std}" \
        -DCMAKE_TOOLCHAIN_FILE="${toolchain_file}" \
        -DCMAKE_INSTALL_PREFIX=${build_dir}/install \
        -DUBS_ATOMIC_VERSION=${package_version} \
        -DUBS_ATOMIC_PACKAGE_RELEASE=${package_release} \
        -DBUILD_TESTS=${enable_test} \
        -DENABLE_COVERAGE=${enable_coverage} \
        -DASAN_BUILD=${enable_asan} \
        -DLSAN_BUILD=${enable_lsan} \
        -DTSAN_BUILD=${enable_tsan} \
        -DUBSAN_BUILD=${enable_ubsan} \
        -DFORCE_COLORED_OUTPUT=${force_colored_output}

    # 确保先构建 Debug 版本的所有代码，生成全面覆盖率报告
    if [[ $enable_coverage == 'ON' && $build_type == 'Debug' && $build_target != 'all' ]]; then
        cmake --build ${build_dir} --target all -j ${jobs}
    fi

    # CMake 构建
    cmake --build ${build_dir} --target ${build_target} -j ${jobs}

    # 生成覆盖率报告
    if [[ $enable_coverage == 'ON' ]]; then
        # 检查是否存在 .gcda 文件
        if find ${build_dir} -name '*.gcda' | grep -q .; then
            cmake --build ${build_dir} --target coverage
        else
            echo "No .gcda files found. Skipping coverage report generation."
        fi
    fi

    local ret=$?
    if [ $ret -ne 0 ]; then
        log_info "build_cmake failed"
        echo_failure
        exit 1
    fi
}

echo $(date +"[%Y-%m-%d %H:%M]"): "$0" "$@"
START_TIME=$(date +%s.%N)

cd "${PROJECT_ROOT_DIR}"

parse_args "$@" # 解析脚本参数
build_cmake     # 执行 CMake 构建

END_TIME=$(date +%s.%N)
EXEC_TIME=$(echo "scale=3; ($END_TIME - $START_TIME) / 1" | bc)
echo -e $(date +"[%Y-%m-%d %H:%M]"): "\033[32m" Build complated in "$EXEC_TIME"s '\033[0m'
