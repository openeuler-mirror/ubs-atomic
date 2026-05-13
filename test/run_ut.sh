#!/bin/bash
set -e
 
CURRENT_PATH=$(cd "$(dirname "$0")"; pwd)
echo "${CURRENT_PATH:?}"
cd "${CURRENT_PATH:?}"
code_dir=$(cd ${CURRENT_PATH}/../ && pwd)

remove_static()
{
    local dir=$1
    find ${dir} -type f -name "*.cpp" | xargs -i sed -i "s/\bstatic\b//g" {}
}

has_cmd()
{
    command -v "$1" >/dev/null 2>&1
}

build_dir=build

cd ${code_dir}/test
pwd
[ ! -d ${build_dir} ] && mkdir -p ${build_dir}
rm -rf ${build_dir}/*

cp -r $CURRENT_PATH/3rdparty/mockcpp_support_arm64.patch $CURRENT_PATH/3rdparty/mockcpp
cd $CURRENT_PATH/3rdparty/mockcpp
dos2unix src/UnixCodeModifier.cpp
PATCH_FILE="mockcpp_support_arm64.patch"
dos2unix $PATCH_FILE
# 检查补丁是否能应用
if git apply --check "$PATCH_FILE" 2>/dev/null; then
    echo "Applying patch $PATCH_FILE..."
    git apply "$PATCH_FILE"
else
    echo "Patch $PATCH_FILE already applied or cannot be applied, skipping."
fi
 
cd ${code_dir}/test

cmake -S . -B ${build_dir}
echo "====== 开始编译 ubs_atomic_ut ======"
cmake --build ${build_dir} --target ubs_atomic_ut -j$(nproc) || exit 1
echo "====== ubs_atomic_ut 编译完成"

cd ${build_dir}
if has_cmd lcov; then
    lcov --directory . --zerocounters
else
    echo "====== 未安装 lcov，跳过覆盖率清零 ======"
fi

echo "====== 开始执行 ubs_atomic_ut ======"
./ubs_atomic_ut
echo "====== ubs_atomic_ut 执行完成 ======"

if ! has_cmd lcov || ! has_cmd genhtml; then
    echo "====== 未安装 lcov/genhtml，跳过覆盖率报告生成 ======"
    exit 0
fi

# 1. 收集覆盖率（忽略所有错误）
lcov -c -d . -o test.info \
    --rc branch_coverage=1 \
    --rc geninfo_unexecuted_blocks=1 \
    --ignore-errors inconsistent,deprecated,mismatch,unused,corrupt

# 2. 只保留 src 目录代码
lcov -e test.info "*/src/*" -o coverage.info \
    --rc branch_coverage=1 \
    --ignore-errors unused

# 3. 移除头文件
lcov --remove coverage.info "*/src/*.h" -o coverage.info \
    --rc branch_coverage=1 \
    --ignore-errors unused

# 4. 生成报告
genhtml coverage.info -o gcovr_report \
    --branch-coverage \
    --show-details \
    --legend
