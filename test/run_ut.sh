#!/bin/bash
 
CURRENT_PATH=$(cd "$(dirname "$0")"; pwd)
echo "${CURRENT_PATH:?}"
cd "${CURRENT_PATH:?}"
code_dir=$(cd ${CURRENT_PATH}/../ && pwd)

remove_static()
{
    local dir=$1
    find ${dir} -type f -name "*.cpp" | xargs -i sed -i "s/\bstatic\b//g" {}
}

remove_static ${code_dir}/src/smap

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
echo "====== 开始编译 ubturbo_tdsql_ut ======"
cmake --build ${build_dir} --target ubturbo_tdsql_ut -j$(nproc) || exit 1
echo "====== ubturbo_tdsql_ut 编译完成"

cd ${build_dir}
lcov --directory . --zerocounters
echo "====== 开始执行 ubturbo_tdsql_ut ======"
./ubturbo_tdsql_ut

lcov --d . --c --output-file ./test.info --rc lcov_branch_coverage=1
lcov -e ./test.info "*/src/*" -output-file ./coverage.info --rc lcov_branch_coverage=1

lcov --remove ./coverage.info "*/src/*.h" -o ../build/coverage.info --rc lcov_branch_coverage=1
lcov --remove ./coverage.info "*/test/3rdparty/googletest/googletest/src/*" -o ./coverage.info --rc lcov_branch_coverage=1

genhtml -o ./gcovr_report ./coverage.info --show-details --legend --rc lcov_branch_coverage=1