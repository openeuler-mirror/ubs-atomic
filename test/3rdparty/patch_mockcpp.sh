#!/bin/bash
set -e

mockcpp_src_dir=$1

cd "${mockcpp_src_dir:?}"

if command -v dos2unix >/dev/null 2>&1; then
    dos2unix -q src/UnixCodeModifier.cpp mockcpp_support_arm64.patch
fi

patch_present=false
if [ -f src/JmpCodeAARCH64.h ] &&
   grep -q "BUILD_FOR_AARCH64" include/mockcpp/mockcpp.h &&
   grep -q "flushCache" include/mockcpp/JmpCode.h; then
    echo "mockcpp patch already present in build copy"
    patch_present=true
fi

if [ "${patch_present}" = false ]; then
    if git apply --reverse --check mockcpp_support_arm64.patch >/dev/null 2>&1; then
        echo "mockcpp patch already applied in build copy"
    elif git apply --check mockcpp_support_arm64.patch; then
        git apply mockcpp_support_arm64.patch
    elif patch -p1 --fuzz=3 --dry-run < mockcpp_support_arm64.patch >/dev/null 2>&1; then
        echo "git apply failed, falling back to patch --fuzz=3"
        patch -p1 --fuzz=3 < mockcpp_support_arm64.patch
    else
        echo "mockcpp patch cannot be applied to build copy" >&2
        exit 1
    fi
fi

if ! grep -q "<typeinfo>" include/mockcpp/MismatchResultHandler.h; then
    sed -i '/#include <mockcpp\/mockcpp.h>/a #include <typeinfo>' include/mockcpp/MismatchResultHandler.h
fi

if ! grep -q "mockcpp/types/Any.h" include/mockcpp/ArgumentsMatchBuilder.h; then
    sed -i '/#include <mockcpp\/mockcpp.h>/a #include <mockcpp/types/Any.h>' include/mockcpp/ArgumentsMatchBuilder.h
fi
