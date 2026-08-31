# ubs-atomic 构建依赖镜像
# 预装全部构建依赖（与 README.md "环境与依赖"章节一致），用于源码编译/UT，
# 避免每次进入环境重复安装工具链。
# 使用方法见 doc/ubs_atomic_installation.md "容器镜像部署（可选）" 章节。
ARG BASE_IMAGE=openeuler/openeuler:24.03-lts
FROM ${BASE_IMAGE}

RUN dnf install -y \
        make gcc gcc-c++ cmake ninja-build \
        libboundscheck findutils git \
    && dnf clean all \
    && rm -rf /var/cache/dnf

WORKDIR /workspace
CMD ["/bin/bash"]
