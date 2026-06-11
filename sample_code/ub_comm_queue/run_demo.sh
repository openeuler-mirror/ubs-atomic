#!/bin/bash
#
# run_demo.sh - UB Comm Queue 交互式功能演示启动脚本
#
# 用法:
#   ./run_demo.sh                                 # 仅启动本地 Node A，等待对端
#   ./run_demo.sh --remote-b <user@host>          # 通过 SSH 在远程节点启动 Node B
#   ./run_demo.sh --msg-size 4096                 # 使用 4KB 消息
#   ./run_demo.sh --remote-b <host> --msg-size 8192
#
# 架构说明:
#   UB Comm Queue 是跨节点共享内存通信，Node A 和 Node B 必须运行在
#   不同的物理机上（各自映射自己的本地内存 + 远端内存）。
#   同一台机器上启动两个节点实例是不合理的，因为 ubsmem 初始化
#   要求每台机器只属于一个节点。
#
#   --remote-b 选项: 通过 SSH 在远端机器自动启动 Node B，
#                    前提是远端已编译好 demo_interactive 并配置了共享内存。
#

set -o errexit

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"

# 默认参数
MSG_SIZE=64
CPU_ID_A=4
CPU_ID_B=200
NODE0_SHM="shm_node0_export"
NODE1_SHM="shm_node1_export"
REMOTE_B=""          # 远端 Node B 的 SSH 地址，空表示不自动启动
DEMO_BIN="${SCRIPT_DIR}/demo_interactive"
REMOTE_DEMO_PATH=""  # 远端 demo_interactive 路径，空则使用同路径

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# SSH 远端 Node B 的进程 ID 文件
B_SSH_PIDFILE="/tmp/ub_demo_nodeb_$$.pid"

# ===================== 清理 =====================

cleanup() {
    # 清理本地 Node B（兼容旧逻辑，一般不会走到）
    if [ -n "$B_PID" ] && kill -0 "$B_PID" 2>/dev/null; then
        echo ""
        echo -e "${YELLOW}[清理] 正在停止本地 Node B (PID=$B_PID)...${NC}"
        kill "$B_PID" 2>/dev/null || true
        wait "$B_PID" 2>/dev/null || true
    fi

    # 清理远端 Node B
    if [ -n "$REMOTE_B" ] && [ -f "$B_SSH_PIDFILE" ]; then
        local remote_pid
        remote_pid=$(cat "$B_SSH_PIDFILE" 2>/dev/null || echo "")
        if [ -n "$remote_pid" ]; then
            echo -e "${YELLOW}[清理] 正在停止远端 Node B (PID=$remote_pid on $REMOTE_B)...${NC}"
            ssh "$REMOTE_B" "kill $remote_pid 2>/dev/null" || true
        fi
        rm -f "$B_SSH_PIDFILE"
    fi

    echo -e "${GREEN}[清理] 完成${NC}"
}
trap cleanup EXIT INT TERM

# ===================== 帮助和 Banner =====================

print_banner() {
    echo -e "${CYAN}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║       UB Comm Queue 交互式功能演示                           ║"
    echo "║                                                              ║"
    echo "║  架构: 跨节点共享内存通信                                    ║"
    echo "║    Node A (本机)  <--共享内存-->  Node B (远端/对端)         ║"
    echo "║                                                              ║"
    echo "║  功能演示:                                                   ║"
    echo "║    • 自发自收 (Local Loopback)                               ║"
    echo "║    • 跨节点通信 (Peer Echo)                                  ║"
    echo "║    • 消息回调注册 (Sync/Async Callback)                      ║"
    echo "║    • 并发发送 (Multi-thread Throughput)                      ║"
    echo "║    • 队列状态查询 (Queue Status)                             ║"
    echo "║    • 流控演示 (Congestion Threshold)                         ║"
    echo "║    • 心跳配置 (Heartbeat Config)                             ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "启动模式:"
    echo "  (默认)              仅启动本地 Node A，你需要手动在另一台机器上启动 Node B"
    echo "  --remote-b <host>   通过 SSH 在远端机器启动 Node B (需免密登录)"
    echo ""
    echo "参数:"
    echo "  --msg-size <bytes>      消息大小 (64|4096|8192, 默认 64)"
    echo "  --cpu-id-a <N>          本机 Node A 的 CPU ID (默认 4)"
    echo "  --cpu-id-b <N>          对端 Node B 的 CPU ID (默认 200)"
    echo "  --node0-shm <name>      Node 0 共享内存名 (默认 shm_node0_export)"
    echo "  --node1-shm <name>      Node 1 共享内存名 (默认 shm_node1_export)"
    echo "  --remote-b <user@host>  远端 Node B 的 SSH 地址"
    echo "  --remote-path <path>    远端 demo_interactive 的路径 (默认与本地同路径)"
    echo "  -h, --help              显示帮助"
    echo ""
    echo "示例:"
    echo "  # 仅启动 Node A，手动在对端启动 Node B:"
    echo "  ./run_demo.sh"
    echo ""
    echo "  # 通过 SSH 自动在对端启动 Node B:"
    echo "  ./run_demo.sh --remote-b root@192.168.1.2"
    echo ""
    echo "  # 手动双机启动:"
    echo "  # 在机器 0 上:"
    echo "  ./demo_interactive --role A --msg-size 64 --cpu-id 4"
    echo "  # 在机器 1 上:"
    echo "  ./demo_interactive --role B --msg-size 64 --cpu-id 200"
}

# ===================== 参数解析 =====================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --msg-size)
                MSG_SIZE="$2"; shift 2 ;;
            --cpu-id-a)
                CPU_ID_A="$2"; shift 2 ;;
            --cpu-id-b)
                CPU_ID_B="$2"; shift 2 ;;
            --node0-shm)
                NODE0_SHM="$2"; shift 2 ;;
            --node1-shm)
                NODE1_SHM="$2"; shift 2 ;;
            --remote-b)
                REMOTE_B="$2"; shift 2 ;;
            --remote-path)
                REMOTE_DEMO_PATH="$2"; shift 2 ;;
            -h|--help)
                print_usage; exit 0 ;;
            *)
                echo "Unknown option: $1"; exit 1 ;;
        esac
    done
}

# ===================== 编译 =====================

compile_demo() {
    echo -e "${CYAN}[编译] 正在编译 demo_interactive...${NC}"

    local include_dirs="-I${PROJECT_ROOT}/include"
    if [ -d "/usr/include/ubs" ]; then
        include_dirs="$include_dirs -I/usr/include/ubs"
    fi
    if [ -d "/usr/local/include/ubs" ]; then
        include_dirs="$include_dirs -I/usr/local/include/ubs"
    fi

    local lib_dir="${PROJECT_ROOT}/dist/release/lib"
    [ ! -d "$lib_dir" ] && lib_dir="${PROJECT_ROOT}/dist/debug/lib"
    [ ! -d "$lib_dir" ] && lib_dir="${PROJECT_ROOT}/build/lib"

    local lib_opts=""
    if [ -d "$lib_dir" ]; then
        lib_opts="-L${lib_dir} -Wl,-rpath,${lib_dir}"
    fi

    local cmd="g++ -std=c++17 -O2 -o ${DEMO_BIN} \
        ${SCRIPT_DIR}/demo_interactive.cpp \
        ${include_dirs} ${lib_opts} \
        -lub-atomic -lrt -lpthread -ldl"

    echo "  命令: $cmd"

    if $cmd 2>&1; then
        echo -e "${GREEN}[编译] 成功!${NC}"
    else
        echo -e "${RED}[编译] 失败!${NC}"
        echo ""
        echo "  手动编译命令:"
        echo "  g++ -std=c++17 -O2 -o demo_interactive demo_interactive.cpp \\"
        echo "      -I${PROJECT_ROOT}/include ${lib_opts} \\"
        echo "      -lub-atomic -lrt -lpthread -ldl"
        exit 1
    fi
}

# ===================== 启动远端 Node B =====================

start_remote_node_b() {
    echo -e "${CYAN}[远端] 正在通过 SSH 在 ${REMOTE_B} 上启动 Node B...${NC}"

    local remote_path="${REMOTE_DEMO_PATH:-${SCRIPT_DIR}/demo_interactive}"

    echo "  远端路径: ${remote_path}"
    echo "  远端命令: ${remote_path} --role B --msg-size ${MSG_SIZE} --cpu-id ${CPU_ID_B}"

    # 在远端启动 Node B，记录 PID
    ssh "$REMOTE_B" "nohup ${remote_path} --role B \
        --msg-size ${MSG_SIZE} \
        --cpu-id ${CPU_ID_B} \
        -0 ${NODE0_SHM} \
        -1 ${NODE1_SHM} \
        > /tmp/ub_demo_nodeb.log 2>&1 & echo \$!" | {
        read pid
        if [ -n "$pid" ]; then
            echo "$pid" > "$B_SSH_PIDFILE"
            echo -e "${GREEN}[远端] Node B 已在 ${REMOTE_B} 上启动 (PID=$pid)${NC}"
        else
            echo -e "${RED}[远端] 启动 Node B 失败${NC}"
        fi
    }

    echo -e "${CYAN}[远端] 等待 Node B 初始化 (5s)...${NC}"
    sleep 5
}

# ===================== 启动本地 Node A =====================

start_node_a() {
    echo -e "${CYAN}[本地] 正在启动 Node A (交互式菜单)...${NC}"
    echo -e "${CYAN}===========================================================${NC}"
    echo ""

    ${DEMO_BIN} --role A \
        --msg-size ${MSG_SIZE} \
        --cpu-id ${CPU_ID_A} \
        -0 ${NODE0_SHM} \
        -1 ${NODE1_SHM}

    echo ""
    echo -e "${CYAN}===========================================================${NC}"
    echo -e "${GREEN}[完成] Node A 已退出${NC}"
}

# ===================== 手动启动提示 =====================

print_manual_b_instructions() {
    echo ""
    echo -e "${YELLOW}============================================================${NC}"
    echo -e "${YELLOW}  请在对端机器上手动启动 Node B:${NC}"
    echo ""
    echo -e "  ${GREEN}./demo_interactive --role B \\${NC}"
    echo -e "  ${GREEN}    --msg-size ${MSG_SIZE} \\${NC}"
    echo -e "  ${GREEN}    --cpu-id ${CPU_ID_B} \\${NC}"
    echo -e "  ${GREEN}    -0 ${NODE0_SHM} \\${NC}"
    echo -e "  ${GREEN}    -1 ${NODE1_SHM}${NC}"
    echo ""
    echo -e "  ${CYAN}或者使用 --remote-b 自动启动:${NC}"
    echo -e "  ${CYAN}./run_demo.sh --remote-b <user@host>${NC}"
    echo -e "${YELLOW}============================================================${NC}"
    echo ""
}

# ===================== 主流程 =====================

parse_args "$@"
print_banner

echo -e "${YELLOW}运行参数:${NC}"
echo "  本机角色    = Node A"
echo "  对端角色    = Node B"
echo "  msg_size    = ${MSG_SIZE} bytes"
echo "  cpu_id (A)  = ${CPU_ID_A}"
echo "  cpu_id (B)  = ${CPU_ID_B}"
echo "  node0_shm   = ${NODE0_SHM}"
echo "  node1_shm   = ${NODE1_SHM}"
if [ -n "$REMOTE_B" ]; then
    echo "  远端 Node B = ${REMOTE_B}"
fi
echo ""

# 编译
if [ ! -x "${DEMO_BIN}" ]; then
    compile_demo
fi

# 启动远端 Node B（如果指定了 --remote-b）
if [ -n "$REMOTE_B" ]; then
    start_remote_node_b
else
    print_manual_b_instructions
fi

# 启动本地 Node A
start_node_a

echo -e "${GREEN}[完成] 演示结束${NC}"