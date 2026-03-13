#!/bin/bash
# Asterion 快速启动脚本
# 使用方法: ./start_asterion.sh [选项]

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目目录
PROJECT_DIR="/Users/jayzhu/web3/Asterion"
VENV_PATH="$PROJECT_DIR/.venv"
DATA_DIR="$PROJECT_DIR/data"

load_project_env() {
    if [ -f "$PROJECT_DIR/.env" ]; then
        set -a
        # shellcheck disable=SC1091
        source "$PROJECT_DIR/.env"
        set +a
    fi
}

# 显示帮助信息
show_help() {
    echo -e "${BLUE}Asterion 快速启动脚本${NC}"
    echo ""
    echo "用法: ./start_asterion.sh [选项]"
    echo ""
    echo "选项:"
    echo "  --help, -h          显示帮助信息"
    echo "  --web, -w           启动 Operator Console (Streamlit)"
    echo "  --data, -d          运行真实天气数据链 (默认启用 weather agents)"
    echo "  --paper, -p         运行 Paper 交易"
    echo "  --all, -a           启动所有服务 (Web + 数据 + Paper)"
    echo "  --setup, -s         首次安装依赖"
    echo "  --stop, -x          停止所有服务"
    echo ""
    echo "示例:"
    echo "  ./start_asterion.sh --web      # 仅启动 Operator Console"
    echo "  ./start_asterion.sh --all      # 启动完整系统"
    echo "  ./start_asterion.sh --setup    # 首次安装"
}

# 检查并激活虚拟环境
activate_venv() {
    if [ ! -d "$VENV_PATH" ]; then
        echo -e "${RED}错误: 虚拟环境不存在，请先运行: ./start_asterion.sh --setup${NC}"
        exit 1
    fi
    
    source "$VENV_PATH/bin/activate"
    load_project_env
    echo -e "${GREEN}✓ 虚拟环境已激活${NC}"
}

# 首次安装依赖
setup() {
    echo -e "${BLUE}🚀 Asterion 首次安装${NC}"
    echo ""
    
    # 检查 Python 版本
    PYTHON_VERSION=$(python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+')
    REQUIRED_VERSION="3.11"
    
    if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
        echo -e "${RED}错误: 需要 Python 3.11+, 当前版本: $PYTHON_VERSION${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Python 版本检查通过: $PYTHON_VERSION${NC}"
    
    # 创建虚拟环境
    if [ ! -d "$VENV_PATH" ]; then
        echo "📦 创建虚拟环境..."
        python3 -m venv "$VENV_PATH"
    fi
    
    # 激活虚拟环境
    source "$VENV_PATH/bin/activate"
    echo -e "${GREEN}✓ 虚拟环境已激活${NC}"
    
    # 升级 pip
    echo "📦 升级 pip..."
    pip install --upgrade pip -q
    
    # 安装依赖
    echo "📦 安装项目依赖..."
    cd "$PROJECT_DIR"
    pip install -e . -q
    
    echo ""
    echo -e "${GREEN}✓ 安装完成！${NC}"
    echo ""
    echo "可以使用以下命令启动系统："
    echo "  ./start_asterion.sh --web     # 启动 Web UI"
    echo "  ./start_asterion.sh --all     # 启动完整系统"
}

# 启动 Operator Console
start_web() {
    echo -e "${BLUE}🌐 启动 Asterion Operator Console...${NC}"
    activate_venv
    
    cd "$PROJECT_DIR"
    
    # 检查 Streamlit 是否安装
    if ! pip show streamlit &> /dev/null; then
        echo "📦 安装 Streamlit..."
        pip install streamlit -q
    fi

    refresh_operator_console_surfaces || true
    
    # 启动 Streamlit
    echo "🚀 启动 Streamlit 服务..."
    echo -e "${GREEN}Operator Console 地址: http://localhost:8501${NC}"
    streamlit run ui/app.py --server.port 8501 --server.address 0.0.0.0
}

refresh_operator_console_surfaces() {
    echo -e "${BLUE}🧭 刷新 readiness / UI lite 读面...${NC}"
    activate_venv

    cd "$PROJECT_DIR"
    mkdir -p "$DATA_DIR/ui" "$DATA_DIR/meta" logs

    local refresh_output
    if refresh_output=$("$VENV_PATH/bin/python" scripts/refresh_operator_console_surfaces.py 2>&1); then
        echo "$refresh_output" > logs/operator_console_refresh.log
        echo -e "${GREEN}✓ readiness / UI lite 刷新完成${NC}"
        echo -e "${YELLOW}📋 刷新日志: logs/operator_console_refresh.log${NC}"
        return 0
    fi

    echo "$refresh_output" > logs/operator_console_refresh.log
    echo -e "${YELLOW}⚠ readiness / UI lite 刷新失败，将继续启动 Web UI${NC}"
    echo -e "${YELLOW}📋 刷新日志: logs/operator_console_refresh.log${NC}"
    return 1
}

# 启动真实天气数据链（后台持续运行）
start_data() {
    echo -e "${BLUE}📊 启动真实天气数据链...${NC}"
    activate_venv
    
    cd "$PROJECT_DIR"
    
    # 创建日志目录
    mkdir -p logs
    
    if pgrep -f "scripts/run_real_weather_chain_loop.py" > /dev/null; then
        echo -e "${YELLOW}♻ 检测到已有真实天气数据链进程，先停止旧进程以加载最新代码${NC}"
        pkill -f "scripts/run_real_weather_chain_loop.py" || true
        sleep 1
    fi

    # 启动 canonical weather ingress loop（后台运行，每10分钟重试一次）
    echo "🔄 启动持续天气链路（events-first，多市场 open-recent 抓取，默认启用 weather agents）..."
    nohup "$VENV_PATH/bin/python" -u scripts/run_real_weather_chain_loop.py \
        --interval-minutes 10 \
        --recent-within-days 14 \
        --force-rebuild-on-start \
        < /dev/null > logs/real_weather_chain_loop.log 2>&1 &
    local loop_pid=$!
    disown "$loop_pid" 2>/dev/null || true
    sleep 1
    if ! kill -0 "$loop_pid" 2>/dev/null; then
        echo -e "${RED}✗ 真实天气数据链启动失败${NC}"
        echo -e "${YELLOW}📋 查看日志: tail -f logs/real_weather_chain_loop.log${NC}"
        return 1
    fi

    echo -e "${GREEN}✓ 真实天气数据链已启动（后台运行，pid=$loop_pid）${NC}"
    echo -e "${YELLOW}📋 查看日志: tail -f logs/real_weather_chain_loop.log${NC}"
    echo -e "${YELLOW}📄 结果报告: data/dev/real_weather_chain/real_weather_chain_report.json${NC}"
}

# 启动 Paper 交易
start_paper() {
    echo -e "${BLUE}⚡ 启动 Paper 交易...${NC}"
    activate_venv
    
    cd "$PROJECT_DIR"
    
    echo "📝 Paper 交易模块已就绪"
    echo "✓ paper_adapter_v1 已加载"
    echo "✓ Paper order journal payload 已配置"
    
    # 显示可用的 paper 功能
    python -c "
from asterion_core.execution.paper_adapter_v1 import (
    build_paper_order,
    build_order_state_transition,
    paper_order_journal_payload
)
print('✓ Paper 交易功能已加载:')
print('  - build_paper_order')
print('  - build_order_state_transition')  
print('  - paper_order_journal_payload')
"
    
    echo -e "${GREEN}✓ Paper 交易已启动${NC}"
}

# 启动所有服务
start_all() {
    echo -e "${BLUE}🚀 启动完整 Asterion 系统...${NC}"
    
    # 数据收集（后台）
    start_data
    
    # Paper 交易
    start_paper

    echo -e "${GREEN}✓ 所有服务已启动${NC}"
    start_web
}

# 停止所有服务
stop_all() {
    echo -e "${YELLOW}🛑 停止所有服务...${NC}"
    
    # 查找并终止相关进程
    pkill -f "streamlit run ui/app.py" || true
    pkill -f "scripts/run_real_weather_chain_loop.py" || true
    pkill -f "python.*asterion" || true
    
    echo -e "${GREEN}✓ 所有服务已停止${NC}"
}

# 主程序
main() {
    # 如果没有参数，显示帮助
    if [ $# -eq 0 ]; then
        show_help
        exit 0
    fi
    
    # 解析参数
    case "$1" in
        --help|-h)
            show_help
            ;;
        --setup|-s)
            setup
            ;;
        --web|-w)
            start_web
            ;;
        --data|-d)
            start_data
            ;;
        --paper|-p)
            start_paper
            ;;
        --all|-a)
            start_all
            ;;
        --stop|-x)
            stop_all
            ;;
        *)
            echo -e "${RED}错误: 未知选项 $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# 运行主程序
main "$@"
