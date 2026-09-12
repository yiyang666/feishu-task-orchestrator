#!/usr/bin/env bash
# 生成并安装两个 launchd 常驻服务（Observer + Projector）。
# 会从 .env 与当前机器环境推导路径，替换模板中的占位符。
#
# 用法: bin/install-launchd.sh [--reload]
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

PYTHON_BIN="${FTO_PYTHON_BIN:-$(command -v python3)}"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
LABEL_PREFIX="${FTO_LAUNCHD_PREFIX:-com.ethan.feishu-task}"
OPENCLAW_HOME_DIR="${OPENCLAW_HOME:-$HOME/.openclaw}"
LARK_CLI_BIN="${FTO_LARK_CLI_BIN:-$(command -v lark-cli || true)}"

mkdir -p "$LAUNCH_AGENTS" var

render() {
  local template="$1" label="$2" out="$3"
  sed \
    -e "s|__LABEL__|$label|g" \
    -e "s|__PYTHON_BIN__|$PYTHON_BIN|g" \
    -e "s|__PROJECT_DIR__|$ROOT_DIR|g" \
    -e "s|__HOME_DIR__|$HOME|g" \
    -e "s|__OPENCLAW_HOME__|$OPENCLAW_HOME_DIR|g" \
    -e "s|__LARK_CLI_BIN__|$LARK_CLI_BIN|g" \
    "$template" > "$out"
  plutil -lint "$out" >/dev/null
}

render docs/launchd/observer.plist.template   "${LABEL_PREFIX}-orchestrator" "$LAUNCH_AGENTS/${LABEL_PREFIX}-orchestrator.plist"
render docs/launchd/projector.plist.template  "${LABEL_PREFIX}-projector"    "$LAUNCH_AGENTS/${LABEL_PREFIX}-projector.plist"

echo "已写入:"
echo "  $LAUNCH_AGENTS/${LABEL_PREFIX}-orchestrator.plist"
echo "  $LAUNCH_AGENTS/${LABEL_PREFIX}-projector.plist"
echo
echo "启用:"
echo "  launchctl bootstrap gui/$(id -u) \"$LAUNCH_AGENTS/${LABEL_PREFIX}-orchestrator.plist\""
echo "  launchctl bootstrap gui/$(id -u) \"$LAUNCH_AGENTS/${LABEL_PREFIX}-projector.plist\""
echo "重载:"
echo "  launchctl kickstart -k gui/$(id -u)/${LABEL_PREFIX}-orchestrator"
echo "  launchctl kickstart -k gui/$(id -u)/${LABEL_PREFIX}-projector"
