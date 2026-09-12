#!/usr/bin/env bash
# 一键开工：登记 root + 记录 planning note，让任务卡在收到任务的第一时间出现。
#
# 用法: task-start.sh "任务标题" ["首条规划说明"]
# 配置: 读取项目根目录 .env（见 .env.example）中的
#       FTO_DM_CHANNEL_KEY / FTO_DM_TARGET_ID
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

TITLE="${1:?用法: task-start.sh \"任务标题\" [规划说明]}"
NOTE="${2:-正在规划任务}"

: "${FTO_DM_CHANNEL_KEY:?请在 .env 中配置 FTO_DM_CHANNEL_KEY（例如 feishu:dm:ou_xxx）}"
: "${FTO_DM_TARGET_ID:?请在 .env 中配置 FTO_DM_TARGET_ID（接收方 open_id）}"

ROOT_ID="tsk-$(date +%Y%m%d-%H%M%S)"

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

python3 -m orchestrator.cli register "$ROOT_ID" \
  --title "$TITLE" \
  --channel-key "$FTO_DM_CHANNEL_KEY" \
  --target-type user \
  --target-id "$FTO_DM_TARGET_ID" >/dev/null

python3 -m orchestrator.cli note "$ROOT_ID" --kind planning --text "$NOTE" >/dev/null

echo "$ROOT_ID"
