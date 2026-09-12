# 常驻部署（macOS launchd）

本项目需要两个常驻进程：

| 进程 | 命令 | 职责 |
|---|---|---|
| Observer | `python3 -m orchestrator.cli` | 轮询 OpenClaw 任务台账 → SQLite |
| Projector | `python3 -m orchestrator.cli project` | 消费事件 → 飞书卡片更新 |

## 一键生成

```bash
cp .env.example .env && $EDITOR .env
bin/install-launchd.sh
```

脚本会从 `.env` 与当前机器环境推导路径，把模板渲染到 `~/Library/LaunchAgents/`：

- `docs/launchd/observer.plist.template` → `com.ethan.feishu-task-orchestrator.plist`
- `docs/launchd/projector.plist.template` → `com.ethan.feishu-task-projector.plist`

## 启用 / 重载 / 停用

```bash
# 启用
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ethan.feishu-task-orchestrator.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ethan.feishu-task-projector.plist

# 查看
launchctl list | grep feishu-task

# 重载（改代码或配置后）
launchctl kickstart -k gui/$(id -u)/com.ethan.feishu-task-orchestrator
launchctl kickstart -k gui/$(id -u)/com.ethan.feishu-task-projector

# 停用
launchctl bootout gui/$(id -u)/com.ethan.feishu-task-orchestrator
launchctl bootout gui/$(id -u)/com.ethan.feishu-task-projector
```

## 环境变量注记

- Projector 需要通过 `lark-cli` 调用飞书 API。`lark-cli` 的配置档定位依赖 `OPENCLAW_HOME`，
  模板里已注入；如果你们的 profile 不在默认工作区，请另外设置 `LARKSUITE_CLI_CONFIG_DIR`。
- 凭据（app secret）由 lark-cli 自行保管（macOS 在 keychain），本项目不读取、不落盘。
- 其余可调项（轮询间隔、超时、数据库路径等）见 `.env.example`。

## 非 macOS

用 systemd / supervisor 等价方式拉起上面两条命令即可，注意：

- 工作目录 = 项目根目录
- 环境变量需包含 `PYTHONPATH=<项目根>/src`、`OPENCLAW_HOME`、`PATH`
- 崩溃自动重启（systemd: `Restart=always`）
