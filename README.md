# 飞书任务投影桥（feishu-task-orchestrator）

把 **OpenClaw 的多 agent 任务运行**投影成 **飞书里的任务卡 + 任务台**，让"谁在干活、干到哪一步、卡在哪"一眼可见。

面向的使用场景：你把一个复杂任务交给一个 planner agent（例如 Jarvis），它拆解后分派给多个 agent 执行。这个项目负责把整条执行链路变成你在飞书里能实时看到的卡片，而不是等你问"进度如何"。

---

## 项目背景

多 agent 协作的常见痛点：

1. **看不见**：任务派给谁了、跑到哪一步、有没有阻塞，只有最终结果出来才知道
2. **不可审计**：agent 之间的派单（消息式委派）不留痕迹，出问题无法回溯
3. **汇报靠记忆**：协调者回答"进度如何"时只能凭上下文回忆，而不是查账

本项目把 OpenClaw 的运行时事实（持久任务台账 + Workboard 看板）投影成两层飞书产物：

- **任务卡**（Card 2.0）：一张卡 = 一个总任务，实时反映全部子任务与协调者动作
- **任务台**（飞书任务清单）：按项目分组的可审计台账，含定时任务健康状态

---

## 架构

```text
OpenClaw 运行时                     本地（本项目）                     飞书
┌───────────────────┐        ┌──────────────────────────┐      ┌──────────────┐
│ 持久任务台账       │ 轮询    │ Observer Worker          │      │              │
│ openclaw tasks    ├───────▶│ 归一化 + 幂等去重         │      │              │
│ list --json       │        │  → SQLite（事实源）       │      │              │
├───────────────────┤        │                          │      │              │
│ Workboard 看板     │ 轮询    │ QueryService / CLI  ◀────┼──────┤ 任务卡 PATCH │
│ （项目/步骤卡）     ├───────▶│  status / list / timeline│      │ （Card 2.0） │
└───────────────────┘        ├──────────────────────────┤      │              │
                             │ Projector Worker          │      │              │
                             │  渲染卡片 + outbox 重试    ├─────▶│              │
                             └──────────────────────────┘      └──────────────┘
```

关键设计：

| 决策 | 理由 |
|---|---|
| **SQLite 是本地事实源** | 飞书侧只是投影；API 失败不影响真实运行状态 |
| **Observer 与 Projector 解耦** | 观测失败不阻塞投递，投递失败不污染观测 |
| **投影全程无模型调用** | 纯 Python + SQLite + 飞书 API；卡片内容由确定性代码渲染 |
| **一张卡 = 一个 root task** | 并发任务互不覆盖；完成即冻结，历史可回溯 |
| **幂等事件 + outbox** | `sha256(task_id\|status\|last_event_at\|summary)` 去重；崩溃重启续投不重复 |
| **tombstone 防复活** | 清理过的测试任务不会被 Workboard 快照重新导入 |

### 卡片结构（单任务焦点）

```text
标题：<任务名> · <状态>            ← 状态直接进标题
子标题：实时任务状态卡
─────────────────────────────
⏱️          📋          ⌛️
18:14:31    2           8秒
开始时间     子任务数      总耗时
─────────────────────────────
📌任务状态
  [已完成] · code · 实现接口 · 6秒
  [进行中] · main · 汇总验收 · 不足1分钟
─────────────────────────────
📝时间线
  18:14:31 · Jarvis · 进行中 · 规划任务
  18:14:35 · Jarvis · 进行中 · 派发子任务
  18:14:41 · code   · 已完成 · 实现接口
```
卡片只承载**身份 / 任务 / 状态 / 时间**，不承载结论——中间结果留在数据层供协调者审计，最终结果由协调者以文字汇报交付。
---

## 示意图：
<img width="1362" height="1304" alt="image" src="https://github.com/user-attachments/assets/f9171355-839a-485d-b151-75f16db6f288" />
<img width="1160" height="1569" alt="image" src="https://github.com/user-attachments/assets/e1ce603f-ac09-4379-824e-62ed4c10eebe" />

---
## 部署步骤

### 0. 前置条件

| 依赖 | 说明 |
|---|---|
| Python 3.11+ | 运行 Observer / Projector / CLI |
| [OpenClaw](https://docs.openclaw.ai) | 提供持久任务台账；需启用 `workboard` 插件 |
| `lark-cli` | 飞书 API 调用；需有可用的 bot profile |
| 飞书自建应用 | 需要任务、消息、卡片（Card 2.0）相关权限 |

飞书侧需要准备：

1. 一个**任务清单**（任务台），并把机器人加为成员（editor 即可）
2. 一个**接收卡片的会话**：群 chat_id 或用户 open_id
3. 机器人对该会话有发消息、更新卡片（`config.update_multi=true`）的权限

### 1. 克隆并安装

```bash
git clone <your-repo-url> feishu-task-orchestrator
cd feishu-task-orchestrator
python3 -m venv .venv && source .venv/bin/activate   # 可选
```

### 2. 配置环境变量

```bash
cp .env.example .env
$EDITOR .env
```

必填项：

| 变量 | 说明 |
|---|---|
| `FTO_LARK_PROFILE` | lark-cli 的 bot profile 名（`lark-cli profile list` 查看） |
| `FTO_LARK_CLI_BIN` | lark-cli 可执行路径（在 PATH 中可省略） |
| `FTO_DM_CHANNEL_KEY` | 默认投递渠道，如 `feishu:dm:ou_xxx` |
| `FTO_DM_TARGET_ID` | 接收方 open_id |
| `FTO_AUTOMATION_SOURCE_IDS` | 需要做健康监控的 cron sourceId（可空） |

> `.env` 已在 `.gitignore` 中；**不要把任何密钥写入仓库**。机器人的 app secret 由 lark-cli 自己保管（macOS 上在 keychain）。

### 3. 自检

```bash
export PYTHONPATH=src

# 单测
python3 -m unittest discover -s tests

# 拉一次账本，生成快照
python3 -m orchestrator.cli --once

# 看一眼当前任务
python3 -m orchestrator.cli list --active --json
```

### 4. 常驻运行（launchd，macOS）

```bash
bin/install-launchd.sh          # 生成两个 plist 到 ~/Library/LaunchAgents
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ethan.feishu-task-orchestrator.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ethan.feishu-task-projector.plist
launchctl list | grep feishu-task     # 确认两个 job 都在跑
```

不想用 launchd 也可以前台跑（两个终端）：

```bash
PYTHONPATH=src python3 -m orchestrator.cli          # Observer
PYTHONPATH=src python3 -m orchestrator.cli project  # Projector
```

---

## 日常使用

```bash
export PYTHONPATH=src

bin/task-start.sh "任务标题" "正在规划：拆为 3 个子任务"   # 收到任务立刻上卡，输出 root_id

python3 -m orchestrator.cli step <root_id> --title "实现接口" --agent code --kind spawn
python3 -m orchestrator.cli note <root_id> --kind dispatch --text "已派发子任务"
python3 -m orchestrator.cli step <root_id> --title "实现接口" --agent code --kind spawn --status succeeded
python3 -m orchestrator.cli note <root_id> --kind summarize --text "汇总结果"
python3 -m orchestrator.cli note <root_id> --kind deliver   --text "交付完成"

python3 -m orchestrator.cli status <root_id> --json        # 查完整状态树
python3 -m orchestrator.cli warnings --json                # 扫未决告警
```

### CLI 速查

| 命令 | 作用 |
|---|---|
| `register <root_id>` | 登记用户来源的总任务 |
| `step <root_id>` | 登记/更新一个子任务（`spawn` / `send` / `inline`） |
| `note <root_id>` | 记录协调者动作（`planning`/`dispatch`/`waiting`/`summarize`/`deliver`/`blocked`） |
| `status <root_id>` | 单任务完整视图（含告警） |
| `list --active` | 进行中的任务 |
| `timeline <task_id>` | 原始事件时间线 |
| `warnings` | 全局未决诊断（含无法归属的运行） |
| `dispatch prepare` | 一次性建好 root 卡 + 步骤卡，返回派生上下文 |
| `bind <run_id> <work_item>` | 人工补绑未匹配的运行 |
| `cleanup --root <id>` | 完整清理（含 tombstone，防复活） |

### 子任务类型（`--kind`）

| kind | 含义 | 终态处理 |
|---|---|---|
| `spawn` | 派给子 agent 的独立运行 | Observer 自动收敛 |
| `send` | 通过消息通道委派给常驻 agent（无单次运行边界） | **协调者需显式关单**：`step ... --status succeeded` |
| `inline` | 协调者自己执行的步骤 | 协调者显式更新状态 |

---

## 运维

| 场景 | 操作 |
|---|---|
| 重启服务 | `launchctl kickstart -k gui/$(id -u)/com.ethan.feishu-task-{orchestrator,projector}` |
| 查看日志 | `tail -f var/worker.log var/projector.log` |
| 卡住的任务 | `fto warnings --json` 看未决诊断，按 `suggested_fix` 补绑/补步骤 |
| 清理测试数据 | `fto cleanup --root <id> --yes`（会写 tombstone，避免被重新导入） |

### 已知约束

- 飞书 Card 2.0 原地更新要求 `config.update_multi=true`，且调用身份与原卡一致；卡片有 14 天有效期（本项目带 generation 轮换）
- 飞书 API 失败不回滚真实运行状态，只记录并在 outbox 中退避重试
- 单任务串行、跨任务并行；迟到事件不覆盖更新的状态

---

## 目录结构

```text
src/orchestrator/
  cli.py                  # 命令行入口
  config.py               # 配置（env / .env）
  event_source/           # 事件源：任务台账、Workboard
  state/                  # SQLite 存储、幂等、checkpoint
  query.py                # 只读查询服务
  dispatch.py             # 派单建模与绑定
  projections/lark_card/  # 卡片渲染 + 投影 worker
  worker/service.py       # Observer 主循环
tests/                    # 42 个单测
docs/launchd/             # launchd 模板
bin/                      # 一键开工 / 安装脚本
```

## 许可

[MIT](LICENSE)
