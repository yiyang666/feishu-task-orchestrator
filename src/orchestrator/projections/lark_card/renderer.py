from __future__ import annotations

import time


STATUS = {
    "queued": ("排队中", "grey", "grey-50"), "running": ("进行中", "blue", "blue-50"),
    "blocked": ("已阻塞", "red", "red-50"), "failed": ("失败", "red", "red-50"),
    "timed_out": ("已超时", "red", "red-50"), "succeeded": ("已完成", "green", "green-50"),
    "cancelled": ("已取消", "grey", "grey-50"),
}


def render_task(view: dict) -> dict:
    label, color, background = STATUS.get(view["status"], (view["status"], "grey", "grey-50"))
    items = view.get("items", [])
    timeline = _compact_timeline(view.get("timeline", []), items)
    elements = [
        {"tag": "column_set", "flex_mode": "none", "background_style": background, "columns": [
            _metric("开始时间", "⏱️", _clock(view.get('created_at'))),
            _metric("子任务数", "📋", str(len(items))),
            _metric("总耗时", "⌛️", _duration(view)),
        ]},
        {"tag": "markdown", "content": "**📌任务状态**"},
    ]
    elements.extend(_step(item) for item in items[:8])
    if not items:
        elements.append(_box("尚未派发子任务", "grey-50"))
    elements.append({"tag": "markdown", "content": "**📝时间线**"})
    elements.append(_box("\n".join(timeline) if timeline else "暂无执行事件", "grey-50"))
    warnings = view.get("warnings") or []
    if warnings:
        warning_text = "\n".join(f"- {_safe(row.get('message'), 180)}" for row in warnings[:3])
        warning_bg = "red-50" if any(row.get("severity") in {"error", "critical"} for row in warnings) else "yellow-50"
        elements.append(_box(f"**需要关注 · 派发关联异常**\n{warning_text}", warning_bg))
    failed = next((item for item in items if item["status"] in {"blocked", "failed", "timed_out"}), None)
    if failed:
        failed_label = STATUS.get(failed["status"], (failed["status"],))[0]
        elements.append(_box(
            f"**需要关注**\n{_safe(failed['agent_id'])} · {_safe(failed['title'])} · {failed_label}",
            "red-50",
        ))
    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "default", "summary": {"content": f"任务：{view['name']} · {label}"}},
        "header": {"title": {"tag": "plain_text", "content": f"{_plain(view['name'], 80)} · {label}"},
                   "subtitle": {"tag": "plain_text", "content": "实时任务状态卡"},
                   "template": color if color in {"blue", "red", "green", "grey"} else "blue",
                   "icon": {"tag": "standard_icon", "token": "todo_colorful"}},
        "body": {"direction": "vertical", "vertical_spacing": "medium", "elements": elements},
    }
    return card


def _metric(name: str, emoji: str, value: str) -> dict:
    return {
        "tag": "column", "width": "weighted", "weight": 1, "padding": "10px 8px",
        "elements": [{"tag": "markdown", "text_align": "center",
                      "content": f"{emoji}\n**{_safe(value, 40)}**\n<font color='grey'>{_safe(name)}</font>"}],
    }


def _step(item: dict) -> dict:
    label, color, bg = STATUS.get(item["status"], (item["status"], "grey", "grey-50"))
    text = (f"<text_tag color='{color}'>{label}</text_tag> · "
            f"**{_safe(item['agent_id'])}** · {_safe(item['title'])} · {_item_duration(item)}")
    return _box(text, bg)


def _box(content: str, bg: str) -> dict:
    return {"tag": "column_set", "flex_mode": "none", "background_style": bg, "columns": [{
        "tag": "column", "width": "weighted", "weight": 1, "padding": "10px",
        "elements": [{"tag": "markdown", "content": content}],
    }]}


def _compact_timeline(events: list[dict], items: list[dict] | None = None) -> list[str]:
    item_by_task = {
        str(item.get("task_id")): item for item in (items or []) if item.get("task_id")
    }
    compact: list[dict] = []
    for event in events:
        display = _display_event(event, item_by_task)
        if compact and compact[-1]["key"] == display["key"]:
            compact[-1] = display
        else:
            compact.append(display)
    selected = compact[-8:]
    return [
        f"{_clock(event['timestamp'])} · **{_safe(event['actor'])}** · "
        f"{event['status_label']} · {_safe(event['description'], 60)}"
        for event in selected
    ]


def _display_event(event: dict, item_by_task: dict[str, dict]) -> dict:
    item = item_by_task.get(str(event.get("task_id") or ""))
    if event.get("event_type") == "planner_action":
        kind = str(event.get("kind") or "")
        actor = "Jarvis"
        status_label = "等待中" if kind == "waiting" else STATUS.get(
            event.get("status"), (str(event.get("status") or "状态更新"),)
        )[0]
        description = {
            "planning": "规划任务", "dispatch": "派发子任务", "waiting": "等待任务结果",
            "summarize": "汇总任务结果", "deliver": "汇总并交付", "blocked": "协调任务受阻",
        }.get(kind, "协调任务")
    elif item:
        actor = str(item.get("agent_id") or "unknown")
        status_label = STATUS.get(
            event.get("status"), (str(event.get("status") or "状态更新"),)
        )[0]
        description = str(item.get("title") or "子任务")
    else:
        actor = str(event.get("actor") or "System")
        status_label = STATUS.get(
            event.get("status"), (str(event.get("status") or "状态更新"),)
        )[0]
        description = "任务状态更新"
    return {
        "timestamp": event.get("timestamp"), "actor": actor,
        "status_label": status_label, "description": description,
        "key": (actor, status_label, " ".join(description.split())),
    }


def _time(value) -> str:
    return time.strftime("%m-%d %H:%M:%S", time.localtime(int(value or 0) / 1000)) if value else "待开始"


def _clock(value) -> str:
    return time.strftime("%H:%M:%S", time.localtime(int(value or 0) / 1000)) if value else "待开始"


def _duration(view: dict) -> str:
    items = view.get("items", [])
    starts = [int(i["started_at"]) for i in items if i.get("started_at")]
    ends = [int(i["ended_at"]) for i in items if i.get("ended_at")]
    start = min(starts) if starts else int(view.get("created_at") or 0)
    end = max(ends) if view["status"] in {"succeeded", "failed", "cancelled", "timed_out"} and ends else int(time.time() * 1000)
    seconds = max(0, (end - start) // 1000)
    return _seconds(seconds) if view["status"] in {"succeeded", "failed", "cancelled", "timed_out"} else _live_seconds(seconds)


def _item_duration(item: dict) -> str:
    start = int(item.get("started_at") or 0)
    end = int(item.get("ended_at") or time.time() * 1000)
    seconds = max(0, (end - start) // 1000)
    return _seconds(seconds) if item.get("ended_at") else _live_seconds(seconds)


def _live_seconds(value: int) -> str:
    if value < 60:
        return "不足1分钟"
    minutes = value // 60
    if minutes < 60:
        return f"{minutes}分钟"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}小时{minutes}分"


def _seconds(value: int) -> str:
    if value < 60: return f"{value}秒"
    minutes, seconds = divmod(value, 60)
    if minutes < 60: return f"{minutes}分{seconds}秒"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}小时{minutes}分"


def _safe(value, limit=100) -> str:
    text = " ".join(str(value or "").split())[:limit]
    for source, target in (("&", "&#38;"), ("<", "&#60;"), (">", "&#62;"), ("*", "&#42;")):
        text = text.replace(source, target)
    return text


def _plain(value, limit=100) -> str:
    return " ".join(str(value or "").split())[:limit]
