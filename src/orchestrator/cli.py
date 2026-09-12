from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime

from orchestrator.config import Config
from orchestrator.dispatch import DispatchPrepareService, WorkboardClient, parse_step
from orchestrator.snapshot import read_snapshot
from orchestrator.query import QueryService, json_text
from orchestrator.state.store import Store
from orchestrator.projections.lark_card.worker import LarkProjectionWorker
from orchestrator.worker.service import Worker


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OpenClaw agent status observer (local snapshot; no Feishu writes)"
    )
    parser.add_argument("--once", action="store_true", help="poll and write the snapshot once")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command")
    status_parser = subparsers.add_parser("status", help="print the latest local snapshot")
    status_parser.add_argument("task_id", nargs="?", help="root task/project id")
    status_parser.add_argument("--snapshot", default=None, help="path to agent_status.json")
    status_parser.add_argument("--json", action="store_true")
    status_parser.add_argument("--log-level", default="INFO")
    list_parser = subparsers.add_parser("list", help="list root tasks")
    list_parser.add_argument("--active", action="store_true")
    list_parser.add_argument("--channel")
    list_parser.add_argument("--json", action="store_true")
    timeline_parser = subparsers.add_parser("timeline", help="show a task timeline")
    timeline_parser.add_argument("task_id")
    timeline_parser.add_argument("--limit", type=int, default=50)
    timeline_parser.add_argument("--json", action="store_true")
    warnings_parser = subparsers.add_parser("warnings", help="list unresolved binding warnings")
    warnings_parser.add_argument("--json", action="store_true")
    register = subparsers.add_parser("register", help="register a user-origin root task")
    register.add_argument("task_id")
    register.add_argument("--title", required=True)
    register.add_argument("--channel-key", required=True)
    register.add_argument("--target-type", required=True, choices=("chat", "user"))
    register.add_argument("--target-id", required=True)
    register.add_argument("--source-message-id")
    dispatch = subparsers.add_parser("dispatch", help="prepare a modeled task for main to spawn")
    dispatch_commands = dispatch.add_subparsers(dest="dispatch_command", required=True)
    prepare = dispatch_commands.add_parser("prepare", help="register root and create Workboard cards")
    prepare.add_argument("task_id")
    prepare.add_argument("--title", required=True)
    prepare.add_argument("--channel-key", required=True)
    prepare.add_argument("--target-type", required=True, choices=("chat", "user"))
    prepare.add_argument("--target-id", required=True)
    prepare.add_argument("--source-message-id")
    prepare.add_argument("--step", action="append", required=True, help="agent:title; repeat for each step")
    prepare.add_argument("--json", action="store_true")
    bind = subparsers.add_parser("bind", help="bind an unmatched Run to a planned Work Item")
    bind.add_argument("run_task_id")
    bind.add_argument("work_item_id")
    bind.add_argument("--json", action="store_true")
    step = subparsers.add_parser("step", help="register or update one root task step")
    step.add_argument("root_id")
    step.add_argument("--title", required=True)
    step.add_argument("--agent", required=True)
    step.add_argument("--kind", required=True, choices=("send", "spawn", "inline"))
    step.add_argument("--task-ref")
    step.add_argument("--status", default="running", choices=(
        "queued", "running", "blocked", "succeeded", "failed", "timed_out", "cancelled"
    ))
    step.add_argument("--json", action="store_true")
    note = subparsers.add_parser("note", help="append a planner action to a root timeline")
    note.add_argument("root_id")
    note.add_argument("--kind", required=True, choices=(
        "planning", "dispatch", "waiting", "summarize", "deliver", "blocked"
    ))
    note.add_argument("--text", required=True)
    note.add_argument("--json", action="store_true")
    cleanup = subparsers.add_parser("cleanup", help="delete test roots and tombstone their Runs")
    cleanup.add_argument("--root", action="append", required=True)
    cleanup.add_argument("--task-id", action="append", default=[])
    cleanup.add_argument("--yes", action="store_true")
    cleanup.add_argument("--json", action="store_true")
    project = subparsers.add_parser("project", help="run the Lark projection worker")
    project.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = Config.from_env()
    store = Store(config.db_path)
    query = QueryService(store)
    if args.command == "status" and args.task_id:
        value = query.status(args.task_id)
        print(json_text(value) if args.json else format_task(value))
        raise SystemExit(0 if value else 1)
    if args.command == "status":
        path = args.snapshot or config.snapshot_path
        raise SystemExit(print_status(path))
    if args.command == "list":
        value = query.list(active_only=args.active, channel_key=args.channel)
        print(json_text(value) if args.json else "\n".join(f"{v['project_id']} · {v['status']} · {v['name']}" for v in value))
        return
    if args.command == "timeline":
        value = query.timeline(args.task_id, args.limit)
        print(json_text(value) if args.json else "\n".join(f"{e['timestamp']} · {e['status']} · {e['summary']}" for e in value))
        return
    if args.command == "warnings":
        value = store.binding_diagnostics()
        print(json_text(value) if args.json else "\n".join(
            f"{item['severity']} · {item['task_id']} · {item['label']} · {item['confidence']}"
            for item in value
        ))
        return
    if args.command == "register":
        value = store.register_root(
            args.task_id, args.title, channel_key=args.channel_key,
            target_type=args.target_type, target_id=args.target_id,
            source_message_id=args.source_message_id,
        )
        verified = store.project_by_id(args.task_id)
        if not verified:
            parser.error(f"root registration did not persist: {args.task_id}")
        print(json_text({"ok": True, "task_id": args.task_id, "project": value}))
        return
    if args.command == "dispatch" and args.dispatch_command == "prepare":
        try:
            steps = [parse_step(value) for value in args.step]
        except ValueError as error:
            parser.error(str(error))
        service = DispatchPrepareService(
            store, WorkboardClient(config.openclaw_bin), config.db_path.parent / "dispatch.lock"
        )
        value = service.prepare(
            args.task_id, args.title, channel_key=args.channel_key,
            target_type=args.target_type, target_id=args.target_id,
            source_message_id=args.source_message_id, steps=steps,
        )
        print(json_text(value) if args.json else format_dispatch(value))
        return
    if args.command == "bind":
        try:
            value = store.bind_run(args.run_task_id, args.work_item_id)
        except ValueError as error:
            parser.error(str(error))
        print(json_text(value) if args.json else (
            f"bound: {value['task_id']} -> {value['work_item_id']} "
            f"({'changed' if value['changed'] else 'unchanged'})"
        ))
        return
    if args.command == "step":
        service = DispatchPrepareService(
            store, WorkboardClient(config.openclaw_bin), config.db_path.parent / "dispatch.lock"
        )
        try:
            value = service.prepare_step_async(
                args.root_id, args.title, agent_id=args.agent, kind=args.kind,
                task_ref=args.task_ref, status=args.status,
            )
        except ValueError as error:
            parser.error(str(error))
        print(json_text(value) if args.json else (
            f"step: {value['work_item_id']} · {value['agent_id']} · {value['status']}"
        ))
        return
    if args.command == "note":
        try:
            value = store.add_planner_note(args.root_id, args.kind, args.text)
        except ValueError as error:
            parser.error(str(error))
        print(json_text(value) if args.json else f"note: {value['kind']} · {value['text']}")
        return
    if args.command == "cleanup":
        if not args.yes:
            parser.error("cleanup is destructive; pass --yes after reviewing root/task ids")
        value = store.cleanup_test_roots(args.root, task_ids=args.task_id)
        print(json_text(value) if args.json else (
            f"cleaned roots={len(value['deleted_roots'])} runs={len(value['tombstoned_task_ids'])}"
        ))
        return
    if args.command == "project":
        projector = LarkProjectionWorker(config, store)
        projector.run_once() if args.once else projector.serve()
        return
    worker = Worker(config)
    if args.once:
        worker.run_once()
    else:
        worker.serve()


def _parse_iso(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def format_dispatch(value: dict) -> str:
    lines = [
        f"prepared: {value['root_task_id']}",
        f"project root: {value['project_root_card_id']}",
    ]
    for step in value["steps"]:
        lines.append(
            f"- {step['agent_id']} · {step['title']} · card={step['card_id']} · "
            f"taskName={step['spawn']['taskName']}"
        )
    return "\n".join(lines)


def _human_duration(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _elapsed(snapshot: dict, generated_at: datetime | None) -> str:
    if not generated_at:
        return "-"
    started = _parse_iso(snapshot.get("started_at")) or _parse_iso(snapshot.get("last_event_at"))
    if not started:
        return "-"
    return _human_duration((generated_at - started).total_seconds())


def format_status(document: dict | None) -> str:
    if not document:
        return "no snapshot found; run the worker once (--once) to generate var/agent_status.json"
    lines: list[str] = []
    generated_at = _parse_iso(document.get("generated_at"))
    lines.append("== Agent Status Snapshot ==")
    lines.append(f"generated_at: {document.get('generated_at')}")
    if generated_at:
        age = (datetime.now(generated_at.tzinfo) - generated_at).total_seconds()
        lines.append(f"age: {_human_duration(age)} ago")
    lines.append(f"poll latency: {document.get('poll_latency_ms')} ms")

    agents = document.get("agents") or {}
    active_agents = [
        (agent_id, entry) for agent_id, entry in agents.items() if entry.get("active")
    ]
    lines.append("")
    lines.append("-- Active tasks --")
    if not active_agents:
        lines.append("(none)")
    for agent_id, entry in active_agents:
        for task in entry["active"]:
            elapsed = _elapsed(task, generated_at)
            progress = task.get("progress_summary") or "-"
            lines.append(
                f"[{agent_id}] {task.get('label')} · {task.get('status')} · {elapsed}\n"
                f"    {progress}"
            )

    idle = [agent_id for agent_id, entry in agents.items() if not entry.get("active")]
    lines.append("")
    lines.append(f"idle agents: {', '.join(idle) if idle else '(none)'}")

    failures: list[tuple[str, dict]] = []
    for agent_id, entry in agents.items():
        for terminal in entry.get("recent_terminal") or []:
            if terminal.get("status") in {"failed", "timed_out", "blocked"}:
                failures.append((agent_id, terminal))
    failures.sort(key=lambda item: item[1].get("ended_at") or "", reverse=True)

    lines.append("")
    lines.append("-- Recent failures (max 5) --")
    if not failures:
        lines.append("(none)")
    for agent_id, terminal in failures[:5]:
        error = terminal.get("error") or terminal.get("terminal_summary") or "-"
        lines.append(f"[{agent_id}] {terminal.get('label')} · {terminal.get('status')}: {error}")

    lines.append("")
    lines.append("-- Automations --")
    automations = document.get("automations") or []
    if not automations:
        lines.append("(none)")
    for item in automations:
        lines.append(
            f"[{item.get('health')}] {item.get('label')} · last={item.get('last_status')} "
            f"@{item.get('last_run_at')} · today ok={item.get('today_success')} "
            f"fail={item.get('today_failed')}"
        )
    workboard = document.get("workboard") or {}
    lines.append("")
    lines.append(
        f"workboard: {workboard.get('boards', 0)} board(s), "
        f"{len(workboard.get('active_cards') or [])} active card(s)"
    )
    source = document.get("source") or {}
    lines.append(f"tasks seen: {source.get('tasks_total')}")
    return "\n".join(lines)


def print_status(path) -> int:
    document = read_snapshot(path)
    print(format_status(document))
    return 0 if document else 1


def format_task(value: dict | None) -> str:
    if not value:
        return "task not found"
    lines = [f"{value['name']} · {value['status']} · {value['project_id']}"]
    for item in value.get("items", []):
        lines.append(f"- [{item['agent_id']}] {item['title']} · {item['status']} · {item['latest_progress']}")
    return "\n".join(lines)


if __name__ == "__main__":
    main()
