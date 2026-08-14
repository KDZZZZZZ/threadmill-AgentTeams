"""TeamHarness integration implemented with QwenPaw 2 public plugin APIs."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
from datetime import datetime
from typing import Any, AsyncGenerator, Callable


PLUGIN_DIR = Path(__file__).resolve().parent
ASSET_DIR = PLUGIN_DIR / "teamharness"
if not (ASSET_DIR / "plugin.yaml").exists():
    ASSET_DIR = PLUGIN_DIR.parent.parent


def _team_prompt(_agent: Any) -> str:
    path = ASSET_DIR / "prompts" / "team" / "TEAMS.md"
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _sanitizer_rules() -> list[str]:
    raw = os.getenv("AGENTTEAMS_OUTPUT_SANITIZE_KEYWORDS", "")
    return [value.strip() for value in raw.split(",") if value.strip()]


def _sanitize(value: Any) -> None:
    rules = _sanitizer_rules()
    if not rules:
        return
    if isinstance(value, dict):
        for key, item in list(value.items()):
            if isinstance(item, str):
                for rule in rules:
                    item = re.sub(
                        re.escape(rule),
                        "[REDACTED]",
                        item,
                        flags=re.IGNORECASE,
                    )
                value[key] = item
            else:
                _sanitize(item)
        return
    if isinstance(value, list):
        for item in value:
            _sanitize(item)
        return
    for attr in ("content", "output", "text"):
        if hasattr(value, attr):
            item = getattr(value, attr)
            if isinstance(item, str):
                for rule in rules:
                    item = re.sub(
                        re.escape(rule),
                        "[REDACTED]",
                        item,
                        flags=re.IGNORECASE,
                    )
                setattr(value, attr, item)
            else:
                _sanitize(item)


def _task_context(workspace: Path, room_id: str) -> dict[str, Any]:
    trace = _load_trace_module()
    if trace is None:
        return {}
    finder = getattr(trace, "find_task_trace_context", None)
    if not callable(finder):
        return {}
    result = finder(workspace, room_id=room_id)
    context = result if isinstance(result, dict) else {}
    if room_id or _trusted_task_id(context):
        return context
    return _unique_active_assignment_context(workspace)


def _canonical_worker_name(value: Any) -> str:
    worker = str(value or "").strip()
    if worker.startswith("@"):
        worker = worker[1:]
    if ":" in worker:
        worker = worker.split(":", 1)[0]
    return worker


def _assignment_time(meta: dict[str, Any]) -> datetime | None:
    value = str(meta.get("assigned_at") or meta.get("assignedAt") or "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _unique_active_assignment_context(workspace: Path) -> dict[str, Any]:
    """Resolve a room-less tool hook from the latest trusted assignment.

    QwenPaw's acting middleware does not expose the Matrix request context. A
    Threadmill carrier has capacity one, while stale TeamHarness task metadata
    may remain active. The newest signed assignment for the configured worker
    is authoritative. Missing timestamps or a tie for newest fail closed; tool
    input and agent request fields are never used.
    """
    worker_name = _canonical_worker_name(os.getenv("AGENTTEAMS_WORKER_NAME", ""))
    if not worker_name.startswith("threadmill-"):
        return {}
    tasks_dir = workspace / "shared" / "tasks"
    if not tasks_dir.is_dir():
        return {}
    matches: list[tuple[datetime, dict[str, Any]]] = []
    try:
        entries = sorted(tasks_dir.iterdir(), key=lambda path: path.name)
    except OSError:
        return {}
    for entry in entries:
        meta_path = entry / "meta.json"
        if not entry.is_dir() or not meta_path.is_file():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(meta, dict):
            continue
        task_id = str(meta.get("task_id") or meta.get("taskId") or "").strip()
        assignee = _canonical_worker_name(
            meta.get("assigned_to") or meta.get("assignedTo") or meta.get("assignee")
        )
        status = str(meta.get("status") or "").strip().lower()
        assigned_at = _assignment_time(meta)
        if (
            task_id == entry.name
            and task_id.startswith("threadmill-")
            and assignee == worker_name
            and status in {"assigned", "in_progress"}
            and assigned_at is not None
        ):
            matches.append((assigned_at, meta))
    if not matches:
        return {}
    newest = max(assigned_at for assigned_at, _ in matches)
    newest_matches = [meta for assigned_at, meta in matches if assigned_at == newest]
    if len(newest_matches) != 1:
        return {}
    active = newest_matches[0]
    return {
        "project_id": str(
            active.get("project_id") or active.get("projectId") or ""
        ).strip(),
        "task": active,
    }


def _trusted_task_id(context: dict[str, Any]) -> str:
    task_id = str(context.get("task_id") or context.get("taskId") or "").strip()
    if task_id:
        return task_id
    task = context.get("task")
    if not isinstance(task, dict):
        return ""
    return str(task.get("task_id") or task.get("taskId") or "").strip()


def _tool_call_data(tool_call: Any) -> tuple[str, dict[str, Any]]:
    if isinstance(tool_call, dict):
        name = str(tool_call.get("name") or tool_call.get("tool_name") or "").strip()
        raw_input = (
            tool_call.get("input")
            or tool_call.get("arguments")
            or tool_call.get("args")
            or {}
        )
    elif hasattr(tool_call, "model_dump"):
        data = tool_call.model_dump()
        name = str(data.get("name") or data.get("tool_name") or "").strip()
        raw_input = data.get("input") or data.get("arguments") or data.get("args") or {}
    else:
        name = str(
            getattr(tool_call, "name", "") or getattr(tool_call, "tool_name", "")
        ).strip()
        raw_input = (
            getattr(tool_call, "input", None)
            or getattr(tool_call, "arguments", None)
            or {}
        )
    if isinstance(raw_input, str):
        try:
            raw_input = json.loads(raw_input)
        except (TypeError, ValueError):
            raw_input = {}
    return name, raw_input if isinstance(raw_input, dict) else {}


def _path_texts(value: Any) -> list[str]:
    if isinstance(value, dict):
        result: list[str] = []
        for item in value.values():
            result.extend(_path_texts(item))
        return result
    if isinstance(value, list):
        result = []
        for item in value:
            result.extend(_path_texts(item))
        return result
    if isinstance(value, str):
        return [value]
    return []


def _is_task_path_allowed(text: str, task_id: str, workspace: Path) -> bool:
    value = str(text or "").strip().replace("\\", "/")
    if not value:
        return True
    if value == "shared/tasks" or value.endswith("/shared/tasks"):
        return False
    marker = "/shared/tasks/" if "/shared/tasks/" in value else "shared/tasks/"
    if marker not in value:
        return True
    _, suffix = value.split(marker, 1)
    suffix = suffix.strip("/")
    if not suffix:
        return False
    first = suffix.split("/", 1)[0]
    if first != task_id:
        return False
    try:
        path = Path(text).expanduser()
        if not path.is_absolute():
            path = workspace / path
        path.resolve().relative_to((workspace / "shared" / "tasks" / task_id).resolve())
    except (OSError, RuntimeError, ValueError):
        return (
            value.startswith(f"shared/tasks/{task_id}/")
            or value == f"shared/tasks/{task_id}"
        )
    return True


def _is_task_workspace_path_allowed(text: str, task_id: str, workspace: Path) -> bool:
    value = str(text or "").strip().replace("\\", "/")
    if not _is_task_path_allowed(text, task_id, workspace):
        return False
    workspace_prefix = f"shared/tasks/{task_id}/workspace"
    if value == workspace_prefix or value.startswith(f"{workspace_prefix}/"):
        return True
    if f"/{workspace_prefix}/" in value or value.endswith(f"/{workspace_prefix}"):
        return True
    try:
        path = Path(text).expanduser()
        if not path.is_absolute():
            path = workspace / path
        path.resolve().relative_to(
            (workspace / "shared" / "tasks" / task_id / "workspace").resolve()
        )
    except (OSError, RuntimeError, ValueError):
        return False
    return True


def _shell_tokens(command: str) -> list[str]:
    try:
        return shlex.split(command, posix=os.name != "nt")
    except ValueError:
        return [command]


def _shell_workspace_violation(command: str, workspace: Path, task_id: str) -> str:
    command = str(command or "").strip()
    tokens = _shell_tokens(command)
    if not command or len(tokens) < 3:
        return (
            "Threadmill task isolation requires shell commands to start with "
            f"cd shared/tasks/{task_id}/workspace &&"
        )
    if "\n" in command or "\r" in command or any(mark in command for mark in ("`", "$", "~")):
        return "Threadmill task isolation blocks shell expansion outside the bound workspace"
    if tokens[0].lower() != "cd" or tokens[2] != "&&":
        return (
            "Threadmill task isolation requires shell commands to start with "
            f"cd shared/tasks/{task_id}/workspace &&"
        )
    if not _is_task_workspace_path_allowed(tokens[1], task_id, workspace):
        return "Threadmill task isolation blocks shell execution outside the current task workspace"
    for index, token in enumerate(tokens):
        normalized = token.strip("'\"").replace("\\", "/")
        lowered = normalized.lower()
        if index > 2 and lowered in {"cd", "pushd", "popd"}:
            return "Threadmill task isolation blocks changing directory after entering the bound workspace"
        if re.search(r"(^|/)\.\.(/|$)", normalized):
            return "Threadmill task isolation blocks shell path traversal"
        if lowered.startswith("/") and lowered != "/dev/null":
            if not _is_task_workspace_path_allowed(normalized, task_id, workspace):
                return "Threadmill task isolation blocks absolute shell paths outside the bound workspace"
        if "shared/tasks/" in lowered and not _is_task_workspace_path_allowed(
            normalized, task_id, workspace
        ):
            return "Threadmill task isolation blocks shell access to another task workspace"
    private_or_host_path = re.compile(
        r"(^|[\s'\"=:(])/(?:root|home|etc|var|opt|proc|sys|run|tmp)(?:/|\b)",
        re.IGNORECASE,
    )
    if private_or_host_path.search(command):
        allowed_root = str(
            (workspace / "shared" / "tasks" / task_id / "workspace").resolve()
        ).replace("\\", "/")
        scrubbed = command.replace(allowed_root, "")
        if private_or_host_path.search(scrubbed):
            return "Threadmill task isolation blocks host or provider-private shell paths"
    return ""


def _threadmill_filesystem_violation(
    tool_name: str, arguments: dict[str, Any], workspace: Path, task_id: str
) -> str:
    tool_name = str(tool_name or "").strip().lower()
    native_file_tools = {
        "read",
        "write",
        "edit",
        "multiedit",
        "grep",
        "glob",
        "read_file",
        "write_file",
        "edit_file",
        "append_file",
        "list_directory",
        "list_files",
        "search_files",
        "grep_search",
        "glob",
        "file_search",
    }
    shell_tools = {
        "bash",
        "execute_shell_command",
        "shell_command",
        "shell",
        "run_shell",
    }
    if tool_name in shell_tools:
        command = str(arguments.get("command") or arguments.get("cmd") or "")
        normalized_command = command.replace("\\", "/").lower()
        private_markers = (
            "/sessions/",
            "/notes/",
            "/memory/",
            "/mem_metadata/",
            "/chats.json",
            "/history.db",
        )
        if any(
            marker in f"/{normalized_command.lstrip('/')}" for marker in private_markers
        ):
            return "Threadmill task isolation blocks provider-private conversation or memory state"
        return _shell_workspace_violation(command, workspace, task_id)
    elif tool_name in native_file_tools:
        path_texts = _path_texts(arguments)
    else:
        return ""
    for text in path_texts:
        normalized = str(text or "").strip().replace("\\", "/").lower()
        private_markers = (
            "/sessions/",
            "/notes/",
            "/memory/",
            "/mem_metadata/",
            "/chats.json",
            "/history.db",
        )
        if normalized in {
            "sessions",
            "notes",
            "memory",
            "mem_metadata",
            "chats.json",
            "history.db",
        } or any(marker in f"/{normalized.lstrip('/')}" for marker in private_markers):
            return "Threadmill task isolation blocks provider-private conversation or memory state"
        if (
            tool_name not in shell_tools
            and "shared/tasks/" not in normalized
            and "/shared/tasks/" not in normalized
        ):
            return (
                "Threadmill task isolation blocks native filesystem access outside "
                f"shared/tasks/{task_id}/"
            )
        if not _is_task_workspace_path_allowed(text, task_id, workspace):
            return (
                "Threadmill task isolation blocks native filesystem access outside "
                f"shared/tasks/{task_id}/workspace/"
            )
    return ""


def _runtime_room_id(agent: Any, input_kwargs: dict[str, Any]) -> str:
    for candidate in (
        input_kwargs.get("request"),
        input_kwargs.get("agent_request"),
        getattr(agent, "_request_context", None),
        getattr(agent, "request_context", None),
    ):
        room = _room_id(candidate)
        if room:
            return room
        if isinstance(candidate, dict):
            room = str(
                candidate.get("room_id") or candidate.get("roomId") or ""
            ).strip()
            if room:
                return room
    return ""


def _enforce_threadmill_task_filesystem_isolation(
    agent: Any, input_kwargs: dict[str, Any]
) -> None:
    tool_call = input_kwargs.get("tool_call")
    if tool_call is None:
        return
    workspace = Path(os.getenv("QWENPAW_WORKING_DIR", ".")) / "workspaces" / "default"
    tool_name, arguments = _tool_call_data(tool_call)
    room_id = _runtime_room_id(agent, input_kwargs)
    context = _task_context(workspace, room_id)
    task_id = _trusted_task_id(context)
    if tool_name.strip().lower() in {
        "recall_history",
        "recall_history_python",
        "search_history",
        "memorysearch",
    }:
        if task_id or os.getenv("AGENTTEAMS_WORKER_NAME", "").strip().startswith(
            "threadmill-"
        ):
            raise PermissionError(
                "Threadmill task isolation requires cross-task memory retrieval through Context Graph"
            )
    if not task_id:
        if _threadmill_filesystem_violation(
            tool_name, arguments, workspace, "__no_active_task__"
        ):
            raise PermissionError(
                "Threadmill task isolation blocks shared/tasks access without an active task identity"
            )
        return
    violation = _threadmill_filesystem_violation(
        tool_name, arguments, workspace, task_id
    )
    if violation:
        raise PermissionError(violation)


def _sanitizer_factory(_ctx: Any, _agent_config: Any):
    try:
        from agentscope.middleware import MiddlewareBase
    except ImportError:
        return None

    class TeamHarnessSanitizer(MiddlewareBase):
        async def on_acting(
            self,
            agent: Any,
            input_kwargs: dict[str, Any],
            next_handler: Callable[..., AsyncGenerator[Any, None]],
        ) -> AsyncGenerator[Any, None]:
            _enforce_threadmill_task_filesystem_isolation(agent, input_kwargs)
            async for item in next_handler(**input_kwargs):
                _sanitize(item)
                yield item

    return TeamHarnessSanitizer()


def _load_trace_module() -> Any | None:
    path = PLUGIN_DIR / "task_trace.py"
    if not path.is_file():
        return None
    spec = importlib.util.spec_from_file_location(
        "agentteams_teamharness_task_trace",
        path,
    )
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _room_id(request: Any) -> str:
    if hasattr(request, "model_dump"):
        data = request.model_dump()
    elif hasattr(request, "__dict__"):
        data = vars(request)
    else:
        data = {}
    meta = data.get("channel_meta") or data.get("meta") or {}
    room = (
        str(meta.get("room_id") or meta.get("roomId") or "")
        if isinstance(meta, dict)
        else ""
    )
    if room:
        return room
    session = str(data.get("session_id") or "")
    for prefix in ("agentteams_matrix:", "matrix:"):
        if session.startswith(prefix):
            return session[len(prefix) :]
    return ""


def _mark_threadmill_request_ephemeral(workspace: Path, request: Any) -> bool:
    """Disable provider-session persistence for one bounded invocation.

    Threadmill carries continuity through Workspace, Context Graph and formal
    artifacts. Reusing QwenPaw's Matrix-room session across independently
    leased invocations both violates that boundary and eventually consumes the
    model context window. The task trace is the trusted assignment identity;
    unbound or non-Threadmill requests retain normal QwenPaw session behavior.
    """
    worker_name = _canonical_worker_name(os.getenv("AGENTTEAMS_WORKER_NAME", ""))
    if not worker_name.startswith("threadmill-"):
        return False
    context = _task_context(workspace, _room_id(request))
    task_id = _trusted_task_id(context)
    if not task_id.startswith("threadmill-"):
        return False
    request_context = getattr(request, "request_context", None)
    if not isinstance(request_context, dict):
        request_context = {}
    else:
        request_context = dict(request_context)
    request_context["qwenpaw.ephemeral"] = True
    setattr(request, "request_context", request_context)
    return True


def _register_trace_hooks(api: Any) -> None:
    try:
        from qwenpaw.runtime.hooks import HookBase, HookResult
        from qwenpaw.runtime.phases import Phase
    except ImportError:
        return
    trace = _load_trace_module()
    if trace is None:
        return
    workspace = Path(os.getenv("QWENPAW_WORKING_DIR", ".")) / "workspaces" / "default"
    trace.register_task_trace_processor(workspace)

    class TraceEnter(HookBase):
        phase = Phase.PRE_DISPATCH
        name = "teamharness_trace_enter"
        priority = 20

        async def run(self, ctx: Any) -> Any:
            room = _room_id(ctx.request)
            if room:
                ctx.extras["teamharness_trace_token"] = trace.set_current_room(room)
            _mark_threadmill_request_ephemeral(workspace, ctx.request)
            return HookResult()

    class TraceExit(HookBase):
        phase = Phase.FINALLY
        name = "teamharness_trace_exit"
        priority = 200

        async def run(self, ctx: Any) -> Any:
            token = ctx.extras.pop("teamharness_trace_token", None)
            if token is not None:
                trace.reset_current_room(token)
            return HookResult()

    api.register_runtime_hook(TraceEnter())
    api.register_runtime_hook(TraceExit())


class TeamHarnessPlugin:
    def register(self, api: Any) -> None:
        api.register_prompt_section(
            "teamharness_context",
            after="workspace",
            provider=_team_prompt,
            priority=40,
        )
        api.register_skill_provider(
            ASSET_DIR / "qwenpaw-skills",
            enabled_by_default=True,
            channels=["all"],
        )
        api.register_middleware(_sanitizer_factory, priority=30)
        _register_trace_hooks(api)
        self._register_http(api)

    def _register_http(self, api: Any) -> None:
        try:
            from fastapi import APIRouter
        except ImportError:
            return
        router = APIRouter()

        @router.get("/health")
        def health() -> dict[str, Any]:
            return {"ok": True, "plugin": "teamharness", "adapter": "qwenpaw-2"}

        @router.post("/sync")
        def sync_endpoint() -> dict[str, Any]:
            return {
                "ok": True,
                "plugin": "teamharness",
                "managedBy": "qwenpaw-plugin-api",
            }

        api.register_http_router(
            router,
            prefix="/teamharness",
            tags=["teamharness"],
        )


plugin = TeamHarnessPlugin()
