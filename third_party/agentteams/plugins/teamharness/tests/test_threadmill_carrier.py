from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import types
import zipfile


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
ADAPTER_PATH = PLUGIN_ROOT / "adapters" / "qwenpaw" / "plugin.py"
MCP_DIR = PLUGIN_ROOT / "mcp"
BUILT_PLUGIN = (
    PLUGIN_ROOT.parents[1]
    / "dist"
    / "adapters"
    / "qwenpaw"
    / "teamharness-qwenpaw.zip"
)


def _load_qwenpaw_adapter():
    spec = importlib.util.spec_from_file_location(
        "teamharness_qwenpaw_threadmill_test",
        ADAPTER_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_mcp_server():
    spec = importlib.util.spec_from_file_location(
        "teamharness_mcp_threadmill_test",
        MCP_DIR / "server.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(MCP_DIR))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(MCP_DIR))
    return module


def test_qwenpaw_adapter_allows_only_current_threadmill_task_filesystem(
    tmp_path, monkeypatch
) -> None:
    module = _load_qwenpaw_adapter()
    workspace = tmp_path / ".qwenpaw" / "workspaces" / "default"
    current = workspace / "shared" / "tasks" / "task-a"
    current_workspace = current / "workspace"
    foreign = workspace / "shared" / "tasks" / "task-b"
    current_workspace.mkdir(parents=True)
    (foreign / "workspace").mkdir(parents=True)
    monkeypatch.setenv("QWENPAW_WORKING_DIR", str(tmp_path / ".qwenpaw"))
    monkeypatch.setattr(
        module,
        "_task_context",
        lambda _workspace, room_id: {"task": {"task_id": "task-a"}}
        if room_id == "!task:hs.local"
        else {},
    )

    agent = types.SimpleNamespace(_request_context={"room_id": "!task:hs.local"})
    module._enforce_threadmill_task_filesystem_isolation(
        agent,
        {
            "tool_call": {
                "name": "Read",
                "input": {"file_path": str(current_workspace / "result.md")},
            }
        },
    )
    module._enforce_threadmill_task_filesystem_isolation(
        agent,
        {
            "tool_call": {
                "name": "Bash",
                "input": {
                    "command": "cd shared/tasks/task-a/workspace && npm test"
                },
            }
        },
    )
    for tool_call in (
        {"name": "Read", "input": {"file_path": str(current / "spec.md")}},
        {"name": "Read", "input": {"file_path": str(foreign / "workspace" / "secret.md")}},
        {"name": "Bash", "input": {"command": "ls shared/tasks"}},
        {
            "name": "Bash",
            "input": {
                "command": "cd shared/tasks/task-a/workspace && pwd && ls -la ~"
            },
        },
        {
            "name": "Bash",
            "input": {
                "command": "cd shared/tasks/task-a/workspace && cd / && ls"
            },
        },
        {"name": "Grep", "input": {"path": "shared/tasks/task-b", "pattern": "secret"}},
        {
            "name": "Read",
            "input": {"file_path": str(workspace / "notes" / "old-task.md")},
        },
        {
            "name": "Read",
            "input": {"file_path": str(workspace / "memory" / "2026-08-13.md")},
        },
    ):
        try:
            module._enforce_threadmill_task_filesystem_isolation(
                agent, {"tool_call": tool_call}
            )
        except PermissionError as exc:
            assert "Threadmill task isolation" in str(exc)
        else:
            raise AssertionError(f"foreign task access was allowed: {tool_call}")

    for tool_call in (
        {"name": "Read", "input": {"file_path": "history.db"}},
        {"name": "Bash", "input": {"command": "sqlite3 history.db '.tables'"}},
        {"name": "MemorySearch", "input": {"query": "previous task"}},
    ):
        try:
            module._enforce_threadmill_task_filesystem_isolation(
                agent, {"tool_call": tool_call}
            )
        except PermissionError as exc:
            assert "Threadmill task isolation" in str(exc)
        else:
            raise AssertionError(
                f"provider-private memory access was allowed: {tool_call}"
            )

    unbound_agent = types.SimpleNamespace(
        _request_context={"room_id": "!unknown:hs.local"}
    )
    try:
        module._enforce_threadmill_task_filesystem_isolation(
            unbound_agent,
            {"tool_call": {"name": "Read", "input": {"file_path": "README.md"}}},
        )
    except PermissionError as exc:
        assert "without an active task identity" in str(exc)
    else:
        raise AssertionError("unbound shared/tasks access was allowed")


def test_qwenpaw_adapter_marks_bound_threadmill_invocation_ephemeral(
    tmp_path, monkeypatch
) -> None:
    module = _load_qwenpaw_adapter()
    workspace = tmp_path / ".qwenpaw" / "workspaces" / "default"
    request = types.SimpleNamespace(
        session_id="matrix:!threadmill:hs.local",
        request_context={"preserved": "value"},
    )
    monkeypatch.setenv("AGENTTEAMS_WORKER_NAME", "threadmill-phase-a")
    monkeypatch.setattr(
        module,
        "_task_context",
        lambda _workspace, _room_id: {
            "task": {"task_id": "threadmill-provider-task-a"}
        },
    )

    assert module._mark_threadmill_request_ephemeral(workspace, request)
    assert request.request_context == {
        "preserved": "value",
        "qwenpaw.ephemeral": True,
    }

    monkeypatch.setenv("AGENTTEAMS_WORKER_NAME", "ordinary-worker")
    ordinary = types.SimpleNamespace(session_id="matrix:!ordinary:hs.local")
    assert not module._mark_threadmill_request_ephemeral(workspace, ordinary)
    assert not hasattr(ordinary, "request_context")


def test_qwenpaw_adapter_decodes_real_tool_call_json_input(
    tmp_path, monkeypatch
) -> None:
    module = _load_qwenpaw_adapter()
    workspace = tmp_path / ".qwenpaw" / "workspaces" / "default"
    current = workspace / "shared" / "tasks" / "task-a" / "workspace"
    current.mkdir(parents=True)
    monkeypatch.setenv("QWENPAW_WORKING_DIR", str(tmp_path / ".qwenpaw"))
    monkeypatch.setattr(
        module,
        "_task_context",
        lambda _workspace, _room_id: {"task": {"task_id": "task-a"}},
    )

    class RealToolCallShape:
        def __init__(self, name: str, arguments: dict[str, str]) -> None:
            self.name = name
            self.arguments = arguments

        def model_dump(self) -> dict[str, str]:
            return {"name": self.name, "input": json.dumps(self.arguments)}

    agent = types.SimpleNamespace(_request_context={"room_id": "!task:hs.local"})
    module._enforce_threadmill_task_filesystem_isolation(
        agent,
        {
            "tool_call": RealToolCallShape(
                "Read", {"file_path": str(current / "result.md")}
            )
        },
    )
    for tool_call in (
        RealToolCallShape("Read", {"file_path": "/root/.qwenpaw/history.db"}),
        RealToolCallShape(
            "Bash",
            {
                "command": (
                    "cd shared/tasks/task-a/workspace && "
                    "find . -maxdepth 2 -type f && ls -la ~"
                )
            },
        ),
    ):
        try:
            module._enforce_threadmill_task_filesystem_isolation(
                agent, {"tool_call": tool_call}
            )
        except PermissionError as exc:
            assert "Threadmill task isolation" in str(exc)
        else:
            raise AssertionError("real QwenPaw ToolCallBlock shape escaped isolation")


def test_qwenpaw_adapter_binds_identity_from_assignment_context(
    tmp_path, monkeypatch
) -> None:
    module = _load_qwenpaw_adapter()
    workspace = tmp_path / ".qwenpaw" / "workspaces" / "default"
    task_id = "threadmill-provider-task-a"
    room_id = "!phase-a-room:hs.local"
    task_workspace = workspace / "shared" / "tasks" / task_id / "workspace"
    task_workspace.mkdir(parents=True)
    (workspace / "shared" / "tasks" / task_id / "meta.json").write_text(
        json.dumps(
            {
                "task_id": task_id,
                "project_id": "project-threadmill",
                "room_id": room_id,
                "assigned_to": "threadmill-phase-a",
                "status": "in_progress",
            }
        ),
        encoding="utf-8",
    )
    foreign_workspace = (
        workspace / "shared" / "tasks" / "threadmill-provider-task-b" / "workspace"
    )
    foreign_workspace.mkdir(parents=True)
    (foreign_workspace.parent / "meta.json").write_text(
        json.dumps(
            {
                "task_id": "threadmill-provider-task-b",
                "project_id": "project-threadmill",
                "room_id": room_id,
                "assigned_to": "threadmill-other",
                "status": "in_progress",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("QWENPAW_WORKING_DIR", str(tmp_path / ".qwenpaw"))
    monkeypatch.setenv("AGENTTEAMS_WORKER_NAME", "threadmill-phase-a")

    class RealToolCallShape:
        def __init__(self, name: str, arguments: dict[str, str]) -> None:
            self.name = name
            self.arguments = arguments

        def model_dump(self) -> dict[str, str]:
            return {"name": self.name, "input": json.dumps(self.arguments)}

    agent = types.SimpleNamespace(_request_context={"room_id": room_id})
    module._enforce_threadmill_task_filesystem_isolation(
        agent,
        {
            "tool_call": RealToolCallShape(
                "Read", {"file_path": str(task_workspace / "input.txt")}
            )
        },
    )
    module._enforce_threadmill_task_filesystem_isolation(
        agent,
        {
            "tool_call": RealToolCallShape(
                "Bash",
                {"command": f"cd shared/tasks/{task_id}/workspace && pwd"},
            )
        },
    )

    for tool_call in (
        RealToolCallShape("Read", {"file_path": str(task_workspace.parent / "spec.md")}),
        RealToolCallShape("Read", {"file_path": str(foreign_workspace / "secret.md")}),
        RealToolCallShape(
            "Bash",
            {
                "command": (
                    f"cd shared/tasks/{task_id}/workspace && "
                    "cat ../spec.md"
                )
            },
        ),
    ):
        try:
            module._enforce_threadmill_task_filesystem_isolation(
                agent,
                {
                    "task_id": "threadmill-provider-task-b",
                    "tool_call": tool_call,
                },
            )
        except PermissionError as exc:
            assert "Threadmill task isolation" in str(exc)
        else:
            raise AssertionError(
                "assignment-bound isolation allowed self-reported or out-of-workspace access"
            )

    untrusted_agent = types.SimpleNamespace(
        _request_context={"room_id": "!unknown-room:hs.local", "task_id": task_id}
    )
    try:
        module._enforce_threadmill_task_filesystem_isolation(
            untrusted_agent,
            {
                "task_id": task_id,
                "tool_call": RealToolCallShape(
                    "Read", {"file_path": str(task_workspace / "input.txt")}
                ),
            },
        )
    except PermissionError as exc:
        assert "without an active task identity" in str(exc)
    else:
        raise AssertionError("agent self-reported task identity was trusted")


def test_qwenpaw_adapter_binds_roomless_hook_only_to_unique_active_assignment(
    tmp_path, monkeypatch
) -> None:
    module = _load_qwenpaw_adapter()
    workspace = tmp_path / ".qwenpaw" / "workspaces" / "default"
    tasks = workspace / "shared" / "tasks"
    task_id = "threadmill-provider-task-a"
    task_workspace = tasks / task_id / "workspace"
    task_workspace.mkdir(parents=True)
    (tasks / task_id / "meta.json").write_text(
        json.dumps(
            {
                "task_id": task_id,
                "project_id": "project-threadmill",
                "room_id": "!phase-a-room:hs.local",
                "assigned_to": "@threadmill-phase-a:hs.local",
                "status": "in_progress",
                "assigned_at": "2026-08-14T09:07:12Z",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("QWENPAW_WORKING_DIR", str(tmp_path / ".qwenpaw"))
    monkeypatch.setenv("AGENTTEAMS_WORKER_NAME", "threadmill-phase-a")

    agent = types.SimpleNamespace()
    tool_call = {
        "name": "Read",
        "input": json.dumps({"file_path": str(task_workspace / "input.txt")}),
    }
    module._enforce_threadmill_task_filesystem_isolation(
        agent, {"tool_call": tool_call}
    )

    second_id = "threadmill-provider-task-b"
    second_workspace = tasks / second_id / "workspace"
    second_workspace.mkdir(parents=True)
    (tasks / second_id / "meta.json").write_text(
        json.dumps(
            {
                "task_id": second_id,
                "project_id": "project-threadmill",
                "room_id": "!phase-a-other:hs.local",
                "assigned_to": "threadmill-phase-a",
                "status": "assigned",
                "assigned_at": "2026-08-14T08:07:12Z",
            }
        ),
        encoding="utf-8",
    )
    module._enforce_threadmill_task_filesystem_isolation(
        agent, {"task_id": second_id, "tool_call": tool_call}
    )

    tied_id = "threadmill-provider-task-c"
    tied_workspace = tasks / tied_id / "workspace"
    tied_workspace.mkdir(parents=True)
    (tasks / tied_id / "meta.json").write_text(
        json.dumps(
            {
                "task_id": tied_id,
                "project_id": "project-threadmill",
                "room_id": "!phase-a-tied:hs.local",
                "assigned_to": "threadmill-phase-a",
                "status": "in_progress",
                "assigned_at": "2026-08-14T09:07:12Z",
            }
        ),
        encoding="utf-8",
    )
    try:
        module._enforce_threadmill_task_filesystem_isolation(
            agent, {"task_id": task_id, "tool_call": tool_call}
        )
    except PermissionError as exc:
        assert "without an active task identity" in str(exc)
    else:
        raise AssertionError("ambiguous room-less assignment was trusted")


def test_packaged_qwenpaw_plugin_enforces_real_tool_call_shape(
    tmp_path, monkeypatch
) -> None:
    with zipfile.ZipFile(BUILT_PLUGIN) as archive:
        entry = next(
            name
            for name in archive.namelist()
            if name.count("/") == 1 and name.endswith("/plugin.py")
        )
        packaged = archive.read(entry).decode("utf-8")
        archive.extractall(tmp_path)

    assert "def _enforce_threadmill_task_filesystem_isolation" in packaged
    for native_tool in ('"bash"', '"read"', '"write"', '"grep"', '"glob"'):
        assert native_tool in packaged
    for private_path in ("notes", "memory", "mem_metadata", "history.db"):
        assert private_path in packaged

    spec = importlib.util.spec_from_file_location(
        "packaged_teamharness_threadmill_test", tmp_path / entry
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    workspace = tmp_path / ".qwenpaw" / "workspaces" / "default"
    task_workspace = workspace / "shared" / "tasks" / "task-a" / "workspace"
    task_workspace.mkdir(parents=True)
    monkeypatch.setenv("QWENPAW_WORKING_DIR", str(tmp_path / ".qwenpaw"))
    monkeypatch.setattr(
        module,
        "_task_context",
        lambda _workspace, _room_id: {"task_id": "task-a"},
    )

    class RealToolCallShape:
        def model_dump(self) -> dict[str, str]:
            return {
                "name": "Read",
                "input": json.dumps({"file_path": "/root/.qwenpaw/history.db"}),
            }

    try:
        module._enforce_threadmill_task_filesystem_isolation(
            types.SimpleNamespace(_request_context={"room_id": "!task:hs.local"}),
            {"tool_call": RealToolCallShape()},
        )
    except PermissionError as exc:
        assert "Threadmill task isolation" in str(exc)
    else:
        raise AssertionError("packaged plugin allowed provider-private Read")


def test_taskflow_submit_accepts_empty_deliverables_and_sets_result_path(
    tmp_path, monkeypatch
) -> None:
    server = _load_mcp_server()
    workspace = tmp_path / "workspace"
    task_id = "threadmill-task-001"
    task_dir = workspace / "shared" / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "result.md").write_text("done\n", encoding="utf-8")
    (task_dir / "meta.json").write_text(
        '{"task_id":"threadmill-task-001","project_id":"carrier","room_id":"!task:hs.local","status":"in_progress"}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(server, "_sync_task", lambda *_args, **_kwargs: True)

    response = server.call_tool(
        "taskflow",
        {
            "workspaceDir": str(workspace),
            "role": "worker",
            "action": "submit_task",
            "taskId": task_id,
            "status": "SUCCESS",
            "summary": "completed",
            "deliverables": [],
        },
    )
    result = json.loads(response["content"][0]["text"])

    assert result["ok"] is True
    assert result["task"]["deliverables"] == []
    assert result["task"]["result_path"] == f"shared/tasks/{task_id}/result.md"
