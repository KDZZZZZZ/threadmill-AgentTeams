from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_DIR = REPO_ROOT / "plugins" / "teamharness" / "mcp"


def _load_server():
    spec = importlib.util.spec_from_file_location(
        "teamharness_mcp_server_workspace_test",
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


def test_default_workspace_prefers_worker_shared_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    server = _load_server()
    shared_dir = tmp_path / "teams" / "demo-team" / "shared"
    monkeypatch.setenv("TEAMHARNESS_SHARED_DIR", str(shared_dir))
    monkeypatch.setenv("QWENPAW_WORKING_DIR", str(tmp_path / "agent" / ".qwenpaw"))

    assert server._default_workspace_dir() == str(shared_dir.parent)


def test_filesync_push_existing_workspace_directory_without_trailing_slash_uses_mirror(
    tmp_path: Path,
) -> None:
    server = _load_server()
    workspace = tmp_path / "workspace"
    local_dir = workspace / "shared" / "tasks" / "task-001" / "workspace"
    local_dir.mkdir(parents=True)

    result = server._filesync(
        {
            "action": "push",
            "path": "shared/tasks/task-001/workspace",
            "workspaceDir": str(workspace),
            "storage": {"sharedPrefix": "teams/demo/shared"},
            "dryRun": True,
        },
    )

    assert result["ok"] is True
    assert result["path"] == "shared/tasks/task-001/workspace/"
    assert result["command"][0:2] == ["mc", "mirror"]
    assert result["command"][1:] == [
        "mirror",
        str(local_dir) + "/",
        "teams/demo/shared/tasks/task-001/workspace/",
        "--overwrite",
    ]


def test_filesync_push_file_without_trailing_slash_still_uses_cp(
    tmp_path: Path,
) -> None:
    server = _load_server()
    workspace = tmp_path / "workspace"
    local_file = workspace / "shared" / "tasks" / "task-001" / "result.md"
    local_file.parent.mkdir(parents=True)
    local_file.write_text("done\n", encoding="utf-8")

    result = server._filesync(
        {
            "action": "push",
            "path": "shared/tasks/task-001/result.md",
            "workspaceDir": str(workspace),
            "storage": {"sharedPrefix": "teams/demo/shared"},
            "dryRun": True,
        },
    )

    assert result["ok"] is True
    assert result["path"] == "shared/tasks/task-001/result.md"
    assert result["command"] == [
        "mc",
        "cp",
        str(local_file),
        "teams/demo/shared/tasks/task-001/result.md",
    ]


def test_filesync_push_missing_workspace_path_without_trailing_slash_stays_file_semantics(
    tmp_path: Path,
) -> None:
    server = _load_server()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    local_path = workspace / "shared" / "tasks" / "task-001" / "workspace"

    result = server._filesync(
        {
            "action": "push",
            "path": "shared/tasks/task-001/workspace",
            "workspaceDir": str(workspace),
            "storage": {"sharedPrefix": "teams/demo/shared"},
            "dryRun": True,
        },
    )

    assert result["ok"] is True
    assert result["path"] == "shared/tasks/task-001/workspace"
    assert result["command"] == [
        "mc",
        "cp",
        str(local_path),
        "teams/demo/shared/tasks/task-001/workspace",
    ]


def test_filesync_push_directory_detection_does_not_relax_boundaries(
    tmp_path: Path,
) -> None:
    server = _load_server()
    workspace = tmp_path / "workspace"
    (workspace / "global-shared" / "tasks").mkdir(parents=True)

    global_result = server._filesync(
        {
            "action": "push",
            "path": "global-shared/tasks",
            "workspaceDir": str(workspace),
            "dryRun": True,
        },
    )
    traversal_result = server._filesync(
        {
            "action": "push",
            "path": "shared/tasks/../workspace",
            "workspaceDir": str(workspace),
            "dryRun": True,
        },
    )

    assert global_result["ok"] is False
    assert "global-shared is read-only" in global_result["error"]
    assert traversal_result["ok"] is False
    assert "without '.', '..', or empty segments" in traversal_result["error"]
