from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from pathlib import Path

from forge_gateway.adapters.http_app import STATIC_DIR


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_static_resources_are_packaged_with_source() -> None:
    assert (STATIC_DIR / "index.html").is_file()
    assert (STATIC_DIR / "app.js").is_file()


def test_source_entrypoint_help_works_outside_project(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "main.py"), "--help"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Forge unified gateway node" in result.stdout


def test_source_node_version_works_outside_project(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    result = subprocess.run(
        [sys.executable, "-m", "forge_gateway", "--version"],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "forge-gateway 1.1.2\n"


def test_uv_project_is_not_a_python_distribution() -> None:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())

    assert project["tool"]["uv"]["package"] is False
    assert "build-system" not in project
    assert "scripts" not in project["project"]
