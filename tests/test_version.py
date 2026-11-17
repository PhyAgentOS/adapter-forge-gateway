from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path

from forge_gateway import __version__
from forge_gateway.adapters.http_app import create_app
from forge_msgs import ToolMessage, ToolMessageSizeError


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeRuntime:
    pass


def test_public_versions_match_project_metadata() -> None:
    metadata = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert metadata["project"]["version"] == __version__ == "1.1.2"
    assert create_app(FakeRuntime()).version == __version__


def test_forge_dependencies_use_vendored_tool() -> None:
    metadata = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = metadata["project"]["dependencies"]

    assert "forge-msgs==1.3.0" in dependencies
    assert "forge-common>=1.0.0,<2" in dependencies
    assert not any(dependency.startswith("forge-tool") for dependency in dependencies)
    assert (PROJECT_ROOT / "src" / "forge_tool" / "__init__.py").is_file()

    notices = (PROJECT_ROOT / "THIRD_PARTY_NOTICES.md").read_text(encoding="utf-8")
    assert "2f3dac8a8ccd33472109d2b763e6d3bd86e091e5" in notices  # pragma: allowlist secret


def test_tool_carrier_ships_with_public_forge_msgs() -> None:
    assert version("forge-msgs") == "1.3.0"
    assert ToolMessage.__module__ == "forge_msgs.tool"
    assert ToolMessageSizeError.__module__ == "forge_msgs.tool"
