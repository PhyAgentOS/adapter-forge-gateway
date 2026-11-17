from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_SYMBOLS = (
    "GatewayConfig",
    "ReadinessConfig",
    "ToolGatewayConfig",
    "ToolProviderRouteConfig",
    "load_config",
    "_clamp_hz",
    "os",
    "dataclass",
    "field",
    "Path",
    "Any",
    "yaml",
)


def _subprocess_env_with_fake_package(tmp_path: Path) -> dict[str, str]:
    fake_root = tmp_path / "fake-installed-package"
    fake_package = fake_root / "forge_gateway"
    fake_package.mkdir(parents=True)
    _ = (fake_package / "__init__.py").write_text(
        'raise RuntimeError("loaded fake forge_gateway instead of checkout source")\n',
        encoding="utf-8",
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(fake_root)
    return env


def test_root_main_help_prefers_checkout_src(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "main.py"), "--help"],
        cwd=tmp_path,
        env=_subprocess_env_with_fake_package(tmp_path),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Forge unified gateway node" in result.stdout


def test_root_main_version_prefers_checkout_src(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "main.py"), "--version"],
        cwd=tmp_path,
        env=_subprocess_env_with_fake_package(tmp_path),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "forge-gateway 1.1.2\n"


def test_root_config_forwards_head_symbols_and_prefers_checkout_src(tmp_path: Path) -> None:
    code = f"""
import sys
from pathlib import Path

import config
from forge_gateway import config as implementation

project_root = Path.cwd().resolve()
expected_src = (project_root / "src").resolve()
assert sys.path[0] == str(expected_src), sys.path
assert Path(implementation.__file__).resolve().is_relative_to(expected_src)
symbols = {CONFIG_SYMBOLS!r}
for name in symbols:
    assert hasattr(config, name), name
    assert getattr(config, name) is getattr(implementation, name), name
assert {{name for name in symbols if not name.startswith("_")}} <= set(config.__all__)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=PROJECT_ROOT,
        env=_subprocess_env_with_fake_package(tmp_path),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
