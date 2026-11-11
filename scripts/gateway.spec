# -*- mode: python ; coding: utf-8 -*-

import os

from PyInstaller.utils.hooks import collect_submodules

_spec_dir = os.path.dirname(os.path.abspath(SPEC))
_root_dir = os.path.dirname(_spec_dir)
_src_dir = os.path.join(_root_dir, "src")
_entry = os.path.join(_src_dir, "forge_gateway", "__main__.py")
_resources_dir = os.path.join(_src_dir, "forge_gateway", "resources")

hiddenimports = (
    collect_submodules("forge_gateway")
    + collect_submodules("uvicorn")
    + collect_submodules("fastapi")
    + collect_submodules("forge_msgs")
    + collect_submodules("forge_tool")
    + [
        "cv2",
        "dora",
        "pyarrow",
    ]
)

a = Analysis(
    [_entry],
    pathex=[_src_dir],
    binaries=[],
    datas=[
        (_resources_dir, os.path.join("forge_gateway", "resources")),
    ],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="gateway",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
