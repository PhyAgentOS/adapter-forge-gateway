# Third-party notices

The repository's original Python code, configuration, static resources, and
documentation are licensed under Apache License 2.0 unless a file states
otherwise.

## Runtime dependencies

Except for the vendored component identified below, runtime dependencies are
installed from package indexes and are not vendored into this source repository.
They retain their respective licenses:

- `dora-rs`: MIT
- `FastAPI`: MIT
- `Uvicorn`: BSD-3-Clause
- `forge-common` and `forge-msgs`: Apache-2.0
- `opencv-python`: Apache-2.0
- `PyYAML`: MIT

Transitive dependencies retain the licenses declared by their distributions.
Consult the locked environment and installed package metadata for the complete
dependency graph.

## Vendored forge-tool

- Component: `forge-tool`
- Version: `0.1.0`
- Upstream project identity: public `Forgelab-Robotics/forge`. Its current
  public branches do not contain `packages/tool`.
- Source used: a local Forge workspace snapshot identified by
  `2f3dac8a8ccd33472109d2b763e6d3bd86e091e5`
- Original source path: `packages/tool/src/forge_tool`
- Vendored path: `src/forge_tool`
- License: Apache License 2.0
- Modifications: redistributed in this repository instead of being installed
  as an external dependency. Tool behavior is unchanged; `dora.py` imports the
  local ToolMessage carrier described below because public `forge-msgs==1.0.1`
  does not export `ToolMessage` or `ToolMessageSizeError`.

The snapshot identifier records the provenance of the local source used here;
it does not imply that `packages/tool` can be retrieved from the current public
repository at that identifier.

## ToolMessage Arrow carrier

- Component: the `ToolMessage` Arrow carrier of `forge-msgs`
- Previously vendored as `src/forge_tool/_tool_message.py`; now provided by the
  public `forge-msgs==1.3.0` dependency instead of a local copy.
- License: Apache License 2.0

## Build dependencies and binary releases

PyInstaller is used only to produce optional standalone executables and is not
vendored into this repository. A bundled executable contains runtime
dependencies and may create license, notice, source-offer, codec, or platform
obligations beyond source distribution. Review the complete bundled artifact
and all dependency licenses before publishing a binary release.

## Forge Tool 2.x dependency

The former `src/forge_tool` copy has been removed. ToolEndpoint and Dora bindings are provided by the separately distributed Apache-2.0 `forge-tool[dora]` package. Historical copy provenance above is retained for audit history.
