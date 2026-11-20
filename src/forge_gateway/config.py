"""Gateway node configuration."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from forge_gateway.domain.tool_catalog import RobotFrameProfile, ToolSpec

__all__ = [
    "GatewayConfig",
    "ReadinessConfig",
    "ToolGatewayConfig",
    "ToolProviderRouteConfig",
    "load_config",
]

_GATEWAY_CONFIG_KEYS = frozenset(
    {
        "joint_order",
        "image_input_ids",
        "host",
        "port",
        "state_broadcast_hz",
        "broadcast_hz",
        "image_broadcast_hz",
        "ws_send_timeout_sec",
        "jpeg_quality",
        "policy_id",
        "command_queue_capacity",
        "readiness",
        "tools",
    }
)
_READINESS_CONFIG_KEYS = frozenset(
    {
        "require_proprio_state",
        "require_images",
        "require_state_client",
        "require_image_client",
        "proprio_stale_after_sec",
        "image_stale_after_sec",
    }
)
_TOOL_GATEWAY_CONFIG_KEYS = frozenset(
    {
        "enabled",
        "lease_ttl_ms",
        "invoke_timeout_ms",
        "ambiguous_fence_ttl_ms",
        "request_input_id",
        "response_output_id",
        "invocation_capacity",
        "event_capacity",
        "retention_sec",
        "specs",
        "providers",
    }
)
_TOOL_PROVIDER_ROUTE_CONFIG_KEYS = frozenset({"endpoint_id", "input_id", "output_id"})
_MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991
_RESERVED_GATEWAY_INPUT_IDS = frozenset(
    {
        "tick",
        "proprio_state",
        "action",
        "runtime_status",
        "record_status",
        "playback_status",
    }
)
_RESERVED_GATEWAY_OUTPUT_IDS = frozenset({"policy_command"})


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"duplicate YAML key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _reject_unknown_keys(
    data: Mapping[str, Any], allowed_keys: frozenset[str], context: str
) -> None:
    unknown_keys = [key for key in data if key not in allowed_keys]
    if unknown_keys:
        rendered_keys = ", ".join(sorted(repr(key) for key in unknown_keys))
        raise ValueError(f"unknown {context} config key(s): {rendered_keys}")


def _require_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _require_finite_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValueError(f"{field_name} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be a finite number")
    return number


def _require_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_non_blank_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_unique_non_empty_strings(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list of strings")

    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item:
            raise ValueError(f"{field_name} entries must be non-empty strings")
        if item in seen:
            raise ValueError(f"{field_name} entries must be unique")
        seen.add(item)
        result.append(item)
    return result


@dataclass(frozen=True)
class ReadinessConfig:
    """Conditions required before /runtime/start may emit start."""

    require_proprio_state: bool = True
    require_images: bool = True
    require_state_client: bool = False
    require_image_client: bool = False
    proprio_stale_after_sec: float | None = 2.0
    image_stale_after_sec: float = 2.0

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ReadinessConfig":
        if data is None:
            return cls()
        if not isinstance(data, Mapping):
            raise ValueError("readiness config must be a YAML mapping or null")
        _reject_unknown_keys(data, _READINESS_CONFIG_KEYS, "readiness")

        proprio_stale_raw = data.get("proprio_stale_after_sec", 2.0)
        proprio_stale_after_sec = (
            None
            if proprio_stale_raw is None
            else max(
                0.1,
                _require_finite_float(
                    proprio_stale_raw, "readiness.proprio_stale_after_sec"
                ),
            )
        )
        return cls(
            require_proprio_state=_require_bool(
                data.get("require_proprio_state", True),
                "readiness.require_proprio_state",
            ),
            require_images=_require_bool(
                data.get("require_images", True), "readiness.require_images"
            ),
            require_state_client=_require_bool(
                data.get("require_state_client", False),
                "readiness.require_state_client",
            ),
            require_image_client=_require_bool(
                data.get("require_image_client", False),
                "readiness.require_image_client",
            ),
            proprio_stale_after_sec=proprio_stale_after_sec,
            image_stale_after_sec=max(
                0.1,
                _require_finite_float(
                    data.get("image_stale_after_sec", 2.0),
                    "readiness.image_stale_after_sec",
                ),
            ),
        )


@dataclass(frozen=True)
class ToolProviderRouteConfig:
    """One configured provider's shared bidirectional Dora route."""

    endpoint_id: str
    input_id: str
    output_id: str

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        context: str = "tools provider",
    ) -> "ToolProviderRouteConfig":
        if not isinstance(data, Mapping):
            raise ValueError(f"{context} must be a YAML mapping")
        _reject_unknown_keys(data, _TOOL_PROVIDER_ROUTE_CONFIG_KEYS, context)
        missing_keys = [
            key for key in _TOOL_PROVIDER_ROUTE_CONFIG_KEYS if key not in data
        ]
        if missing_keys:
            rendered_keys = ", ".join(sorted(missing_keys))
            raise ValueError(f"missing {context} config key(s): {rendered_keys}")
        string_fields = {
            field_name: _require_non_blank_string(
                data[field_name], f"{context}.{field_name}"
            )
            for field_name in _TOOL_PROVIDER_ROUTE_CONFIG_KEYS
        }
        return cls(**string_fields)


@dataclass(frozen=True)
class ToolGatewayConfig:
    """Gateway-owned Tool discovery and Query/Action invocation configuration."""

    enabled: bool = False
    lease_ttl_ms: int = 15_000
    invoke_timeout_ms: int = 5_000
    # How long an Action closed as `unknown` may keep its concurrency fence while the
    # provider has still not reported any outcome. After this window the fence is
    # released so one silent provider cannot block its (endpoint, operation) slot for
    # the lifetime of the process.
    ambiguous_fence_ttl_ms: int = 30_000
    request_input_id: str = "tool_request"
    response_output_id: str = "tool_response"
    invocation_capacity: int = 128
    event_capacity: int = 128
    retention_sec: float = 300.0
    specs: list[ToolSpec] = field(default_factory=list)
    providers: list[ToolProviderRouteConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ToolGatewayConfig":
        if data is None:
            return cls()
        if not isinstance(data, Mapping):
            raise ValueError("tools config must be a YAML mapping or null")
        _reject_unknown_keys(data, _TOOL_GATEWAY_CONFIG_KEYS, "tools")

        lease_ttl_ms = _positive_safe_integer(
            data.get("lease_ttl_ms", 15_000), "tools.lease_ttl_ms"
        )
        invoke_timeout_ms = _positive_safe_integer(
            data.get("invoke_timeout_ms", 5_000), "tools.invoke_timeout_ms"
        )
        ambiguous_fence_ttl_ms = _positive_safe_integer(
            data.get("ambiguous_fence_ttl_ms", 30_000),
            "tools.ambiguous_fence_ttl_ms",
        )
        request_input_id = _require_non_blank_string(
            data.get("request_input_id", "tool_request"),
            "tools.request_input_id",
        )
        response_output_id = _require_non_blank_string(
            data.get("response_output_id", "tool_response"),
            "tools.response_output_id",
        )
        invocation_capacity = _positive_safe_integer(
            data.get("invocation_capacity", 128), "tools.invocation_capacity"
        )
        event_capacity = _positive_safe_integer(
            data.get("event_capacity", 128), "tools.event_capacity"
        )
        retention_sec = _require_finite_float(
            data.get("retention_sec", 300.0), "tools.retention_sec"
        )
        if retention_sec < 0:
            raise ValueError("tools.retention_sec must be non-negative")
        specs_raw = data.get("specs", [])
        if not isinstance(specs_raw, list):
            raise ValueError("tools.specs must be a list")
        specs = [
            _tool_spec_from_dict(spec, context=f"tools.specs[{index}]")
            for index, spec in enumerate(specs_raw)
        ]
        if len({spec.tool_id for spec in specs}) != len(specs):
            raise ValueError("tools.specs tool_id values must be unique")
        bindings = [(spec.endpoint_id, spec.operation) for spec in specs]
        if len(set(bindings)) != len(bindings):
            raise ValueError(
                "tools.specs endpoint_id/operation bindings must be unique"
            )
        providers_raw = data.get("providers", [])
        if not isinstance(providers_raw, list):
            raise ValueError("tools.providers must be a list")
        providers = [
            ToolProviderRouteConfig.from_dict(
                provider,
                context=f"tools.providers[{index}]",
            )
            for index, provider in enumerate(providers_raw)
        ]
        enabled = _require_bool(data.get("enabled", False), "tools.enabled")
        if enabled and not providers:
            raise ValueError("tools.providers must not be empty when enabled")
        config = cls(
            enabled=enabled,
            lease_ttl_ms=lease_ttl_ms,
            invoke_timeout_ms=invoke_timeout_ms,
            ambiguous_fence_ttl_ms=ambiguous_fence_ttl_ms,
            request_input_id=request_input_id,
            response_output_id=response_output_id,
            invocation_capacity=invocation_capacity,
            event_capacity=event_capacity,
            retention_sec=retention_sec,
            specs=specs,
            providers=providers,
        )
        configured_endpoints = {provider.endpoint_id for provider in providers}
        unbound_specs = [
            spec.tool_id
            for spec in specs
            if spec.endpoint_id not in configured_endpoints
        ]
        if enabled and unbound_specs:
            raise ValueError(
                "tools.specs reference unconfigured endpoint(s): "
                + ", ".join(sorted(unbound_specs))
            )
        _validate_tool_port_ids(config)
        return config


_TOOL_SPEC_KEYS = frozenset(
    {
        "tool_id",
        "implementation_id",
        "endpoint_id",
        "operation",
        "semantics",
        "description",
        "input_schema",
        "output_schema",
        "readiness",
        "robot_frame_profile",
    }
)
_ROBOT_FRAME_PROFILE_KEYS = frozenset(
    {"robot_id", "base_frame", "tool_frame", "frames", "directions"}
)
_ROBOT_DIRECTION_KEYS = frozenset({"frame", "axis", "sign"})


def _tool_spec_from_dict(data: Any, *, context: str) -> ToolSpec:
    if not isinstance(data, Mapping):
        raise ValueError(f"{context} must be a YAML mapping")
    _reject_unknown_keys(data, _TOOL_SPEC_KEYS, context)
    required = ("tool_id", "implementation_id", "endpoint_id", "operation", "semantics")
    missing = [key for key in required if key not in data]
    if missing:
        raise ValueError(f"missing {context} config key(s): {', '.join(missing)}")
    input_schema = data.get("input_schema", {})
    output_schema = data.get("output_schema", {})
    if not isinstance(input_schema, Mapping) or not isinstance(output_schema, Mapping):
        raise ValueError(f"{context} schemas must be YAML mappings")
    readiness_raw = data.get("readiness", [])
    readiness = tuple(
        _require_unique_non_empty_strings(readiness_raw, f"{context}.readiness")
    )
    frame_raw = data.get("robot_frame_profile")
    frame_profile = (
        None
        if frame_raw is None
        else _robot_frame_profile_from_dict(
            frame_raw, context=f"{context}.robot_frame_profile"
        )
    )
    return ToolSpec(
        tool_id=_require_non_blank_string(data["tool_id"], f"{context}.tool_id"),
        implementation_id=_require_non_blank_string(
            data["implementation_id"], f"{context}.implementation_id"
        ),
        endpoint_id=_require_non_blank_string(
            data["endpoint_id"], f"{context}.endpoint_id"
        ),
        operation=_require_non_blank_string(data["operation"], f"{context}.operation"),
        semantics=_require_non_blank_string(data["semantics"], f"{context}.semantics"),
        description=data.get("description", ""),
        input_schema=dict(input_schema),
        output_schema=dict(output_schema),
        readiness=readiness,
        robot_frame_profile=frame_profile,
    )


def _robot_frame_profile_from_dict(data: Any, *, context: str) -> RobotFrameProfile:
    if not isinstance(data, Mapping):
        raise ValueError(f"{context} must be a YAML mapping")
    _reject_unknown_keys(data, _ROBOT_FRAME_PROFILE_KEYS, context)
    if "robot_id" not in data or "base_frame" not in data:
        raise ValueError(f"{context} requires robot_id and base_frame")
    frames = data.get("frames", {})
    if not isinstance(frames, Mapping):
        raise ValueError(f"{context}.frames must be a YAML mapping")
    directions = _robot_directions_from_dict(
        data.get("directions", {}), context=f"{context}.directions"
    )
    tool_frame = data.get("tool_frame")
    return RobotFrameProfile(
        robot_id=_require_non_blank_string(data["robot_id"], f"{context}.robot_id"),
        base_frame=_require_non_blank_string(
            data["base_frame"], f"{context}.base_frame"
        ),
        tool_frame=(
            None
            if tool_frame is None
            else _require_non_blank_string(tool_frame, f"{context}.tool_frame")
        ),
        frames=dict(frames),
        directions=directions,
    )


def _robot_directions_from_dict(
    data: Any, *, context: str
) -> dict[str, dict[str, str | int]]:
    if not isinstance(data, Mapping):
        raise ValueError(f"{context} must be a YAML mapping")  # noqa: TRY004
    directions: dict[str, dict[str, str | int]] = {}
    for raw_name, raw_direction in data.items():
        name = _require_non_blank_string(raw_name, f"{context} direction name")
        direction_context = f"{context}.{name}"
        if not isinstance(raw_direction, Mapping):
            raise ValueError(  # noqa: TRY004
                f"{direction_context} must be a YAML mapping"
            )
        _reject_unknown_keys(raw_direction, _ROBOT_DIRECTION_KEYS, direction_context)
        missing = sorted(_ROBOT_DIRECTION_KEYS - set(raw_direction))
        if missing:
            raise ValueError(
                f"missing {direction_context} config key(s): {', '.join(missing)}"
            )
        axis = _require_non_blank_string(
            raw_direction["axis"], f"{direction_context}.axis"
        )
        if axis not in ("x", "y", "z"):
            raise ValueError(f"{direction_context}.axis must be x, y, or z")
        sign = _require_int(raw_direction["sign"], f"{direction_context}.sign")
        if sign not in (-1, 1):
            raise ValueError(f"{direction_context}.sign must be -1 or 1")
        directions[name] = {
            "frame": _require_non_blank_string(
                raw_direction["frame"], f"{direction_context}.frame"
            ),
            "axis": axis,
            "sign": sign,
        }
    return directions


def _positive_safe_integer(value: Any, field_name: str) -> int:
    result = _require_int(value, field_name)
    if not 1 <= result <= _MAX_SAFE_JSON_INTEGER:
        raise ValueError(
            f"{field_name} must be a positive integer not greater than "
            f"{_MAX_SAFE_JSON_INTEGER}"
        )
    return result


def _validate_tool_port_ids(
    tools: ToolGatewayConfig,
    *,
    image_input_ids: list[str] | None = None,
) -> None:
    if not tools.enabled:
        return
    input_ids = [(tools.request_input_id, "request_input_id")]
    output_ids = [(tools.response_output_id, "response_output_id")]
    endpoint_ids: list[tuple[str, str]] = []
    for index, provider in enumerate(tools.providers):
        endpoint_ids.append((provider.endpoint_id, f"providers[{index}].endpoint_id"))
        input_ids.append((provider.input_id, f"providers[{index}].input_id"))
        output_ids.append((provider.output_id, f"providers[{index}].output_id"))

    reserved_inputs = _RESERVED_GATEWAY_INPUT_IDS | frozenset(image_input_ids or ())
    for identifier, field_name in input_ids:
        if identifier in reserved_inputs:
            raise ValueError(
                f"Tool {field_name} {identifier!r} conflicts with a reserved "
                "Gateway input"
            )
    for identifier, field_name in output_ids:
        if identifier in _RESERVED_GATEWAY_OUTPUT_IDS:
            raise ValueError(
                f"Tool {field_name} {identifier!r} conflicts with a reserved "
                "Gateway output"
            )

    for identifier_kind, identifiers in (
        ("endpoint", endpoint_ids),
        ("input", input_ids),
        ("output", output_ids),
    ):
        seen: dict[str, str] = {}
        for identifier, field_name in identifiers:
            previous_field = seen.get(identifier)
            if previous_field is not None:
                raise ValueError(
                    f"duplicate Tool {identifier_kind} ID {identifier!r} "
                    f"in {previous_field} and {field_name}"
                )
            seen[identifier] = field_name


@dataclass(frozen=True)
class GatewayConfig:
    """Unified HTTP/WebSocket gateway configuration."""

    joint_order: list[str]
    image_input_ids: list[str] = field(default_factory=list)
    host: str = "127.0.0.1"
    port: int = 9001
    state_broadcast_hz: float = 50.0
    image_broadcast_hz: float = 24.0
    ws_send_timeout_sec: float = 1.0
    jpeg_quality: int = 85
    policy_id: str = "default"
    command_queue_capacity: int = 256
    readiness: ReadinessConfig = field(default_factory=ReadinessConfig)
    tools: ToolGatewayConfig = field(default_factory=ToolGatewayConfig)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GatewayConfig":
        if not isinstance(data, Mapping):
            raise ValueError("gateway config must be a YAML mapping")
        _reject_unknown_keys(data, _GATEWAY_CONFIG_KEYS, "gateway")
        if "state_broadcast_hz" in data and "broadcast_hz" in data:
            raise ValueError(
                "state_broadcast_hz and legacy broadcast_hz cannot both be set"
            )

        joint_order = _require_unique_non_empty_strings(
            data.get("joint_order", []), "joint_order"
        )
        image_input_ids = _require_unique_non_empty_strings(
            data.get("image_input_ids", []), "image_input_ids"
        )
        host = _require_non_empty_string(data.get("host", "127.0.0.1"), "host")
        policy_id = _require_non_empty_string(
            data.get("policy_id", "default"), "policy_id"
        )

        port = _require_int(data.get("port", 9001), "port")
        if not 1 <= port <= 65535:
            raise ValueError("port must be in [1, 65535]")

        jpeg_quality = _require_int(data.get("jpeg_quality", 85), "jpeg_quality")
        if not 1 <= jpeg_quality <= 100:
            raise ValueError("jpeg_quality must be in [1, 100]")

        command_queue_capacity = _require_int(
            data.get("command_queue_capacity", 256), "command_queue_capacity"
        )
        if command_queue_capacity < 1:
            raise ValueError("command_queue_capacity must be at least 1")

        state_broadcast_raw = (
            data["state_broadcast_hz"]
            if "state_broadcast_hz" in data
            else data.get("broadcast_hz", 50.0)
        )
        tools = ToolGatewayConfig.from_dict(data.get("tools"))
        _validate_tool_readiness(tools, image_input_ids)
        _validate_tool_port_ids(tools, image_input_ids=image_input_ids)
        return cls(
            joint_order=joint_order,
            image_input_ids=image_input_ids,
            host=host,
            port=port,
            state_broadcast_hz=_clamp_hz(
                _require_finite_float(state_broadcast_raw, "state_broadcast_hz")
            ),
            image_broadcast_hz=_clamp_hz(
                _require_finite_float(
                    data.get("image_broadcast_hz", 24.0), "image_broadcast_hz"
                )
            ),
            ws_send_timeout_sec=max(
                0.1,
                _require_finite_float(
                    data.get("ws_send_timeout_sec", 1.0), "ws_send_timeout_sec"
                ),
            ),
            jpeg_quality=jpeg_quality,
            policy_id=policy_id,
            command_queue_capacity=command_queue_capacity,
            readiness=ReadinessConfig.from_dict(data.get("readiness")),
            tools=tools,
        )

    @classmethod
    def from_yaml_path(cls, path: str | Path) -> "GatewayConfig":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"配置文件不存在: {p}")
        with p.open(encoding="utf-8") as f:
            data = yaml.load(f, Loader=_UniqueKeySafeLoader)
        if not data:
            raise ValueError(f"配置文件为空: {p}")
        if not isinstance(data, dict):
            raise ValueError(f"配置文件必须是 YAML mapping: {p}")
        return cls.from_dict(data)


def _clamp_hz(value: float) -> float:
    return max(0.1, min(120.0, value))


def _validate_tool_readiness(
    tools: ToolGatewayConfig, image_input_ids: list[str]
) -> None:
    allowed = {
        "proprio_state",
        "ws:state",
        "ws:images",
        *(f"image:{image_id}" for image_id in image_input_ids),
    }
    invalid = sorted(
        {
            requirement
            for spec in tools.specs
            for requirement in spec.readiness
            if requirement not in allowed
        }
    )
    if invalid:
        raise ValueError(
            "tools.specs readiness contains unknown requirement(s): "
            + ", ".join(invalid)
        )


def load_config(config_path: str | Path | None = None) -> GatewayConfig:
    """Load config from explicit path or GATEWAY_CONFIG."""

    path = config_path or os.environ.get("GATEWAY_CONFIG")
    if path:
        return GatewayConfig.from_yaml_path(path)
    return GatewayConfig.from_dict({"joint_order": [], "image_input_ids": []})
