from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
import os
import tomllib


MAX_ACTION_TIMEOUT_SECONDS = 25.0


class ConfigError(ValueError):
    """Raised when the probe configuration is unsafe or incomplete."""


@dataclass(frozen=True)
class ServerConfig:
    host: str
    port: int
    path: str
    expected_self_id: str
    access_token: str = field(repr=False)


@dataclass(frozen=True)
class SourceConversation:
    message_type: str
    conversation_id: str


@dataclass(frozen=True)
class ProbeTarget:
    name: str
    action: str
    params: dict[str, Any]
    expected_message_type: str
    expected_conversation_id: str
    expected_sub_type: str | None = None


@dataclass(frozen=True)
class ProbeConfig:
    server: ServerConfig
    source: SourceConversation
    output_directory: Path
    action_timeout_seconds: float
    targets: dict[str, ProbeTarget]


def load_config(path: str | Path) -> ProbeConfig:
    config_path = Path(path).resolve()
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"Configuration file not found: {config_path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {config_path}: {exc}") from exc

    server_data = _mapping(raw, "server")
    host = _required_string(server_data, "host")
    if host not in {"127.0.0.1", "::1"}:
        raise ConfigError("server.host must be 127.0.0.1 or ::1")

    port = _required_int(server_data, "port")
    if not 1 <= port <= 65535:
        raise ConfigError("server.port must be between 1 and 65535")

    path_value = _required_string(server_data, "path")
    if not path_value.startswith("/") or "?" in path_value or "#" in path_value:
        raise ConfigError("server.path must be an absolute path without query or fragment")

    expected_self_id = _required_identity(server_data, "expected_self_id")
    token_env = _required_string(server_data, "access_token_env")
    access_token = os.environ.get(token_env)
    if not access_token:
        raise ConfigError(f"Environment variable {token_env} is required and must not be empty")

    source_data = _mapping(raw, "source")
    source_type = _required_message_type(source_data, "message_type")
    source = SourceConversation(
        message_type=source_type,
        conversation_id=_required_identity(source_data, "id"),
    )

    output_data = _mapping(raw, "output")
    output_text = _required_string(output_data, "directory")
    output_directory = (config_path.parent / output_text).resolve()
    action_timeout = _required_float(output_data, "action_timeout_seconds")
    if action_timeout <= 0:
        raise ConfigError("output.action_timeout_seconds must be greater than zero")
    if action_timeout > MAX_ACTION_TIMEOUT_SECONDS:
        raise ConfigError(
            f"output.action_timeout_seconds must not exceed {MAX_ACTION_TIMEOUT_SECONDS:g} seconds"
        )

    targets_data = _mapping(raw, "test_targets")
    targets: dict[str, ProbeTarget] = {}
    for name, target_raw in targets_data.items():
        if not isinstance(name, str) or not name:
            raise ConfigError("test target names must be non-empty strings")
        target_data = _mapping_value(target_raw, f"test_targets.{name}")
        enabled = target_data.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ConfigError(f"test_targets.{name}.enabled must be true or false")
        if not enabled:
            continue

        action = _required_string(target_data, "action")
        if action.startswith("REPLACE_"):
            raise ConfigError(f"test_targets.{name}.action still contains a placeholder")
        params = _mapping(target_data, "params")
        expected_message_type = _required_message_type(target_data, "expected_message_type")
        expected_conversation_id = _required_target_conversation_id(
            target_data,
            "expected_conversation_id",
        )
        expected_sub_type = _optional_string(target_data, "expected_sub_type")
        if expected_sub_type is not None and expected_sub_type.startswith("REPLACE_"):
            raise ConfigError(f"test_targets.{name}.expected_sub_type still contains a placeholder")
        targets[name] = ProbeTarget(
            name=name,
            action=action,
            params=dict(params),
            expected_message_type=expected_message_type,
            expected_conversation_id=expected_conversation_id,
            expected_sub_type=expected_sub_type,
        )

    if not targets:
        raise ConfigError("at least one enabled test target is required")
    _validate_distinct_control_targets(targets, expected_self_id)

    return ProbeConfig(
        server=ServerConfig(
            host=host,
            port=port,
            path=path_value,
            expected_self_id=expected_self_id,
            access_token=access_token,
        ),
        source=source,
        output_directory=output_directory,
        action_timeout_seconds=action_timeout,
        targets=targets,
    )


def _validate_distinct_control_targets(targets: Mapping[str, ProbeTarget], self_id: str) -> None:
    self_target = targets.get("self")
    computer_target = targets.get("my_computer")
    if self_target is None or computer_target is None:
        return
    if _target_route(self_target, self_id) == _target_route(computer_target, self_id):
        raise ConfigError(
            "self and my_computer must use distinct expected event routes; "
            "leave my_computer disabled until its observed route is known"
        )


def _target_route(target: ProbeTarget, self_id: str) -> tuple[str, str, str | None]:
    conversation_id = (
        self_id if target.expected_conversation_id == "${self_id}" else target.expected_conversation_id
    )
    return target.expected_message_type, conversation_id, target.expected_sub_type


def _mapping(raw: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    try:
        value = raw[key]
    except KeyError as exc:
        raise ConfigError(f"Missing [{key}] configuration section") from exc
    return _mapping_value(value, key)


def _mapping_value(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{label} must be a TOML table")
    return value


def _required_string(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key} must be a non-empty string")
    return value.strip()


def _optional_string(data: Mapping[str, Any], key: str) -> str | None:
    if key not in data:
        return None
    return _required_string(data, key)


def _required_identity(data: Mapping[str, Any], key: str) -> str:
    value = _required_string(data, key)
    if value.startswith("REPLACE_") or value.startswith("填写"):
        raise ConfigError(f"{key} still contains a placeholder")
    return value


def _required_target_conversation_id(data: Mapping[str, Any], key: str) -> str:
    value = _required_string(data, key)
    if value == "${self_id}":
        return value
    if value.startswith("REPLACE_") or value.startswith("填写"):
        raise ConfigError(f"{key} still contains a placeholder")
    return value


def _required_message_type(data: Mapping[str, Any], key: str) -> str:
    value = _required_string(data, key)
    if value not in {"private", "group"}:
        raise ConfigError(f"{key} must be private or group")
    return value


def _required_int(data: Mapping[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{key} must be an integer")
    return value


def _required_float(data: Mapping[str, Any], key: str) -> float:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{key} must be a number")
    return float(value)
