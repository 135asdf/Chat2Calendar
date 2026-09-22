from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


class EventRedactor:
    """Preserve safe protocol facts while removing all unrecognized payload values."""

    _TEXT_FIELDS = {
        "message",
        "raw_message",
        "text",
        "content",
        "alt_message",
        "message_chain",
    }
    _SECRET_FIELDS = {
        "access_token",
        "token",
        "authorization",
        "api_key",
        "cookie",
    }
    _DISPLAY_FIELDS = {"nickname", "card", "remark", "title", "name", "url", "path"}
    _IDENTIFIER_FIELDS = {
        # QQ account fields share one namespace so fixtures retain relationships
        # such as user_id == self_id in a self-chat without retaining the UIN.
        "self_id": "account",
        "user_id": "account",
        "target_id": "account",
        "operator_id": "account",
        "group_id": "group",
        "message_id": "message",
        "echo": "echo",
        "id": "opaque",
        "file_id": "opaque",
        "flag": "opaque",
    }
    _SAFE_STRING_VALUES = {
        "message_type": {"private", "group"},
        "meta_event_type": {"heartbeat", "lifecycle"},
        "notice_type": {
            "client_status",
            "essence",
            "friend_add",
            "friend_recall",
            "group_admin",
            "group_ban",
            "group_card",
            "group_decrease",
            "group_increase",
            "group_msg_emoji_like",
            "group_recall",
            "group_upload",
            "notify",
            "offline_file",
        },
        "post_type": {"message", "message_sent", "meta_event", "notice", "request"},
        "request_type": {"friend", "group"},
        "status": {"async", "failed", "ok"},
        "sub_type": {
            "anonymous",
            "approve",
            "friend",
            "group",
            "group_self",
            "invite",
            "leave",
            "normal",
            "notice",
            "other",
            "reject",
        },
    }
    _SAFE_NUMBER_FIELDS = {"font", "interval", "retcode", "time"}
    _KNOWN_FIELD_NAMES = (
        _TEXT_FIELDS
        | _SECRET_FIELDS
        | _DISPLAY_FIELDS
        | _IDENTIFIER_FIELDS.keys()
        | _SAFE_STRING_VALUES.keys()
        | _SAFE_NUMBER_FIELDS
        | {
            "anonymous",
            "data",
            "file",
            "group_openid",
            "group_id",
            "message_seq",
            "params",
            "sender",
            "size",
            "version",
        }
    )

    def __init__(self) -> None:
        self._identifiers: dict[tuple[str, str], str] = {}
        self._counts: dict[str, int] = {}
        self._unknown_field_count = 0

    def redact(self, value: Any, field_name: str | None = None) -> Any:
        normalized = field_name.lower() if field_name else ""
        if normalized in self._SECRET_FIELDS:
            return "<redacted:secret>"
        if normalized in self._TEXT_FIELDS:
            return self._redact_text(value)
        if normalized in self._DISPLAY_FIELDS:
            return self._redact_scalar(value, "display-name")
        if normalized in self._IDENTIFIER_FIELDS:
            return None if value is None else self._pseudonym(self._IDENTIFIER_FIELDS[normalized], value)
        if normalized.endswith("_id") and value is not None:
            return self._pseudonym("opaque", value)

        if isinstance(value, Mapping):
            return self._redact_mapping(value)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return [self.redact(item) for item in value]
        if isinstance(value, bytes):
            return f"<redacted:bytes:{len(value)}>"
        if isinstance(value, bool) or value is None:
            return value
        allowed_values = self._SAFE_STRING_VALUES.get(normalized)
        if isinstance(value, str) and allowed_values is not None and value in allowed_values:
            return value
        if normalized in self._SAFE_NUMBER_FIELDS and isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            return self._redact_scalar(value, "string")
        if isinstance(value, (int, float)):
            return "<redacted:number>"
        return f"<redacted:{type(value).__name__}>"

    def _redact_mapping(self, value: Mapping[Any, Any]) -> dict[str, Any]:
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            original_key = str(key)
            output_key = self._output_key(original_key)
            redacted[output_key] = self.redact(item, original_key)
        return redacted

    def _output_key(self, key: str) -> str:
        if key.lower() in self._KNOWN_FIELD_NAMES or key.lower().endswith("_id"):
            return key
        self._unknown_field_count += 1
        return f"unknown_field_{self._unknown_field_count:03d}"

    def _pseudonym(self, category: str, value: Any) -> str:
        key = (category, str(value))
        pseudonym = self._identifiers.get(key)
        if pseudonym is None:
            ordinal = self._counts.get(category, 0) + 1
            self._counts[category] = ordinal
            pseudonym = f"{category}_{ordinal:03d}"
            self._identifiers[key] = pseudonym
        return pseudonym

    @staticmethod
    def _redact_text(value: Any) -> dict[str, Any]:
        if isinstance(value, str):
            return {"redacted": True, "kind": "text", "length": len(value)}
        if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
            return {"redacted": True, "kind": "segments", "count": len(value)}
        return {"redacted": True, "kind": type(value).__name__}

    @staticmethod
    def _redact_scalar(value: Any, kind: str) -> dict[str, Any]:
        if isinstance(value, str):
            return {"redacted": True, "kind": kind, "length": len(value)}
        return {"redacted": True, "kind": kind}
