from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
import json
import os
import platform
import sys
import uuid

import websockets

from .redaction import EventRedactor


class ProbeRecorder:
    """Writes a redacted JSONL fixture and a compact status report."""

    def __init__(self, output_directory: Path) -> None:
        self.output_directory = output_directory
        self.output_directory.mkdir(parents=True, exist_ok=True)
        self.run_id = uuid.uuid4().hex[:12]
        timestamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
        self.events_path = self.output_directory / f"events-{timestamp}-{self.run_id}.jsonl"
        self.report_path = self.output_directory / "report.json"
        self._redactor = EventRedactor()
        self._sequence = 0
        self._counts: Counter[str] = Counter()
        self._report: dict[str, Any] = {
            "schema_version": 1,
            "run_id": self.run_id,
            "started_at": _utc_now().isoformat(),
            "environment": {
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "websockets": websockets.__version__,
            },
            "connections": [],
            "actions": [],
            "message_events": [],
            "counts": {},
            "events_file": self.events_path.name,
        }
        self._write_report()

    def record_connection(self, state: str, **details: Any) -> None:
        entry = {"at": _utc_now().isoformat(), "state": state, **details}
        self._report["connections"].append(entry)
        self._append("connection", entry)

    def record_message_event(self, **details: Any) -> str:
        observation_id = f"message-{len(self._report['message_events']) + 1:04d}"
        entry = {
            "at": _utc_now().isoformat(),
            "observation_id": observation_id,
            **details,
        }
        self._report["message_events"].append(entry)
        self._append("message_event", entry)
        return observation_id

    def record_message_correlation(
        self,
        observation_id: str,
        *,
        action_echo: str,
        target_name: str | None,
        target_route_match: bool,
        ordering: str,
    ) -> None:
        for entry in reversed(self._report["message_events"]):
            if entry["observation_id"] == observation_id:
                entry["program_action_match"] = True
                entry["correlation_state"] = "matched"
                entry["correlation_order"] = ordering
                entry["target_name"] = target_name
                entry["target_route_match"] = target_route_match
                break
        self._append(
            "message_correlation",
            {
                "observation_id": observation_id,
                "program_action_match": True,
                "correlation_order": ordering,
                "target_name": target_name,
                "target_route_match": target_route_match,
                "action_echo": action_echo,
            },
        )

    def record_action(self, **details: Any) -> None:
        entry = {"at": _utc_now().isoformat(), **details}
        self._report["actions"].append(entry)
        self._append("action", entry)

    def record_payload(self, direction: str, payload: Mapping[str, Any]) -> None:
        self._append(direction, {"payload": self._redactor.redact(payload)})

    def record_note(self, category: str, **details: Any) -> None:
        self._append(category, details)

    def close(self) -> None:
        self._report["finished_at"] = _utc_now().isoformat()
        self._write_report()

    def _append(self, category: str, details: Mapping[str, Any]) -> None:
        self._sequence += 1
        self._counts[category] += 1
        record = {
            "sequence": self._sequence,
            "at": _utc_now().isoformat(),
            "category": category,
            **details,
        }
        with self.events_path.open("a", encoding="utf-8", newline="\n") as handle:
            json.dump(record, handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
        self._report["counts"] = dict(sorted(self._counts.items()))
        self._write_report()

    def _write_report(self) -> None:
        temporary_path = self.report_path.with_suffix(".tmp")
        temporary_path.write_text(
            json.dumps(self._report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(temporary_path, self.report_path)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
