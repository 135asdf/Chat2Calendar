from __future__ import annotations

from argparse import Namespace
import asyncio
import contextlib
import io
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from chat2calendar_phase0.cli import _endpoint_url, _serve
from chat2calendar_phase0.config import (
    ConfigError,
    ProbeConfig,
    ProbeTarget,
    ServerConfig,
    SourceConversation,
    load_config,
)
from chat2calendar_phase0.redaction import EventRedactor
from chat2calendar_phase0.server import NapCatProbeServer, ProbeError


class EventRedactorTests(unittest.TestCase):
    def test_removes_text_tokens_and_real_identifiers(self) -> None:
        redactor = EventRedactor()
        event = {
            "self_id": 10001,
            "user_id": 10001,
            "message_id": 30003,
            "raw_message": "private message that must not escape",
            "sender": {"nickname": "private name"},
            "access_token": "secret-token",
        }

        redacted = redactor.redact(event)
        serialized = json.dumps(redacted, ensure_ascii=False)

        self.assertNotIn("private message", serialized)
        self.assertNotIn("private name", serialized)
        self.assertNotIn("secret-token", serialized)
        self.assertNotIn("10001", serialized)
        self.assertEqual(redacted["self_id"], "account_001")
        self.assertEqual(redacted["user_id"], "account_001")
    def test_removes_nested_notice_metadata_and_unknown_values(self) -> None:
        redactor = EventRedactor()
        event = {
            "post_type": "notice",
            "notice_type": "group_upload",
            "group_id": 123456,
            "user_id": 654321,
            "file": {
                "id": "private-file-id",
                "name": "private schedule.xlsx",
                "url": "https://example.invalid/private-upload",
                "size": 987654,
            },
            "private custom field": "private custom value",
            "status": "private custom status",
        }

        serialized = json.dumps(redactor.redact(event), ensure_ascii=False)

        for secret in (
            "123456",
            "654321",
            "private-file-id",
            "private schedule.xlsx",
            "https://example.invalid/private-upload",
            "987654",
            "private custom field",
            "private custom value",
            "private custom status",
        ):
            self.assertNotIn(secret, serialized)


class ConfigTests(unittest.TestCase):
    def test_loads_token_only_from_environment_and_resolves_output_directory(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "probe.toml"
            config_path.write_text(
                """[server]
host = "127.0.0.1"
port = 8765
path = "/onebot/v11/ws"
access_token_env = "PHASE0_TEST_TOKEN"
expected_self_id = "10001"

[source]
message_type = "private"
id = "20002"

[output]
directory = "fixtures"
action_timeout_seconds = 10

[test_targets.self]
action = "send_private_msg"
params = { user_id = "${self_id}", message = "test" }
expected_message_type = "private"
expected_conversation_id = "${self_id}"
""",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"PHASE0_TEST_TOKEN": "token-from-environment"}, clear=False):
                config = load_config(config_path)

        self.assertEqual(config.server.access_token, "token-from-environment")
        self.assertEqual(config.output_directory, (config_path.parent / "fixtures").resolve())
        self.assertNotIn("token-from-environment", repr(config))

    def test_rejects_duplicate_self_and_my_computer_routes(self) -> None:
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "probe.toml"
            config_path.write_text(
                """[server]
host = "127.0.0.1"
port = 8765
path = "/onebot/v11/ws"
access_token_env = "PHASE0_TEST_TOKEN"
expected_self_id = "10001"

[source]
message_type = "private"
id = "20002"

[output]
directory = "fixtures"
action_timeout_seconds = 10

[test_targets.self]
action = "send_private_msg"
params = { user_id = "${self_id}", message = "test" }
expected_message_type = "private"
expected_conversation_id = "${self_id}"

[test_targets.my_computer]
action = "send_private_msg"
params = { user_id = "${self_id}", message = "test" }
expected_message_type = "private"
expected_conversation_id = "${self_id}"
""",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"PHASE0_TEST_TOKEN": "token-from-environment"}, clear=False):
                with self.assertRaises(ConfigError):
                    load_config(config_path)

    def test_formats_ipv6_endpoint_with_brackets(self) -> None:
        self.assertEqual(_endpoint_url("::1", 8765, "/onebot/v11/ws"), "ws://[::1]:8765/onebot/v11/ws")


class CliTests(unittest.IsolatedAsyncioTestCase):
    async def test_duration_returns_failure_when_requested_action_never_runs(self) -> None:
        with TemporaryDirectory() as directory:
            config = ProbeConfig(
                server=ServerConfig(
                    host="127.0.0.1",
                    port=0,
                    path="/onebot/v11/ws",
                    expected_self_id="10001",
                    access_token="test-token",
                ),
                source=SourceConversation(message_type="private", conversation_id="20002"),
                output_directory=Path(directory) / "phase0",
                action_timeout_seconds=1,
                targets={
                    "self": ProbeTarget(
                        name="self",
                        action="send_private_msg",
                        params={"user_id": "${self_id}", "message": "test"},
                        expected_message_type="private",
                        expected_conversation_id="10001",
                    )
                },
            )
            args = Namespace(
                send_test=["self"],
                wait_for_connection_seconds=1,
                exit_after_tests=False,
                duration_seconds=0.05,
            )
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                result = await _serve(config, args)
            self.assertEqual(result, 1)
    async def test_startup_failure_finalizes_the_report(self) -> None:
        with TemporaryDirectory() as directory:
            output_directory = Path(directory) / "phase0"
            config = ProbeConfig(
                server=ServerConfig(
                    host="127.0.0.1",
                    port=8765,
                    path="/onebot/v11/ws",
                    expected_self_id="10001",
                    access_token="test-token",
                ),
                source=SourceConversation(message_type="private", conversation_id="20002"),
                output_directory=output_directory,
                action_timeout_seconds=1,
                targets={
                    "self": ProbeTarget(
                        name="self",
                        action="send_private_msg",
                        params={"user_id": "${self_id}", "message": "test"},
                        expected_message_type="private",
                        expected_conversation_id="10001",
                    )
                },
            )
            args = Namespace(
                send_test=[],
                wait_for_connection_seconds=1,
                exit_after_tests=False,
                duration_seconds=0.05,
            )
            with patch(
                "chat2calendar_phase0.cli.NapCatProbeServer.start",
                new=AsyncMock(side_effect=OSError("address already in use")),
            ):
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    result = await _serve(config, args)
            report = json.loads((output_directory / "report.json").read_text(encoding="utf-8"))

        self.assertEqual(result, 1)
        self.assertIn("finished_at", report)


class ProbeServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        output_directory = Path(self.temporary_directory.name) / "phase0"
        self.config = ProbeConfig(
            server=ServerConfig(
                host="127.0.0.1",
                port=0,
                path="/onebot/v11/ws",
                expected_self_id="10001",
                access_token="test-token",
            ),
            source=SourceConversation(message_type="private", conversation_id="20002"),
            output_directory=output_directory,
            action_timeout_seconds=2,
            targets={
                "self": ProbeTarget(
                    name="self",
                    action="send_private_msg",
                    params={
                        "user_id": "${self_id}",
                        "message": "[Chat2Calendar:PHASE0:${probe_id}] self test",
                    },
                    expected_message_type="private",
                    expected_conversation_id="10001",
                )
            },
        )
        self.server = NapCatProbeServer(self.config)
        await self.server.start()
        self.uri = f"ws://127.0.0.1:{self.server.listening_port}/onebot/v11/ws"

    async def asyncTearDown(self) -> None:
        await self.server.stop()
        self.temporary_directory.cleanup()

    async def test_rejects_invalid_access_token(self) -> None:
        async with connect(self.uri, additional_headers={"Authorization": "Bearer wrong-token"}) as client:
            with self.assertRaises(ConnectionClosed) as closed:
                await client.recv()
        self.assertEqual(closed.exception.rcvd.code, 4401)

    async def test_message_without_self_id_is_not_observed(self) -> None:
        async with connect(self.uri, additional_headers={"Authorization": "Bearer test-token"}) as client:
            await client.send(
                json.dumps(
                    {
                        "self_id": 10001,
                        "post_type": "meta_event",
                        "meta_event_type": "lifecycle",
                    }
                )
            )
            await self.server.wait_for_connection(1)
            await client.send(
                json.dumps(
                    {
                        "post_type": "message",
                        "message_type": "private",
                        "message_id": 30005,
                        "user_id": 20002,
                        "raw_message": "must not count without self_id",
                    }
                )
            )
            await asyncio.sleep(0.05)

        report = json.loads(self.server.recorder.report_path.read_text(encoding="utf-8"))
        self.assertEqual(report["message_events"], [])

    async def test_wrong_self_id_does_not_replace_an_active_connection(self) -> None:
        async with connect(self.uri, additional_headers={"Authorization": "Bearer test-token"}) as active_client:
            await active_client.send(
                json.dumps(
                    {
                        "self_id": 10001,
                        "post_type": "meta_event",
                        "meta_event_type": "lifecycle",
                    }
                )
            )
            original = await self.server.wait_for_connection(1)

            async with connect(self.uri, additional_headers={"Authorization": "Bearer test-token"}) as wrong_client:
                await wrong_client.send(
                    json.dumps(
                        {
                            "self_id": 99999,
                            "post_type": "meta_event",
                            "meta_event_type": "lifecycle",
                        }
                    )
                )
                with self.assertRaises(ConnectionClosed) as closed:
                    await wrong_client.recv()
            self.assertEqual(closed.exception.rcvd.code, 4403)

            remaining = await self.server.wait_for_connection(1)
            self.assertEqual(remaining.connection_id, original.connection_id)
            await active_client.send(json.dumps({"self_id": 10001, "post_type": "meta_event"}))

    async def test_rejects_message_event_on_the_wrong_target_route(self) -> None:
        async with connect(self.uri, additional_headers={"Authorization": "Bearer test-token"}) as client:
            await client.send(
                json.dumps(
                    {
                        "self_id": 10001,
                        "post_type": "meta_event",
                        "meta_event_type": "lifecycle",
                    }
                )
            )
            await self.server.wait_for_connection(1)

            action_task = asyncio.create_task(self.server.send_test("self"))
            action = json.loads(await client.recv())
            await client.send(
                json.dumps(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"message_id": 32003},
                        "echo": action["echo"],
                    }
                )
            )
            await client.send(
                json.dumps(
                    {
                        "self_id": 10001,
                        "post_type": "message",
                        "message_type": "private",
                        "message_id": 32003,
                        "user_id": 99999,
                        "raw_message": "wrong target route",
                    }
                )
            )
            with self.assertRaises(ProbeError):
                await action_task

        report = json.loads(self.server.recorder.report_path.read_text(encoding="utf-8"))
        self.assertTrue(any(event["target_route_match"] is False for event in report["message_events"]))

    async def test_waits_for_matching_event_after_action_response(self) -> None:
        async with connect(self.uri, additional_headers={"Authorization": "Bearer test-token"}) as client:
            await client.send(
                json.dumps(
                    {
                        "self_id": 10001,
                        "post_type": "meta_event",
                        "meta_event_type": "lifecycle",
                    }
                )
            )
            await self.server.wait_for_connection(1)

            action_task = asyncio.create_task(self.server.send_test("self"))
            action = json.loads(await client.recv())
            await client.send(
                json.dumps(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"message_id": 31003},
                        "echo": action["echo"],
                    }
                )
            )
            await asyncio.sleep(0.05)
            self.assertFalse(action_task.done())

            await client.send(
                json.dumps(
                    {
                        "self_id": 10001,
                        "post_type": "message",
                        "message_type": "private",
                        "message_id": 31003,
                        "user_id": 10001,
                        "raw_message": "program message event after action response",
                    }
                )
            )
            response = await action_task
            self.assertEqual(response["status"], "ok")

    async def test_receives_source_message_and_correlates_echo_response(self) -> None:
        async with connect(self.uri, additional_headers={"Authorization": "Bearer test-token"}) as client:
            await client.send(
                json.dumps(
                    {
                        "time": 1,
                        "self_id": 10001,
                        "post_type": "meta_event",
                        "meta_event_type": "lifecycle",
                    }
                )
            )
            await self.server.wait_for_connection(1)

            action_task = asyncio.create_task(self.server.send_test("self"))
            action = json.loads(await client.recv())
            self.assertEqual(action["action"], "send_private_msg")
            self.assertEqual(action["params"]["user_id"], 10001)
            self.assertIn("echo", action)

            await client.send(
                json.dumps(
                    {
                        "time": 2,
                        "self_id": 10001,
                        "post_type": "message",
                        "message_type": "private",
                        "message_id": 30003,
                        "user_id": 10001,
                        "raw_message": "program message event before action response",
                    }
                )
            )
            await client.send(
                json.dumps(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"message_id": 30003},
                        "echo": action["echo"],
                    }
                )
            )
            response = await action_task
            self.assertEqual(response["status"], "ok")

            await client.send(
                json.dumps(
                    {
                        "time": 3,
                        "self_id": 10001,
                        "post_type": "message",
                        "message_type": "private",
                        "message_id": 30004,
                        "user_id": 20002,
                        "raw_message": "source conversation test message",
                    }
                )
            )
            await asyncio.sleep(0.05)

        report = json.loads(self.server.recorder.report_path.read_text(encoding="utf-8"))
        self.assertTrue(any(event["source_match"] for event in report["message_events"]))
        self.assertTrue(any(event["program_action_match"] for event in report["message_events"]))
        self.assertTrue(any(event["target_route_match"] for event in report["message_events"]))
        self.assertTrue(
            any(
                event.get("correlation_order") == "event_before_action_response"
                for event in report["message_events"]
            )
        )
        fixture = self.server.recorder.events_path.read_text(encoding="utf-8")
        self.assertNotIn("source conversation test message", fixture)
        self.assertNotIn("20002", fixture)


if __name__ == "__main__":
    unittest.main()
