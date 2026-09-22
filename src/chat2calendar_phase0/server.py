from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Mapping
import asyncio
import contextlib
import hmac
import json
import time
import uuid

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from .config import ProbeConfig
from .recorder import ProbeRecorder


CORRELATION_TTL_SECONDS = 60.0
UNMATCHED_OBSERVATION_TTL_SECONDS = 30.0
CORRELATION_MAX_ENTRIES = 512
CorrelationKey = tuple[str, str]


class ProbeError(RuntimeError):
    """Raised for a failed Phase 0 protocol action."""


@dataclass
class PendingAction:
    future: asyncio.Future[dict[str, Any]]
    action: str
    target_name: str | None
    expected_message_type: str | None
    expected_conversation_id: str | None
    expected_sub_type: str | None


@dataclass
class ProgramMessage:
    action_echo: str
    target_name: str | None
    expected_message_type: str | None
    expected_conversation_id: str | None
    expected_sub_type: str | None
    expires_at: float


@dataclass
class UnmatchedObservation:
    observation_id: str
    message_type: str
    conversation_id: str | None
    sub_type: str | None
    expires_at: float


@dataclass
class CorrelationResult:
    target_route_match: bool
    expires_at: float


@dataclass
class ConnectionState:
    connection_id: str
    websocket: Any
    peer: str
    self_id: str | None = None
    identity_ready: asyncio.Event = field(default_factory=asyncio.Event)
    pending: dict[str, PendingAction] = field(default_factory=dict)


class NapCatProbeServer:
    """Minimal reverse WebSocket endpoint for exercising NapCat's OneBot 11 path."""

    def __init__(self, config: ProbeConfig) -> None:
        self.config = config
        self.recorder = ProbeRecorder(config.output_directory)
        self._server: Any | None = None
        self._active: ConnectionState | None = None
        self._state_lock = asyncio.Lock()
        self._connected = asyncio.Event()
        self._program_messages: OrderedDict[CorrelationKey, ProgramMessage] = OrderedDict()
        self._unmatched_observations: OrderedDict[CorrelationKey, UnmatchedObservation] = OrderedDict()
        self._correlation_results: OrderedDict[CorrelationKey, CorrelationResult] = OrderedDict()
        self._correlation_events: OrderedDict[CorrelationKey, asyncio.Event] = OrderedDict()
        self._cleanup_task: asyncio.Task[None] | None = None
        self.listening_port: int | None = None

    async def start(self) -> None:
        self._server = await serve(
            self._handle_connection,
            self.config.server.host,
            self.config.server.port,
            max_size=1_048_576,
            ping_interval=20,
            ping_timeout=20,
        )
        sockets = self._server.sockets
        if not sockets:
            raise ProbeError("WebSocket server started without a listening socket")
        self.listening_port = int(sockets[0].getsockname()[1])
        self._cleanup_task = asyncio.create_task(self._cleanup_correlations())
        self.recorder.record_connection(
            "listening",
            host=self.config.server.host,
            port=self.listening_port,
            path=self.config.server.path,
        )

    async def stop(self) -> None:
        cleanup_task = self._cleanup_task
        self._cleanup_task = None
        if cleanup_task is not None:
            cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cleanup_task
        async with self._state_lock:
            active = self._active
            self._active = None
            self._connected.clear()
        if active is not None:
            with contextlib.suppress(Exception):
                await active.websocket.close(code=1001, reason="Probe shutting down")
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        self.recorder.record_connection("stopped")
        self.recorder.close()

    async def wait_for_connection(self, timeout_seconds: float | None = None) -> ConnectionState:
        try:
            if timeout_seconds is None:
                await self._connected.wait()
            else:
                await asyncio.wait_for(self._connected.wait(), timeout=timeout_seconds)
        except TimeoutError as exc:
            raise ProbeError("Timed out waiting for an authenticated NapCat connection") from exc
        async with self._state_lock:
            if self._active is None:
                raise ProbeError("NapCat disconnected before an action could be sent")
            return self._active

    async def send_test(self, target_name: str) -> dict[str, Any]:
        try:
            target = self.config.targets[target_name]
        except KeyError as exc:
            choices = ", ".join(sorted(self.config.targets))
            raise ProbeError(f"Unknown test target {target_name!r}; configured targets: {choices}") from exc

        state = await self.wait_for_connection()
        await self._wait_for_expected_identity(state)
        replacements = {
            "probe_id": self.recorder.run_id,
            "self_id": self._self_id_for(state),
            "target_name": target.name,
        }
        params = _render_template(target.params, replacements)
        expected_conversation_id = str(
            _render_template(target.expected_conversation_id, replacements)
        )
        response = await self._send_action(
            state,
            target.action,
            params,
            target_name=target.name,
            expected_message_type=target.expected_message_type,
            expected_conversation_id=expected_conversation_id,
            expected_sub_type=target.expected_sub_type,
        )
        status = str(response.get("status", ""))
        retcode = response.get("retcode")
        if status != "ok" or retcode not in {0, None}:
            self.recorder.record_action(
                target=target.name,
                action=target.action,
                status=status,
                retcode=retcode,
                response_received=True,
                target_route_verified=False,
            )
            raise ProbeError(
                f"NapCat returned status={status!r}, retcode={retcode!r} for target {target.name!r}"
            )

        message_id = _response_message_id(response)
        if message_id is None:
            self.recorder.record_action(
                target=target.name,
                action=target.action,
                status=status,
                retcode=retcode,
                response_received=True,
                message_id_returned=False,
                target_route_verified=False,
            )
            raise ProbeError(f"OneBot response for target {target.name!r} did not return a message_id")

        try:
            target_route_match = await self._wait_for_target_correlation(state, message_id)
        except ProbeError:
            self.recorder.record_action(
                target=target.name,
                action=target.action,
                status=status,
                retcode=retcode,
                response_received=True,
                message_id_returned=True,
                target_route_verified=False,
                target_event_timeout=True,
            )
            raise
        self.recorder.record_action(
            target=target.name,
            action=target.action,
            status=status,
            retcode=retcode,
            response_received=True,
            message_id_returned=True,
            target_route_verified=target_route_match,
        )
        if not target_route_match:
            raise ProbeError(
                f"OneBot message event for target {target.name!r} did not match its configured route"
            )
        return response

    async def _handle_connection(self, websocket: Any) -> None:
        request_path = _request_path(websocket)
        if request_path != self.config.server.path:
            self.recorder.record_connection("rejected_path")
            await websocket.close(code=4404, reason="Unexpected OneBot path")
            return

        authorization = _authorization_header(websocket)
        expected = f"Bearer {self.config.server.access_token}"
        if not authorization or not hmac.compare_digest(authorization, expected):
            self.recorder.record_connection("rejected_token")
            await websocket.close(code=4401, reason="Unauthorized")
            return

        connection = ConnectionState(
            connection_id=uuid.uuid4().hex[:12],
            websocket=websocket,
            peer=_peer_name(websocket),
        )
        self.recorder.record_connection(
            "authenticated_pending_self_id",
            connection_id=connection.connection_id,
            peer=connection.peer,
            path=request_path,
        )
        close_code: int | None = None
        try:
            async for raw_payload in websocket:
                await self._handle_payload(connection, raw_payload)
        except ConnectionClosed as exc:
            close_code = _close_code(exc, websocket)
        finally:
            if close_code is None:
                websocket_code = getattr(websocket, "close_code", None)
                close_code = int(websocket_code) if websocket_code is not None else None
            self.recorder.record_connection(
                "disconnected",
                connection_id=connection.connection_id,
                close_code=close_code,
            )
            for pending in connection.pending.values():
                if not pending.future.done():
                    pending.future.set_exception(
                        ProbeError("NapCat disconnected before the action response arrived")
                    )
            connection.pending.clear()
            async with self._state_lock:
                if self._active is connection:
                    self._active = None
                    self._connected.clear()

    async def _handle_payload(self, connection: ConnectionState, raw_payload: str | bytes) -> None:
        if isinstance(raw_payload, bytes):
            self.recorder.record_note("invalid_payload", reason="binary_frame", length=len(raw_payload))
            return
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            self.recorder.record_note("invalid_payload", reason="invalid_json", length=len(raw_payload))
            return
        if not isinstance(payload, dict):
            self.recorder.record_note("invalid_payload", reason="json_not_object")
            return

        self.recorder.record_payload("inbound", payload)
        echo = payload.get("echo")
        if echo is not None:
            pending = connection.pending.pop(str(echo), None)
            if pending is not None and not pending.future.done():
                message_id = _response_message_id(payload)
                if message_id is not None:
                    self._register_program_message(connection, message_id, str(echo), pending)
                pending.future.set_result(payload)
                self.recorder.record_action(
                    echo=str(echo),
                    target=pending.target_name,
                    status=str(payload.get("status", "")),
                    retcode=payload.get("retcode"),
                    response_received=True,
                    message_id_returned=message_id is not None,
                )
                return

        self_id = payload.get("self_id")
        if self_id is not None:
            connection.self_id = str(self_id)
            if connection.self_id != self.config.server.expected_self_id:
                self.recorder.record_connection(
                    "rejected_self_id",
                    connection_id=connection.connection_id,
                )
                await connection.websocket.close(code=4403, reason="Unexpected self_id")
                return
            await self._activate_connection(connection)

        if payload.get("post_type") == "message":
            if self_id is None:
                self.recorder.record_note(
                    "ignored_message_without_self_id",
                    connection_id=connection.connection_id,
                )
                return
            if not connection.identity_ready.is_set() or self._active is not connection:
                self.recorder.record_note(
                    "ignored_message_before_verified_identity",
                    connection_id=connection.connection_id,
                )
                return
            self._observe_message_event(connection, payload)

    async def _activate_connection(self, connection: ConnectionState) -> None:
        previous: ConnectionState | None
        async with self._state_lock:
            if self._active is connection:
                return
            previous = self._active
            self._active = connection
            self._connected.set()
        if previous is not None:
            self.recorder.record_connection(
                "replaced_connection",
                previous_connection_id=previous.connection_id,
                connection_id=connection.connection_id,
            )
            with contextlib.suppress(Exception):
                await previous.websocket.close(code=4000, reason="Replaced by newer NapCat connection")
        self.recorder.record_connection(
            "connected",
            connection_id=connection.connection_id,
            peer=connection.peer,
            path=self.config.server.path,
        )
        connection.identity_ready.set()

    async def _send_action(
        self,
        connection: ConnectionState,
        action: str,
        params: Mapping[str, Any],
        *,
        target_name: str | None,
        expected_message_type: str | None,
        expected_conversation_id: str | None,
        expected_sub_type: str | None,
    ) -> dict[str, Any]:
        echo = f"phase0-{uuid.uuid4().hex}"
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        pending = PendingAction(
            future=future,
            action=action,
            target_name=target_name,
            expected_message_type=expected_message_type,
            expected_conversation_id=expected_conversation_id,
            expected_sub_type=expected_sub_type,
        )
        connection.pending[echo] = pending
        payload = {"action": action, "params": dict(params), "echo": echo}
        self.recorder.record_payload("outbound_action", payload)
        self.recorder.record_action(action=action, target=target_name, echo=echo, submitted=True)
        try:
            await connection.websocket.send(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            return await asyncio.wait_for(future, timeout=self.config.action_timeout_seconds)
        except TimeoutError as exc:
            self.recorder.record_action(
                action=action,
                target=target_name,
                echo=echo,
                response_received=False,
                timeout=True,
            )
            raise ProbeError(f"Timed out waiting for OneBot response to {action!r}") from exc
        except ConnectionClosed as exc:
            self.recorder.record_action(
                action=action,
                target=target_name,
                echo=echo,
                response_received=False,
                connection_closed=True,
            )
            raise ProbeError(f"NapCat disconnected while sending {action!r}") from exc
        except ProbeError:
            self.recorder.record_action(
                action=action,
                target=target_name,
                echo=echo,
                response_received=False,
                connection_closed=True,
            )
            raise
        finally:
            connection.pending.pop(echo, None)

    async def _wait_for_expected_identity(self, connection: ConnectionState) -> None:
        if connection.identity_ready.is_set():
            return
        try:
            await asyncio.wait_for(
                connection.identity_ready.wait(), timeout=self.config.action_timeout_seconds
            )
        except TimeoutError as exc:
            raise ProbeError("Timed out waiting for a OneBot event with the expected self_id") from exc

    async def _wait_for_target_correlation(
        self,
        connection: ConnectionState,
        message_id: str,
    ) -> bool:
        key = (connection.connection_id, message_id)
        self._prune_correlations()
        existing = self._correlation_results.get(key)
        if existing is not None:
            return existing.target_route_match
        event = self._correlation_events.setdefault(key, asyncio.Event())
        self._correlation_events.move_to_end(key)
        try:
            await asyncio.wait_for(event.wait(), timeout=self.config.action_timeout_seconds)
        except TimeoutError as exc:
            raise ProbeError(
                f"Timed out waiting for a message event for OneBot message_id returned by target"
            ) from exc
        result = self._correlation_results.get(key)
        if result is None:
            raise ProbeError("Message correlation ended without a route result")
        return result.target_route_match

    async def _cleanup_correlations(self) -> None:
        try:
            while True:
                await asyncio.sleep(5)
                self._prune_correlations()
        except asyncio.CancelledError:
            return

    def _observe_message_event(
        self,
        connection: ConnectionState,
        payload: Mapping[str, Any],
    ) -> None:
        self._prune_correlations()
        message_type, conversation_id, sub_type = _message_route(payload)
        raw_message_id = payload.get("message_id")
        key = (connection.connection_id, str(raw_message_id)) if raw_message_id is not None else None
        program_message = self._program_messages.get(key) if key is not None else None
        target_route_match = (
            self._matches_program_route(program_message, message_type, conversation_id, sub_type)
            if program_message is not None
            else None
        )
        observation_id = self.recorder.record_message_event(
            connection_id=connection.connection_id,
            message_type=message_type,
            source_match=self._matches_source(payload),
            post_type="message",
            program_action_match=program_message is not None,
            target_name=program_message.target_name if program_message is not None else None,
            target_route_match=target_route_match,
            correlation_state=(
                "matched_from_prior_response"
                if program_message is not None
                else "not_matched_at_observation"
            ),
        )
        if key is not None and program_message is not None:
            self._set_correlation_result(key, bool(target_route_match))
        elif key is not None:
            self._unmatched_observations[key] = UnmatchedObservation(
                observation_id=observation_id,
                message_type=message_type,
                conversation_id=conversation_id,
                sub_type=sub_type,
                expires_at=time.monotonic() + UNMATCHED_OBSERVATION_TTL_SECONDS,
            )
            self._unmatched_observations.move_to_end(key)
        self._prune_correlations()

    def _register_program_message(
        self,
        connection: ConnectionState,
        message_id: str,
        action_echo: str,
        pending: PendingAction,
    ) -> None:
        self._prune_correlations()
        key = (connection.connection_id, message_id)
        program_message = ProgramMessage(
            action_echo=action_echo,
            target_name=pending.target_name,
            expected_message_type=pending.expected_message_type,
            expected_conversation_id=pending.expected_conversation_id,
            expected_sub_type=pending.expected_sub_type,
            expires_at=time.monotonic() + CORRELATION_TTL_SECONDS,
        )
        self._program_messages[key] = program_message
        self._program_messages.move_to_end(key)
        observation = self._unmatched_observations.pop(key, None)
        if observation is not None:
            target_route_match = self._matches_program_route(
                program_message,
                observation.message_type,
                observation.conversation_id,
                observation.sub_type,
            )
            self.recorder.record_message_correlation(
                observation.observation_id,
                action_echo=action_echo,
                target_name=program_message.target_name,
                target_route_match=target_route_match,
                ordering="event_before_action_response",
            )
            self._set_correlation_result(key, target_route_match)
        self._prune_correlations()

    def _set_correlation_result(self, key: CorrelationKey, target_route_match: bool) -> None:
        self._correlation_results[key] = CorrelationResult(
            target_route_match=target_route_match,
            expires_at=time.monotonic() + CORRELATION_TTL_SECONDS,
        )
        self._correlation_results.move_to_end(key)
        event = self._correlation_events.get(key)
        if event is not None:
            event.set()

    def _prune_correlations(self) -> None:
        now = time.monotonic()
        for key, message in list(self._program_messages.items()):
            if message.expires_at <= now:
                self._program_messages.pop(key, None)
        for key, observation in list(self._unmatched_observations.items()):
            if observation.expires_at <= now:
                self._unmatched_observations.pop(key, None)
        for key, result in list(self._correlation_results.items()):
            if result.expires_at <= now:
                self._correlation_results.pop(key, None)
        while len(self._program_messages) > CORRELATION_MAX_ENTRIES:
            self._program_messages.popitem(last=False)
        while len(self._unmatched_observations) > CORRELATION_MAX_ENTRIES:
            self._unmatched_observations.popitem(last=False)
        while len(self._correlation_results) > CORRELATION_MAX_ENTRIES:
            self._correlation_results.popitem(last=False)
        active_keys = (
            set(self._program_messages)
            | set(self._unmatched_observations)
            | set(self._correlation_results)
        )
        for key in list(self._correlation_events):
            if key not in active_keys:
                self._correlation_events.pop(key, None)
        while len(self._correlation_events) > CORRELATION_MAX_ENTRIES:
            self._correlation_events.popitem(last=False)

    def _matches_program_route(
        self,
        program_message: ProgramMessage,
        message_type: str,
        conversation_id: str | None,
        sub_type: str | None,
    ) -> bool:
        return (
            program_message.expected_message_type == message_type
            and program_message.expected_conversation_id == conversation_id
            and (
                program_message.expected_sub_type is None
                or program_message.expected_sub_type == sub_type
            )
        )

    def _self_id_for(self, connection: ConnectionState) -> str:
        return connection.self_id or self.config.server.expected_self_id

    def _matches_source(self, payload: Mapping[str, Any]) -> bool:
        if payload.get("message_type") != self.config.source.message_type:
            return False
        field = "user_id" if self.config.source.message_type == "private" else "group_id"
        value = payload.get(field)
        return value is not None and str(value) == self.config.source.conversation_id


def _authorization_header(websocket: Any) -> str:
    headers = getattr(websocket, "request_headers", None)
    if headers is None:
        request = getattr(websocket, "request", None)
        headers = getattr(request, "headers", None)
    if headers is None:
        return ""
    return str(headers.get("Authorization", ""))


def _request_path(websocket: Any) -> str:
    request = getattr(websocket, "request", None)
    path = getattr(request, "path", None)
    if path is None:
        path = getattr(websocket, "path", "")
    return str(path).split("?", 1)[0]


def _peer_name(websocket: Any) -> str:
    peer = getattr(websocket, "remote_address", None)
    if isinstance(peer, tuple) and peer:
        return str(peer[0])
    return "unknown"


def _close_code(exception: ConnectionClosed, websocket: Any) -> int | None:
    received = getattr(exception, "rcvd", None)
    code = getattr(received, "code", None)
    if code is not None:
        return int(code)
    sent = getattr(exception, "sent", None)
    code = getattr(sent, "code", None)
    if code is not None:
        return int(code)
    websocket_code = getattr(websocket, "close_code", None)
    return int(websocket_code) if websocket_code is not None else None


def _response_message_id(payload: Mapping[str, Any]) -> str | None:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    message_id = data.get("message_id")
    return str(message_id) if message_id is not None else None


def _message_route(payload: Mapping[str, Any]) -> tuple[str, str | None, str | None]:
    message_type = str(payload.get("message_type", ""))
    conversation_field = "user_id" if message_type == "private" else "group_id"
    conversation_value = payload.get(conversation_field)
    conversation_id = str(conversation_value) if conversation_value is not None else None
    sub_type_value = payload.get("sub_type")
    sub_type = str(sub_type_value) if sub_type_value is not None else None
    return message_type, conversation_id, sub_type


def _render_template(value: Any, values: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        for key, replacement in values.items():
            placeholder = "${" + key + "}"
            if value == placeholder:
                return int(replacement) if key == "self_id" and replacement.isdecimal() else replacement
            value = value.replace(placeholder, replacement)
        return value
    if isinstance(value, Mapping):
        return {str(key): _render_template(item, values) for key, item in value.items()}
    if isinstance(value, list):
        return [_render_template(item, values) for item in value]
    return value
