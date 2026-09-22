from __future__ import annotations

import argparse
import asyncio
import contextlib
from collections.abc import Sequence
from pathlib import Path
import sys

from .config import ConfigError, ProbeConfig, load_config
from .server import NapCatProbeServer, ProbeError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chat2calendar-napcat-probe",
        description="Run the Chat2Calendar Phase 0 NapCat / OneBot 11 feasibility probe.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    serve_parser = subcommands.add_parser("serve", help="Start the reverse WebSocket probe server")
    serve_parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/napcat-probe.toml"),
        help="Path to the local TOML configuration",
    )
    serve_parser.add_argument(
        "--send-test",
        action="append",
        default=[],
        metavar="TARGET",
        help="Send a configured target action after NapCat connects; may be repeated",
    )
    serve_parser.add_argument(
        "--wait-for-connection-seconds",
        type=float,
        default=45,
        help="Timeout for automatic --send-test actions (default: 45)",
    )
    serve_parser.add_argument(
        "--exit-after-tests",
        action="store_true",
        help="Stop after all automatic test actions finish",
    )
    serve_parser.add_argument(
        "--duration-seconds",
        type=float,
        help="Stop after this duration; useful for scripted capture windows",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    if args.exit_after_tests and not args.send_test:
        print("--exit-after-tests requires at least one --send-test TARGET", file=sys.stderr)
        return 2
    if args.wait_for_connection_seconds <= 0:
        print("--wait-for-connection-seconds must be greater than zero", file=sys.stderr)
        return 2
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        print("--duration-seconds must be greater than zero", file=sys.stderr)
        return 2

    try:
        return asyncio.run(_serve(config, args))
    except KeyboardInterrupt:
        print("Probe stopped by user.")
        return 0


async def _serve(config: ProbeConfig, args: argparse.Namespace) -> int:
    server = NapCatProbeServer(config)
    try:
        await server.start()
    except Exception as exc:
        server.recorder.record_note("startup_failed", error_type=type(exc).__name__)
        await server.stop()
        print(f"Unable to start Phase 0 listener: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(
        "Listening for NapCat reverse WebSocket on "
        f"{_endpoint_url(server.config.server.host, server.listening_port, server.config.server.path)}"
    )
    print(f"Redacted events: {server.recorder.events_path}")
    print(f"Report: {server.recorder.report_path}")
    if not args.send_test:
        print("Waiting for NapCat. Press Ctrl+C after the capture window is complete.")

    done = asyncio.Event()
    tests_finished = asyncio.Event()
    errors: list[str] = []

    async def run_tests() -> None:
        try:
            await server.wait_for_connection(args.wait_for_connection_seconds)
            for target_name in args.send_test:
                response = await server.send_test(target_name)
                print(
                    f"Test target {target_name!r}: "
                    f"status={response.get('status')!r}, retcode={response.get('retcode')!r}"
                )
        except ProbeError as exc:
            errors.append(str(exc))
            print(f"Phase 0 probe failure: {exc}", file=sys.stderr)
        except Exception as exc:  # Keep the server report available after a protocol failure.
            errors.append(f"Unexpected probe failure: {type(exc).__name__}")
            print(f"Unexpected probe failure: {type(exc).__name__}: {exc}", file=sys.stderr)
        finally:
            tests_finished.set()
            if args.exit_after_tests:
                done.set()

    test_task: asyncio.Task[None] | None = None
    if args.send_test:
        test_task = asyncio.create_task(run_tests())
    try:
        if args.duration_seconds is not None:
            if args.exit_after_tests:
                try:
                    await asyncio.wait_for(done.wait(), timeout=args.duration_seconds)
                except asyncio.TimeoutError:
                    errors.append("Probe duration expired before automatic tests completed")
                    print(errors[-1], file=sys.stderr)
            else:
                await asyncio.sleep(args.duration_seconds)
                if args.send_test and not tests_finished.is_set():
                    errors.append("Probe duration expired before automatic tests completed")
                    print(errors[-1], file=sys.stderr)
        elif args.exit_after_tests:
            await done.wait()
        else:
            await asyncio.Future()
    finally:
        if test_task is not None:
            if not test_task.done():
                test_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await test_task
        await server.stop()
    return 1 if errors else 0


def _endpoint_url(host: str, port: int | None, path: str) -> str:
    display_host = f"[{host}]" if ":" in host else host
    return f"ws://{display_host}:{port}{path}"
