# Phase 0 NapCat Feasibility Probe

This tool is limited to OneBot 11 connectivity, event observation, action-response correlation, and redacted protocol capture. It does not upload chat content, call an AI model, create reminders, or read historical messages.

## 1. Prepare the local configuration

Open PowerShell in the repository root:

```powershell
Copy-Item config/napcat-probe.example.toml config/napcat-probe.toml
$env:CHAT2CALENDAR_NAPCAT_TOKEN = "replace-with-the-same-random-token-configured-in-NapCat"
```

Before running, edit `config/napcat-probe.toml`:

- Replace `expected_self_id` with the logged-in QQ number.
- Set `[source]` to one private chat or one group selected for the reception test.
- Keep the token out of TOML. The process reads it only from the named environment variable.
- Leave `my_computer` disabled for the initial self-chat check. Its action and expected event route are intentionally not guessed.

## 2. Configure NapCat

In NapCat, configure a OneBot 11 reverse WebSocket connection to:

```text
ws://127.0.0.1:8765/onebot/v11/ws
```

Configure the same random Access Token in NapCat and the `CHAT2CALENDAR_NAPCAT_TOKEN` environment variable. Do not expose this listener outside `127.0.0.1`.

The probe closes bad tokens with WebSocket close code `4401`. A valid-token connection is only made active after an event reports the configured `self_id`; an unexpected identity closes with `4403` and cannot replace an already verified connection. Every message used as source or target-route evidence must also carry that same `self_id`.

## 3. Run the self-chat check

Create the virtual environment and lock dependencies once:

```powershell
uv venv --python 3.12
uv lock
uv sync --locked
```

Start the self-chat action test:

```powershell
uv run chat2calendar-napcat-probe serve --config config/napcat-probe.toml --send-test self --exit-after-tests
```

Success requires all of the following:

- An authenticated connection reports the configured `self_id`.
- The OneBot action returns `status: "ok"`, `retcode: 0`, and a `message_id`.
- A message event with that returned ID arrives during the action timeout.
- That event matches the target's configured message type, conversation ID, and optional subtype.

The report records response arrival, whether a message ID was returned, and `target_route_verified`. The action does not count as a working control path merely because `send_private_msg` returned `ok`.

## 4. Measure the "My Computer" route separately

A successful self-chat action does not prove that "My Computer" is usable. First run a passive capture window or inspect the self-chat fixture to determine the actual OneBot action, parameter shape, and event route:

```powershell
uv run chat2calendar-napcat-probe serve --config config/napcat-probe.toml --duration-seconds 120
```

Then set `test_targets.my_computer.enabled = true` and replace every `REPLACE_WITH_OBSERVED_*` value. The expected route must differ from `self` by message type, conversation ID, or subtype; otherwise configuration fails rather than producing a false positive.

Run the second check only after that configuration is known:

```powershell
uv run chat2calendar-napcat-probe serve --config config/napcat-probe.toml --send-test my_computer --exit-after-tests
```

If NapCat exposes no distinct route, record that limitation as the Phase 0 result. Do not treat an identical self-chat target with different message text as "My Computer" validation.

## 5. Perform source-message and reconnect checks

While a capture is running:

1. Send a new text message in the configured `[source]` private chat or group.
2. Send a plain command-like reply in each tested control conversation.
3. Confirm that `report.json` records `source_match: true` for the selected source and marks action-originated events through `program_action_match` plus `target_route_match`.
4. Restart NapCat once and confirm the report has a new verified connection lifecycle entry.

The probe does not parse or act on commands. Its purpose is to collect evidence for a later command parser to distinguish a user-entered command from a message emitted by the program.

## 6. Preserve the feasibility result safely

Each run writes `data/phase0/report.json` and a timestamped JSONL fixture. The fixture removes message text, display names, tokens, headers, unknown strings, unknown numbers, URLs, file metadata values, and identifiers. It retains safe protocol enums and anonymous equality relationships, such as `self_id` and `user_id` both mapping to `account_001` in a self-chat.

Message-ID correlation is scoped to one verified connection and retained for at most one minute (unmatched pre-response events for 30 seconds) or 512 entries. `data/phase0/` is ignored by Git; inspect it before sharing because operational metadata still describes the local test run.

For each target, record QQ desktop and NapCat versions, observed action/response order, reconnection behavior, the exact sanitized route result, and any response-loss or missing-message-ID behavior.
