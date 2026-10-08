# Adapter-only STT update: verification

## Result and scope

The updated source passes local transport, transcript-buffer, and lifecycle
tests, plus real Soniox STT tests using synthesized English speech. The supplied
key was accepted by Soniox `stt-rt-v5` and produced real transcripts. The local
fault tests use loopback WebSocket/TCP connections, a mock Soniox provider, and
a gateway registry model of the inspected old-close callback behavior.

This is a provider-verified adapter-only workaround candidate. No Render
deployment, real FreeSWITCH process, Runtime session, or telephone call was used.
Historical incident causality and live staging call success remain unverified.

## Implemented changes

- On `stop`, an STT socket that already emitted a final closes immediately.
  Unfinalized tail audio on that cancelled recognizer is discarded.
- Before the first final, pending provider finalization is allowed **200 ms**.
  A slower final is cancelled so shutdown cannot indefinitely delay replacement.
- Every terminal STT close attempts a normal close frame and permits **250 ms**
  for the peer reply, then terminates the socket. This includes stop, provider
  completion, and provider error. The maximum programmed pending-stop wait is
  **450 ms** (200 ms drain + 250 ms close), subject to scheduling/network delays.
- All Soniox response tokens are processed in order after endpoint markers.
  Completed segments are emitted separately; next-segment tokens are retained.
- STT logs correlate `connectionId` with `start.options.callSid`, when present,
  and record stop receipt and elapsed shutdown time.
- The publishing allowlist includes the new lifecycle test and this report.
  Blank `.env.example` and `.gitignore` templates were added; they were absent
  from the supplied ZIP despite being referenced by its publishing script.

The TTS implementation was compared against the supplied archive and is
byte-for-byte unchanged. No gateway or Runtime source was modified.

## Checks completed

| Check | Runtime | Result |
|---|---|---|
| All eight JavaScript module syntax checks | Node 22.23.3 | PASS |
| Publishing script syntax (`bash -n`; script not executed) | Local Bash | PASS |
| Existing STT/TTS/auth/PCM/language/health suite with corrected token expectations | Node 22.23.3 and 24.20.0 | PASS |
| Expanded lifecycle suite | Node 22.23.3, 24.20.0, 26.8.1 | PASS |
| Original adapter, same modeled replacement scenario | Node 24.20.0 | Expected FAIL: reaches gateway forced-close branch |
| Credential check | Package files | PASS: no real keys; blank deployment key fields |

All tests used exact dependency `ws@8.21.0`. Node 22 was obtained from the
official Node distribution and its archive SHA-256 was checked against the
official checksum listing. The Docker daemon was unavailable, so the Linux
container image/build itself was not tested. Node 22 is the major version used
by the supplied Dockerfile.

## Scenarios exercised

| Scenario | Observed on Node 22 |
|---|---|
| Five successive STT recognizers, each producing a final from a mock provider endpoint | All five transcribed; completed-turn stop about 1–2 ms |
| Tail PCM plus an in-flight finalize after the completed turn | Stop does not wait for another provider endpoint |
| Provider never returns a final | Close about 202 ms |
| Provider final arrives too late | Close about 203 ms; no late final is forwarded |
| Provider WebSocket handshake stalls | Close about 203 ms |
| Duplicate gateway stop | One finalization request and one shutdown |
| Stop before start | Normal prompt shutdown |
| Peer ignores the WebSocket close frame | TCP released about 252 ms |
| Provider stalls and peer ignores close | TCP released about 455 ms |
| Provider completes, then stop arrives while close is pending | TCP released about 254 ms |
| Provider error and peer ignores close | TCP released about 254 ms |
| Replacement registered while old connection closes, using modeled gateway lookup | Second and third turns transcribe; no three-second forced-close path |
| Two complete segments in one Soniox response | Both final transcripts emitted |
| Next-segment final tokens following an endpoint, completed in a later response | Tokens retained and emitted, not assumed to replay |

The first five-turn finals are automatic mock endpoints. Extra finalize controls
after those finals deliberately exercise cancellation of an in-flight finalize;
they do not generate the first final of each turn.

The replacement test deliberately withholds a close acknowledgement. The
original adapter reaches the modeled three-second gateway timeout; the update
terminates the old socket first and preserves the replacement registry entry.
This isolates a concrete failure condition, not proof that an absent close reply
was the historical reason the hosted service did not close.


## Real Soniox STT verification

Run on **2026-10-08, 14:10:24–14:10:45 IST** (08:40:24–08:40:45 UTC),
using Node 22.23.3, the updated local adapter, and live `stt-rt-v5`.
The API key was read from the private Runtime environment file, never printed,
and is absent from the source, evidence, and archive.

| Scenario | Observed result |
|---|---|
| Three recognizers; start replacement immediately after sending old stop | All three requests transcribed; client-observed closes 7, 6, and 3 ms |
| One persistent recognizer; three requests; adapter silence fallback disabled | All three requests finalized by Soniox endpoint detection; close 4 ms |
| Default silence policy, stop-and-reconnect | No explicit downstream finalize controls; normal adapter configuration |
| Authentication and model access | Real successful transcripts confirm the key is valid for `stt-rt-v5` |

Exact final transcripts, received in both scenarios:

1. `I want to book a dermatology appointment in Dubai Marina.`
2. `Please show me the available morning appointments.`
3. `Yes, please book the appointment at 10 in the morning.`

The initial live attempt stopped at a test assertion expecting the word `ten`.
Soniox had emitted the valid final with numeral `10`. Only the test's numeric
comparison was corrected; no adapter source change was required. The complete
rerun passed all six utterances. The real provider results and adapter event
sequence are included in `verification/live-soniox-stt-result.json`.

These tests establish real provider authentication, transcript processing, and
local adapter lifecycle behavior. They do not establish deployment, gateway
media forwarding, agent responses, or caller-audible success.


## Real Soniox TTS smoke verification

The unchanged TTS implementation also passed a live `tts-rt-v2` / `Adrian`
smoke test on 2026-10-08, 14:11:57–14:12:03 IST (08:41:57–08:42:03 UTC).
Two successive generations on the same downstream socket returned aligned,
non-silent 16 kHz mono PCM and a `done` control for each generation:

- `What specialty would you like to book?` — 73,728 bytes, 2.304 seconds.
- `There is an appointment available at ten in the morning.` — 90,112 bytes,
  2.816 seconds.

This verifies provider authentication and audio transport, not intelligibility,
gateway playback, or caller-audible success. The investigated calls used
ElevenLabs TTS, so this Soniox TTS test does not establish their TTS behavior.
The result is included in `verification/live-soniox-tts-result.json`.

## Reproduce locally

Install dependencies from this folder, then run:

```sh
pnpm install --frozen-lockfile
pnpm check
pnpm test
```

These commands need no real API keys. All fake credentials are defined inside
the tests and all speech-provider URLs used by tests point to loopback servers.
The publishing script was only syntax-checked; it was not run.

## Live staging acceptance remains required

Deploy this source to their Render service using their existing secrets and
adapter registration. Keep real secrets in Render/environment configuration,
not in this package. Existing provider catalog/channel settings need not be
changed to exercise the updated STT path.

1. Confirm the deployed service logs `ASR_STOP_RECEIVED` with Call-SID.
2. Make an actual gateway call with at least three user utterances and responses.
3. Verify every replacement's `ASR_AUDIO` count increases as the caller speaks,
   and every turn produces `ASR_FINAL` followed by a gateway response.
4. Verify old-socket `ASR_SOCKET_CLOSE.stopElapsedMs` remains below the gateway's
   observed three-second timeout and gateway logs do not show forced old closes.
5. If `ASR_STOP_RECEIVED` is missing, investigate delivery/deployed revision;
   this update cannot execute its stop policy for a control it never receives.

The existing fifteen-minute session limit and PCM endpoint fallback policy are
unchanged. The existing call-loop probe keeps a persistent STT socket and does
not substitute for the actual gateway acceptance steps above.

## Evidence and packaging

Local test outputs and the expected original-adapter failure are included under
`verification/`. `SHA256SUMS` records packaged file hashes. Deployment keys in
`.env.example` are blank; registration examples contain placeholders. No
`node_modules`, runtime binary, audio fixture, real `.env`, incident log export, or live API key
is included in the ZIP.

Fresh archive extraction was also verified on Node 22.23.3: all module and shell
syntax checks, the offline suite, and the full lifecycle suite passed. The
dependency was copied into the temporary extraction for testing only; it is
not packaged. The final ZIP was checked for CRC integrity, file hashes,
unexpected paths, symlinks, and credential patterns.


Optional live-provider harnesses are included as `verification/live-stt-test.mjs`
and `verification/live-tts-test.mjs`. They are not run by `pnpm test` and require
a real Soniox key via the process environment or `--key-file` pointing outside
this delivery folder. The STT harness additionally requires `--fixtures-dir`
with `manifest.json` and the three matching 16 kHz mono PCM files. The fixture
phrases and hashes are in `verification/live-fixture-manifest.json`; test audio
is kept outside the delivery ZIP. These scripts use localhost and real Soniox,
not a deployed gateway. Their path/argument configuration is portable.
