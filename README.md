# HealthHub Soniox BYO STT adapter

This is the first implementation increment for the AFG HealthHub Pipeline
Voice path. It is an STT-only WebSocket adapter: Artemis sends raw caller PCM
to `/stt`, and this service translates it to the Soniox real-time STT protocol.

The initial pilot deliberately supports English only. Arabic can be added later
by extending the language map, the provider catalog, and the channel test
matrix together. TTS remains the existing HealthHub provider and is not exposed
by this service.

## Protocol implemented

Gateway to adapter:

- `Authorization: Bearer <ADAPTER_AUTH_TOKEN>` during the WebSocket upgrade.
- One JSON start message with `type: "start"`, `format: "raw"`,
  `encoding: "LINEAR16"`, `language`, and `sampleRateHz`.
- Binary, headerless signed 16-bit PCM audio frames.

Adapter to gateway:

```json
{
  "type": "transcription",
  "is_final": true,
  "alternatives": [{ "transcript": "recognized text", "confidence": 0.92 }]
}
```

Errors use a sanitized `type: "error"` envelope. Final transcripts are emitted
at the Soniox utterance endpoint, not once per stable token, to avoid duplicate
agent turns.

## Local setup

Use a Node environment with npm or pnpm. The reference setup used Node 26 and
`ws@8.21.0`; the implementation uses Node 18-compatible APIs.

```bash
cd soniox-byo-adapter
pnpm install
pnpm run check
pnpm test
```

The offline test uses a fake upstream WebSocket and fake credentials. It does
not call Soniox and does not require a real adapter token.

Run the service only after injecting secrets through the deployment mechanism:

```bash
ADAPTER_AUTH_TOKEN='...' \\
SONIOX_API_KEY='...' \\
pnpm start
```

The command above is illustrative only. Prefer a secret manager so credentials
do not enter shell history. The Soniox key must never be pasted into Artemis's
BYO API-key field.

Health check:

```bash
curl --fail http://127.0.0.1:8080/health
```

Expected response:

```json
{"status":"ok"}
```

`/health` proves process readiness only. It does not verify the Soniox key,
public WebSocket reachability, or an Artemis call.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `ADAPTER_AUTH_TOKEN` | required | Token Artemis sends to this adapter |
| `SONIOX_API_KEY` | required | Key used only for the adapter-to-Soniox connection |
| `ADAPTER_HOST` | `127.0.0.1` | Bind address; use `0.0.0.0` only inside a protected deployment |
| `ADAPTER_PORT` | `8080` | HTTP/WebSocket listener port |
| `SONIOX_STT_MODEL` | `stt-rt-v5` | Soniox real-time STT model |
| `SONIOX_STT_WS_URL` | `wss://stt-rt.soniox.com/transcribe-websocket` | Upstream Soniox WebSocket |

The public Artemis registration will use the eventual hosted URL:

```text
Service type: custom:soniox-adapter
STT endpoint: wss://<host>/stt
STT languages: en
TTS: disabled
API key: ADAPTER_AUTH_TOKEN
```

The hosting URL is intentionally not hard-coded because it will be selected
later.

Placeholder payloads are available in
`artemis-provider-registration.example.json` and
`artemis-pipeline-voice-speech.example.json`. The normal UI flow is preferred;
the examples contain placeholders only and are not complete channel-update
requests.

To publish the adapter as a private GitHub repository for Render, run
`publish-to-github.sh`. It stages only the reviewed adapter files and refuses to
continue if it finds `.env`, obvious API-key material, private keys, or local
audio fixtures. If the GitHub CLI is installed and authenticated, the script
creates the private repository automatically. Otherwise, create an empty
private repository in the browser and rerun it with
`GITHUB_REMOTE_URL=https://github.com/<account>/<repo>.git`.

## Free test deployment on Render

The repository includes `Dockerfile` and `render.yaml` for a Render Free Web
Service. Render terminates public TLS, so after deployment the adapter endpoint
will be:

```text
wss://<render-service-name>.onrender.com/stt
```

Deploy it as follows:

1. Push this `soniox-byo-adapter` directory to a private GitHub repository, or
   connect the repository that contains it to Render.
2. In Render, choose **New → Blueprint** and select the repository.
3. If the adapter is inside the larger HealthHub repository, set the blueprint
   root directory to `soniox-byo-adapter` or create the service manually with
   that directory as its root.
4. Keep the service on the **Free** plan.
5. Set `ADAPTER_AUTH_TOKEN` to a newly generated wrapper token.
6. Set `SONIOX_API_KEY` to the rotated Soniox token through Render's secret
   environment-variable UI.
7. Deploy and open the generated `/health` URL. It must return
   `{"status":"ok"}`.
8. Use the generated `wss://` URL in the Artemis BYO provider registration.

Render supports public WebSocket connections and automatically supplies a
`PORT` value; the adapter honors that value. The Free plan is for testing:
Render can spin the service down after 15 minutes without inbound traffic, and
the next connection can take roughly a minute to wake it. Warm the service with
`/health` before a test call, and do not treat this setup as production-ready.

## Live PCM probe

Prepare an approved or synthetic mono 16 kHz signed 16-bit PCM fixture. For a
WAV input, the setup document gives this conversion:

```bash
ffmpeg -i approved-input.wav -ac 1 -ar 16000 -f s16le caller-16k-mono.pcm
```

Then run against the deployed adapter after explicitly authorizing a live
provider test:

```bash
ADAPTER_AUTH_TOKEN='...' \\
node probe-stt.mjs wss://<host>/stt caller-16k-mono.pcm en
```

The probe logs final transcript text, so use only approved non-sensitive
fixtures.

## HealthHub channel configuration

On the dedicated Pipeline Voice test channel, register an STT-only BYO
provider and select the saved service instance in Speech Recognition:

```json
{
  "asrVendor": "custom:soniox-adapter",
  "asrServiceInstanceId": "<registered-service-instance-id>",
  "asrLanguage": "en"
}
```

Keep the existing working TTS provider selected under Speech Synthesis. Do not
set the Soniox model in the channel; it is adapter configuration.

## Limits and next steps

The adapter includes conservative pilot limits for payload size, queued audio,
connection count, session length, start timeout, transcript size, and
backpressure. It intentionally fails the active stream rather than replaying
audio after an upstream disconnect.

Before production use, complete the hosted WSS deployment, live English probe,
English HealthHub smoke call, interruption/hangup checks, and the existing
voice booking regression scenarios. Add Arabic only as a coordinated change to
the adapter, provider language catalog, channel, and validation pack.
