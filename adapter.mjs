import http from 'node:http';
import { pathToFileURL } from 'node:url';
import { timingSafeEqual } from 'node:crypto';
import WebSocket, { WebSocketServer } from 'ws';

const MAX_PAYLOAD = 1024 * 1024;
const MAX_PENDING_AUDIO = 256 * 1024;
const MAX_TEXT = 16_000;
const MAX_CONNECTIONS = 25;
const MAX_SESSION_MS = 15 * 60 * 1000;
const START_TIMEOUT_MS = 10_000;
const SUPPORTED_SAMPLE_RATES = new Set([8000, 16000, 24000, 48000]);

// The first HealthHub pilot is English-only. Add Arabic here only when the
// adapter registration and the HealthHub channel are expanded together.
const LANGUAGE_HINTS = new Map([
  ['en', 'en'],
  ['en-US', 'en'],
  ['en-GB', 'en'],
]);

function languageHint(value) {
  const hint = LANGUAGE_HINTS.get(value);
  if (!hint) throw new Error('UNSUPPORTED_LANGUAGE');
  return hint;
}

function sendJson(ws, value) {
  if (ws.readyState !== WebSocket.OPEN) return;
  sendBounded(ws, JSON.stringify(value));
}

function sendBounded(ws, value, binary = false) {
  if (ws.readyState !== WebSocket.OPEN) throw new Error('SOCKET_NOT_OPEN');
  const bytes = Buffer.isBuffer(value) ? value.length : Buffer.byteLength(value);
  if (ws.bufferedAmount + bytes > MAX_PAYLOAD) {
    throw new Error('BACKPRESSURE_LIMIT');
  }
  ws.send(value, { binary });
}

function authorized(header, expected) {
  if (typeof header !== 'string' || !expected) return false;
  const match = header.match(/^Bearer (\S+)$/i);
  if (!match) return false;
  const actual = Buffer.from(match[1]);
  const wanted = Buffer.from(expected);
  return actual.length === wanted.length && timingSafeEqual(actual, wanted);
}

function clearTimer(timer) {
  if (timer) clearTimeout(timer);
}

export function createTranscriptBuffer() {
  let stable = [];
  let interim = [];
  let lastInterim = '';

  const summarize = (tokens, isFinal) => {
    const transcript = tokens
      .map((token) => token.text)
      .join('')
      .trim();
    if (!transcript) return null;
    if (transcript.length > MAX_TEXT) throw new Error('TRANSCRIPT_TOO_LARGE');

    const scored = tokens.filter(
      (token) =>
        token.text.trim() &&
        Number.isFinite(token.confidence) &&
        token.confidence >= 0 &&
        token.confidence <= 1,
    );
    const alternative = { transcript };
    if (scored.length) {
      alternative.confidence =
        scored.reduce((sum, token) => sum + token.confidence, 0) / scored.length;
    }
    return { type: 'transcription', is_final: isFinal, alternatives: [alternative] };
  };

  const flush = () => {
    const result = summarize(stable, true);
    stable = [];
    interim = [];
    lastInterim = '';
    return result;
  };

  return {
    consume(result, emitInterim) {
      const output = [];
      interim = [];

      for (const token of result.tokens ?? []) {
        if (!token || typeof token.text !== 'string') {
          throw new Error('INVALID_TOKEN');
        }
        if (token.text === '<end>') {
          const final = flush();
          if (final) output.push(final);
        } else if (token.translation_status !== 'translation') {
          (token.is_final ? stable : interim).push(token);
        }
      }

      if (stable.length + interim.length > MAX_TEXT) {
        throw new Error('TOO_MANY_TOKENS');
      }

      const provisional = summarize([...stable, ...interim], false);
      if (
        emitInterim &&
        provisional &&
        provisional.alternatives[0].transcript !== lastInterim
      ) {
        lastInterim = provisional.alternatives[0].transcript;
        output.push(provisional);
      }

      if (result.finished) {
        const final = flush();
        if (final) output.push(final);
      }
      return output;
    },
  };
}

function attachStt(downstream, cfg) {
  let upstream;
  let started = false;
  let closing = false;
  let emitInterim = false;
  let queuedBytes = 0;
  let queue = [];
  const transcripts = createTranscriptBuffer();

  const startDeadline = setTimeout(() => fail('START_TIMEOUT'), START_TIMEOUT_MS);
  const lifetime = setTimeout(() => fail('SESSION_LIMIT'), MAX_SESSION_MS);
  const keepalive = setInterval(() => {
    if (upstream?.readyState === WebSocket.OPEN) {
      try {
        sendJson(upstream, { type: 'keepalive' });
      } catch {
        fail('KEEPALIVE_FAILED');
      }
    }
  }, 10_000);

  function cleanup() {
    clearTimer(startDeadline);
    clearTimer(lifetime);
    clearInterval(keepalive);
    queue = [];
    queuedBytes = 0;
    if (upstream && upstream.readyState !== WebSocket.CLOSED) {
      upstream.terminate();
    }
  }

  function fail(code) {
    if (closing) return;
    closing = true;
    cleanup();
    try {
      sendJson(downstream, {
        type: 'error',
        code,
        message: 'STT adapter could not continue',
      });
    } catch {
      // The gateway may already have disconnected.
    }
    if (downstream.readyState === WebSocket.OPEN) {
      downstream.close(1011, 'STT adapter error');
    }
  }

  function complete() {
    if (closing) return;
    closing = true;
    cleanup();
    if (downstream.readyState === WebSocket.OPEN) {
      downstream.close(1000, 'Transcription complete');
    }
  }

  downstream.on('error', () => fail('DOWNSTREAM_ERROR'));
  downstream.on('close', cleanup);

  downstream.on('message', (data, binary) => {
    if (closing) return;

    try {
      if (binary) {
        if (!started) return fail('AUDIO_BEFORE_START');
        if (data.length % 2 !== 0) return fail('INVALID_PCM_FRAME');

        if (upstream?.readyState === WebSocket.OPEN) {
          sendBounded(upstream, data, true);
        } else {
          queuedBytes += data.length;
          if (queuedBytes > MAX_PENDING_AUDIO) return fail('AUDIO_QUEUE_LIMIT');
          queue.push(Buffer.from(data));
        }
        return;
      }

      const start = JSON.parse(data.toString());
      if (
        started ||
        start.type !== 'start' ||
        start.format !== 'raw' ||
        start.encoding !== 'LINEAR16'
      ) {
        return fail('INVALID_START');
      }
      if (!SUPPORTED_SAMPLE_RATES.has(start.sampleRateHz)) {
        return fail('UNSUPPORTED_SAMPLE_RATE');
      }

      const language = languageHint(start.language);
      started = true;
      emitInterim = start.interimResults === true;
      clearTimer(startDeadline);

      upstream = new WebSocket(cfg.sttUrl, {
        headers: { Authorization: `Bearer ${cfg.sonioxKey}` },
        handshakeTimeout: START_TIMEOUT_MS,
        maxPayload: MAX_PAYLOAD,
        perMessageDeflate: false,
      });

      upstream.on('error', () => fail('SONIOX_CONNECTION_ERROR'));
      upstream.on('close', () => {
        if (!closing) fail('SONIOX_DISCONNECTED');
      });
      upstream.on('open', () => {
        if (closing) return upstream.terminate();
        try {
          sendJson(upstream, {
            model: cfg.sttModel,
            audio_format: 'pcm_s16le',
            sample_rate: start.sampleRateHz,
            num_channels: 1,
            language_hints: [language],
            enable_endpoint_detection: true,
          });
          for (const chunk of queue) sendBounded(upstream, chunk, true);
          queue = [];
          queuedBytes = 0;
        } catch {
          fail('AUDIO_FORWARD_ERROR');
        }
      });
      upstream.on('message', (body, isBinary) => {
        if (closing) return;
        try {
          if (isBinary) return fail('INVALID_SONIOX_RESPONSE');
          const result = JSON.parse(body.toString());
          if (result.error_code || result.error_type) return fail('SONIOX_API_ERROR');

          for (const transcript of transcripts.consume(result, emitInterim)) {
            sendBounded(downstream, JSON.stringify(transcript));
          }
          if (result.finished) complete();
        } catch {
          fail('TRANSCRIPT_PROCESSING_ERROR');
        }
      });
    } catch (error) {
      if (error?.message === 'UNSUPPORTED_LANGUAGE') return fail('UNSUPPORTED_LANGUAGE');
      fail('INVALID_STT_MESSAGE');
    }
  });
}

function validateConfig(config) {
  if (!config?.adapterToken || !config?.sonioxKey) throw new Error('MISSING_SECRETS');
  if (!config.sttUrl) throw new Error('MISSING_STT_URL');
  if (!config.sttModel) throw new Error('MISSING_STT_MODEL');
}

export function createAdapter(config) {
  validateConfig(config);

  const server = http.createServer((req, res) => {
    if (req.url !== '/health') {
      res.writeHead(404);
      res.end();
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok' }));
  });

  const sockets = new WebSocketServer({
    noServer: true,
    maxPayload: MAX_PAYLOAD,
    perMessageDeflate: false,
  });

  server.on('upgrade', (req, socket, head) => {
    const path = new URL(req.url, 'http://adapter.local').pathname;
    const reject = (status) =>
      socket.end(`HTTP/1.1 ${status}\r\nConnection: close\r\n\r\n`);

    if (path !== '/stt') return reject('404 Not Found');
    if (!authorized(req.headers.authorization, config.adapterToken)) {
      return reject('401 Unauthorized');
    }
    if (sockets.clients.size >= (config.maxConnections ?? MAX_CONNECTIONS)) {
      return reject('503 Service Unavailable');
    }

    sockets.handleUpgrade(req, socket, head, (ws) => attachStt(ws, config));
  });

  return {
    server,
    async close() {
      for (const client of sockets.clients) client.terminate();
      await new Promise((resolve) => sockets.close(resolve));
      if (!server.listening) return;
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const adapter = createAdapter({
      adapterToken: process.env.ADAPTER_AUTH_TOKEN,
      sonioxKey: process.env.SONIOX_API_KEY,
      sttUrl:
        process.env.SONIOX_STT_WS_URL ||
        'wss://stt-rt.soniox.com/transcribe-websocket',
      sttModel: process.env.SONIOX_STT_MODEL || 'stt-rt-v5',
    });

    adapter.server.listen(
      Number(process.env.ADAPTER_PORT || process.env.PORT || 8080),
      process.env.ADAPTER_HOST || '127.0.0.1',
    );
    adapter.server.on('listening', () => {
      console.log('Adapter listening; health path /health; STT path /stt');
    });
    adapter.server.on('error', () => {
      console.error('ADAPTER_LISTEN_FAILED');
      process.exit(1);
    });
    for (const signal of ['SIGINT', 'SIGTERM']) {
      process.once(signal, () => {
        adapter.close().then(() => process.exit(0));
      });
    }
  } catch {
    console.error('ADAPTER_CONFIGURATION_FAILED');
    process.exit(1);
  }
}
