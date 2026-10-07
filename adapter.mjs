import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { timingSafeEqual } from 'node:crypto';
import WebSocket, { WebSocketServer } from 'ws';

const MAX_PAYLOAD = 1024 * 1024;
const MAX_PENDING_AUDIO = 256 * 1024;
const MAX_TEXT = 16_000;
const MAX_CONNECTIONS = 25;
const MAX_SESSION_MS = 15 * 60 * 1000;
const START_TIMEOUT_MS = 10_000;
const DEFAULT_AUTO_FINALIZE_SILENCE_MS = 750;
const DEFAULT_SILENCE_RMS_THRESHOLD = 200;
const STOP_FINALIZE_TIMEOUT_MS = 2_000;
const SUPPORTED_SAMPLE_RATES = new Set([8000, 16000, 24000, 48000]);
const SUPPORTED_TTS_SAMPLE_RATES = new Set([8000, 16000, 24000, 44100, 48000]);
const LOG_TRANSCRIPTS = process.env.ADAPTER_LOG_TRANSCRIPTS === 'true';

function logEvent(event, fields = {}) {
  console.log(`[soniox-adapter] ${event} ${JSON.stringify(fields)}`);
}

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

function authorized(header, expected, allowLegacyTtsHeader = false) {
  if (typeof header !== 'string' || !expected) return false;
  const match = header.match(
    allowLegacyTtsHeader ? /^Bearer\s*(\S+)$/i : /^Bearer (\S+)$/i,
  );
  if (!match) return false;
  const actual = Buffer.from(match[1]);
  const wanted = Buffer.from(expected);
  return actual.length === wanted.length && timingSafeEqual(actual, wanted);
}

function clearTimer(timer) {
  if (timer) clearTimeout(timer);
}

function nonNegativeNumber(value, fallback) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric >= 0 ? numeric : fallback;
}

function pcmRms(pcm) {
  if (!Buffer.isBuffer(pcm) || pcm.length < 2) return 0;

  let sum = 0;
  let count = 0;
  for (let offset = 0; offset + 1 < pcm.length; offset += 2) {
    sum += pcm.readInt16LE(offset);
    count += 1;
  }
  if (!count) return 0;

  const mean = sum / count;
  let squared = 0;
  for (let offset = 0; offset + 1 < pcm.length; offset += 2) {
    const centered = pcm.readInt16LE(offset) - mean;
    squared += centered * centered;
  }
  return Math.sqrt(squared / count);
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
      let endpointSeen = false;

      for (const token of result.tokens ?? []) {
        if (!token || typeof token.text !== 'string') {
          throw new Error('INVALID_TOKEN');
        }
        // Soniox may include tokens after <end>/<fin> that belong to the
        // response replay. They are returned again in a later response, so
        // never let them leak into the next downstream turn.
        if (endpointSeen) continue;
        if (token.text === '<end>' || token.text === '<fin>') {
          const final = flush();
          if (final) output.push(final);
          endpointSeen = true;
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

function attachStt(downstream, cfg, connectionId) {
  let upstream;
  let started = false;
  let closing = false;
  let emitInterim = false;
  let queuedBytes = 0;
  let queue = [];
  let controlQueue = [];
  let sampleRateHz;
  let language;
  let silenceTimer;
  let stopTimer;
  let stopRequested = false;
  let finalizationInFlight = false;
  let audioSequence = 0;
  let finalizedAudioSequence = 0;
  let finalizeRequestSequence = 0;
  let speechSinceEndpoint = false;
  let lastSpeechAt = 0;
  let audioBytes = 0;
  let audioFrames = 0;
  let finalCount = 0;
  const transcripts = createTranscriptBuffer();

  logEvent('ASR_SOCKET_OPEN', { connectionId });

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
    clearTimer(silenceTimer);
    clearTimer(stopTimer);
    queue = [];
    queuedBytes = 0;
    controlQueue = [];
    if (upstream && upstream.readyState !== WebSocket.CLOSED) {
      upstream.terminate();
    }
  }

  function fail(code) {
    if (closing) return;
    closing = true;
    logEvent('ASR_ERROR', {
      connectionId,
      code,
      audioBytes,
      audioFrames,
      finalCount,
    });
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

  function finishStop() {
    if (closing) return;
    closing = true;
    logEvent('ASR_STOP', {
      connectionId,
      audioBytes,
      audioFrames,
      finalCount,
    });
    cleanup();
    if (downstream.readyState === WebSocket.OPEN) {
      downstream.close(1000, 'STT stopped');
    }
  }

  function requestFinalize(reason) {
    if (closing || !started) return false;
    if (finalizationInFlight || audioSequence <= finalizedAudioSequence) return false;

    finalizationInFlight = true;
    finalizeRequestSequence = audioSequence;
    logEvent('ASR_FINALIZE_REQUEST', {
      connectionId,
      reason,
      audioFrames,
      audioBytes,
    });
    try {
      if (upstream?.readyState === WebSocket.OPEN) {
        sendJson(upstream, { type: 'finalize' });
      } else {
        controlQueue.push({ type: 'finalize' });
      }
      return true;
    } catch {
      fail('FINALIZE_FORWARD_ERROR');
      return false;
    }
  }

  function scheduleSilenceFinalize() {
    clearTimer(silenceTimer);
    silenceTimer = undefined;
    if (
      closing ||
      !speechSinceEndpoint ||
      finalizationInFlight ||
      cfg.sttAutoFinalizeSilenceMs <= 0
    ) {
      return;
    }

    const elapsed = Date.now() - lastSpeechAt;
    const waitMs = Math.max(25, cfg.sttAutoFinalizeSilenceMs - elapsed);
    silenceTimer = setTimeout(() => {
      silenceTimer = undefined;
      if (Date.now() - lastSpeechAt < cfg.sttAutoFinalizeSilenceMs) {
        scheduleSilenceFinalize();
        return;
      }
      requestFinalize('pcm_silence');
    }, waitMs);
  }

  function stop() {
    if (closing || stopRequested) return;
    stopRequested = true;
    clearTimer(silenceTimer);

    const needsFinalization =
      finalizationInFlight ||
      (audioSequence > finalizedAudioSequence && (speechSinceEndpoint || finalCount === 0));
    if (!needsFinalization) return finishStop();

    if (!finalizationInFlight) requestFinalize('gateway_stop');
    stopTimer = setTimeout(finishStop, STOP_FINALIZE_TIMEOUT_MS);
  }

  function complete() {
    if (closing) return;
    closing = true;
    logEvent('ASR_COMPLETE', {
      connectionId,
      audioBytes,
      audioFrames,
      finalCount,
    });
    cleanup();
    if (downstream.readyState === WebSocket.OPEN) {
      downstream.close(1000, 'Transcription complete');
    }
  }

  downstream.on('error', () => fail('DOWNSTREAM_ERROR'));
  downstream.on('close', () => {
    if (!closing) {
      logEvent('ASR_SOCKET_CLOSE', {
        connectionId,
        audioBytes,
        audioFrames,
        finalCount,
      });
    }
    cleanup();
  });

  downstream.on('message', (data, binary) => {
    if (closing) return;

    try {
      if (binary) {
        if (!started) return fail('AUDIO_BEFORE_START');
        if (data.length % 2 !== 0) return fail('INVALID_PCM_FRAME');

        audioBytes += data.length;
        audioFrames += 1;
        audioSequence += 1;
        const speechFrame = pcmRms(data) >= cfg.sttSilenceRmsThreshold;
        if (speechFrame) {
          speechSinceEndpoint = true;
          lastSpeechAt = Date.now();
        }
        scheduleSilenceFinalize();
        if (audioFrames === 1 || audioFrames % 50 === 0) {
          logEvent('ASR_AUDIO', {
            connectionId,
            audioFrames,
            audioBytes,
          });
        }

        if (upstream?.readyState === WebSocket.OPEN) {
          sendBounded(upstream, data, true);
        } else {
          queuedBytes += data.length;
          if (queuedBytes > MAX_PENDING_AUDIO) return fail('AUDIO_QUEUE_LIMIT');
          queue.push(Buffer.from(data));
        }
        return;
      }

      const message = JSON.parse(data.toString());
      if (message.type === 'stop') return stop();
      if (started && message.type === 'finalize') {
        requestFinalize('gateway_finalize');
        return;
      }

      if (started && message.type === 'start') {
        // The Artemis/Jambonz contract sends start once per socket. Accept an
        // identical duplicate defensively because some gateway revisions may
        // replay the start envelope when a sticky gather is re-armed.
        if (message.sampleRateHz !== sampleRateHz || languageHint(message.language) !== language) {
          return fail('INVALID_START');
        }
        logEvent('ASR_START_DUPLICATE', { connectionId });
        return;
      }

      if (
        started ||
        message.type !== 'start' ||
        message.format !== 'raw' ||
        message.encoding !== 'LINEAR16'
      ) {
        return fail('INVALID_START');
      }
      if (!SUPPORTED_SAMPLE_RATES.has(message.sampleRateHz)) {
        return fail('UNSUPPORTED_SAMPLE_RATE');
      }

      language = languageHint(message.language);
      sampleRateHz = message.sampleRateHz;
      started = true;
      emitInterim = message.interimResults === true;
      clearTimer(startDeadline);
      logEvent('ASR_START', {
        connectionId,
        language: message.language,
        sampleRateHz: message.sampleRateHz,
        interimResults: emitInterim,
      });

      upstream = new WebSocket(cfg.sttUrl, {
        headers: { Authorization: `Bearer ${cfg.sonioxKey}` },
        handshakeTimeout: START_TIMEOUT_MS,
        maxPayload: MAX_PAYLOAD,
        perMessageDeflate: false,
      });

      upstream.on('error', () => {
        logEvent('ASR_UPSTREAM_ERROR', { connectionId });
        fail('SONIOX_CONNECTION_ERROR');
      });
      upstream.on('close', () => {
        logEvent('ASR_UPSTREAM_CLOSE', { connectionId });
        if (!closing) fail('SONIOX_DISCONNECTED');
      });
      upstream.on('open', () => {
        if (closing) return upstream.terminate();
        try {
          logEvent('ASR_UPSTREAM_OPEN', { connectionId });
          sendJson(upstream, {
            model: cfg.sttModel,
            audio_format: 'pcm_s16le',
            sample_rate: sampleRateHz,
            num_channels: 1,
            language_hints: [language],
            enable_endpoint_detection: true,
          });
          for (const chunk of queue) sendBounded(upstream, chunk, true);
          queue = [];
          queuedBytes = 0;
          for (const control of controlQueue) sendJson(upstream, control);
          controlQueue = [];
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
            if (transcript.is_final) {
              finalCount += 1;
              const finalText = transcript.alternatives?.[0]?.transcript || '';
              logEvent('ASR_FINAL', {
                connectionId,
                sequence: finalCount,
                characters: finalText.length,
                ...(LOG_TRANSCRIPTS ? { transcript: finalText } : {}),
              });
            } else {
              logEvent('ASR_INTERIM', {
                connectionId,
                characters: transcript.alternatives?.[0]?.transcript?.length || 0,
              });
            }
            sendBounded(downstream, JSON.stringify(transcript));
          }
          const endpointSeen = (result.tokens ?? []).some(
            (token) => token?.text === '<end>' || token?.text === '<fin>',
          );
          if (endpointSeen) {
            const finalizedThrough = finalizationInFlight ? finalizeRequestSequence : audioSequence;
            finalizedAudioSequence = Math.max(finalizedAudioSequence, finalizedThrough);
            finalizationInFlight = false;
            if (audioSequence <= finalizedAudioSequence) {
              speechSinceEndpoint = false;
              lastSpeechAt = 0;
              clearTimer(silenceTimer);
              silenceTimer = undefined;
            } else {
              scheduleSilenceFinalize();
            }
            if (stopRequested) finishStop();
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

function attachTts(downstream, requestUrl, cfg, connectionId) {
  const query = new URL(requestUrl, 'http://adapter.local').searchParams;
  let language;
  let sampleRate;
  let current;
  let generationCount = 0;
  let audioBytes = 0;

  logEvent('TTS_SOCKET_OPEN', { connectionId });

  const stop = () => {
    const old = current;
    current = undefined;
    if (old) {
      clearTimeout(old.deadline);
      old.ws.terminate();
      logEvent('TTS_STOP', { connectionId, generationId: old.id });
    }
  };

  const fail = (code) => {
    logEvent('TTS_ERROR', { connectionId, code });
    sendJson(downstream, { type: 'data', data: { error: code } });
    downstream.close(1011, 'TTS adapter error');
    stop();
  };

  const lifetime = setTimeout(() => fail('SESSION_LIMIT'), MAX_SESSION_MS);
  downstream.on('error', () => fail('DOWNSTREAM_ERROR'));
  downstream.on('close', () => {
    clearTimeout(lifetime);
    logEvent('TTS_SOCKET_CLOSE', { connectionId, audioBytes, generationCount });
    stop();
  });

  try {
    language = languageHint(query.get('language') || 'en');
    sampleRate = Number(query.get('sampleRate') || 16000);
    if (!SUPPORTED_TTS_SAMPLE_RATES.has(sampleRate)) {
      throw new Error('UNSUPPORTED_SAMPLE_RATE');
    }
  } catch {
    fail('INVALID_TTS_PARAMETERS');
    return;
  }

  const voice = query.get('voice') || cfg.ttsVoice;
  logEvent('TTS_START', { connectionId, language, sampleRate, voice });
  sendJson(downstream, { type: 'connect', data: { sample_rate: sampleRate } });

  const createGeneration = () => {
    const generation = {
      id: randomUUID(),
      pending: [],
      textLength: 0,
      flushed: false,
      audioEnd: false,
      ws: new WebSocket(cfg.ttsUrl, {
        headers: { Authorization: `Bearer ${cfg.sonioxKey}` },
        handshakeTimeout: START_TIMEOUT_MS,
        maxPayload: MAX_PAYLOAD,
        perMessageDeflate: false,
      }),
    };
    current = generation;
    generationCount += 1;
    generation.deadline = setTimeout(() => {
      if (current === generation) fail('SYNTHESIS_TIMEOUT');
    }, 120_000);
    logEvent('TTS_UPSTREAM_CONNECTING', {
      connectionId,
      generationId: generation.id,
    });
    generation.ws.on('error', () => {
      if (current === generation) {
        logEvent('TTS_UPSTREAM_ERROR', {
          connectionId,
          generationId: generation.id,
        });
        fail('SONIOX_CONNECTION_ERROR');
      }
    });
    generation.ws.on('close', () => {
      if (current === generation) {
        logEvent('TTS_UPSTREAM_CLOSE', {
          connectionId,
          generationId: generation.id,
        });
        fail('SONIOX_DISCONNECTED');
      }
    });
    generation.ws.on('open', () => {
      if (current !== generation) return generation.ws.terminate();
      try {
        logEvent('TTS_UPSTREAM_OPEN', {
          connectionId,
          generationId: generation.id,
        });
        sendJson(generation.ws, {
          stream_id: generation.id,
          model: cfg.ttsModel,
          language,
          voice,
          audio_format: 'pcm_s16le',
          sample_rate: sampleRate,
        });
        for (const message of generation.pending) sendBounded(generation.ws, message);
        generation.pending = [];
      } catch {
        fail('TEXT_FORWARD_ERROR');
      }
    });
    generation.ws.on('message', (body, binary) => {
      if (current !== generation) return;
      try {
        if (binary) return fail('INVALID_SONIOX_RESPONSE');
        const response = JSON.parse(body.toString());
        if (response.error_code || response.error_type) return fail('SONIOX_API_ERROR');
        if (response.stream_id && response.stream_id !== generation.id) return;
        if (response.audio) {
          const pcm = Buffer.from(response.audio, 'base64');
          if (generation.audioEnd || pcm.length % 2 !== 0) {
            return fail('INVALID_PCM_RESPONSE');
          }
          if (pcm.length) {
            audioBytes += pcm.length;
            sendBounded(downstream, pcm, true);
          }
        }
        if (response.audio_end) generation.audioEnd = true;
        if (response.terminated) {
          if (!generation.audioEnd) return fail('INCOMPLETE_AUDIO');
          current = undefined;
          clearTimeout(generation.deadline);
          logEvent('TTS_DONE', {
            connectionId,
            generationId: generation.id,
            audioBytes,
          });
          sendJson(downstream, { type: 'done' });
          generation.ws.close();
        }
      } catch {
        fail('AUDIO_PROCESSING_ERROR');
      }
    });
    return generation;
  };

  downstream.on('message', (body, binary) => {
    try {
      if (binary) return fail('INVALID_TTS_MESSAGE');
      const message = JSON.parse(body.toString());
      logEvent('TTS_MESSAGE', { connectionId, type: message.type });
      if (message.type === 'stop') return stop();
      if (message.type === 'flush' && !current) {
        sendJson(downstream, { type: 'done' });
        return;
      }
      if (message.type !== 'stream' && message.type !== 'flush') {
        return fail('INVALID_TTS_MESSAGE');
      }
      if (message.type === 'stream' && typeof message.text !== 'string') {
        return fail('INVALID_TEXT');
      }
      if (message.type === 'stream' && message.text.length === 0) return;

      const generation = current ?? createGeneration();
      if (generation.flushed) {
        if (message.type === 'flush') return;
        return fail('OVERLAPPING_GENERATION');
      }
      const text = message.type === 'stream' ? message.text : '';
      generation.textLength += text.length;
      if (generation.textLength > MAX_TEXT) return fail('TEXT_TOO_LARGE');
      generation.flushed = message.type === 'flush';
      const payload = JSON.stringify({
        stream_id: generation.id,
        text,
        text_end: generation.flushed,
      });
      if (generation.ws.readyState === WebSocket.OPEN) sendBounded(generation.ws, payload);
      else {
        if (generation.pending.length >= 256) return fail('TEXT_QUEUE_LIMIT');
        generation.pending.push(payload);
      }
    } catch {
      fail('INVALID_TTS_MESSAGE');
    }
  });
}

function validateConfig(config) {
  if (!config?.adapterToken || !config?.sonioxKey) throw new Error('MISSING_SECRETS');
  if (!config.sttUrl) throw new Error('MISSING_STT_URL');
  if (!config.sttModel) throw new Error('MISSING_STT_MODEL');
  if (!config.ttsUrl) throw new Error('MISSING_TTS_URL');
  if (!config.ttsModel) throw new Error('MISSING_TTS_MODEL');
  if (!config.ttsVoice) throw new Error('MISSING_TTS_VOICE');
}

export function createAdapter(config) {
  const effectiveConfig = {
    ...config,
    ttsUrl: config?.ttsUrl || 'wss://tts-rt.soniox.com/tts-websocket',
    ttsModel: config?.ttsModel || 'tts-rt-v2',
    ttsVoice: config?.ttsVoice || 'Adrian',
    sttAutoFinalizeSilenceMs: nonNegativeNumber(
      config?.sttAutoFinalizeSilenceMs,
      DEFAULT_AUTO_FINALIZE_SILENCE_MS,
    ),
    sttSilenceRmsThreshold: nonNegativeNumber(
      config?.sttSilenceRmsThreshold,
      DEFAULT_SILENCE_RMS_THRESHOLD,
    ),
  };
  validateConfig(effectiveConfig);

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

    if (path !== '/stt' && path !== '/tts') {
      logEvent('WS_REJECT', { path, reason: 'NOT_FOUND' });
      return reject('404 Not Found');
    }
    if (!authorized(req.headers.authorization, effectiveConfig.adapterToken, path === '/tts')) {
      logEvent('WS_REJECT', { path, reason: 'UNAUTHORIZED' });
      return reject('401 Unauthorized');
    }
    if (sockets.clients.size >= (effectiveConfig.maxConnections ?? MAX_CONNECTIONS)) {
      logEvent('WS_REJECT', { path, reason: 'CONNECTION_LIMIT' });
      return reject('503 Service Unavailable');
    }

    const connectionId = randomUUID().slice(0, 8);
    logEvent('WS_ACCEPT', {
      path,
      connectionId,
      activeConnections: sockets.clients.size + 1,
    });
    sockets.handleUpgrade(req, socket, head, (ws) => {
      if (path === '/stt') attachStt(ws, effectiveConfig, connectionId);
      else attachTts(ws, req.url, effectiveConfig, connectionId);
    });
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
      sttAutoFinalizeSilenceMs: Number(
        process.env.SONIOX_STT_AUTO_FINALIZE_SILENCE_MS || DEFAULT_AUTO_FINALIZE_SILENCE_MS,
      ),
      sttSilenceRmsThreshold: Number(
        process.env.SONIOX_STT_SILENCE_RMS_THRESHOLD || DEFAULT_SILENCE_RMS_THRESHOLD,
      ),
      ttsUrl:
        process.env.SONIOX_TTS_WS_URL ||
        'wss://tts-rt.soniox.com/tts-websocket',
      ttsModel: process.env.SONIOX_TTS_MODEL || 'tts-rt-v2',
      ttsVoice: process.env.SONIOX_TTS_VOICE || 'Adrian',
    });

    adapter.server.listen(
      Number(process.env.ADAPTER_PORT || process.env.PORT || 8080),
      process.env.ADAPTER_HOST || '127.0.0.1',
    );
    adapter.server.on('listening', () => {
      console.log('Adapter listening; health path /health; STT path /stt; TTS path /tts');
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
