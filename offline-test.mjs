import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import WebSocket, { WebSocketServer } from 'ws';
import { createAdapter, createTranscriptBuffer } from './adapter.mjs';

const FAKE_ADAPTER_TOKEN = 'offline-adapter-token';
const FAKE_SONIOX_KEY = 'offline-soniox-key';
const pcm = Buffer.from([0, 0, 1, 0, 2, 0, 3, 0]);
const upstreamConfigs = [];

async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

async function openClient(url, authorization = `Bearer ${FAKE_ADAPTER_TOKEN}`) {
  const ws = new WebSocket(url, { headers: { Authorization: authorization } });
  await once(ws, 'open');
  return ws;
}

function waitFor(ws, predicate) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => finish(new Error('offline wait timed out')), 3000);
    const onMessage = (data, binary) => {
      try {
        const value = binary ? data : JSON.parse(data.toString());
        if (predicate(value, binary)) finish(null, value);
      } catch (error) {
        finish(error);
      }
    };
    const onClose = () => finish(new Error('socket closed before expected message'));
    const finish = (error, value) => {
      clearTimeout(timeout);
      ws.off('message', onMessage);
      ws.off('close', onClose);
      error ? reject(error) : resolve(value);
    };
    ws.on('message', onMessage);
    ws.on('close', onClose);
  });
}

// Provisional text is replaced, and only the endpoint emits a final turn.
const buffer = createTranscriptBuffer();
assert.equal(
  buffer.consume({ tokens: [{ text: 'wrong', is_final: false }] }, true)[0].is_final,
  false,
);
const final = buffer.consume(
  {
    tokens: [
      { text: 'Hello', confidence: 0.9, is_final: true },
      { text: ' world!', confidence: 0.9, is_final: true },
      { text: '<end>', is_final: true },
    ],
  },
  true,
);
assert.equal(final.length, 1);
assert.equal(final[0].alternatives[0].transcript, 'Hello world!');
assert.equal(final[0].is_final, true);
assert.deepEqual(buffer.consume({ tokens: [{ text: '<end>', is_final: true }] }, true), []);

const fakeHttp = http.createServer();
const fakeWs = new WebSocketServer({ server: fakeHttp });
fakeWs.on('connection', (ws, req) => {
  assert.equal(req.headers.authorization, `Bearer ${FAKE_SONIOX_KEY}`);
  const path = new URL(req.url, 'http://offline-upstream').pathname;
  let config;
  let text = '';
  ws.on('message', (body, binary) => {
    if (binary) {
      assert.equal(path, '/stt');
      assert.deepEqual(body, pcm);
      ws.send(JSON.stringify({ tokens: [{ text: 'Hello', is_final: false }] }));
      ws.send(
        JSON.stringify({
          tokens: [
            { text: 'Hello', confidence: 0.9, is_final: true },
            { text: ' world!', confidence: 0.9, is_final: true },
            { text: '<end>', is_final: true },
          ],
        }),
      );
      ws.send(JSON.stringify({ finished: true }));
      return;
    }
    const message = JSON.parse(body.toString());
    if (message.model) {
      config = message;
      upstreamConfigs.push({ path, config });
      return;
    }
    if (path !== '/tts' || !config) return;
    assert.equal(message.stream_id, config.stream_id);
    text += message.text;
    if (!message.text_end) return;
    assert.equal(text, 'Hello from TTS');
    ws.send(
      JSON.stringify({
        stream_id: config.stream_id,
        audio: pcm.toString('base64'),
        audio_end: true,
      }),
    );
    ws.send(JSON.stringify({ stream_id: config.stream_id, terminated: true }));
  });
});

const fakePort = await listen(fakeHttp);
const adapter = createAdapter({
  adapterToken: FAKE_ADAPTER_TOKEN,
  sonioxKey: FAKE_SONIOX_KEY,
  sttUrl: `ws://127.0.0.1:${fakePort}/stt`,
  sttModel: 'offline-stt',
  ttsUrl: `ws://127.0.0.1:${fakePort}/tts`,
  ttsModel: 'offline-tts',
  ttsVoice: 'offline-voice',
});
const adapterPort = await listen(adapter.server);

try {
  const health = await fetch(`http://127.0.0.1:${adapterPort}/health`);
  assert.equal(health.status, 200);
  assert.deepEqual(await health.json(), { status: 'ok' });

  const stt = await openClient(`ws://127.0.0.1:${adapterPort}/stt`);
  const transcriptPromise = waitFor(stt, (value) => value.is_final === true);
  stt.send(
    JSON.stringify({
      type: 'start',
      language: 'en',
      format: 'raw',
      encoding: 'LINEAR16',
      sampleRateHz: 16000,
      interimResults: true,
    }),
  );
  stt.send(pcm);
  const result = await transcriptPromise;
  assert.equal(result.type, 'transcription');
  assert.equal(result.alternatives[0].transcript, 'Hello world!');
  const config = upstreamConfigs.find((entry) => entry.path === '/stt').config;
  assert.equal(config.audio_format, 'pcm_s16le');
  assert.equal(config.sample_rate, 16000);
  assert.equal(config.num_channels, 1);
  assert.deepEqual(config.language_hints, ['en']);
  stt.close();

  const tts = new WebSocket(
    `ws://127.0.0.1:${adapterPort}/tts?voice=offline-voice&language=en&sampleRate=8000`,
    { headers: { Authorization: `Bearer${FAKE_ADAPTER_TOKEN}` } },
  );
  const connectPromise = waitFor(tts, (value) => value.type === 'connect');
  await once(tts, 'open');
  const connect = await connectPromise;
  assert.deepEqual(connect, { type: 'connect', data: { sample_rate: 8000 } });
  let audioFrames = 0;
  tts.on('message', (data, binary) => {
    if (binary) {
      assert.deepEqual(data, pcm);
      audioFrames += 1;
    }
  });
  const done = waitFor(tts, (value) => value.type === 'done');
  tts.send(JSON.stringify({ type: 'stream', text: 'Hello from ' }));
  tts.send(JSON.stringify({ type: 'stream', text: 'TTS' }));
  tts.send(JSON.stringify({ type: 'flush' }));
  await done;
  assert.equal(audioFrames, 1);
  const ttsConfig = upstreamConfigs.find((entry) => entry.path === '/tts').config;
  assert.equal(ttsConfig.model, 'offline-tts');
  assert.equal(ttsConfig.language, 'en');
  assert.equal(ttsConfig.audio_format, 'pcm_s16le');
  assert.equal(ttsConfig.sample_rate, 8000);
  tts.close();

  const invalidLanguage = await openClient(`ws://127.0.0.1:${adapterPort}/stt`);
  const languageError = waitFor(invalidLanguage, (value) => value.type === 'error');
  invalidLanguage.send(
    JSON.stringify({
      type: 'start',
      language: 'ar',
      format: 'raw',
      encoding: 'LINEAR16',
      sampleRateHz: 16000,
    }),
  );
  assert.equal((await languageError).code, 'UNSUPPORTED_LANGUAGE');
  invalidLanguage.terminate();

  const beforeStart = await openClient(`ws://127.0.0.1:${adapterPort}/stt`);
  const startError = waitFor(beforeStart, (value) => value.type === 'error');
  beforeStart.send(pcm);
  assert.equal((await startError).code, 'AUDIO_BEFORE_START');
  beforeStart.terminate();

  const badAuth = new WebSocket(`ws://127.0.0.1:${adapterPort}/stt`, {
    headers: { Authorization: 'Bearer incorrect' },
  });
  const [authError] = await once(badAuth, 'error');
  assert.match(authError.message, /401/);
  badAuth.terminate();

  console.log(
    'PASS: offline English STT/TTS, authentication, PCM, language and health checks',
  );
} finally {
  await adapter.close();
  for (const client of fakeWs.clients) client.terminate();
  await new Promise((resolve) => fakeWs.close(resolve));
  await new Promise((resolve) => fakeHttp.close(resolve));
}
