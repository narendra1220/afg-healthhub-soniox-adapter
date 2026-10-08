import assert from 'node:assert/strict';
import http from 'node:http';
import net from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';
import WebSocket, { WebSocketServer } from 'ws';

// No credentials or provider requests: all transport is loopback. The registry
// model below mirrors the inspected gateway callback, not a FreeSWITCH runtime.
const adapterArg = process.argv.indexOf('--adapter');
const modulePath = adapterArg < 0 ? './adapter.mjs' : process.argv[adapterArg + 1];
const { createAdapter } = await import(pathToFileURL(resolve(modulePath)));
const onlyReplacement = process.argv.includes('--only-replacement');
const token = 'lifecycle-fake-adapter-token';
const providerKey = 'lifecycle-fake-provider-key';
const pcm = Buffer.from([0, 0, 1, 0, 2, 0, 3, 0]);
const start = (callSid) => ({ type: 'start', language: 'en', format: 'raw',
  encoding: 'LINEAR16', sampleRateHz: 16000, interimResults: true, options: { callSid } });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const resources = new Set();
const providerTimers = new Set();
const stats = { audio: 0, finalize: 0, config: 0 };

function waitEvent(emitter, event, predicate = () => true, timeoutMs = 4500) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => finish(new Error(`Timed out waiting for ${event}`)), timeoutMs);
    const handler = (...args) => { if (predicate(...args)) finish(null, args); };
    const error = error => finish(error);
    function finish(error, args) {
      clearTimeout(timer);
      emitter.off(event, handler);
      emitter.off('error', errorHandler);
      error ? reject(error) : resolve(args);
    }
    const errorHandler = error;
    emitter.on(event, handler);
    emitter.on('error', errorHandler);
  });
}

async function listen(server) {
  const ready = waitEvent(server, 'listening');
  server.listen(0, '127.0.0.1');
  await ready;
  return server.address().port;
}

const providerHttp = http.createServer();
const providerWs = new WebSocketServer({ noServer: true });
providerHttp.on('connection', socket => {
  resources.add(socket);
  socket.on('close', () => resources.delete(socket));
});
providerHttp.on('upgrade', (req, socket, head) => {
  assert.equal(req.headers.authorization, `Bearer ${providerKey}`);
  if (req.url === '/handshake-stall') return; // intentionally never completes
  providerWs.handleUpgrade(req, socket, head, ws => providerWs.emit('connection', ws, req));
});
providerWs.on('connection', (ws, req) => {
  const mode = req.url.slice(1);
  let audioFrames = 0;
  const timers = new Set();
  const final = () => {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ tokens: [
      { text: 'A recognized turn.', is_final: true }, { text: '<end>', is_final: true },
    ], ...(mode === 'finished' ? { finished: true } : {}) }));
  };
  ws.on('close', () => { for (const timer of timers) { clearTimeout(timer); providerTimers.delete(timer); } });
  ws.on('message', (data, binary) => {
    if (binary) {
      assert.deepEqual(data, pcm);
      stats.audio += 1;
      audioFrames += 1;
      if ((mode === 'auto' || mode === 'finished') && audioFrames === 1) final();
      else ws.send(JSON.stringify({ tokens: [{ text: 'Pending', is_final: false }] }));
      return;
    }
    const message = JSON.parse(data.toString());
    if (message.model) {
      stats.config += 1;
      if (mode === 'provider-error') ws.send(JSON.stringify({ error_code: 'LOCAL_FAILURE' }));
      return;
    }
    if (message.type !== 'finalize') return;
    stats.finalize += 1;
    if (mode === 'prompt') final();
    if (mode === 'late') {
      const timer = setTimeout(final, 1000);
      timers.add(timer); providerTimers.add(timer);
    }
    // /stall deliberately never returns a final; /auto ignores later finalize.
  });
});
const providerPort = await listen(providerHttp);

function makeAdapter(mode) {
  const adapter = createAdapter({ adapterToken: token, sonioxKey: providerKey,
    sttUrl: `ws://127.0.0.1:${providerPort}/${mode}`, sttModel: 'local-lifecycle-model',
    sttAutoFinalizeSilenceMs: 0, ttsUrl: `ws://127.0.0.1:${providerPort}/unused-tts`,
  });
  return adapter;
}

async function client(port) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/stt`, { headers: { Authorization: `Bearer ${token}` } });
  await waitEvent(ws, 'open');
  return ws;
}

function transcript(ws, final = true) {
  return waitEvent(ws, 'message', (body, binary) => !binary && JSON.parse(body.toString()).is_final === final);
}

async function withAdapter(mode, task) {
  const adapter = makeAdapter(mode);
  const port = await listen(adapter.server);
  try { await task(port); }
  finally { await adapter.close(); }
}

function maskedFrame(value, opcode = 1) {
  const payload = Buffer.from(value);
  assert.ok(payload.length < 65536);
  const mask = randomBytes(4);
  const header = Buffer.alloc(payload.length < 126 ? 2 : 4);
  header[0] = 0x80 | opcode;
  if (payload.length < 126) header[1] = 0x80 | payload.length;
  else { header[1] = 0x80 | 126; header.writeUInt16BE(payload.length, 2); }
  const masked = Buffer.from(payload);
  for (let i = 0; i < masked.length; i++) masked[i] ^= mask[i % 4];
  return Buffer.concat([header, mask, masked]);
}

// Deliberately uncooperative downstream peer: parses the close frame but never
// acknowledges it. Uses RFC6455 wire frames, not ws private implementation APIs.
async function rawClient(port) {
  const peer = new EventEmitter();
  const socket = net.createConnection({ host: '127.0.0.1', port });
  resources.add(socket);
  let upgraded = false;
  let pending = Buffer.alloc(0);
  peer.socket = socket;
  peer.send = value => socket.write(maskedFrame(JSON.stringify(value)));
  peer.sendPcm = () => socket.write(maskedFrame(pcm, 2));
  peer.closeFrames = 0;
  socket.on('error', error => peer.emit('error', error));
  socket.on('close', () => { resources.delete(socket); peer.emit('closed'); });
  socket.on('data', data => {
    pending = Buffer.concat([pending, data]);
    if (!upgraded) {
      const end = pending.indexOf('\r\n\r\n');
      if (end < 0) return;
      assert.match(pending.subarray(0, end).toString(), /^HTTP\/1\.1 101 /);
      pending = pending.subarray(end + 4);
      upgraded = true;
      peer.emit('ready');
    }
    while (pending.length >= 2) {
      let length = pending[1] & 127;
      let offset = 2;
      if (length === 126) {
        if (pending.length < 4) return;
        length = pending.readUInt16BE(2); offset = 4;
      }
      assert.notEqual(length, 127);
      assert.equal(pending[1] & 128, 0);
      if (pending.length < offset + length) return;
      const opcode = pending[0] & 15;
      const payload = pending.subarray(offset, offset + length);
      pending = pending.subarray(offset + length);
      if (opcode === 8) { peer.closeFrames += 1; peer.emit('close-frame'); }
      if (opcode === 1) peer.emit('message', JSON.parse(payload.toString()));
    }
  });
  const ready = waitEvent(peer, 'ready');
  await waitEvent(socket, 'connect');
  socket.write(`GET /stt HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ${randomBytes(16).toString('base64')}\r\nAuthorization: Bearer ${token}\r\n\r\n`);
  await ready;
  return peer;
}

async function replacementScenario() {
  await withAdapter('prompt', async port => {
    const old = await rawClient(port);
    const first = waitEvent(old, 'message', value => value.is_final);
    old.send(start('local-registry-call'));
    // Establish a completed first turn using masked binary PCM and an explicit
    // provider finalization, then test stop independently of endpoint timing.
    old.sendPcm();
    old.send({ type: 'finalize' });
    await first;

    const registry = { pipe: { name: 'replacement', active: true } };
    const replacement = registry.pipe;
    let forcedByGateway = false;
    // Reused media-bug lookup: a forced old close clears the current pipe;
    // a remote close for finished=true does not. This models inspected source.
    const closed = waitEvent(old, 'closed');
    const timer = setTimeout(() => { forcedByGateway = true; old.socket.destroy(); }, 3000);
    old.on('closed', () => {
      if (forcedByGateway) { registry.pipe.active = false; registry.pipe = null; }
    });
    const began = performance.now();
    old.send({ type: 'stop' });
    const next = await client(port);
    next.send(JSON.stringify(start('local-registry-call')));
    const second = transcript(next);
    next.send(pcm);
    next.send(JSON.stringify({ type: 'finalize' }));
    await second;
    try {
      await closed;
      assert.equal(forcedByGateway, false, 'Old recognizer reached gateway forced-close branch');
      assert.equal(registry.pipe, replacement);
      assert.equal(replacement.active, true);
      assert.ok(old.closeFrames > 0, 'No normal close frame sent');
      assert.ok(performance.now() - began < 1000, 'Close handshake was not bounded');
      const third = transcript(next);
      if (replacement.active) next.send(pcm);
      next.send(JSON.stringify({ type: 'finalize' }));
      await third;
      const nextClosed = waitEvent(next, 'close');
      next.send(JSON.stringify({ type: 'stop' }));
      await nextClosed;
      console.log('PASS replacement: second and third turns survive old close; no 3-second forced cleanup');
    } finally { clearTimeout(timer); old.socket.destroy(); next.terminate(); }
  });
}

try {
  if (!onlyReplacement) {
    await withAdapter('auto', async port => {
      let pipe;
      let forwarded = 0;
      const stops = [];
      for (let turn = 0; turn < 5; turn++) {
        const ws = await client(port);
        const current = { ws, active: true };
        pipe = current;
        const final = transcript(ws);
        ws.send(JSON.stringify(start('local-five-turn-call')));
        assert.equal(pipe, current);
        ws.send(pcm); forwarded += 1;
        await final;
        // Tail PCM and a pending finalize after a delivered endpoint must not
        // keep the old socket open or interfere with the replacement turn.
        ws.send(pcm);
        ws.send(JSON.stringify({ type: 'finalize' }));
        const closed = waitEvent(ws, 'close');
        const began = performance.now();
        ws.send(JSON.stringify({ type: 'stop' }));
        pipe = { active: true }; // replacement registry slot already exists
        await closed;
        stops.push(Math.round(performance.now() - began));
        assert.ok(pipe.active);
        assert.ok(stops.at(-1) < 750);
      }
      assert.equal(forwarded, 5);
      console.log(`PASS five successive STT sockets without manual turn finalization: stop ms=${stops}`);
    });

    for (const mode of ['stall', 'late', 'handshake-stall']) {
      await withAdapter(mode, async port => {
        const ws = await client(port);
        const received = [];
        ws.on('message', (body, binary) => { if (!binary) received.push(JSON.parse(body.toString())); });
        ws.send(JSON.stringify(start(`local-${mode}-call`)));
        ws.send(pcm);
        const closed = waitEvent(ws, 'close');
        const began = performance.now();
        ws.send(JSON.stringify({ type: 'stop' }));
        await closed;
        const elapsed = Math.round(performance.now() - began);
        assert.ok(elapsed < 750, `${mode} stop took ${elapsed}ms`);
        assert.ok(received.every(value => !value.is_final));
        await delay(20);
        console.log(`PASS stop while ${mode}: ${elapsed}ms; no late final or error`);
        assert.ok(received.every(value => value.type !== 'error'));
      });
    }

    await withAdapter('stall', async port => {
      const ws = await client(port);
      ws.send(JSON.stringify(start('local-duplicate-stop-call')));
      ws.send(pcm);
      await delay(20);
      const before = stats.finalize;
      const closed = waitEvent(ws, 'close');
      ws.send(JSON.stringify({ type: 'stop' }));
      ws.send(JSON.stringify({ type: 'stop' }));
      await closed;
      assert.equal(stats.finalize - before, 1);
      console.log('PASS duplicate stop: one finalize and one shutdown');
    });

    await withAdapter('stall', async port => {
      const ws = await client(port);
      const closed = waitEvent(ws, 'close');
      ws.send(JSON.stringify({ type: 'stop' }));
      await closed;
      console.log('PASS stop before start: immediate normal shutdown');
    });

    await withAdapter('stall', async port => {
      const raw = await rawClient(port);
      const closed = waitEvent(raw, 'closed');
      const began = performance.now();
      raw.send({ type: 'stop' });
      await closed;
      assert.ok(raw.closeFrames > 0);
      const elapsed = Math.round(performance.now() - began);
      assert.ok(elapsed < 750);
      console.log(`PASS missing close acknowledgement: TCP released after ${elapsed}ms`);
    });

    await withAdapter('stall', async port => {
      const raw = await rawClient(port);
      raw.send(start('local-drain-and-close-call'));
      raw.sendPcm();
      const closed = waitEvent(raw, 'closed');
      const began = performance.now();
      raw.send({ type: 'stop' });
      await closed;
      const elapsed = Math.round(performance.now() - began);
      assert.ok(raw.closeFrames > 0);
      assert.ok(elapsed >= 150 && elapsed < 1000);
      console.log(`PASS stalled provider plus missing close acknowledgement: TCP released after ${elapsed}ms`);
    });

    for (const mode of ['finished', 'provider-error']) {
      await withAdapter(mode, async port => {
        const raw = await rawClient(port);
        const response = waitEvent(raw, 'message', value => value.is_final || value.type === 'error');
        const closed = waitEvent(raw, 'closed');
        const began = performance.now();
        raw.send(start(`local-${mode}-call`));
        raw.sendPcm();
        const [value] = await response;
        if (mode === 'finished') {
          assert.equal(value.alternatives[0].transcript, 'A recognized turn.');
          raw.send({ type: 'stop' }); // arrives after completion initiated close
        } else assert.equal(value.code, 'SONIOX_API_ERROR');
        await closed;
        assert.ok(raw.closeFrames > 0);
        const elapsed = Math.round(performance.now() - began);
        assert.ok(elapsed < 1000);
        console.log(`PASS ${mode} with missing close acknowledgement: ${elapsed}ms`);
      });
    }
  }
  await replacementScenario();
  console.log('PASS: local STT lifecycle regression suite (mock provider / modeled gateway registry)');
} finally {
  for (const timer of providerTimers) clearTimeout(timer);
  for (const ws of providerWs.clients) ws.terminate();
  for (const socket of resources) socket.destroy();
  await new Promise(resolve => providerWs.close(resolve));
  await new Promise(resolve => providerHttp.close(resolve));
}
