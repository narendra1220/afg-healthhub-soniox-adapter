import { execFile } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { once } from 'node:events';
import { promisify } from 'node:util';
import WebSocket from 'ws';

const execFileAsync = promisify(execFile);
const FRAME_BYTES = 3_200; // 100 ms of mono 16 kHz LINEAR16 audio.
const SILENCE_FRAMES = 10; // 1 second to trigger Soniox endpoint detection.
const TIMEOUT_MS = 90_000; // Allow Render Free to wake before the first socket.

function usage() {
  throw new Error(
    'Usage: ADAPTER_AUTH_TOKEN=... node probe-call-loop.mjs <render-url> <audio-1> <audio-2> [audio-3 ...] --tts <reply-1> <reply-2> [reply-3 ...]',
  );
}

function makeWsUrl(input, path) {
  const url = new URL(input.includes('://') ? input : `https://${input}`);
  if (url.protocol === 'https:') url.protocol = 'wss:';
  if (url.protocol === 'http:') url.protocol = 'ws:';
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('The Render URL must use http(s) or ws(s)');
  }
  url.pathname = path;
  url.search = '';
  return url;
}

async function loadPcm(file) {
  if (file.toLowerCase().endsWith('.pcm')) {
    const pcm = await readFile(file);
    if (pcm.length % 2 !== 0) throw new Error(`PCM file is not 16-bit aligned: ${file}`);
    return pcm;
  }

  const { stdout } = await execFileAsync(
    'ffmpeg',
    ['-v', 'error', '-i', file, '-ac', '1', '-ar', '16000', '-f', 's16le', 'pipe:1'],
    { encoding: 'buffer', maxBuffer: 64 * 1024 * 1024 },
  );
  if (stdout.length % 2 !== 0) throw new Error(`Converted PCM is not 16-bit aligned: ${file}`);
  return stdout;
}

function waitForMessage(ws, predicate, label, timeoutMs = TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => finish(new Error(`${label} timed out`)), timeoutMs);
    const onMessage = (data, binary) => {
      try {
        const value = binary ? data : JSON.parse(data.toString());
        if (predicate(value, binary)) finish(null, value);
      } catch (error) {
        finish(error);
      }
    };
    const onClose = () => finish(new Error(`${label} socket closed`));
    const finish = (error, value) => {
      clearTimeout(timeout);
      ws.off('message', onMessage);
      ws.off('close', onClose);
      error ? reject(error) : resolve(value);
    };
    ws.on('message', onMessage);
    ws.once('close', onClose);
  });
}

function openSocket(url, token) {
  const ws = new WebSocket(url, {
    headers: { Authorization: `Bearer ${token}` },
    perMessageDeflate: false,
  });
  const opened = new Promise((resolve, reject) => {
    ws.once('open', resolve);
    ws.once('error', reject);
  });
  return { ws, opened };
}

async function sendPcmRealtime(ws, pcm) {
  for (let offset = 0; offset < pcm.length; offset += FRAME_BYTES) {
    if (ws.readyState !== WebSocket.OPEN) throw new Error('STT socket closed while sending audio');
    ws.send(pcm.subarray(offset, offset + FRAME_BYTES));
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  for (let index = 0; index < SILENCE_FRAMES; index += 1) {
    if (ws.readyState !== WebSocket.OPEN) throw new Error('STT socket closed during endpoint silence');
    ws.send(Buffer.alloc(FRAME_BYTES));
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

function closeSocket(ws) {
  if (!ws || ws.readyState === WebSocket.CLOSED) return;
  ws.close();
  setTimeout(() => ws.terminate(), 1_000).unref();
}

const rawArgs = process.argv.slice(2);
const ttsIndex = rawArgs.indexOf('--tts');
if (ttsIndex < 2) usage();

const [renderUrl, ...beforeTts] = rawArgs;
const audioFiles = beforeTts.slice(0, ttsIndex - 1);
const ttsTexts = rawArgs.slice(ttsIndex + 1);
const adapterToken = process.env.ADAPTER_AUTH_TOKEN;

if (!adapterToken || audioFiles.length === 0 || ttsTexts.length !== audioFiles.length) {
  usage();
}

const sttUrl = makeWsUrl(renderUrl, '/stt');
const ttsUrl = makeWsUrl(renderUrl, '/tts');
ttsUrl.searchParams.set('voice', process.env.SONIOX_TTS_VOICE || 'Adrian');
ttsUrl.searchParams.set('language', 'en');
ttsUrl.searchParams.set('sampleRate', '16000');

const pcmFiles = await Promise.all(audioFiles.map((file) => loadPcm(file)));
const outputDir = `/tmp/soniox-call-loop-${Date.now()}`;
await mkdir(outputDir, { recursive: true });

const stt = openSocket(sttUrl, adapterToken);
const tts = openSocket(ttsUrl, adapterToken);
let ttsError;
let ttsGeneration;

const ttsConnect = waitForMessage(
  tts.ws,
  (value) => value.type === 'connect',
  'TTS connect acknowledgement',
);

tts.ws.on('error', (error) => {
  ttsError = error;
});

try {
  await Promise.all([stt.opened, tts.opened]);

  const connect = await ttsConnect;
  console.log(`TTS_CONNECTED sampleRate=${connect.data?.sample_rate || 'unknown'}`);

  stt.ws.on('message', (data, binary) => {
    if (binary) return;
    const value = JSON.parse(data.toString());
    if (value.type === 'error') {
      console.error(`ASR_ERROR code=${value.code || 'UNKNOWN'}`);
      return;
    }
    if (value.is_final) {
      const text = value.alternatives?.[0]?.transcript || '';
      console.log(`ASR_FINAL ${text}`);
    }
  });

  tts.ws.on('message', (data, binary) => {
    if (binary) {
      if (ttsGeneration) ttsGeneration.audio.push(Buffer.from(data));
      return;
    }
    const value = JSON.parse(data.toString());
    if (value.type === 'data' && value.data?.error) {
      ttsError = new Error(`TTS_ERROR:${value.data.error}`);
    }
    if (value.type === 'done' && ttsGeneration) {
      const finished = ttsGeneration;
      ttsGeneration = undefined;
      finished.resolve(Buffer.concat(finished.audio));
    }
  });

  stt.ws.send(
    JSON.stringify({
      type: 'start',
      language: 'en',
      format: 'raw',
      encoding: 'LINEAR16',
      sampleRateHz: 16_000,
      interimResults: true,
      options: { callSid: `render-loop-${Date.now()}` },
    }),
  );

  for (let index = 0; index < pcmFiles.length; index += 1) {
    console.log(`CALL_TURN_START index=${index + 1} audio=${audioFiles[index]}`);
    const finalBefore = new Promise((resolve, reject) => {
      const listener = (data, binary) => {
        if (binary) return;
        const value = JSON.parse(data.toString());
        if (value.type === 'error') {
          stt.ws.off('message', listener);
          reject(new Error(`ASR_ERROR:${value.code || 'UNKNOWN'}`));
        } else if (value.is_final) {
          stt.ws.off('message', listener);
          resolve(value.alternatives?.[0]?.transcript || '');
        }
      };
      stt.ws.on('message', listener);
      setTimeout(() => {
        stt.ws.off('message', listener);
        reject(new Error('ASR final transcript timed out'));
      }, TIMEOUT_MS).unref();
    });

    await sendPcmRealtime(stt.ws, pcmFiles[index]);
    // Explicitly finalize this turn so the next turn can reuse the same STT
    // socket even when automatic endpoint detection is slow.
    stt.ws.send(JSON.stringify({ type: 'finalize' }));
    const transcript = await finalBefore;
    console.log(`CALL_TURN_ASR index=${index + 1} transcript=${transcript}`);

    const ttsDone = new Promise((resolve, reject) => {
      ttsGeneration = { audio: [], resolve, reject };
      setTimeout(() => reject(new Error('TTS done timed out')), TIMEOUT_MS).unref();
    });
    const reply = ttsTexts[index];
    const midpoint = Math.max(1, Math.floor(reply.length / 2));
    tts.ws.send(JSON.stringify({ type: 'stream', text: reply.slice(0, midpoint) }));
    await new Promise((resolve) => setTimeout(resolve, 100));
    tts.ws.send(JSON.stringify({ type: 'stream', text: reply.slice(midpoint) }));
    tts.ws.send(JSON.stringify({ type: 'flush' }));
    const audio = await ttsDone;
    if (ttsError) throw ttsError;
    if (audio.length === 0) throw new Error('TTS returned no audio');
    await writeFile(`${outputDir}/turn-${index + 1}.pcm`, audio);

    console.log(`CALL_TURN_TTS index=${index + 1} audioBytes=${audio.length} reply=${reply}`);
  }

  console.log(`CALL_LOOP_PASS turns=${pcmFiles.length}`);
  console.log(`TTS_PCM_OUTPUT_DIR=${outputDir}`);
} finally {
  closeSocket(stt.ws);
  closeSocket(tts.ws);
}
