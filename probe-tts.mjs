import { setTimeout as delay } from 'node:timers/promises';
import WebSocket from 'ws';

const [endpoint, text = 'Hello from the Soniox TTS test.', voice = 'Adrian', language = 'en'] =
  process.argv.slice(2);
const adapterToken = process.env.ADAPTER_AUTH_TOKEN;

if (!endpoint || !adapterToken) {
  throw new Error(
    'Usage: ADAPTER_AUTH_TOKEN=... node probe-tts.mjs <wss-endpoint> [text] [voice] [language]',
  );
}

const url = new URL(endpoint);
url.searchParams.set('voice', voice);
url.searchParams.set('language', language);
url.searchParams.set('sampleRate', '16000');

const socket = new WebSocket(url, {
  headers: { Authorization: `Bearer ${adapterToken}` },
});
const audio = [];
const deadline = setTimeout(() => socket.terminate(), 130_000);

socket.on('error', () => console.error('WRAPPER_TTS_CONNECTION_ERROR'));
socket.on('close', () => {
  clearTimeout(deadline);
  if (audio.length) {
    console.log(`TTS_AUDIO_BYTES:${audio.reduce((sum, chunk) => sum + chunk.length, 0)}`);
  }
});
socket.on('message', (data, binary) => {
  if (binary) {
    audio.push(Buffer.from(data));
    return;
  }
  const result = JSON.parse(data.toString());
  if (result.type === 'connect') {
    console.log(`TTS_CONNECTED:${result.data?.sample_rate || 'unknown'}`);
  } else if (result.type === 'data' && result.data?.error) {
    console.error(`WRAPPER_TTS_ERROR:${result.data.error}`);
    socket.close();
  } else if (result.type === 'done') {
    console.log('TTS_DONE');
    socket.close();
  }
});

socket.on('open', async () => {
  socket.send(JSON.stringify({ type: 'stream', text }));
  await delay(100);
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: 'flush' }));
  }
});
