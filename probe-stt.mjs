import { readFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket from 'ws';

const [endpoint, file, language = 'en'] = process.argv.slice(2);
const adapterToken = process.env.ADAPTER_AUTH_TOKEN;

if (!endpoint || !file || !adapterToken) {
  throw new Error('Usage: ADAPTER_AUTH_TOKEN=... node probe-stt.mjs <wss-endpoint> <pcm-file> [en]');
}
if (language !== 'en') {
  throw new Error('This first pilot probe supports English only');
}

const pcm = await readFile(file);
if (pcm.length > 32_000 * 60 || pcm.length % 2 !== 0) {
  throw new Error('Use an aligned mono 16 kHz PCM fixture of at most 60 seconds');
}

const socket = new WebSocket(endpoint, {
  headers: { Authorization: `Bearer ${adapterToken}` },
});
const deadline = setTimeout(() => socket.terminate(), 90_000);

socket.on('error', () => console.error('WRAPPER_CONNECTION_ERROR'));
socket.on('close', () => clearTimeout(deadline));
socket.on('message', (data, binary) => {
  if (binary) return;
  const result = JSON.parse(data.toString());
  if (result.type === 'error') {
    console.error(`WRAPPER_STT_ERROR:${result.code || 'UNKNOWN'}`);
    socket.close();
  } else if (result.is_final) {
    console.log(result.alternatives?.[0]?.transcript || '');
  }
});

socket.on('open', async () => {
  socket.send(
    JSON.stringify({
      type: 'start',
      language,
      format: 'raw',
      encoding: 'LINEAR16',
      sampleRateHz: 16_000,
      interimResults: true,
    }),
  );

  for (
    let offset = 0;
    offset < pcm.length && socket.readyState === WebSocket.OPEN;
    offset += 3200
  ) {
    socket.send(pcm.subarray(offset, offset + 3200));
    await delay(100);
  }

  if (socket.readyState === WebSocket.OPEN) {
    socket.send(Buffer.alloc(32_000));
    socket.send(JSON.stringify({ type: 'finalize' }));
    await delay(5000);
    socket.close();
  }
});
