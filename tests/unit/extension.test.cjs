const test = require('node:test');
const assert = require('node:assert/strict');
const { extension, flush, deferred, FakeWebSocket } = require('../helpers/harness.cjs');
const job = { id: 'req', url: 'https://example.test/api', method: 'GET', headers: {} };
test('request classification distinguishes stream hints from ordinary downloads', async () => {
  const e = await extension();
  const cases = [
    [{ headers: { Accept: 'text/event-stream' } }, 'stream'],
    [{ headers: { accept: 'text/event-stream;q=0, application/json' } }, 'normal'],
    [{ headers: { accept: 'application/x-ndjson; charset=utf-8' } }, 'stream'],
    [{ url: 'https://example.test?stream=true' }, 'stream'],
    [{ url: 'https://example.test?alt=sse' }, 'stream'],
    [{ url: 'https://example.test/v1/model:streamGenerateContent' }, 'stream'],
    [{ body: Buffer.from('{"stream":true}').toString('base64') }, 'stream'],
    [{ body: Buffer.from('{"stream":false}').toString('base64') }, 'normal'],
    [{ headers: { 'transfer-encoding': 'chunked' } }, 'normal'],
    [{ url: 'invalid', body: 'not json' }, 'normal'],
  ];
  for (const [input, expected] of cases) assert.equal(e.call('getRequestType', { ...job, ...input }), expected, JSON.stringify(input));
});
test('native ping and invalid request return deterministic responses without fetch', async () => {
  const e = await extension();
  await e.port.onMessage.emit({ id: 'ping', ping: true });
  await e.port.onMessage.emit({ id: 'bad' }); await e.port.onMessage.emit(null);
  assert.equal(e.sent[0].pong, true); assert.match(e.sent[1].error, /Missing 'url'/);
});
test('binary POST body and streamed response preserve every byte', async () => {
  let input;
  const e = await extension({ fetch: async (url, options) => {
    input = { url, options };
    return new Response(new Uint8Array([0, 128, 255]), { status: 201, headers: { 'content-type': 'application/octet-stream' } });
  } });
  await e.port.onMessage.emit({ ...job, method: 'POST', body: 'AID/' }); await flush();
  assert.deepEqual([...input.options.body], [0, 128, 255]);
  assert.equal(input.options.method, 'POST');
  assert.deepEqual(e.sent.map(m => m.progress || m.stream), ['queued', 'fetching', 'start', 'data', 'end']);
  assert.equal(e.sent[2].status, 201); assert.equal(e.sent[3].body, 'AID/');
  assert.equal(e.time.timers.size, 0);
});
test('HEAD / 204 completes without waiting for a body', async () => {
  const e = await extension({ fetch: async () => new Response(null, { status: 204 }) });
  await e.port.onMessage.emit({ ...job, method: 'HEAD' }); await flush();
  assert.deepEqual(e.sent.filter(m => m.stream).map(m => m.stream), ['start', 'end']);
  assert.equal(e.time.timers.size, 0);
});
test('response Content-Type upgrades body deadline to stream policy', async () => {
  const e = await extension({ fetch: async () => new Response('data: hello\n\n', { headers: { 'content-type': 'text/event-stream' } }) });
  await e.port.onMessage.emit(job); await flush();
  assert.equal(e.sent.find(m => m.progress === 'fetching').timeoutMs, 45000);
  assert.equal(e.sent.find(m => m.stream === 'start').timeoutMs, 180000);
  assert.equal(e.sent.find(m => m.stream === 'data').timeoutMs, 90000);
});
test('normal and stream header deadlines abort at their respective limits', async () => {
  for (const [headers, deadline] of [[{}, 45000], [{ accept: 'text/event-stream' }, 180000]]) {
    const e = await extension({ fetch: (url, options) => new Promise((resolve, reject) => {
      options.signal.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
    }) });
    await e.port.onMessage.emit({ ...job, headers }); await flush();
    e.time.advance(deadline - 1); await flush(); assert.equal(e.sent.some(m => m.error), false);
    e.time.advance(1); await flush(); assert.match(e.sent.at(-1).error, /waiting for response headers/);
    assert.equal(e.time.timers.size, 0);
  }
});
test('first-byte stall reports stream error after headers', async () => {
  const reads = [], pendingRead = deferred(); let signal;
  const e = await extension({ fetch: async (url, options) => {
    signal = options.signal;
    signal.addEventListener('abort', () => pendingRead.reject(new DOMException('aborted', 'AbortError')));
    return { status: 200, headers: new Headers(), body: { getReader: () => ({ read: () => {
      reads.push(true); return pendingRead.promise;
    } }) } };
  } });
  await e.port.onMessage.emit(job); await flush();
  e.time.advance(45000); await flush();
  assert.equal(signal.aborted, true); assert.equal(e.sent.at(-1).stream, 'error');
  assert.match(e.sent.at(-1).error, /first response chunk/); assert.equal(reads.length, 1);
});
test('six fetch slots enforce FIFO queue and release slots after completion', async () => {
  const started = [], waiters = [];
  const e = await extension({ fetch: url => {
    started.push(url); const d = deferred(); waiters.push(d); return d.promise;
  } });
  for (let i = 0; i < 8; i++) await e.port.onMessage.emit({ ...job, id: String(i), url: `${job.url}/${i}` });
  assert.equal(started.length, 6);
  waiters[0].resolve(new Response(null, { status: 204 })); await flush();
  assert.equal(started.length, 7); assert.equal(started[6], `${job.url}/6`);
  waiters[1].resolve(new Response(null, { status: 204 })); await flush();
  assert.equal(started[7], `${job.url}/7`);
  for (const d of waiters) d.resolve(new Response(null, { status: 204 })); await flush();
  assert.equal(e.time.timers.size, 0);
});
test('queued POST expires without later executing upstream', async () => {
  const started = [], waiters = [];
  const e = await extension({ fetch: url => { started.push(url); const d = deferred(); waiters.push(d); return d.promise; } });
  for (let i = 0; i < 7; i++) await e.port.onMessage.emit({ ...job, id: String(i), headers: { accept: 'text/event-stream' }, method: 'POST' });
  e.time.advance(45000); await flush();
  assert.match(e.sent.find(m => m.id === '6' && m.error).error, /queue/);
  for (const d of waiters) d.resolve(new Response(null, { status: 204 })); await flush();
  assert.equal(started.length, 6); assert.equal(e.time.timers.size, 0);
});
test('network error before headers reports an ordinary error and releases slot', async () => {
  const e = await extension({ fetch: async () => { throw new Error('network unavailable'); } });
  await e.port.onMessage.emit(job); await flush();
  assert.match(e.sent.at(-1).error, /network unavailable/); assert.equal(e.sent.at(-1).stream, undefined);
  assert.equal(e.time.timers.size, 0);
});
test('large binary base64 encode/decode roundtrip crosses apply argument boundary', async () => {
  const e = await extension(), bytes = new Uint8Array(100000);
  for (let i = 0; i < bytes.length; i++) bytes[i] = i % 256;
  assert.deepEqual(e.call('base64ToUint8Array', e.call('uint8ArrayToBase64', bytes)), bytes);
});
test('disconnected native port reconnects on timer', async () => {
  const e = await extension(); assert.equal(e.connects, 1);
  await e.port.onDisconnect.emit(); e.time.advance(2000); await flush();
  assert.equal(e.connects, 2); assert.equal(e.storage.chrometunnel_connected, true);
});
test('WS rejects invalid schemes before constructing an upstream socket', async () => {
  const e = await extension({ WebSocket: FakeWebSocket });
  await e.port.onMessage.emit({ id: 'ws', kind: 'ws-open', url: 'https://example.test' });
  assert.match(e.sent.at(-1).wsError, /non-ws/);
});
test('WS text and binary messages relay; normal close clears extension session', async () => {
  const e = await extension({ WebSocket: FakeWebSocket });
  await e.port.onMessage.emit({ id: 'ws', kind: 'ws-open', url: 'wss://example.test', headers: { 'sec-websocket-protocol': 'chat, json' } });
  const ws = FakeWebSocket.instances.at(-1); ws.open();
  assert.deepEqual(Array.from(ws.protocols), ['chat', 'json']);
  await e.port.onMessage.emit({ id: 'ws', wsSend: { payload: Buffer.from('سلام').toString('base64') } });
  assert.equal(ws.sent[0], 'سلام');
  ws.onmessage({ data: new Uint8Array([0, 255]).buffer });
  assert.equal(e.sent.at(-1).wsMessage, 'AP8='); assert.equal(e.sent.at(-1).isBinary, true);
  await e.port.onMessage.emit({ id: 'ws', wsClose: { code: 1000, reason: 'done' } });
  assert.equal(e.get('wsSessions.size'), 0);
});
