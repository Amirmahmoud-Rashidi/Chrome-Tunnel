const test = require('node:test');
const assert = require('node:assert/strict');
const { load, clock, plain } = require('../helpers/harness.cjs');
function setup(send) {
  const time = clock(), sent = [];
  const { createRelay } = load('native-host/core/relay.js', { globals: time });
  const relay = createRelay({ sendToExtension: send || (m => sent.push(m)) });
  return { time, sent, relay };
}
const request = { url: 'https://example.test', method: 'GET', headers: {} };
test('request headers, binary body and out-of-order response correlation', async () => {
  const { relay, sent, time } = setup();
  const headers = { host: 'example.test', connection: 'close', 'proxy-connection': 'close', 'content-length': '3', authorization: 'Bearer sample' };
  const a = relay.relayToExtension({ ...request, headers, bodyBuffer: Buffer.from([0, 128, 255]) });
  const b = relay.relayToExtension(request);
  assert.deepEqual(plain(sent[0].headers), { authorization: 'Bearer sample' });
  assert.equal(headers.host, 'example.test');
  assert.equal(sent[0].body, 'AID/');
  relay.handleExtensionResponse({ id: sent[1].id, status: 201 });
  relay.handleExtensionResponse({ id: sent[0].id, status: 200 });
  assert.equal((await a).status, 200); assert.equal((await b).status, 201);
  assert.equal(time.timers.size, 0);
});
test('send failure rejects without leaving a watchdog', async () => {
  const { relay, time } = setup(() => { throw new Error('disconnected'); });
  await assert.rejects(relay.relayToExtension(request), /disconnected/);
  assert.equal(time.timers.size, 0);
});
test('default fallback expires at 60 seconds', async () => {
  const { relay, time } = setup();
  const p = relay.relayToExtension(request), check = assert.rejects(p, { code: 'EXTENSION_INACTIVITY_TIMEOUT' });
  time.advance(60000); await check; assert.equal(time.timers.size, 0);
});
test('stream startup announcement permits 180 seconds plus delivery grace', async () => {
  const { relay, time, sent } = setup();
  const p = relay.relayToExtension(request);
  relay.handleExtensionResponse({ id: sent[0].id, progress: 'fetching', requestType: 'stream', timeoutMs: 180000 });
  time.advance(194999);
  relay.handleExtensionResponse({ id: sent[0].id, status: 200 });
  assert.equal((await p).status, 200);
});
test('duplicate progress does not extend a stalled request', async () => {
  const { relay, time, sent } = setup();
  const p = relay.relayToExtension(request), check = assert.rejects(p, /phase=headers/);
  const msg = { id: sent[0].id, progress: 'fetching', timeoutMs: 45000 };
  relay.handleExtensionResponse(msg); time.advance(59000);
  relay.handleExtensionResponse(msg); time.advance(1000); await check;
});
test('empty body chunks do not keep a stalled stream alive', async () => {
  const { relay, time, sent } = setup();
  const p = relay.relayToExtension(request), check = assert.rejects(p, /first-chunk/);
  relay.handleExtensionResponse({ id: sent[0].id, stream: 'start', timeoutMs: 45000 });
  time.advance(59000);
  relay.handleExtensionResponse({ id: sent[0].id, stream: 'data', body: '' });
  time.advance(1000); await check;
});
test('invalid announced deadlines use the finite default', async () => {
  for (const value of [-1, 0, 300001, '180000', 1.5]) {
    const { relay, time, sent } = setup();
    const p = relay.relayToExtension(request), check = assert.rejects(p, /60000ms/);
    relay.handleExtensionResponse({ id: sent[0].id, progress: 'fetching', timeoutMs: value });
    time.advance(60000); await check;
  }
});
test('long active streams exceed total timeout and preserve byte order', async () => {
  const { relay, time, sent } = setup(), received = [];
  const p = relay.relayToExtension({ ...request,
    onResponseStart: s => received.push(s.status),
    onResponseChunk: b => received.push(b.toString()), onResponseEnd: () => received.push('end') });
  const id = sent[0].id;
  relay.handleExtensionResponse({ id, stream: 'start', status: 200, timeoutMs: 180000 });
  for (let i = 0; i < 10; i++) {
    time.advance(80000);
    relay.handleExtensionResponse({ id, stream: 'data', body: Buffer.from(String(i)).toString('base64'), timeoutMs: 90000 });
  }
  relay.handleExtensionResponse({ id, stream: 'end' });
  assert.equal((await p).streamed, true);
  assert.deepEqual(received, [200, ...Array.from({ length: 10 }, (_, i) => String(i)), 'end']);
  assert.equal(time.timers.size, 0);
});
test('stream callback failure rejects and removes pending state', async () => {
  const { relay, sent, time } = setup();
  const p = relay.relayToExtension({ ...request, onResponseChunk() { throw new Error('client gone'); } });
  relay.handleExtensionResponse({ id: sent[0].id, stream: 'data', body: 'YQ==' });
  await assert.rejects(p, /client gone/); assert.equal(time.timers.size, 0);
  assert.doesNotThrow(() => relay.handleExtensionResponse({ id: sent[0].id, stream: 'end' }));
});
test('extension stream errors reject with a distinct error code', async () => {
  const { relay, sent } = setup(); const p = relay.relayToExtension(request);
  relay.handleExtensionResponse({ id: sent[0].id, stream: 'error', error: 'upstream reset' });
  await assert.rejects(p, { code: 'EXTENSION_STREAM_ERROR' });
});
test('stray, null and duplicate messages are harmless', async () => {
  const { relay, sent } = setup();
  for (const msg of [null, {}, { id: 'missing' }]) assert.doesNotThrow(() => relay.handleExtensionResponse(msg));
  const p = relay.relayToExtension(request), response = { id: sent[0].id, status: 200 };
  relay.handleExtensionResponse(response); relay.handleExtensionResponse(response); await p;
});
test('WS open, bidirectional data, listener unsubscribe and upstream close', async () => {
  const { relay, sent, time } = setup(), events = [];
  const unsubscribe = relay.onWsInbound(m => events.push(m));
  const p = relay.relayWsOpen({ url: 'wss://example.test/chat' }); const id = sent[0].id;
  relay.handleExtensionResponse({ id, wsAccepted: true }); assert.equal((await p).accepted, true);
  assert.equal(await relay.relayWsMessage({ id, wsSend: { payload: 'aGk=', isBinary: false } }), true);
  relay.handleExtensionResponse({ id, wsMessage: 'aGk=', isBinary: false });
  assert.equal(events[0].payload, 'aGk='); unsubscribe();
  relay.handleExtensionResponse({ id, wsClose: { code: 1000, reason: 'done' } });
  assert.equal(events.length, 1); assert.equal(time.timers.size, 0);
  assert.equal(await relay.relayWsMessage({ id, wsSend: {} }), false);
});
test('WS handshake failure and timeout clean up sessions', async () => {
  const { relay, sent, time } = setup();
  const p = relay.relayWsOpen({ url: 'wss://example.test' });
  relay.handleExtensionResponse({ id: sent[0].id, wsError: 'refused' });
  assert.equal((await p).error, 'refused');
  const q = relay.relayWsOpen({ url: 'wss://example.test' });
  time.advance(45000); assert.match((await q).error, /timed out/);
  assert.equal(time.timers.size, 0);
});
