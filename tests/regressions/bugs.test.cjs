// These assert correct behavior and deliberately FAIL on the audited upstream.
// No skip/todo/expected-failure switch: fixing production turns each test green.
const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { load, clock, extension, FakeWebSocket, FakeSocket, flush } = require('../helpers/harness.cjs');
const { sendChunked, createChunkReassembler } = require('../../native-host/chunking');
const frames = require('../../native-host/protocols/ws/ws-frames');
const { proxy, tlsRequest, tlsFixture } = require('../helpers/network.cjs');
const { X509Certificate } = require('node:crypto');
function assertFrameLimit(sent) {
  const largest = Math.max(...sent.map(m => Buffer.byteLength(JSON.stringify(m), 'utf8')));
  assert.ok(largest <= 1024 * 1024, `largest native frame = ${largest} bytes, above 1 MiB`);
}
test('BUG-01 host chunk threshold must measure UTF-8 bytes for Unicode', () => {
  const sent = []; sendChunked(m => sent.push(m), { id: 'unicode', text: '漢'.repeat(400000) });
  assertFrameLimit(sent);
});
test('BUG-01 extension must honor its documented conservative 1 MiB chunk budget for Unicode', async () => {
  const e = await extension(); e.call('sendToNative', { id: 'unicode', text: '漢'.repeat(400000) });
  assertFrameLimit(e.sent);
});
test('BUG-02 nested chunk JSON escaping must fit the actual native frame limit', () => {
  const sent = []; sendChunked(m => sent.push(m), { id: 'escapes', text: '"'.repeat(500000) });
  assertFrameLimit(sent);
});
test('BUG-03 host must reject malformed chunk metadata without RangeError', () => {
  assert.doesNotThrow(() => createChunkReassembler().handle({ chunkId: 'bad', total: -1, seq: 0, data: '{}' }));
});
test('BUG-03 extension must reject malformed chunk metadata without rejected handler', async () => {
  const e = await extension();
  await assert.doesNotReject(e.port.onMessage.emit({ chunkId: 'bad', total: -1, seq: 0, data: '{}' }));
});
test('BUG-04 WS 64-bit length above 4 GiB must not be truncated to low 32 bits', () => {
  const wire = Buffer.from([0x82, 0x7f, 0, 0, 0, 1, 0, 0, 0, 0]);
  assert.throws(() => frames.createFrameParser().push(wire), /overflow|limit|large/i);
});
async function wsHandler() {
  const server = new EventEmitter(), client = new FakeSocket(), sent = [], controls = [];
  const ws = load('native-host/protocols/ws/index.js', { mocks: { '../../failure-logger': { logFailedRequest() {} } } });
  ws.attach(server, {
    relayWsOpen: async () => ({ id: 's', accepted: true }),
    relayWsMessage: async m => { sent.push(m); }, relayWsControl: async m => { controls.push(m); }, wsTargets: new Map(),
  });
  server.emit('upgrade', { url: '/chat', headers: { host: 'example.test', upgrade: 'websocket', 'sec-websocket-key': 'dGhlIHNhbXBsZSBub25jZQ==' } }, client, Buffer.alloc(0));
  await flush();
  return { client, sent, controls };
}
test('BUG-05 oversized client WS frame must close that connection without throwing from data handler', async () => {
  const { client, controls } = await wsHandler();
  const header = Buffer.from([0x82, 0xff, 0, 0, 0, 0, 0, 0x20, 0, 0]);
  assert.doesNotThrow(() => client.emit('data', header));
  assert.ok(client.ended || client.destroyed || controls.length > 0, 'malformed frame must close the session');
});
test('BUG-06 fragmented WS text must arrive upstream as one complete message', async () => {
  const { client, sent } = await wsHandler();
  const a = frames.encodeFrame({ opcode: frames.OP_TEXT, payload: 'hel', mask: true }); a[0] &= 0x7f;
  const b = frames.encodeFrame({ opcode: frames.OP_CONTINUATION, payload: 'lo', mask: true });
  client.emit('data', Buffer.concat([a, b]));
  assert.deepEqual(sent.map(m => Buffer.from(m.wsSend.payload, 'base64').toString()), ['hello']);
});
test('BUG-07 negotiated WS subprotocol must be returned in accepted headers', async () => {
  const e = await extension({ WebSocket: FakeWebSocket });
  await e.port.onMessage.emit({ id: 'ws', kind: 'ws-open', url: 'wss://example.test', headers: { 'sec-websocket-protocol': 'chat' } });
  FakeWebSocket.instances.at(-1).open('chat');
  assert.equal(e.sent.find(m => m.wsAccepted).headers['sec-websocket-protocol'], 'chat');
});
test('BUG-08 client ping must not be injected into upstream application data', async () => {
  const e = await extension({ WebSocket: FakeWebSocket });
  await e.port.onMessage.emit({ id: 'ws', kind: 'ws-open', url: 'wss://example.test' });
  const ws = FakeWebSocket.instances.at(-1); ws.open();
  await e.port.onMessage.emit({ id: 'ws', wsSend: { payload: 'cGluZw==', kind: 'ping', isBinary: true } });
  assert.equal(ws.sent.length, 0, 'browser WebSocket.send creates application data, not a control ping');
});
test('BUG-09 client disconnect code 1006 must close browser WS with an API-legal code', async () => {
  const e = await extension({ WebSocket: FakeWebSocket });
  await e.port.onMessage.emit({ id: 'ws', kind: 'ws-open', url: 'wss://example.test' });
  const ws = FakeWebSocket.instances.at(-1); ws.open();
  await e.port.onMessage.emit({ id: 'ws', wsClose: { code: 1006, reason: 'Client disconnected' } });
  assert.equal(ws.closed.length, 1, 'upstream socket remains open after close() rejects 1006');
});
test('BUG-10 stale session connected flag must not permanently suppress reconnection', async () => {
  const e = await extension({ storedConnected: true });
  await e.chrome.alarms.onAlarm.emit({ name: 'chrometunnel-keep-alive' }); await flush();
  assert.equal(e.connects, 1, 'no live port exists in this new worker');
});
test('BUG-11 HTTPS CONNECT must preserve the target nondefault port', async t => {
  const p = await proxy(t);
  await tlsRequest(t, p, 'GET /api HTTP/1.1\r\nHost: example.test:8443\r\n\r\n', 'example.test:8443');
  assert.equal(p.jobs[0].url, 'https://example.test:8443/api');
});
test('BUG-12 HTTPS chunked POST must parse body without throwing or silently truncating', async () => {
  const server = new EventEmitter(), jobs = []; let decrypted;
  class Decrypted extends FakeSocket { constructor() { super(); decrypted = this; } }
  const https = load('native-host/protocols/https/index.js', { mocks: {
    tls: { TLSSocket: Decrypted }, './tls-mitm': { getCertificateForHost: () => ({}) },
    '../../failure-logger': { logFailedRequest() {} },
  } });
  https.attach(server, { relayToExtension: async job => { jobs.push(job); return { status: 200 }; } });
  const client = new FakeSocket(); server.emit('connect', { url: 'example.test:443', socket: client }, client, Buffer.alloc(0));
  const input = Buffer.from('POST /api HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n');
  assert.doesNotThrow(() => decrypted.emit('data', input));
  assert.equal(jobs[0].bodyBuffer.toString(), 'hello');
});
test('BUG-13 HTTP proxy credentials must be stripped before crossing the native bridge', async () => {
  const time = clock(), jobs = [];
  const { createRelay } = load('native-host/core/relay.js', { globals: time });
  const relay = createRelay({ sendToExtension: job => jobs.push(job) });
  const p = relay.relayToExtension({ url: 'https://example.test', method: 'GET',
    headers: { 'proxy-authorization': 'Basic cHJveHk6c2VjcmV0', connection: 'x-private', 'x-private': 'connection-only' } });
  relay.handleExtensionResponse({ id: jobs[0].id, status: 200 }); await p;
  assert.equal(jobs[0].headers['proxy-authorization'], undefined, 'proxy credentials leaked into extension fetch headers');
  assert.equal(jobs[0].headers['x-private'], undefined, 'Connection-nominated header forwarded');
});
test('BUG-14 IP HTTPS certificate must contain an IP subjectAltName', () => {
  const pair = tlsFixture().getCertificateForHost('127.0.0.1');
  const cert = new X509Certificate(pair.certPem);
  assert.equal(cert.checkIP('127.0.0.1'), '127.0.0.1');
});
test('BUG-15 WSS upgrade inside CONNECT must use the WebSocket relay, not ordinary fetch', async t => {
  const p = await proxy(t);
  await tlsRequest(t, p, 'GET /chat HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n');
  assert.equal(p.jobs[0].kind, 'ws-open');
});
test('BUG-16 newly accepted but silent WS session must start its idle watchdog', async () => {
  const time = clock(), jobs = [], events = [];
  const { createRelay } = load('native-host/core/relay.js', { globals: time });
  const relay = createRelay({ sendToExtension: job => jobs.push(job) });
  relay.onWsInbound(event => events.push(event));
  const p = relay.relayWsOpen({ url: 'wss://example.test' });
  relay.handleExtensionResponse({ id: jobs[0].id, wsAccepted: true }); await p;
  time.advance(120001);
  assert.equal(events.filter(event => event.kind === 'close').length, 1, 'idle deadline is never armed until first inbound message');
});
