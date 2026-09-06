const test = require('node:test');
const assert = require('node:assert/strict');
const { sendChunked, createChunkReassembler } = require('../../native-host/chunking');
const { extension, clock, load, FakeWebSocket, FakeSocket, flush } = require('../helpers/harness.cjs');
const frames = require('../../native-host/protocols/ws/ws-frames');
const { stripHopByHop } = require('../../native-host/core/headers');
const { EventEmitter } = require('node:events');

test('fixed Unicode and JSON-escaped messages roundtrip in both native directions', async () => {
  const e = await extension();
  const message = { id: 'unicode', ping: true, text: '🌍\\"漢'.repeat(180000) };
  const sent = []; sendChunked(m => sent.push(m), message);
  for (const frame of sent.reverse()) {
    assert.ok(Buffer.byteLength(JSON.stringify(frame)) < 1048576);
    await e.port.onMessage.emit(frame);
  }
  assert.equal(e.sent.at(-1).pong, true);
  e.sent.length = 0; e.call('sendToNative', message);
  const decoder = createChunkReassembler(); let complete;
  for (const frame of e.sent.reverse()) {
    assert.ok(Buffer.byteLength(JSON.stringify(frame)) < 1048576);
    complete = decoder.handle(frame) || complete;
  }
  assert.deepEqual(complete, message);
});

test('fixed reassemblers reject invalid or inconsistent metadata and recover', async () => {
  const e = await extension(), decoder = createChunkReassembler();
  for (const patch of [{ total: 0 }, { total: 1e9 }, { seq: -1 }, { seq: 2 }, { total: 1.5 }, { data: {} }, { chunkId: 42 }]) {
    const msg = { chunkId: 'bad', total: 2, seq: 0, data: '{}', ...patch };
    assert.equal(decoder.handle(msg), null); await assert.doesNotReject(e.port.onMessage.emit(msg));
  }
  decoder.handle({ chunkId: 'x', total: 2, seq: 0, data: '{' });
  assert.equal(decoder.handle({ chunkId: 'x', total: 1, seq: 0, data: '{}' }), null);
  assert.deepEqual(decoder.handle({ chunkId: 'fresh', total: 1, seq: 0, data: '{"ok":true}' }), { ok: true });
});

test('fixed WS fragmentation permits interleaved ping but rejects orphan continuations and aggregate overflow', () => {
  const first = frames.encodeFrame({ opcode: frames.OP_TEXT, payload: 'hel', mask: true }); first[0] &= 0x7f;
  const ping = frames.encodeFrame({ opcode: frames.OP_PING, payload: 'p', mask: true });
  const last = frames.encodeFrame({ opcode: frames.OP_CONTINUATION, payload: 'lo', mask: true });
  const parser = frames.createFrameParser({ requireMasked: true });
  assert.deepEqual(parser.push(first), []);
  assert.equal(parser.push(ping)[0].opcode, frames.OP_PING);
  assert.equal(parser.push(last)[0].payload.toString(), 'hello');
  assert.throws(() => frames.createFrameParser().push(last), /continuation/);
  const small = frames.createFrameParser({ maxFrameBytes: 4 }); small.push(first);
  assert.throws(() => small.push(last), /limit/);
});

test('fixed client frame parser rejects unmasked frames and invalid UTF-8', () => {
  assert.throws(() => frames.createFrameParser({ requireMasked: true }).push(Buffer.from([0x81, 1, 65])), /masked/);
  assert.throws(() => frames.createFrameParser().push(Buffer.from([0x81, 1, 255])), /UTF-8/);
});

test('fixed hop-by-hop stripping is case-insensitive and removes Connection tokens', () => {
  const original = { Connection: 'X-Private, TE', 'X-Private': 'hidden', TE: 'trailers', 'Proxy-Authorization': 'sample', Authorization: 'keep' };
  assert.deepEqual(stripHopByHop(original), { Authorization: 'keep' });
  assert.equal(original['X-Private'], 'hidden');
});

test('fixed WS idle deadline refreshes on outbound data and closes upstream at expiry', async () => {
  const time = clock(), jobs = [], events = [];
  const relay = load('native-host/core/relay.js', { globals: time }).createRelay({ sendToExtension: j => jobs.push(j) });
  relay.onWsInbound(e => events.push(e));
  const p = relay.relayWsOpen({ url: 'wss://example.test' }); const id = jobs[0].id;
  relay.handleExtensionResponse({ id, wsAccepted: true }); await p;
  time.advance(119000); await relay.relayWsMessage({ id, wsSend: { payload: 'eA==' } });
  time.advance(119000); assert.equal(events.length, 0);
  time.advance(1000); assert.equal(events[0].kind, 'close');
  assert.equal(jobs.at(-1).wsClose.reason, 'Idle timeout'); assert.equal(time.timers.size, 0);
});

test('fixed WS local close clears timers even without an upstream close callback', async () => {
  const time = clock(), jobs = [];
  const relay = load('native-host/core/relay.js', { globals: time }).createRelay({ sendToExtension: j => jobs.push(j) });
  const p = relay.relayWsOpen({ url: 'wss://example.test' }); const id = jobs[0].id;
  relay.handleExtensionResponse({ id, wsAccepted: true }); await p;
  assert.equal(await relay.relayWsControl({ id, wsClose: { code: 1000 } }), true);
  assert.equal(time.timers.size, 0);
  assert.equal(await relay.relayWsMessage({ id, wsSend: {} }), false);
});

test('fixed browser WS close normalizes reserved codes and bounds UTF-8 reason', async () => {
  const e = await extension({ WebSocket: FakeWebSocket });
  await e.port.onMessage.emit({ id: 's', kind: 'ws-open', url: 'wss://example.test' });
  const ws = FakeWebSocket.instances.at(-1);
  await e.port.onMessage.emit({ id: 's', wsClose: { code: 1001, reason: '🌍'.repeat(100) } });
  ws.open();
  assert.equal(ws.closed[0].code, 1000); assert.ok(Buffer.byteLength(ws.closed[0].reason) <= 123);
  assert.equal(e.get('wsSessions.size'), 0);
});

test('fixed plaintext WS absolute URL preserves scheme, port and path', async () => {
  const server = new EventEmitter(), client = new FakeSocket(); let url;
  const ws = load('native-host/protocols/ws/index.js', { mocks: { '../../failure-logger': { logFailedRequest() {} } } });
  ws.attach(server, { relayWsOpen: async job => { url = job.url; return { error: 'test complete' }; } });
  server.emit('upgrade', { url: 'ws://example.test:8080/chat?q=1', headers: { host: 'example.test:8080', upgrade: 'websocket' } }, client, Buffer.alloc(0));
  await flush(); assert.equal(url, 'ws://example.test:8080/chat?q=1');
});
