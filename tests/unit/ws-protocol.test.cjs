const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { load, FakeSocket, flush } = require('../helpers/harness.cjs');
const frames = require('../../native-host/protocols/ws/ws-frames');
const logger = { logFailedRequest() {} };
function protocol() { return load('native-host/protocols/ws/index.js', { mocks: { '../../failure-logger': logger } }); }
test('WS downstream text and binary frames preserve bytes and opcodes', () => {
  const ws = protocol(), socket = new FakeSocket(), targets = new Map([['s', { socket }]]);
  ws.handleExtensionWsMessage(targets, 's', Buffer.from('سلام'), false);
  ws.handleExtensionWsMessage(targets, 's', Buffer.from([0, 255]), true);
  const parsed = frames.createFrameParser().push(Buffer.concat(socket.writes));
  assert.equal(parsed[0].opcode, frames.OP_TEXT); assert.equal(parsed[0].payload.toString(), 'سلام');
  assert.equal(parsed[1].opcode, frames.OP_BINARY); assert.deepEqual(parsed[1].payload, Buffer.from([0, 255]));
});
test('WS remote close writes once and removes the target', () => {
  const ws = protocol(), socket = new FakeSocket(), targets = new Map([['s', { socket }]]);
  ws.handleExtensionWsClose(targets, 's', 1000, 'done'); ws.handleExtensionWsClose(targets, 's', 1000, 'done');
  assert.equal(socket.writes.length, 1); assert.equal(targets.size, 0);
  assert.equal(frames.createFrameParser().push(socket.writes[0])[0].opcode, frames.OP_CLOSE);
});
test('WS messages for missing or destroyed downstream sockets are ignored', () => {
  const ws = protocol(), socket = new FakeSocket(); socket.destroy();
  const targets = new Map([['s', { socket }]]);
  ws.handleExtensionWsMessage(targets, 's', Buffer.from('x'), false);
  ws.handleExtensionWsMessage(targets, 'missing', Buffer.from('x'), false);
  assert.equal(socket.writes.length, 0);
});
test('WS upstream handshake error sends a complete 502 response', async () => {
  const ws = protocol(), server = new EventEmitter(), socket = new FakeSocket();
  ws.attach(server, { relayWsOpen: async () => ({ error: 'refused' }) });
  server.emit('upgrade', { url: '/chat', headers: { upgrade: 'websocket', host: 'example.test' } }, socket, Buffer.alloc(0));
  await flush(); const raw = Buffer.concat(socket.writes).toString();
  assert.match(raw, /^HTTP\/1.1 502/); assert.match(raw, /refused/); assert.equal(socket.ended, true);
  const [headers, body] = raw.split('\r\n\r\n');
  assert.equal(Number(headers.match(/Content-Length: (\d+)/)[1]), Buffer.byteLength(body));
});
test('WS missing Host rejects locally before opening upstream', () => {
  const ws = protocol(), server = new EventEmitter(), socket = new FakeSocket(); let opens = 0;
  ws.attach(server, { relayWsOpen: async () => { opens++; } });
  server.emit('upgrade', { url: '/chat', headers: { upgrade: 'websocket' } }, socket, Buffer.alloc(0));
  assert.equal(opens, 0); assert.match(Buffer.concat(socket.writes).toString(), /^HTTP\/1.1 400/);
});
