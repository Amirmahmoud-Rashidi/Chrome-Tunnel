const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { load, plain } = require('../helpers/harness.cjs');
function setup() {
  const stdin = new EventEmitter(), stdout = new EventEmitter(), writes = [], exits = [];
  stdout.write = b => writes.push(b);
  const { createNativeMessagingHost } = load('native-host/native-messaging.js', {
    globals: { process: { stdin, stdout, exit: code => exits.push(code) } },
  });
  return { host: createNativeMessagingHost(), stdin, stdout, writes, exits };
}
function wire(value) {
  const b = Buffer.from(value), h = Buffer.alloc(4); h.writeUInt32LE(b.length); return Buffer.concat([h, b]);
}
test('outbound prefix measures UTF-8 bytes, not character count', () => {
  const { host, writes } = setup(); host.send({ text: 'سلام 🌍' });
  assert.equal(writes[0].readUInt32LE(0), writes[0].length - 4);
  assert.deepEqual(JSON.parse(writes[0].subarray(4)), { text: 'سلام 🌍' });
});
test('every single-byte transport split and coalesced frames parse correctly', () => {
  const { host, stdin } = setup(), received = []; host.onMessage(m => received.push(plain(m)));
  const input = Buffer.concat([wire('{"text":"سلام"}'), wire('{"id":2}')]);
  for (const byte of input) stdin.emit('data', Buffer.from([byte]));
  assert.deepEqual(received, [{ text: 'سلام' }, { id: 2 }]);
  stdin.emit('data', Buffer.concat([wire('{"id":3}'), wire('{"id":4}')]));
  assert.deepEqual(received.slice(2), [{ id: 3 }, { id: 4 }]);
});
test('bad JSON and throwing listeners do not poison later frames', () => {
  const { host, stdin } = setup(), received = [];
  host.onMessage(() => { throw new Error('bad callback'); }); host.onMessage(m => received.push(plain(m)));
  stdin.emit('data', Buffer.concat([wire('{'), wire('{"ok":true}')]));
  assert.deepEqual(received, [{ ok: true }]);
});
test('asynchronous EPIPE shuts down cleanly and prevents further writes', () => {
  const { host, stdout, writes, exits } = setup();
  stdout.emit('error', Object.assign(new Error('closed'), { code: 'EPIPE' })); host.send({ id: 'late' });
  assert.deepEqual(exits, [0]); assert.equal(writes.length, 0);
});
test('synchronous EPIPE is contained without throwing to caller', () => {
  const { host, stdout } = setup(); let calls = 0;
  stdout.write = () => { calls++; throw Object.assign(new Error('closed'), { code: 'EPIPE' }); };
  assert.doesNotThrow(() => host.send({ id: 1 })); host.send({ id: 2 }); assert.equal(calls, 1);
});
