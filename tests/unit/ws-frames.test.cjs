const test = require('node:test');
const assert = require('node:assert/strict');
const ws = require('../../native-host/protocols/ws/ws-frames');
test('accept key matches independent RFC 6455 example', () => {
  assert.equal(ws.computeAcceptKey('dGhlIHNhbXBsZSBub25jZQ=='), 's3pPLMBiTxaQ9kYGzzhZRbK+xOo=');
});
for (const size of [0, 1, 125, 126, 65535, 65536, 1048576]) {
  for (const mask of [false, true]) {
    test(`binary framing ${size} bytes mask=${mask} survives transport splits`, () => {
      const payload = Buffer.alloc(size);
      for (let i = 0; i < size; i++) payload[i] = i % 251;
      const encoded = ws.encodeFrame({ opcode: ws.OP_BINARY, payload, mask, maskKey: Buffer.from([1, 2, 3, 4]) });
      const p = ws.createFrameParser(), frames = [];
      for (const [a, b] of [[0, 1], [1, 3], [3, 9], [9, encoded.length]]) {
        frames.push(...p.push(encoded.subarray(a, b)));
      }
      assert.equal(frames.length, 1); assert.equal(frames[0].opcode, ws.OP_BINARY);
      assert.deepEqual(frames[0].payload, payload);
    });
  }
}
test('masked frame decodes against independently constructed wire bytes', () => {
  const frame = Buffer.from([0x81, 0x82, 1, 2, 3, 4, 0x69, 0x6b]);
  assert.equal(ws.createFrameParser().push(frame)[0].payload.toString(), 'hi');
});
test('multiple coalesced frames preserve order', () => {
  const wire = Buffer.concat(['one', 'two'].map(payload => ws.encodeFrame({ opcode: ws.OP_TEXT, payload })));
  assert.deepEqual(ws.createFrameParser().push(wire).map(f => f.payload.toString()), ['one', 'two']);
});
test('oversized frame rejected from header before body allocation', () => {
  assert.throws(() => ws.createFrameParser({ maxFrameBytes: 10 }).push(Buffer.from([0x82, 126, 0, 11])), /exceeds limit/);
});
test('reset discards a partial previous frame', () => {
  const p = ws.createFrameParser(); p.push(Buffer.from([0x81])); p.reset();
  assert.equal(p.push(Buffer.from([0x81, 1, 65]))[0].payload.toString(), 'A');
});
test('close payload includes network-order code and UTF-8 reason', () => {
  const p = ws.buildClosePayload(1000, 'سلام');
  assert.equal(p.readUInt16BE(0), 1000); assert.equal(p.subarray(2).toString(), 'سلام');
});
