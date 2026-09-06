const test = require('node:test');
const assert = require('node:assert/strict');
const { sendChunked, createChunkReassembler, CHUNK_THRESHOLD_BYTES } = require('../../native-host/chunking');
test('small message passes through unchanged', () => {
  const msg = { id: 'small', value: 'سلام' }, sent = [];
  sendChunked(m => sent.push(m), msg);
  assert.equal(sent.length, 1); assert.equal(sent[0], msg);
});
for (const size of [CHUNK_THRESHOLD_BYTES - 40, CHUNK_THRESHOLD_BYTES + 1, 2_200_000]) {
  test(`ASCII payload round trip at ${size} characters, reversed delivery`, () => {
    const msg = { id: 'large', body: 'A'.repeat(size) }, sent = [];
    sendChunked(m => sent.push(m), msg);
    const reassembler = createChunkReassembler();
    const complete = sent.reverse().map(reassembler.handle).filter(Boolean);
    assert.deepEqual(complete, [msg]);
    for (const frame of sent) assert.ok(Buffer.byteLength(JSON.stringify(frame)) < 1024 * 1024);
  });
}
test('interleaved messages and repeated partial chunks remain independent', () => {
  const r = createChunkReassembler();
  assert.equal(r.handle({ chunkId: 'a', seq: 0, total: 2, data: '{"a":' }), null);
  assert.equal(r.handle({ chunkId: 'b', seq: 0, total: 2, data: '{"b":' }), null);
  assert.equal(r.handle({ chunkId: 'a', seq: 0, total: 2, data: '{"a":' }), null);
  assert.deepEqual(r.handle({ chunkId: 'b', seq: 1, total: 2, data: '2}' }), { b: 2 });
  assert.deepEqual(r.handle({ chunkId: 'a', seq: 1, total: 2, data: '1}' }), { a: 1 });
});
test('malformed JSON does not prevent a later valid message', () => {
  const r = createChunkReassembler();
  assert.equal(r.handle({ chunkId: 'bad', seq: 0, total: 1, data: '{' }), null);
  assert.deepEqual(r.handle({ chunkId: 'ok', seq: 0, total: 1, data: '{"ok":true}' }), { ok: true });
});
test('51st incomplete transfer evicts the oldest; newer transfer completes', () => {
  const r = createChunkReassembler();
  for (let i = 0; i < 51; i++) r.handle({ chunkId: `c${i}`, seq: 0, total: 2, data: '{"ok":' });
  assert.equal(r.handle({ chunkId: 'c0', seq: 1, total: 2, data: 'true}' }), null);
  assert.deepEqual(r.handle({ chunkId: 'c50', seq: 1, total: 2, data: 'true}' }), { ok: true });
});
