const test = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const path = require('node:path');
const { once } = require('node:events');
test('real child stdin/stdout handles fragmented native frames and clean EOF', async t => {
  const child = spawn(process.execPath, [path.join(__dirname, '../fixtures/native-echo.cjs')], { stdio: ['pipe', 'pipe', 'pipe'] });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  const out = [], errors = []; child.stdout.on('data', c => out.push(c)); child.stderr.on('data', c => errors.push(c));
  const exited = once(child, 'close');
  const body = Buffer.from(JSON.stringify({ id: 'child', text: 'سلام 🌍' }));
  const prefix = Buffer.alloc(4); prefix.writeUInt32LE(body.length);
  const wire = Buffer.concat([prefix, body]);
  child.stdin.write(wire.subarray(0, 2)); child.stdin.write(wire.subarray(2, 7)); child.stdin.end(wire.subarray(7));
  const [code] = await exited; assert.equal(code, 0, Buffer.concat(errors).toString());
  const result = Buffer.concat(out); assert.equal(result.readUInt32LE(), result.length - 4);
  assert.deepEqual(JSON.parse(result.subarray(4)), { id: 'child', echo: 'سلام 🌍' });
});
