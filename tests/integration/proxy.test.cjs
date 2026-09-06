const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { once } = require('node:events');
const { proxy, request, tlsRequest, tlsFixture } = require('../helpers/network.cjs');
const { X509Certificate } = require('node:crypto');
test('HTTP end-to-end reaches a real localhost origin via extension fetch', async t => {
  const origin = http.createServer((req, res) => {
    const chunks = []; req.on('data', c => chunks.push(c));
    req.on('end', () => { res.writeHead(201, { 'content-type': 'application/octet-stream' }); res.end(Buffer.concat(chunks)); });
  });
  origin.listen(0, '127.0.0.1'); await once(origin, 'listening');
  t.after(() => new Promise(resolve => { origin.closeAllConnections(); origin.close(resolve); }));
  const p = await proxy(t, globalThis.fetch);
  const body = Buffer.from([0, 1, 128, 255]);
  const result = await request(p.port, { path: `http://127.0.0.1:${origin.address().port}/echo?q=1`, method: 'POST', body });
  assert.equal(result.status, 201); assert.deepEqual(result.body, body);
  assert.equal(p.logs.length, 0); assert.equal(p.jobs.length, 1);
});
test('large download crosses native message chunks without data loss', async t => {
  const body = Buffer.alloc(1_700_000, 173);
  const p = await proxy(t, async () => new Response(body));
  const result = await request(p.port); assert.deepEqual(result.body, body);
});
test('large upload crosses host-to-extension chunk reassembly', async t => {
  const body = Buffer.alloc(1_200_000, 201); let received;
  const p = await proxy(t, async (url, options) => { received = Buffer.from(options.body); return new Response('saved'); });
  const result = await request(p.port, { method: 'POST', body });
  assert.equal(result.status, 200); assert.deepEqual(received, body);
});
test('relative-form requests are rejected before relay', async t => {
  const p = await proxy(t); const result = await request(p.port, { path: '/local' });
  assert.equal(result.status, 400); assert.equal(p.jobs.length, 0);
});
test('CORS preflight short-circuits and ordinary response merges Origin', async t => {
  const p = await proxy(t);
  const preflight = await request(p.port, { method: 'OPTIONS', headers: {
    origin: 'https://client.test', 'access-control-request-method': 'POST', 'access-control-request-headers': 'authorization',
  } });
  assert.equal(preflight.status, 204); assert.equal(p.jobs.length, 0);
  assert.equal(preflight.headers['access-control-allow-headers'], 'authorization');
  const response = await request(p.port, { headers: { origin: 'https://client.test' } });
  assert.equal(response.headers['access-control-allow-origin'], 'https://client.test');
});
test('decoded response removes stale compression and length metadata', async t => {
  const p = await proxy(t, async () => new Response('decoded', { headers: { 'content-encoding': 'gzip', 'content-length': '123' } }));
  const result = await request(p.port);
  assert.equal(result.body.toString(), 'decoded'); assert.equal(result.headers['content-encoding'], undefined);
  assert.notEqual(result.headers['content-length'], '123');
});
test('upstream failure gives 502 and a failure log entry', async t => {
  const p = await proxy(t, async () => { throw new Error('upstream unavailable'); });
  const result = await request(p.port); assert.equal(result.status, 502);
  assert.match(result.body.toString(), /upstream unavailable/);
  assert.equal(p.logs[0].source, 'extension');
});
test('upstream 404 status and payload survive streaming and are logged', async t => {
  const p = await proxy(t, async () => new Response('missing', { status: 404 }));
  const result = await request(p.port); assert.equal(result.status, 404); assert.equal(result.body.toString(), 'missing');
  assert.equal(p.logs[0].status, 404);
});
test('HTTPS CONNECT verifies temporary CA and returns valid chunked response', async t => {
  const p = await proxy(t, async () => new Response('hello', { status: 200 }));
  const raw = await tlsRequest(t, p, 'GET /path?q=1 HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n');
  assert.match(raw, /^HTTP\/1.1 200/); assert.match(raw, /transfer-encoding: chunked/i);
  assert.ok(raw.endsWith('5\r\nhello\r\n0\r\n\r\n'));
  assert.equal(p.jobs[0].url, 'https://example.test/path?q=1');
});
test('HTTPS content-length POST preserves binary request body', async t => {
  let body;
  const p = await proxy(t, async (url, opts) => { body = Buffer.from(opts.body); return new Response('ok'); });
  await tlsRequest(t, p, 'POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\n\r\nabc');
  assert.equal(body.toString(), 'abc');
});
test('HTTPS HEAD response has no data chunks or terminating chunk', async t => {
  const p = await proxy(t, async () => new Response(null, { status: 200 }));
  const raw = await tlsRequest(t, p, 'HEAD / HTTP/1.1\r\nHost: example.test\r\n\r\n');
  assert.doesNotMatch(raw, /transfer-encoding/i); assert.equal(raw.split('\r\n\r\n')[1], '');
});
test('generated DNS certificate verifies against CA, matches hostname and is cached', () => {
  const fixture = tlsFixture(), pair = fixture.getCertificateForHost('example.test');
  const leaf = new X509Certificate(pair.certPem), ca = new X509Certificate(fixture.caCertPem);
  assert.equal(leaf.verify(ca.publicKey), true); assert.equal(leaf.checkHost('example.test'), 'example.test');
  assert.equal(leaf.checkHost('other.test'), undefined); assert.equal(leaf.ca, false); assert.equal(ca.ca, true);
  assert.equal(fixture.getCertificateForHost('example.test'), pair);
});
