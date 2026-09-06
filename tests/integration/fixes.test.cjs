const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { proxy, connectTls, tlsRequest, tlsFixture } = require('../helpers/network.cjs');
const { FakeWebSocket, deferred } = require('../helpers/harness.cjs');
const frames = require('../../native-host/protocols/ws/ws-frames');
const { X509Certificate } = require('node:crypto');

function readUntil(socket, complete) {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0);
    const timer = setTimeout(() => finish(new Error('Timed out reading local socket')), 4000);
    function finish(err, result) {
      clearTimeout(timer); socket.off('data', onData); socket.off('error', onError); socket.off('end', onEnd);
      if (err) reject(err); else resolve(result);
    }
    function onError(err) { finish(err); }
    function onEnd() { finish(new Error('Socket ended before expected bytes')); }
    function onData(chunk) {
      buffer = Buffer.concat([buffer, chunk]);
      try { if (complete(buffer)) finish(null, buffer); } catch (err) { finish(err); }
    }
    socket.on('data', onData); socket.once('error', onError); socket.once('end', onEnd);
  });
}

test('fixed HTTPS chunked upload survives separate TLS writes and consumes trailers', async t => {
  let received;
  const p = await proxy(t, async (url, opts) => { received = Buffer.from(opts.body); return new Response('saved'); });
  const client = await connectTls(t, p);
  const read = readUntil(client, b => b.includes(Buffer.from('0\r\n\r\n')));
  for (const part of ['POST /upload HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n',
    '3;ext=yes\r', '\nabc\r\n2\r\n', 'de\r\n0\r\nX-Checksum: yes\r\n', '\r\n']) {
    await new Promise((resolve, reject) => client.write(part, err => err ? reject(err) : setImmediate(resolve)));
  }
  assert.match((await read).toString(), /^HTTP\/1.1 200/);
  assert.equal(received.toString(), 'abcde'); assert.equal(p.jobs.length, 1);
  assert.equal(p.jobs[0].headers['transfer-encoding'], undefined);
});

for (const [name, fields, body] of [
  ['conflicting framing', 'Transfer-Encoding: chunked\r\nContent-Length: 5', '0\r\n\r\n'],
  ['invalid length', 'Content-Length: -1', ''],
  ['bad chunk size', 'Transfer-Encoding: chunked', 'nope\r\n'],
  ['duplicate length', 'Content-Length: 1\r\nContent-Length: 2', 'ab'],
]) {
  test(`fixed HTTPS rejects ${name} locally and leaves the dispatcher usable`, async t => {
    const p = await proxy(t);
    const raw = await tlsRequest(t, p, `POST / HTTP/1.1\r\nHost: example.test\r\n${fields}\r\n\r\n${body}`);
    assert.match(raw, /^HTTP\/1.1 400/); assert.equal(p.jobs.length, 0);
    const next = await tlsRequest(t, p, 'GET /ok HTTP/1.1\r\nHost: example.test\r\n\r\n');
    assert.match(next, /^HTTP\/1.1 200/);
  });
}

test('fixed HTTPS sends 100 Continue before waiting for an upload body', async t => {
  let body;
  const p = await proxy(t, async (url, opts) => { body = Buffer.from(opts.body); return new Response('ok'); });
  const client = await connectTls(t, p);
  const interim = readUntil(client, b => b.includes(Buffer.from('\r\n\r\n')));
  client.write('POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\nExpect: 100-continue\r\n\r\n');
  assert.match((await interim).toString(), /^HTTP\/1.1 100 Continue/);
  const response = readUntil(client, b => b.includes(Buffer.from('0\r\n\r\n'))); client.write('abc');
  assert.match((await response).toString(), /^HTTP\/1.1 200/); assert.equal(body.toString(), 'abc');
});

test('fixed WSS supports verified TLS, subprotocol, fragmented text, binary, ping and close', async t => {
  const p = await proxy(t); const sent = deferred(); let upstream;
  class Upstream extends FakeWebSocket {
    constructor(url, protocols) { super(url, protocols); upstream = this; queueMicrotask(() => this.open('chat')); }
    send(data) { super.send(data); sent.resolve(data); }
  }
  p.extension.context.WebSocket = Upstream;
  const client = await connectTls(t, p, 'example.test:8443');
  const handshake = readUntil(client, b => b.includes(Buffer.from('\r\n\r\n')));
  client.write('GET /chat?q=1 HTTP/1.1\r\nHost: example.test:8443\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: chat\r\n\r\n');
  const headers = (await handshake).toString();
  assert.match(headers, /^HTTP\/1.1 101/); assert.match(headers, /sec-websocket-protocol: chat/i);
  assert.equal(upstream.url, 'wss://example.test:8443/chat?q=1');
  const text = Buffer.from('سلام 🌍');
  const first = frames.encodeFrame({ opcode: frames.OP_TEXT, payload: text.subarray(0, 3), mask: true }); first[0] &= 0x7f;
  client.write(first);
  client.write(frames.encodeFrame({ opcode: frames.OP_CONTINUATION, payload: text.subarray(3), mask: true }));
  assert.equal(await sent.promise, text.toString()); assert.equal(upstream.sent.length, 1);
  const binaryRead = readUntil(client, b => b.length >= 4);
  upstream.onmessage({ data: new Uint8Array([0, 255]).buffer });
  const binary = frames.createFrameParser().push(await binaryRead)[0];
  assert.equal(binary.opcode, frames.OP_BINARY); assert.deepEqual(binary.payload, Buffer.from([0, 255]));
  const pongRead = readUntil(client, b => b.length >= 4);
  client.write(frames.encodeFrame({ opcode: frames.OP_PING, payload: 'hi', mask: true }));
  const pong = frames.createFrameParser().push(await pongRead)[0];
  assert.equal(pong.opcode, frames.OP_PONG); assert.equal(pong.payload.toString(), 'hi');
  assert.equal(upstream.sent.length, 1);
  const closed = once(client, 'end');
  client.write(frames.encodeFrame({ opcode: frames.OP_CLOSE, payload: frames.buildClosePayload(1000, 'done'), mask: true }));
  await closed; assert.equal(upstream.closed.length, 1);
});

test('fixed IPv6 leaf certificate has an IP SAN', () => {
  const cert = new X509Certificate(tlsFixture().getCertificateForHost('::1').certPem);
  assert.equal(cert.checkIP('::1'), '::1');
});
