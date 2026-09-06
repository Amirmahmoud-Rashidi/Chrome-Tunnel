const http = require('node:http');
const tls = require('node:tls');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { once } = require('node:events');
const test = require('node:test');
const { load, extension, flush } = require('./harness.cjs');
const { sendChunked, createChunkReassembler } = require('../../native-host/chunking');
let certificates;
function tlsFixture() {
  if (certificates) return certificates;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chrometunnel-test-'));
  test.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  certificates = load('native-host/protocols/https/tls-mitm.js', {
    dirname: path.join(dir, 'protocols', 'https'),
  });
  return certificates;
}
async function proxy(t, fetch = async () => new Response('ok')) {
  const logs = [], jobs = [], logger = { logFailedRequest: entry => logs.push(entry) };
  const cert = tlsFixture();
  const httpsProtocol = load('native-host/protocols/https/index.js', {
    mocks: { './tls-mitm': cert, '../../failure-logger': logger },
  });
  const httpProtocol = load('native-host/protocols/http/index.js', { mocks: { '../../failure-logger': logger } });
  const wsProtocol = load('native-host/protocols/ws/index.js', { mocks: { '../../failure-logger': logger } });
  const { createDispatcher } = load('native-host/core/dispatcher.js', { mocks: {
    '../protocols/https': httpsProtocol, '../protocols/http': httpProtocol, '../protocols/ws': wsProtocol,
  } });
  const e = await extension({ fetch });
  const reassemble = createChunkReassembler();
  const d = createDispatcher({ port: 0, sendToExtension(job) {
    jobs.push(job);
    sendChunked(part => { e.port.onMessage.emit(part).catch(err => { throw err; }); }, job);
  } });
  e.port.postMessage = message => {
    const complete = reassemble.handle(message);
    if (complete) d.handleExtensionResponse(complete);
  };
  const sockets = new Set();
  d.server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => d.server.close(resolve));
    await flush();
  });
  await once(d.server, 'listening');
  return { ...d, port: d.server.address().port, jobs, logs, extension: e, cert };
}
function request(port, { path = 'http://example.test/', method = 'GET', headers = {}, body } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path, method, headers, agent: false }, res => {
      const chunks = []; res.on('data', c => chunks.push(c));
      res.on('error', reject); res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.setTimeout(5000, () => req.destroy(new Error('Local HTTP test deadline')));
    req.on('error', reject); req.end(body);
  });
}
async function connectTls(t, p, authority = 'example.test:443', servername = 'example.test') {
  const tunnel = http.request({ host: '127.0.0.1', port: p.port, method: 'CONNECT', path: authority, agent: false });
  const connected = once(tunnel, 'connect');
  tunnel.setTimeout(5000, () => tunnel.destroy(new Error('CONNECT test deadline')));
  tunnel.end();
  const [res, socket, head] = await connected;
  if (res.statusCode !== 200) throw new Error(`CONNECT failed: ${res.statusCode}`);
  if (head.length) socket.unshift(head);
  const client = tls.connect({ socket, servername, ca: p.cert.caCertPem, rejectUnauthorized: true });
  t.after(() => client.destroy());
  client.setTimeout(5000, () => client.destroy(new Error('Local TLS test deadline')));
  await once(client, 'secureConnect');
  return client;
}
async function tlsRequest(t, p, text, authority) {
  const client = await connectTls(t, p, authority);
  const chunks = []; client.on('data', c => chunks.push(c));
  const ended = once(client, 'end'); client.write(text); await ended;
  return Buffer.concat(chunks).toString();
}
module.exports = { proxy, request, connectTls, tlsRequest, tlsFixture };
