const test = require('node:test');
const assert = require('node:assert/strict');
const cors = require('../../native-host/core/cors');
test('only real preflight signatures are answered locally', () => {
  const headers = { origin: 'https://client.example', 'access-control-request-method': 'POST' };
  assert.equal(cors.isCorsPreflight('https://asset.example/a', 'OPTIONS', headers), true);
  for (const [url, method, h] of [
    ['https://asset.example', 'GET', headers], ['invalid', 'OPTIONS', headers],
    ['https://asset.example', 'OPTIONS', {}], ['https://asset.example', 'OPTIONS', { origin: 'x' }],
  ]) assert.equal(cors.isCorsPreflight(url, method, h), false);
});
test('preflight echoes requested origin, methods and headers', () => {
  const h = cors.buildCorsPreflightHeaders({ origin: 'https://client.example',
    'access-control-request-method': 'PUT', 'access-control-request-headers': 'authorization' });
  assert.equal(h['access-control-allow-origin'], 'https://client.example');
  assert.equal(h['access-control-allow-methods'], 'PUT');
  assert.equal(h['access-control-allow-headers'], 'authorization');
  assert.equal(h['content-length'], '0');
});
test('response Vary merges Origin once, preserving existing values', () => {
  const h = { vary: 'Accept-Encoding' }, context = { requestHeaders: { origin: 'https://client.example' } };
  cors.applyCorsResponseHeaders(h, context); cors.applyCorsResponseHeaders(h, context);
  assert.equal(h.vary, 'Accept-Encoding, Origin');
  assert.equal(h['access-control-allow-origin'], 'https://client.example');
});
test('requests without Origin leave response headers unchanged', () => {
  const h = { vary: 'Accept-Encoding' }; cors.applyCorsResponseHeaders(h, {});
  assert.deepEqual(h, { vary: 'Accept-Encoding' });
});
