const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { EventEmitter } = require('node:events');
const root = path.resolve(__dirname, '../..');
const quiet = { log() {}, error() {}, warn() {}, info() {} };
const plain = value => JSON.parse(JSON.stringify(value));
async function flush() { for (let i = 0; i < 24; i++) await Promise.resolve(); }
function deferred() {
  let resolve, reject;
  const promise = new Promise((a, b) => { resolve = a; reject = b; });
  return { promise, resolve, reject };
}
function clock() {
  let now = 0, sequence = 0;
  const timers = new Map();
  class FakeDate extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }
    static now() { return now; }
  }
  const setTimeout = (fn, delay = 0) => {
    const id = ++sequence;
    timers.set(id, { at: now + Number(delay), fn });
    return id;
  };
  function advance(ms) {
    const target = now + ms;
    for (;;) {
      const next = [...timers].filter(([, timer]) => timer.at <= target)
        .sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
      if (!next) break;
      now = next[1].at;
      timers.delete(next[0]);
      next[1].fn();
    }
    now = target;
  }
  return { Date: FakeDate, setTimeout, clearTimeout: id => timers.delete(id),
    advance, timers, get now() { return now; } };
}
// Evaluate the actual, unmodified CommonJS source with narrow boundary doubles.
function load(relative, { mocks = {}, globals = {}, dirname } = {}) {
  const filename = path.join(root, relative);
  const localRequire = createRequire(filename);
  const module = { exports: {} };
  const context = vm.createContext({
    module, exports: module.exports,
    require: name => Object.hasOwn(mocks, name) ? mocks[name] : localRequire(name),
    __filename: filename, __dirname: dirname || path.dirname(filename),
    Buffer, URL, console: quiet, setTimeout, clearTimeout, process, ...globals,
  });
  vm.runInContext(fs.readFileSync(filename, 'utf8'), context, { filename });
  return module.exports;
}
function event() {
  const listeners = [];
  return { addListener(fn) { listeners.push(fn); },
    emit(...args) { return Promise.all(listeners.map(fn => fn(...args))); } };
}
async function extension({ fetch, WebSocket, storedConnected = false } = {}) {
  const time = clock(), sent = [], storage = { chrometunnel_connected: storedConnected };
  const port = { postMessage: msg => sent.push(plain(msg)), onMessage: event(), onDisconnect: event() };
  let connects = 0;
  const chrome = {
    alarms: { create() {}, onAlarm: event() },
    runtime: { onStartup: event(), onInstalled: event(),
      connectNative() { connects++; return port; } },
    storage: { session: {
      async get() { return { ...storage }; },
      async set(value) { Object.assign(storage, value); },
    } },
  };
  const context = vm.createContext({
    chrome, console: quiet, URL, TextDecoder, TextEncoder, Uint8Array, ArrayBuffer, Blob,
    AbortController, btoa, atob, setTimeout: time.setTimeout, clearTimeout: time.clearTimeout,
    Date: time.Date, fetch: fetch || (async () => { throw new Error('Unexpected fetch'); }), WebSocket,
  });
  const filename = path.join(root, 'extension/background.js');
  vm.runInContext(fs.readFileSync(filename, 'utf8'), context, { filename });
  await flush();
  return { context, time, sent, port, chrome, storage,
    get connects() { return connects; },
    call: (name, ...args) => context[name](...args),
    get: expression => vm.runInContext(expression, context) };
}
class FakeWebSocket {
  static OPEN = 1;
  static CONNECTING = 0;
  static instances = [];
  constructor(url, protocols) {
    this.url = url; this.protocols = protocols; this.protocol = '';
    this.readyState = 0; this.sent = []; this.closed = [];
    FakeWebSocket.instances.push(this);
  }
  open(protocol = '') { this.protocol = protocol; this.readyState = 1; this.onopen?.(); }
  send(data) { this.sent.push(data); }
  close(code, reason) {
    // Model the browser API restriction, including reserved codes from the host.
    if (code !== undefined && code !== 1000 && !(code >= 3000 && code <= 4999)) {
      throw new DOMException('Invalid close code', 'InvalidAccessError');
    }
    this.closed.push({ code, reason }); this.readyState = 3;
    this.onclose?.({ code: code || 1000, reason: reason || '' });
  }
}
class FakeSocket extends EventEmitter {
  constructor() { super(); this.writes = []; this.destroyed = false; this.remoteAddress = '127.0.0.1'; this.remotePort = 1; }
  write(data) { this.writes.push(Buffer.from(data)); return true; }
  end(data) { if (data) this.write(data); this.ended = true; }
  destroy() { this.destroyed = true; this.emit('close'); }
  unshift() {}
}
module.exports = { root, quiet, plain, flush, deferred, clock, load, extension, FakeWebSocket, FakeSocket };
