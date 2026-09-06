// background.js — chrometunnel bridge (Manifest V3 service worker)
//
// Response streaming protocol sent to the native host:
//   { id, stream: "start", status, statusText, headers }
//   { id, stream: "data", body }          // body is base64
//   { id, stream: "end" }
//   { id, stream: "error", error }
//
// Ordinary requests and application streams have separate timeout policies.
// Queue time is independent. There is no total response lifetime limit: after
// the first body chunk, only a lack of new data can expire the body timer.

const NATIVE_HOST_NAME = "local.chrometunnel.host";
const SESSION_KEY_CONNECTED = "chrometunnel_connected";

// Wire helpers mirror native-host/chunking.js.
const CHUNK_THRESHOLD_BYTES = 800 * 1024;
const CHUNK_SIZE_BYTES = 700 * 1024;
const MAX_MESSAGE_BYTES = 64 * 1024 * 1024;
const MAX_CHUNKS = 512;

// Budget the serialized string, including JSON escaping. Envelope overhead
// remains well below the headroom between 700 KiB and Chrome's 1 MiB limit.
function splitNativeMessage(message, prefix, byteLength) {
  const json = JSON.stringify(message);
  if (byteLength(json) > MAX_MESSAGE_BYTES) throw new Error("Native message exceeds 64 MiB reassembly limit");
  if (byteLength(json) <= CHUNK_THRESHOLD_BYTES) return [message];
  const chunkId = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const parts = [];
  for (let start = 0; start < json.length;) {
    let lo = 1, hi = Math.min(CHUNK_SIZE_BYTES, json.length - start), count = 0;
    while (lo <= hi) {
      const mid = Math.floor((lo + hi) / 2);
      if (byteLength(JSON.stringify(json.slice(start, start + mid))) <= CHUNK_SIZE_BYTES) {
        count = mid; lo = mid + 1;
      } else hi = mid - 1;
    }
    parts.push(json.slice(start, start + count));
    start += count;
  }
  if (parts.length > MAX_CHUNKS) throw new Error("Too many native message chunks");
  return parts.map((data, seq) => ({ chunkId, seq, total: parts.length, data }));
}

function createBoundedReassembler(byteLength) {
  const buffers = new Map();
  let bufferedBytes = 0;
  function drop(id) {
    const entry = buffers.get(id);
    if (entry) bufferedBytes -= entry.bytes;
    buffers.delete(id);
  }
  function handle(message) {
    if (!message || !Object.prototype.hasOwnProperty.call(message, "chunkId")) return message;
    const { chunkId, seq, total, data } = message;
    if (typeof chunkId !== "string" || !chunkId || chunkId.length > 200 ||
        !Number.isInteger(total) || total < 1 || total > MAX_CHUNKS ||
        !Number.isInteger(seq) || seq < 0 || seq >= total || typeof data !== "string") {
      drop(chunkId); return null;
    }
    const size = byteLength(data);
    if (size > 1024 * 1024) { drop(chunkId); return null; }
    let entry = buffers.get(chunkId);
    if (entry && entry.parts.length !== total) { drop(chunkId); return null; }
    if (!entry) {
      if (buffers.size >= 50) drop(buffers.keys().next().value);
      entry = { parts: new Array(total).fill(null), count: 0, bytes: 0 };
      buffers.set(chunkId, entry);
    }
    if (entry.parts[seq] !== null) {
      if (entry.parts[seq] !== data) drop(chunkId);
      return null;
    }
    if (bufferedBytes + size > MAX_MESSAGE_BYTES) { drop(chunkId); return null; }
    entry.parts[seq] = data; entry.count++; entry.bytes += size; bufferedBytes += size;
    if (entry.count !== total) return null;
    drop(chunkId);
    try { return JSON.parse(entry.parts.join("")); } catch { return null; }
  }
  return { handle };
}

const nativeEncoder = new TextEncoder();
const nativeByteLength = text => nativeEncoder.encode(text).byteLength;
const incomingReassembler = createBoundedReassembler(nativeByteLength);

const QUEUE_TIMEOUT_MS = 45_000;
const REQUEST_TIMEOUTS = Object.freeze({
  normal: Object.freeze({ headersMs: 45_000, firstChunkMs: 45_000, idleMs: 45_000 }),
  stream: Object.freeze({ headersMs: 180_000, firstChunkMs: 180_000, idleMs: 90_000 }),
});

function getHeader(headers, name) {
  const entry = Object.entries(headers || {}).find(
    ([key]) => key.toLowerCase() === name
  );
  return entry ? String(entry[1]) : "";
}

function isStreamingContentType(value) {
  const type = String(value || "").split(";", 1)[0].trim().toLowerCase();
  return ["text/event-stream", "application/x-ndjson", "application/json-seq"].includes(type);
}

// Detect intent BEFORE fetch(), because response Content-Type arrives too late
// to choose the response-header deadline. Do not classify every chunked HTTP
// transfer (downloads, JSON, etc.) as an application stream.
function getRequestType({ url, headers, body }) {
  const acceptsStream = getHeader(headers, "accept").split(",").some((value) => {
    const excluded = /;\s*q\s*=\s*0(?:\.0*)?\s*(?:;|$)/i.test(value);
    return !excluded && isStreamingContentType(value);
  });
  if (acceptsStream) return "stream";

  try {
    const target = new URL(url);
    if (
      target.searchParams.get("alt")?.toLowerCase() === "sse" ||
      /^(true|1)$/i.test(target.searchParams.get("stream") || "") ||
      /:streamGenerateContent$/i.test(target.pathname)
    ) {
      return "stream";
    }
  } catch {
    // fetch() reports malformed URLs through the normal error path.
  }

  const contentType = getHeader(headers, "content-type").split(";", 1)[0].trim().toLowerCase();
  if (body && (!contentType || contentType === "application/json" || contentType.endsWith("+json"))) {
    try {
      const payload = JSON.parse(new TextDecoder().decode(base64ToUint8Array(body)));
      if (payload && payload.stream === true) return "stream";
    } catch {
      // Classification must not change, reject, or reserialize the request body.
    }
  }
  return "normal";
}

let port = null;
let connectInFlight = false;

const pendingResponses = [];
const MAX_PENDING_RESPONSES = 200;

const MAX_CONCURRENT_FETCHES = 6;
let activeFetchCount = 0;
const fetchQueue = [];

function runWithConcurrencyLimit(task, onQueueTimeout) {
  const entry = { task, timer: null, expiresAt: null, onQueueTimeout };
  if (activeFetchCount < MAX_CONCURRENT_FETCHES) {
    startFetchTask(entry);
  } else {
    entry.expiresAt = Date.now() + QUEUE_TIMEOUT_MS;
    entry.timer = setTimeout(() => {
      const index = fetchQueue.indexOf(entry);
      if (index === -1) return;
      fetchQueue.splice(index, 1);
      onQueueTimeout();
    }, QUEUE_TIMEOUT_MS);
    fetchQueue.push(entry);
  }
}

function startFetchTask(entry) {
  while (entry) {
    if (entry.timer) clearTimeout(entry.timer);
    // A slot can open in the same event-loop turn as the queue timer expires.
    // Check the absolute deadline before dispatch so an expired POST never runs.
    if (entry.expiresAt !== null && Date.now() >= entry.expiresAt) {
      entry.onQueueTimeout();
      entry = fetchQueue.shift();
      continue;
    }
    activeFetchCount++;
    entry.task().finally(() => {
      activeFetchCount--;
      const next = fetchQueue.shift();
      if (next) startFetchTask(next);
    });
    return;
  }
}

// ---- WebSocket sessions ----------------------------------------------------
//
// One entry per active WebSocket connection. Keyed by the relay id. The
// session is removed on close (from either side). Inbound messages from
// the upstream server are forwarded to the native host as
// { id, wsMessage, isBinary }. Upstream close becomes { id, wsClose }.
const wsSessions = new Map();

function closeBrowserSocket(ws, { code, reason } = {}) {
  const legalCode = Number.isInteger(code) && (code === 1000 || (code >= 3000 && code <= 4999)) ? code : 1000;
  // Browser close reasons have a 123-byte UTF-8 limit; preserve code points.
  let text = "";
  for (const char of String(reason || "")) {
    if (nativeByteLength(text + char) > 123) break;
    text += char;
  }
  try { ws.close(legalCode, text); }
  catch (err) {
    try { ws.close(); } catch { console.error("[chrometunnel] ws close failed:", err); }
  }
}

function handleWebSocketOpen(message) {
  const { id, url, headers = {} } = message;

  if (typeof WebSocket === "undefined") {
    sendToNative({ id, wsError: "WebSocket API is not available in this context." });
    return;
  }

  // The native host has already vetted the URL is wss:// or ws://, but
  // we double-check here so a malformed message can't trick us.
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    sendToNative({ id, wsError: `Invalid WebSocket URL: ${url}` });
    return;
  }
  if (parsed.protocol !== "ws:" && parsed.protocol !== "wss:") {
    sendToNative({
      id,
      wsError: `Refusing to open WebSocket with non-ws(s) protocol: ${parsed.protocol}`,
    });
    return;
  }

  // We allow only ws:// and wss://; the URL host is required.
  if (!parsed.host) {
    sendToNative({ id, wsError: "WebSocket URL has no host." });
    return;
  }

  // Pass through subprotocols if the client sent one.
  const subprotocols = headers["sec-websocket-protocol"];
  let ws;
  try {
    if (subprotocols) {
      // The header is a comma-separated list per RFC 6455. Pass it as
      // the second arg so the API picks one that the server supports.
      const list = subprotocols
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      ws = new WebSocket(url, list.length > 0 ? list : undefined);
    } else {
      ws = new WebSocket(url);
    }
  } catch (err) {
    sendToNative({
      id,
      wsError: `Failed to construct WebSocket: ${err && err.message ? err.message : err}`,
    });
    return;
  }

  // Hold a reference so a CONNECTING WebSocket survives between turns
  // and so we can call close() on it if the client closed first.
  const session = { ws, closeOnOpen: null };
  wsSessions.set(id, session);

  ws.binaryType = "arraybuffer"; // Keep binary and text event delivery in order.

  ws.onopen = () => {
    // Report accepted. The native host writes the 101 to the client and
    // starts relaying bytes in both directions.
    //
    // Note: the browser WebSocket API doesn't expose the response
    // status code or headers to JS — they are part of the internal
    // browser pipeline that fetch() also uses. The browser will reject
    // non-101 handshakes by firing onerror before onopen ever fires,
    // so the absence of an "error" means a successful 101.
    sendToNative({ id, wsAccepted: true, headers: ws.protocol ? { "sec-websocket-protocol": ws.protocol } : {} });
    if (session.closeOnOpen) {
      try {
        closeBrowserSocket(ws, session.closeOnOpen);
      } catch (err) {
        console.error("[chrometunnel] deferred ws close failed for", id, err);
      }
      session.closeOnOpen = null;
    }
  };

  ws.onmessage = (event) => {
    if (!wsSessions.has(id)) return;
    const data = event.data;
    let payload;
    let isBinary = false;
    if (data instanceof ArrayBuffer) {
      payload = uint8ArrayToBase64(new Uint8Array(data));
      isBinary = true;
    } else if (ArrayBuffer.isView(data)) {
      payload = uint8ArrayToBase64(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
      isBinary = true;
    } else if (typeof Blob !== "undefined" && data instanceof Blob) {
      // Binary blob — read it and forward as base64.
      data
        .arrayBuffer()
        .then((buf) => {
          if (!wsSessions.has(id)) return;
          sendToNative({
            id,
            wsMessage: uint8ArrayToBase64(new Uint8Array(buf)),
            isBinary: true,
          });
        })
        .catch((err) => {
          console.error("[chrometunnel] ws blob read failed for", id, err);
        });
      return;
    } else {
      // String message.
      const text = String(data);
      payload = btoa(unescape(encodeURIComponent(text))); // UTF-8 safe
      isBinary = false;
    }
    sendToNative({ id, wsMessage: payload, isBinary });
  };

  ws.onerror = (event) => {
    // We can't read the actual error reason from the service-worker
    // side — browsers intentionally hide that. Report a generic
    // message; details are only visible in the chrome://extensions
    // service-worker console.
    const reason =
      (event && event.message) ||
      "WebSocket failed to connect (the upstream may be down, the URL may be wrong, or the host is unreachable through the configured proxy).";
    if (wsSessions.has(id)) {
      sendToNative({ id, wsError: reason });
      wsSessions.delete(id);
    }
  };

  ws.onclose = (event) => {
    if (wsSessions.has(id)) {
      const code = typeof event.code === "number" ? event.code : 1005;
      const reason = typeof event.reason === "string" ? event.reason : "";
      sendToNative({ id, wsClose: { code, reason } });
      wsSessions.delete(id);
    }
  };
}

// ---- Keep-alive -----------------------------------------------------------
const KEEP_ALIVE_ALARM = "chrometunnel-keep-alive";
chrome.alarms.create(KEEP_ALIVE_ALARM, { periodInMinutes: 0.4 });
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === KEEP_ALIVE_ALARM) ensureConnected();
});
chrome.runtime.onStartup.addListener(ensureConnected);
chrome.runtime.onInstalled.addListener(ensureConnected);

// ---- Native messaging connection ----------------------------------------
async function ensureConnected() {
  if (port) return;
  if (connectInFlight) return;

  connectInFlight = true;
  try {
    // Session storage is diagnostic, not proof this worker owns a live port.
    connect();
  } finally {
    connectInFlight = false;
  }
}

function connect() {
  try {
    port = chrome.runtime.connectNative(NATIVE_HOST_NAME);
  } catch (err) {
    console.error("[chrometunnel] connectNative threw:", err);
    port = null;
    return;
  }

  chrome.storage.session.set({ [SESSION_KEY_CONNECTED]: true });
  port.onMessage.addListener(handleNativeMessage);
  port.onDisconnect.addListener(() => {
    const err = chrome.runtime.lastError;
    console.warn(
      "[chrometunnel] native port disconnected",
      err ? err.message : ""
    );
    port = null;
    chrome.storage.session.set({ [SESSION_KEY_CONNECTED]: false });
    setTimeout(ensureConnected, 2000);
  });

  console.log("[chrometunnel] connected to native host:", NATIVE_HOST_NAME);
  flushPendingResponses();
}

function flushPendingResponses() {
  if (pendingResponses.length === 0) return;
  console.log(
    `[chrometunnel] flushing ${pendingResponses.length} buffered response(s) after reconnect`
  );
  const toSend = pendingResponses.splice(0, pendingResponses.length);
  for (const message of toSend) sendToNative(message);
}

// ---- Message handling -----------------------------------------------------
async function handleNativeMessage(message) {
  if (!message || typeof message !== "object") {
    console.warn("[chrometunnel] ignoring malformed message:", message);
    return;
  }

  if (Object.prototype.hasOwnProperty.call(message, "chunkId")) {
    const complete = incomingReassembler.handle(message);
    if (complete !== null) return handleNativeMessage(complete);
    return;
  }

  const { id, ping, url, method, headers, body } = message;

  if (ping) {
    sendToNative({ id, pong: true, time: Date.now() });
    return;
  }

  // ---- WebSocket control plane ------------------------------------------
  //
  // A live WebSocket session is identified by the relay id. The native
  // host sends three kinds of follow-up messages for an open session:
  //   { id, wsSend: { payload, isBinary, kind } }   — client→server frame
  //   { id, wsClose: { code, reason } }             — client closed
  //
  // We route them to the live WebSocket stored in wsSessions[id].
  if (id && message.wsSend && wsSessions.has(id)) {
    const session = wsSessions.get(id);
    const { payload, isBinary, kind } = message.wsSend;
    const bytes = base64ToUint8Array(payload || "");
    if (session.ws.readyState === WebSocket.OPEN) {
      try {
        // The host answers control pings locally. Never turn them into data.
        if (kind !== "ping" && kind !== "pong") {
          session.ws.send(isBinary ? bytes : new TextDecoder().decode(bytes));
        }
      } catch (err) {
        console.error("[chrometunnel] ws send failed for", id, err);
      }
    }
    return;
  }

  if (id && message.wsClose && wsSessions.has(id)) {
    const session = wsSessions.get(id);
    const { code, reason } = message.wsClose || {};
    try {
      if (session.ws.readyState === WebSocket.OPEN) {
        closeBrowserSocket(session.ws, { code, reason });
      } else if (session.ws.readyState === WebSocket.CONNECTING) {
        // Will be closed by the ws.onopen / ws.onerror path. We can
        // remember the intent and apply on open.
        session.closeOnOpen = {
          code: typeof code === "number" ? code : 1000,
          reason: typeof reason === "string" ? reason : "",
        };
      }
    } catch (err) {
      console.error("[chrometunnel] ws close failed for", id, err);
    }
    return;
  }

  if (id && (message.wsSend || message.wsClose)) return;

  if (!url) {
    sendToNative({ id, error: "Missing 'url' in request message." });
    return;
  }

  // ---- WebSocket handshake ---------------------------------------------
  //
  // The native host has seen a client HTTP Upgrade: websocket. It asks
  // us to open a real WebSocket to the upstream server (which is the
  // only path that will pick up the user's VPN/proxy extension in
  // Chrome). The handshake response code + headers we observe are
  // forwarded back to the host, which writes the 101 to the client.
  if (message.kind === "ws-open") {
    handleWebSocketOpen(message);
    return;
  }

  const receivedAt = Date.now();
  let requestType = getRequestType(message);

  // Progress messages carry the active deadline to the native watchdog. They
  // are sent only on phase changes, not as heartbeats that could hide a stall.
  sendToNative({ id, progress: "queued", requestType, timeoutMs: QUEUE_TIMEOUT_MS });

  runWithConcurrencyLimit(async () => {
    const fetchStartedAt = Date.now();
    const queueMs = fetchStartedAt - receivedAt;
    let policy = REQUEST_TIMEOUTS[requestType];
    let headersMs = null;
    const controller = new AbortController();
    let timeoutHandle = null;
    let timeoutMs = policy.headersMs;
    let timeoutPhase = "waiting for response headers";
    let streamStarted = false;

    function armTimeout(ms, phase) {
      if (timeoutHandle) clearTimeout(timeoutHandle);
      timeoutMs = ms;
      timeoutPhase = phase;
      timeoutHandle = setTimeout(() => controller.abort(), ms);
    }

    armTimeout(policy.headersMs, "waiting for response headers");
    sendToNative({
      id, progress: "fetching", requestType, timeoutMs: policy.headersMs, queueMs,
    });

    try {
      const fetchOptions = {
        method: method || "GET",
        headers: headers || {},
        signal: controller.signal,
      };

      if (body) fetchOptions.body = base64ToUint8Array(body);

      // The response-header budget starts when this fetch starts, so time in
      // the queue cannot consume the stream's larger startup allowance.
      const response = await fetch(url, fetchOptions);
      headersMs = Date.now() - fetchStartedAt;

      const responseHeaders = {};
      for (const [key, value] of response.headers.entries()) {
        responseHeaders[key] = value;
      }

      // Servers may reveal a stream only in their response. This upgrades body
      // timeouts; only request-side hints can extend the earlier header wait.
      if (isStreamingContentType(getHeader(responseHeaders, "content-type"))) {
        requestType = "stream";
        policy = REQUEST_TIMEOUTS.stream;
      }

      // Headers can arrive before the model generates its first output. Give
      // that first body chunk its own startup allowance, then use idleMs.
      armTimeout(policy.firstChunkMs, "waiting for first response chunk");
      streamStarted = true;
      sendToNative({
        id,
        stream: "start",
        status: response.status,
        statusText: response.statusText,
        headers: responseHeaders,
        requestType,
        timeoutMs: policy.firstChunkMs,
        queueMs,
        headersMs,
      });

      // A response with no body (HEAD/204/etc.) is complete immediately.
      if (!response.body) {
        if (timeoutHandle) clearTimeout(timeoutHandle);
        sendToNative({ id, stream: "end" });
        return;
      }

      const reader = response.body.getReader();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        if (value && value.byteLength > 0) {
          armTimeout(policy.idleMs, "waiting for next response chunk");
          // Forward each browser response chunk immediately instead of first
          // buffering the entire response into response.arrayBuffer().
          sendToNative({
            id,
            stream: "data",
            body: uint8ArrayToBase64(value),
            timeoutMs: policy.idleMs,
          });
        }
      }

      if (timeoutHandle) clearTimeout(timeoutHandle);
      sendToNative({ id, stream: "end" });
    } catch (err) {
      if (timeoutHandle) clearTimeout(timeoutHandle);

      const aborted =
        controller.signal.aborted || (err && err.name === "AbortError");
      const timing = `type=${requestType}; queueMs=${queueMs}; ` +
        `headersMs=${headersMs === null ? "pending" : headersMs}; ` +
        `fetchMs=${Date.now() - fetchStartedAt}`;
      const errorMessage = aborted
        ? `Extension fetch timed out: ${timeoutPhase} (limitMs=${timeoutMs}; ${timing}).`
        : `${err && err.message
          ? err.message
          : String(err)} (${timing})`;

      console.error("[chrometunnel] fetch failed for", url, errorMessage);

      if (streamStarted) {
        sendToNative({ id, stream: "error", error: errorMessage });
      } else {
        sendToNative({ id, error: errorMessage });
      }
    }
  }, () => {
    sendToNative({
      id,
      error: `Request timed out while waiting in extension fetch queue ` +
        `(type=${requestType}; limitMs=${QUEUE_TIMEOUT_MS}; queueMs=${Date.now() - receivedAt}).`,
    });
  });
}

function bufferPendingMessage(message) {
  pendingResponses.push(message);
  if (pendingResponses.length > MAX_PENDING_RESPONSES) {
    const dropped = pendingResponses.shift();
    console.error(
      "[chrometunnel] pending response buffer full, dropping oldest:",
      dropped && dropped.id
    );
  }
}

function sendToNative(message) {
  if (!port) {
    console.warn(
      "[chrometunnel] native port not connected, buffering response:",
      message.id
    );
    bufferPendingMessage(message);
    ensureConnected();
    return;
  }

  try {
    for (const part of splitNativeMessage(message, "ext", nativeByteLength)) port.postMessage(part);
  } catch (err) {
    console.error("[chrometunnel] native send failed:", err);
    // Keep retry buffering bounded by the same individual-message budget.
    if (nativeByteLength(JSON.stringify(message)) <= MAX_MESSAGE_BYTES) bufferPendingMessage(message);
  }
}

// ---- base64 <-> Uint8Array helpers ---------------------------------------
function uint8ArrayToBase64(bytes) {
  let binary = "";
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize);
    binary += String.fromCharCode.apply(null, chunk);
  }
  return btoa(binary);
}

function base64ToUint8Array(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

ensureConnected();
