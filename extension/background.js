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

// Chrome native messaging has a roughly 1 MB single-message limit. Generic
// outgoing messages larger than this are split and reassembled by host.js.
const CHUNK_THRESHOLD_BYTES = 800 * 1024;
const CHUNK_SIZE_BYTES = 700 * 1024;

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
const incomingChunkBuffers = new Map();

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
    const stored = await chrome.storage.session.get(SESSION_KEY_CONNECTED);
    if (stored[SESSION_KEY_CONNECTED]) return;
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

  // Reassemble large incoming request messages from host.js.
  if (message.chunkId) {
    const { chunkId, seq, total, data } = message;
    let buf = incomingChunkBuffers.get(chunkId);
    if (!buf) {
      buf = new Array(total).fill(null);
      incomingChunkBuffers.set(chunkId, buf);
    }
    buf[seq] = data;

    if (buf.every((part) => part !== null)) {
      incomingChunkBuffers.delete(chunkId);
      let reassembled;
      try {
        reassembled = JSON.parse(buf.join(""));
      } catch (err) {
        console.error(
          "[chrometunnel] failed to reassemble chunked message:",
          chunkId,
          err
        );
        return;
      }
      return handleNativeMessage(reassembled);
    }
    return;
  }

  const { id, ping, url, method, headers, body } = message;

  if (ping) {
    sendToNative({ id, pong: true, time: Date.now() });
    return;
  }

  if (!url) {
    sendToNative({ id, error: "Missing 'url' in request message." });
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

  const json = JSON.stringify(message);

  if (json.length <= CHUNK_THRESHOLD_BYTES) {
    try {
      port.postMessage(message);
    } catch (err) {
      console.error(
        "[chrometunnel] postMessage failed, buffering for retry:",
        err
      );
      bufferPendingMessage(message);
    }
    return;
  }

  const chunkId = `ext-${message.id || Date.now()}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;
  const total = Math.ceil(json.length / CHUNK_SIZE_BYTES);
  console.log(
    `[chrometunnel] message for ${chunkId} is ${json.length} bytes, splitting into ${total} chunks`
  );

  try {
    for (let seq = 0; seq < total; seq++) {
      const data = json.slice(
        seq * CHUNK_SIZE_BYTES,
        (seq + 1) * CHUNK_SIZE_BYTES
      );
      port.postMessage({ chunkId, seq, total, data });
    }
  } catch (err) {
    console.error(
      "[chrometunnel] postMessage failed mid-chunk, buffering whole message for retry:",
      err
    );
    bufferPendingMessage(message);
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
