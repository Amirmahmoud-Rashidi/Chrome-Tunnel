// background.js — chrometunnel bridge (Manifest V3 service worker)
//
// Response streaming protocol sent to the native host:
//   { id, stream: "start", status, statusText, headers }
//   { id, stream: "data", body }          // body is base64
//   { id, stream: "end" }
//   { id, stream: "error", error }
//
// REQUEST_TIMEOUT_MS is deliberately an inactivity/startup deadline, NOT a
// maximum lifetime for the entire response. A response may run for minutes as
// long as new data keeps arriving before the inactivity deadline expires.

const NATIVE_HOST_NAME = "local.chrometunnel.host";
const SESSION_KEY_CONNECTED = "chrometunnel_connected";

// Chrome native messaging has a roughly 1 MB single-message limit. Generic
// outgoing messages larger than this are split and reassembled by host.js.
const CHUNK_THRESHOLD_BYTES = 800 * 1024;
const CHUNK_SIZE_BYTES = 700 * 1024;

// 45 seconds remains the extension-side safety value, but now means:
//   1) maximum queue + response-header wait before fetch starts/responds, and
//   2) maximum period with NO response-body progress after headers arrive.
const REQUEST_TIMEOUT_MS = 45_000;

let port = null;
let connectInFlight = false;

const pendingResponses = [];
const MAX_PENDING_RESPONSES = 200;
const incomingChunkBuffers = new Map();

const MAX_CONCURRENT_FETCHES = 6;
let activeFetchCount = 0;
const fetchQueue = [];

function runWithConcurrencyLimit(task) {
  if (activeFetchCount < MAX_CONCURRENT_FETCHES) {
    activeFetchCount++;
    task().finally(() => {
      activeFetchCount--;
      const next = fetchQueue.shift();
      if (next) runWithConcurrencyLimit(next);
    });
  } else {
    fetchQueue.push(task);
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

  runWithConcurrencyLimit(async () => {
    const elapsedMs = Date.now() - receivedAt;
    const startupRemainingMs = REQUEST_TIMEOUT_MS - elapsedMs;

    if (startupRemainingMs <= 0) {
      sendToNative({
        id,
        error: `Request timed out while waiting in extension fetch queue (${REQUEST_TIMEOUT_MS}ms deadline).`,
      });
      return;
    }

    const controller = new AbortController();
    let timeoutHandle = null;
    let timeoutPhase = "waiting for response headers";
    let streamStarted = false;

    function armTimeout(ms, phase) {
      if (timeoutHandle) clearTimeout(timeoutHandle);
      timeoutPhase = phase;
      timeoutHandle = setTimeout(() => controller.abort(), ms);
    }

    function resetBodyInactivityTimeout() {
      armTimeout(
        REQUEST_TIMEOUT_MS,
        `waiting for next response chunk (${REQUEST_TIMEOUT_MS}ms inactivity limit)`
      );
    }

    armTimeout(startupRemainingMs, "waiting for response headers");

    try {
      const fetchOptions = {
        method: method || "GET",
        headers: headers || {},
        signal: controller.signal,
      };

      if (body) fetchOptions.body = base64ToUint8Array(body);

      // fetch() resolves when response headers are available. From this point
      // onward we switch from a total deadline to an inactivity deadline.
      const response = await fetch(url, fetchOptions);

      const responseHeaders = {};
      for (const [key, value] of response.headers.entries()) {
        responseHeaders[key] = value;
      }

      streamStarted = true;
      sendToNative({
        id,
        stream: "start",
        status: response.status,
        statusText: response.statusText,
        headers: responseHeaders,
      });

      // A response with no body (HEAD/204/etc.) is complete immediately.
      if (!response.body) {
        if (timeoutHandle) clearTimeout(timeoutHandle);
        sendToNative({ id, stream: "end" });
        return;
      }

      const reader = response.body.getReader();
      resetBodyInactivityTimeout();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        if (value && value.byteLength > 0) {
          // Forward each browser response chunk immediately instead of first
          // buffering the entire response into response.arrayBuffer().
          sendToNative({
            id,
            stream: "data",
            body: uint8ArrayToBase64(value),
          });
          resetBodyInactivityTimeout();
        }
      }

      if (timeoutHandle) clearTimeout(timeoutHandle);
      sendToNative({ id, stream: "end" });
    } catch (err) {
      if (timeoutHandle) clearTimeout(timeoutHandle);

      const aborted =
        controller.signal.aborted || (err && err.name === "AbortError");
      const errorMessage = aborted
        ? `Extension fetch timed out: ${timeoutPhase}.`
        : err && err.message
          ? err.message
          : String(err);

      console.error("[chrometunnel] fetch failed for", url, errorMessage);

      if (streamStarted) {
        sendToNative({ id, stream: "error", error: errorMessage });
      } else {
        sendToNative({ id, error: errorMessage });
      }
    }
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
