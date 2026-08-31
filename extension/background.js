// background.js — chrometunnel bridge (Manifest V3 service worker)
//
// Responsibilities:
//   1. Connect to the native host via chrome.runtime.connectNative().
//   2. Receive {id, url, method, headers, body} messages from the native host.
//   3. Perform fetch() inside Chrome (so DotVPN's chrome.proxy setting applies).
//   4. Send {id, status, statusText, headers, body(base64)} back, or {id, error}.
//   5. Keep the service worker alive (MV3 idles workers after ~30s of inactivity).
//
// IMPORTANT: every time the MV3 service worker goes idle and wakes back up,
// its JS state (including the `port` variable) resets to nothing — even
// though the underlying native host PROCESS Chrome launched may still be
// alive and holding the proxy's TCP port. If the keep-alive alarm just
// calls connectNative() again unconditionally, each wake spawns a brand
// new native host process that immediately fails (or fights over) the
// port the previous one is still holding. To avoid that, we track a
// "connectionActive" flag via chrome.storage.session (which, unlike plain
// variables, survives a service worker restart) and only reconnect when
// we know for certain the previous connection actually disconnected.
const NATIVE_HOST_NAME = "local.chrometunnel.host";
const SESSION_KEY_CONNECTED = "chrometunnel_connected";
// Chrome enforces an undocumented ~1MB cap on a single native-messaging
// payload. Sending anything larger causes the port to silently disconnect
// ("Error when communicating with the native messaging host"), which is
// what was happening whenever VS Code / Marketplace / Copilot fetched a
// response bigger than that (a VSIX package, a large API response, etc).
// To work around it, any outgoing message larger than this threshold is
// split into multiple chunks and reassembled on the other end. Kept well
// under 1MB for safety margin (base64 body + JSON overhead).
const CHUNK_THRESHOLD_BYTES = 800 * 1024;
const CHUNK_SIZE_BYTES = 700 * 1024;
// Keep the extension's own deadline shorter than relay.js's 60s
// timeout, so a stuck fetch can be aborted and reported while the native
// host is still waiting for the response. Time spent in the fetch queue
// counts toward this same deadline.
const REQUEST_TIMEOUT_MS = 45_000;
let port = null;
// Synchronous guard against overlapping connect attempts. ensureConnected
// is async (it awaits chrome.storage.session), so if several callers
// (e.g. many sendToNative() calls firing in quick succession while
// disconnected) all call it before the first one finishes, they can all
// pass the `if (port) return` check before `port` is actually set —
// each one then calls connectNative() and Chrome launches a SEPARATE
// host.js process for each, all fighting over the same TCP port. This
// flag is set synchronously the instant we decide to connect, closing
// that window.
let connectInFlight = false;
// Responses that were ready to send but the native port was disconnected
// at that moment (e.g. mid-idle-cycle). Buffered here and flushed once we
// reconnect, instead of being silently dropped — this was the cause of
// requests (like Copilot Chat calls) appearing to hang or fail even
// though the actual fetch() succeeded and a response was ready.
const pendingResponses = [];
const MAX_PENDING_RESPONSES = 200; // safety cap so a long outage can't grow this unbounded
// Buffer for reassembling incoming chunked messages (large request bodies
// coming from the native host), keyed by chunkId. Each entry collects
// parts until `total` have arrived, then reassembles and processes it as
// a normal message.
const incomingChunkBuffers = new Map();
// Concurrency limiter for outgoing fetch() calls. A burst of many
// simultaneous requests (e.g. VS Code's Marketplace check, which fires
// 25+ parallel "get latest version" calls at once) was overwhelming the
// service worker and causing it to be forcibly terminated by Chrome —
// not from normal 30s idle timeout, but from the sheer number of
// concurrent in-flight fetch()es and their callbacks. Limiting how many
// run at once keeps the worker responsive. No request is dropped: excess
// jobs simply wait in a FIFO queue until a slot frees up.
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
// MV3 service workers are killed when idle. A periodic alarm wakes this
// script back up. We do NOT blindly reconnect on every wake — only if we
// have no live `port` in this instance AND we're not already marked
// connected from a prior instance that's still alive.
const KEEP_ALIVE_ALARM = "chrometunnel-keep-alive";

chrome.alarms.create(KEEP_ALIVE_ALARM, { periodInMinutes: 0.4 }); // ~24s, under the 30s idle timeout
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === KEEP_ALIVE_ALARM) {
    ensureConnected();
  }
});

chrome.runtime.onStartup.addListener(ensureConnected);
chrome.runtime.onInstalled.addListener(ensureConnected);

// ---- Native messaging connection ------------------------------------------
async function ensureConnected() {
  if (port) return; // this service worker instance already has a live port
  if (connectInFlight) return; // another caller is already in the middle of connecting

  connectInFlight = true; // set synchronously, before any await, to close the race
  try {
    // Guard against a fresh service-worker instance reconnecting while a
    // previous instance's port is still actually alive (can happen right
    // around a worker restart). chrome.storage.session persists across
    // worker restarts within the same browser session.
    const stored = await chrome.storage.session.get(SESSION_KEY_CONNECTED);
    if (stored[SESSION_KEY_CONNECTED]) {
      // We believe a connection is already active elsewhere; do nothing.
      return;
    }
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
    console.warn("[chrometunnel] native port disconnected", err ? err.message : "");
    port = null;
    chrome.storage.session.set({ [SESSION_KEY_CONNECTED]: false });
    // Try to reconnect shortly; the alarm will also retry periodically.
    setTimeout(ensureConnected, 2000);
  });

  console.log("[chrometunnel] connected to native host:", NATIVE_HOST_NAME);
  // Flush anything that piled up while we were disconnected.
  flushPendingResponses();
}

function flushPendingResponses() {
  if (pendingResponses.length === 0) return;
  console.log(`[chrometunnel] flushing ${pendingResponses.length} buffered response(s) after reconnect`);
  const toSend = pendingResponses.splice(0, pendingResponses.length);
  for (const message of toSend) {
    sendToNative(message);
  }
}

// ---- Message handling -------------------------------------------------------
async function handleNativeMessage(message) {
  if (!message || typeof message !== "object") {
    console.warn("[chrometunnel] ignoring malformed message:", message);
    return;
  }
  // Chunked message reassembly: a large payload sent by proxy-server.js
  // arrives as multiple {chunkId, seq, total, data} messages instead of
  // one. Buffer parts until all have arrived, then reassemble into the
  // real JSON message and process it exactly as if it had arrived whole.
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
        console.error("[chrometunnel] failed to reassemble chunked message:", chunkId, err);
        return;
      }
      return handleNativeMessage(reassembled);
    }
    return; // wait for the remaining chunks
  }
  // Manual test messages (step 1 of the rollout plan) can just be
  // { id, ping: true } — handle that before assuming it's a fetch job.
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
    const remainingMs = REQUEST_TIMEOUT_MS - elapsedMs;

    // Do not start network work for a request whose native-side client is
    // already close to timing out. This also prevents stale queued jobs
    // from consuming one of the limited fetch slots later.
    if (remainingMs <= 0) {
      sendToNative({
        id,
        error: `Request timed out while waiting in extension fetch queue (${REQUEST_TIMEOUT_MS}ms deadline).`,
      });
      return;
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), remainingMs);

    try {
      const fetchOptions = {
        method: method || "GET",
        headers: headers || {},
        signal: controller.signal,
      };

      // body may arrive as a base64 string for binary-safety; decode if present.
      if (body) {
        fetchOptions.body = base64ToUint8Array(body);
      }

      const response = await fetch(url, fetchOptions);
      const responseHeaders = {};
      for (const [key, value] of response.headers.entries()) {
        responseHeaders[key] = value;
      }

      const arrayBuffer = await response.arrayBuffer();
      const bodyBase64 = uint8ArrayToBase64(new Uint8Array(arrayBuffer));
      sendToNative({
        id,
        status: response.status,
        statusText: response.statusText,
        headers: responseHeaders,
        body: bodyBase64,
      });
    } catch (err) {
      const errorMessage =
        controller.signal.aborted || (err && err.name === "AbortError")
          ? `Extension fetch timed out after ${REQUEST_TIMEOUT_MS}ms.`
          : err && err.message
            ? err.message
            : String(err);
      console.error("[chrometunnel] fetch failed for", url, errorMessage);
      sendToNative({
        id,
        error: errorMessage,
      });
    } finally {
      clearTimeout(timeout);
    }
  });
}
function sendToNative(message) {
  if (!port) {
    console.warn("[chrometunnel] native port not connected, buffering response:", message.id);
    pendingResponses.push(message);
    if (pendingResponses.length > MAX_PENDING_RESPONSES) {
      // Drop the oldest one rather than growing forever if the native
      // host stays unreachable for a long time.
      const dropped = pendingResponses.shift();
      console.error("[chrometunnel] pending response buffer full, dropping oldest:", dropped.id);
    }
    // Nudge a reconnect attempt now rather than waiting for the next
    // keep-alive alarm tick, so buffered responses get flushed sooner.
    ensureConnected();
    return;
  }
  const json = JSON.stringify(message);

  if (json.length <= CHUNK_THRESHOLD_BYTES) {
    try {
      port.postMessage(message);
    } catch (err) {
      console.error("[chrometunnel] postMessage failed, buffering for retry:", err);
      pendingResponses.push(message);
    }
    return;
  }
  // Message is too large for a single native-messaging payload (see the
  // CHUNK_THRESHOLD_BYTES comment above) — split it into parts. Each part
  // carries the same chunkId so the native host can reassemble them, plus
  // its position (seq) and the total part count.
  const chunkId = `ext-${message.id || Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const total = Math.ceil(json.length / CHUNK_SIZE_BYTES);
  console.log(`[chrometunnel] message for ${chunkId} is ${json.length} bytes, splitting into ${total} chunks`);
  try {
    for (let seq = 0; seq < total; seq++) {
      const data = json.slice(seq * CHUNK_SIZE_BYTES, (seq + 1) * CHUNK_SIZE_BYTES);
      port.postMessage({ chunkId, seq, total, data });
    }
  } catch (err) {
    console.error("[chrometunnel] postMessage failed mid-chunk, buffering whole message for retry:", err);
    pendingResponses.push(message);
  }
}
// ---- base64 <-> Uint8Array helpers ------------------------------------------
// NOTE: these are custom utility functions written for this extension,
// not built-in browser APIs. atob/btoa only handle binary strings, not
// raw bytes directly, so we bridge through String.fromCharCode/charCodeAt.
function uint8ArrayToBase64(bytes) {
  let binary = "";
  const chunkSize = 0x8000; // avoid call-stack limits on String.fromCharCode(...bigArray)
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

// Kick off the initial connection when the service worker first loads.
ensureConnected();
