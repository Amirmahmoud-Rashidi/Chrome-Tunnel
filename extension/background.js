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

let port = null;

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

  // Guard against a fresh service-worker instance reconnecting while a
  // previous instance's port is still actually alive (can happen right
  // around a worker restart). chrome.storage.session persists across
  // worker restarts within the same browser session.
  const stored = await chrome.storage.session.get(SESSION_KEY_CONNECTED);
  if (stored[SESSION_KEY_CONNECTED]) {
    // We believe a connection is already active elsewhere; do nothing.
    // If that belief is wrong (the other instance actually died without
    // updating storage), onDisconnect handling below will have cleared
    // this flag already in the normal case.
    return;
  }

  connect();
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
}

// ---- Message handling -------------------------------------------------------

async function handleNativeMessage(message) {
  // Manual test messages (step 1 of the rollout plan) can just be
  // { id, ping: true } — handle that before assuming it's a fetch job.
  if (!message || typeof message !== "object") {
    console.warn("[chrometunnel] ignoring malformed message:", message);
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

  try {
    const fetchOptions = {
      method: method || "GET",
      headers: headers || {},
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
    console.error("[chrometunnel] fetch failed for", url, err);
    sendToNative({
      id,
      error: err && err.message ? err.message : String(err),
    });
  }
}

function sendToNative(message) {
  if (!port) {
    console.error("[chrometunnel] cannot send, native port is not connected:", message);
    return;
  }
  try {
    port.postMessage(message);
  } catch (err) {
    console.error("[chrometunnel] postMessage failed:", err);
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
