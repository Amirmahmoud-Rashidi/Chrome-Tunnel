// core/relay.js — shared native-message request/response relay.
//
// Long responses are streamed as a sequence of messages:
//   { id, stream: "start", status, statusText, headers }
//   { id, stream: "data", body }          // body is base64
//   { id, stream: "end" }
//   { id, stream: "error", error }
//
// The extension owns normal/stream timeout policy. Its queue/fetch phase
// messages and body messages announce the next deadline. This watchdog waits
// that long plus delivery grace; it must not cut a slow stream off at 60s.
// Without timing metadata (older extensions), keep the original 60s fallback.
const crypto = require("crypto");

const DEFAULT_TIMEOUT_MS = 60_000;
const DELIVERY_GRACE_MS = 15_000;
const MAX_EXTENSION_TIMEOUT_MS = 300_000;

function createRelay({ sendToExtension, timeoutMs = DEFAULT_TIMEOUT_MS }) {
  const pending = new Map();

  function clearEntry(id) {
    const entry = pending.get(id);
    if (!entry) return null;
    if (entry.timer) clearTimeout(entry.timer);
    pending.delete(id);
    return entry;
  }

  function announcedTimeout(message) {
    const ms = message.timeoutMs;
    return Number.isInteger(ms) && ms > 0 && ms <= MAX_EXTENSION_TIMEOUT_MS
      ? ms + DELIVERY_GRACE_MS
      : timeoutMs;
  }

  function armInactivityTimer(id, entry, waitMs = timeoutMs) {
    if (entry.timer) clearTimeout(entry.timer);
    entry.timer = setTimeout(() => {
      if (!pending.has(id)) return;
      pending.delete(id);
      const err = new Error(
        `Timed out waiting for extension activity (${waitMs}ms without progress; ` +
          `phase=${entry.phase}; type=${entry.requestType || "unknown"}).`
      );
      err.code = "EXTENSION_INACTIVITY_TIMEOUT";
      entry.reject(err);
    }, waitMs);
  }

  function failEntry(id, entry, err) {
    if (entry.timer) clearTimeout(entry.timer);
    pending.delete(id);
    entry.reject(err instanceof Error ? err : new Error(String(err)));
  }

  function callStreamHandler(id, entry, handler, arg) {
    if (typeof handler !== "function") return true;
    try {
      handler(arg);
      return true;
    } catch (err) {
      failEntry(id, entry, err);
      return false;
    }
  }

  function handleExtensionResponse(message) {
    const { id } = message || {};
    if (!id || !pending.has(id)) {
      // Stray/duplicate response or ping reply.
      return;
    }

    const entry = pending.get(id);

    // These are control messages, not completed HTTP responses. Only genuine
    // forward phase transitions refresh the timer; duplicate announcements
    // must not postpone a deadline indefinitely.
    if (message.progress) {
      if (message.progress === "queued" && entry.phase === "bridge") {
        entry.phase = "queue";
      } else if (
        message.progress === "fetching" &&
        (entry.phase === "bridge" || entry.phase === "queue")
      ) {
        entry.phase = "headers";
      } else {
        return;
      }
      entry.requestType = message.requestType;
      armInactivityTimer(id, entry, announcedTimeout(message));
      return;
    }

    if (message.stream === "start") {
      if (entry.phase === "first-chunk" || entry.phase === "body") return;
      entry.phase = "first-chunk";
      entry.requestType = message.requestType || entry.requestType;
      armInactivityTimer(id, entry, announcedTimeout(message));
      callStreamHandler(id, entry, entry.onResponseStart, message);
      return;
    }

    if (message.stream === "data") {
      const chunk = message.body
        ? Buffer.from(message.body, "base64")
        : Buffer.alloc(0);
      if (chunk.length === 0) return;
      entry.phase = "body";
      armInactivityTimer(id, entry, announcedTimeout(message));
      callStreamHandler(id, entry, entry.onResponseChunk, chunk);
      return;
    }

    if (message.stream === "end") {
      if (entry.timer) clearTimeout(entry.timer);
      pending.delete(id);
      try {
        if (typeof entry.onResponseEnd === "function") {
          entry.onResponseEnd();
        }
        entry.resolve({ id, streamed: true });
      } catch (err) {
        entry.reject(err);
      }
      return;
    }

    if (message.stream === "error") {
      const err = new Error(message.error || "Extension stream failed.");
      err.code = "EXTENSION_STREAM_ERROR";
      failEntry(id, entry, err);
      return;
    }

    // Backward compatibility with the original single-message response.
    const finished = clearEntry(id);
    if (finished) finished.resolve(message);
  }

  function relayToExtension({
    url,
    method,
    headers,
    bodyBuffer,
    onResponseStart,
    onResponseChunk,
    onResponseEnd,
  }) {
    const id = crypto.randomUUID();
    const forwardHeaders = { ...headers };
    delete forwardHeaders["proxy-connection"];
    delete forwardHeaders["connection"];
    delete forwardHeaders["host"];
    delete forwardHeaders["content-length"];

    const job = { id, url, method, headers: forwardHeaders };
    if (bodyBuffer && bodyBuffer.length > 0) {
      job.body = bodyBuffer.toString("base64");
    }

    const responsePromise = new Promise((resolve, reject) => {
      const entry = {
        resolve,
        reject,
        timer: null,
        phase: "bridge",
        requestType: null,
        onResponseStart,
        onResponseChunk,
        onResponseEnd,
      };
      pending.set(id, entry);
      armInactivityTimer(id, entry);
    });

    try {
      sendToExtension(job);
    } catch (err) {
      const entry = clearEntry(id);
      if (entry) entry.reject(err);
    }

    return responsePromise;
  }

  return { relayToExtension, handleExtensionResponse };
}

module.exports = { createRelay, DEFAULT_TIMEOUT_MS };
