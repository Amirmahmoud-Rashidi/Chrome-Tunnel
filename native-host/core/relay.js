// core/relay.js — shared native-message request/response relay.
//
// Long responses are streamed as a sequence of messages:
//   { id, stream: "start", status, statusText, headers }
//   { id, stream: "data", body }          // body is base64
//   { id, stream: "end" }
//   { id, stream: "error", error }
//
// The 60s timeout is an INACTIVITY timeout, not a maximum request lifetime.
// Every stream message resets it, so a response may continue indefinitely as
// long as progress is still being made.
const crypto = require("crypto");

const DEFAULT_TIMEOUT_MS = 60_000;

function createRelay({ sendToExtension, timeoutMs = DEFAULT_TIMEOUT_MS }) {
  const pending = new Map();

  function clearEntry(id) {
    const entry = pending.get(id);
    if (!entry) return null;
    if (entry.timer) clearTimeout(entry.timer);
    pending.delete(id);
    return entry;
  }

  function armInactivityTimer(id, entry) {
    if (entry.timer) clearTimeout(entry.timer);
    entry.timer = setTimeout(() => {
      if (!pending.has(id)) return;
      pending.delete(id);
      const err = new Error(
        `Timed out waiting for extension activity (${timeoutMs}ms without progress).`
      );
      err.code = "EXTENSION_INACTIVITY_TIMEOUT";
      entry.reject(err);
    }, timeoutMs);
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

    // Streaming protocol. Any stream event counts as activity and refreshes
    // the native-side inactivity deadline.
    if (message.stream === "start") {
      armInactivityTimer(id, entry);
      callStreamHandler(id, entry, entry.onResponseStart, message);
      return;
    }

    if (message.stream === "data") {
      armInactivityTimer(id, entry);
      const chunk = message.body
        ? Buffer.from(message.body, "base64")
        : Buffer.alloc(0);
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
