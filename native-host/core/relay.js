// core/relay.js — the one piece every protocol handler shares: turning a
// {url, method, headers, body} job into a promise that resolves with the
// extension's fetch() result (or rejects on error/timeout).
//
// This is intentionally the ONLY thing that knows about the
// native-messaging request/response id map. It doesn't know or care
// whether the caller is the plain-HTTP handler, the HTTPS/MITM handler,
// or (later) a WebSocket or other protocol handler — every one of them
// just needs "send this job to the extension, get a response back",
// which is exactly what relayToExtension() provides.
//
// Splitting this out of proxy-server.js is what lets protocol handlers
// live in their own files/folders under protocols/ without each one
// re-implementing its own id map and timeout logic.

const crypto = require("crypto");

const DEFAULT_TIMEOUT_MS = 60000;

/**
 * @param {(job: object) => void} sendToExtension - forwards a job
 *        {id, url, method, headers, body?} to the extension via native
 *        messaging.
 * @returns {{
 *   relayToExtension: (req: {url: string, method: string, headers: object, bodyBuffer?: Buffer}) => Promise<object>,
 *   handleExtensionResponse: (message: object) => void,
 * }}
 */
function createRelay({ sendToExtension, timeoutMs = DEFAULT_TIMEOUT_MS }) {
  // Map of request id -> { resolve, reject } for in-flight requests.
  // Shared across every protocol handler that calls relayToExtension().
  const pending = new Map();

  function handleExtensionResponse(message) {
    const { id } = message || {};
    if (!id || !pending.has(id)) {
      // Could be a stray/duplicate message, or a ping reply — ignore safely.
      return;
    }
    const { resolve } = pending.get(id);
    pending.delete(id);
    resolve(message);
  }

  /**
   * Sends {method, url, headers, body} to the extension and resolves with
   * the extension's {status, statusText, headers, body} or rejects on
   * error/timeout.
   */
  function relayToExtension({ url, method, headers, bodyBuffer }) {
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
      pending.set(id, { resolve, reject });
      setTimeout(() => {
        if (pending.has(id)) {
          pending.delete(id);
          reject(new Error("Timed out waiting for extension response."));
        }
      }, timeoutMs);
    });

    sendToExtension(job);
    return responsePromise;
  }

  return { relayToExtension, handleExtensionResponse };
}

module.exports = { createRelay, DEFAULT_TIMEOUT_MS };
