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

// WebSocket connections are long-lived; the HTTP request/response
// timeout policy doesn't apply. Idle = no traffic in this many ms
// (refreshed on every message). Handshake has its own shorter deadline.
const WS_HANDSHAKE_TIMEOUT_MS = 45_000;
const WS_IDLE_TIMEOUT_MS = 120_000;

function createRelay({ sendToExtension, timeoutMs = DEFAULT_TIMEOUT_MS }) {
  const pending = new Map();
  // Live WebSocket sessions, keyed by id. Each holds the long-lived
  // promise resolvers + bookkeeping for the inactivity timer.
  const wsSessions = new Map();
  // Callbacks the protocols/ws module registers so it can be told when
  // the extension sends a server→client message or closes the socket.
  const wsInboundListeners = new Set();

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
    if (!id || (!pending.has(id) && !wsSessions.has(id))) {
      // Stray/duplicate response or ping reply.
      return;
    }

    // WebSocket control messages live in wsSessions, not pending — route
    // them there before falling into the HTTP pending-entry lookup below.
    if (
      wsSessions.has(id) &&
      (message.wsAccepted !== undefined ||
        message.wsError !== undefined ||
        message.wsMessage !== undefined ||
        message.wsClose !== undefined)
    ) {
      handleWsControlMessage(id, message);
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

  // ---- WebSocket relay API ----------------------------------------------
  //
  // Three operations make up the full client→extension→server data path:
  //
  //   relayWsOpen({url, headers})
  //     → Promise<{accepted, headers} | {error}>
  //     Asks the extension to open a real WebSocket to the upstream.
  //     Resolves on first wsAccepted / wsError message; any later
  //     messages with this id are routed to the inbound listeners.
  //
  //   relayWsMessage({id, wsSend: {payload, isBinary}})
  //     Forwards a client frame upstream. Best-effort: the WebSocket
  //     might already be closed by the time the extension processes it.
  //
  //   relayWsControl({id, wsClose: {code, reason}})
  //     Tells the extension to close the upstream WebSocket cleanly.
  //     Used when the client side closes first, so the upstream
  //     observes a WebSocket close (not a TCP reset).
  //
  // Inbound messages (server → client) are dispatched to listeners
  // registered via onWsInbound(). The protocols/ws module wires its
  // client socket writes through this channel.

  // ---- WebSocket control plane --------------------------------------
  //
  // A live WebSocket session sends three kinds of control messages, all
  // routed here by handleExtensionResponse (they live in wsSessions, not
  // the HTTP pending map):
  //   { id, wsAccepted, headers }  — handshake succeeded upstream
  //   { id, wsError }              — handshake failed
  //   { id, wsMessage, isBinary }  — frame from upstream server
  //   { id, wsClose, code, reason }— upstream server closed
  //
  // These use a separate id-space (uuid) from HTTP pending entries, so
  // there is no collision risk between the two maps.
  function handleWsControlMessage(id, message) {
    if (message.wsAccepted !== undefined || message.wsError !== undefined) {
      const session = wsSessions.get(id);
      if (!session) return;
      if (session.handshakeDone) return;
      session.handshakeDone = true;
      if (session.handshakeTimer) {
        clearTimeout(session.handshakeTimer);
        session.handshakeTimer = null;
      }
      if (message.wsError) {
        session.resolve({ id, error: String(message.wsError) });
        wsSessions.delete(id);
      } else {
        session.resolve({ id, accepted: true, headers: message.headers || {} });
      }
      return;
    }

    if (message.wsMessage !== undefined) {
      const session = wsSessions.get(id);
      if (!session) return;
      armWsIdleTimer(id, session);
      for (const listener of wsInboundListeners) {
        try {
          listener({
            kind: "message",
            id,
            payload: message.wsMessage, // base64
            isBinary: Boolean(message.isBinary),
          });
        } catch (err) {
          console.error("[relay] ws message listener threw:", err);
        }
      }
      return;
    }

    if (message.wsClose) {
      const session = wsSessions.get(id);
      if (!session) return;
      clearWsTimers(session);
      wsSessions.delete(id);
      for (const listener of wsInboundListeners) {
        try {
          listener({
            kind: "close",
            id,
            code: typeof message.wsClose === "object" ? message.wsClose.code : 1000,
            reason: typeof message.wsClose === "object" ? message.wsClose.reason : "",
          });
        } catch (err) {
          console.error("[relay] ws close listener threw:", err);
        }
      }
      return;
    }
  }

  function armWsIdleTimer(id, session) {
    if (session.idleTimer) clearTimeout(session.idleTimer);
    session.idleTimer = setTimeout(() => {
      if (!wsSessions.has(id)) return;
      wsSessions.delete(id);
      for (const listener of wsInboundListeners) {
        try {
          listener({ kind: "close", id, code: 1001, reason: "Idle timeout" });
        } catch (err) {
          console.error("[relay] ws idle listener threw:", err);
        }
      }
    }, WS_IDLE_TIMEOUT_MS);
  }

  function clearWsTimers(session) {
    if (session.handshakeTimer) {
      clearTimeout(session.handshakeTimer);
      session.handshakeTimer = null;
    }
    if (session.idleTimer) {
      clearTimeout(session.idleTimer);
      session.idleTimer = null;
    }
  }

  function relayWsOpen({ url, headers }) {
    const id = crypto.randomUUID();
    const job = {
      kind: "ws-open",
      id,
      url,
      headers: stripHopByHop(headers || {}),
    };

    return new Promise((resolve) => {
      const handshakeTimer = setTimeout(() => {
        if (!wsSessions.has(id)) return;
        wsSessions.delete(id);
        resolve({ id, error: "WebSocket handshake timed out" });
      }, WS_HANDSHAKE_TIMEOUT_MS);

      const session = {
        resolve,
        handshakeTimer,
        idleTimer: null,
        handshakeDone: false,
      };
      wsSessions.set(id, session);

      try {
        sendToExtension(job);
      } catch (err) {
        clearWsTimers(session);
        wsSessions.delete(id);
        resolve({ id, error: err.message });
      }
    });
  }

  function relayWsMessage({ id, wsSend }) {
    if (!wsSessions.has(id)) {
      return Promise.resolve(false); // already closed
    }
    try {
      sendToExtension({
        id,
        wsSend: {
          payload: wsSend.payload,
          isBinary: Boolean(wsSend.isBinary),
          kind: wsSend.kind || "message",
        },
      });
      return Promise.resolve(true);
    } catch (err) {
      return Promise.resolve(false);
    }
  }

  function relayWsControl({ id, wsClose }) {
    if (!wsSessions.has(id)) {
      return Promise.resolve(false);
    }
    try {
      sendToExtension({ id, wsClose });
      return Promise.resolve(true);
    } catch (err) {
      return Promise.resolve(false);
    }
  }

  function onWsInbound(listener) {
    wsInboundListeners.add(listener);
    return () => wsInboundListeners.delete(listener);
  }

  return {
    relayToExtension,
    relayWsOpen,
    relayWsMessage,
    relayWsControl,
    onWsInbound,
    handleExtensionResponse,
  };
}

function stripHopByHop(headers) {
  const out = { ...headers };
  const hop = new Set([
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
  ]);
  for (const k of hop) delete out[k.toLowerCase()];
  return out;
}

module.exports = { createRelay, DEFAULT_TIMEOUT_MS };
