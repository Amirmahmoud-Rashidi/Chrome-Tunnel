// protocols/https/index.js — CONNECT + local TLS MITM handler.
const tls = require("tls");
const { getCertificateForHost } = require("./tls-mitm");
const { logFailedRequest } = require("../../failure-logger");
const {
  isCorsPreflight,
  buildCorsPreflightHeaders,
  applyCorsResponseHeaders,
} = require("../../core/cors");

function attach(server, { relayToExtension }) {
  server.on("connect", (req, clientSocket, head) => {
    const [hostname] = req.url.split(":");
    console.error(`[protocols/https] CONNECT ${req.url}`);

    let cert;
    try {
      cert = getCertificateForHost(hostname);
    } catch (err) {
      console.error(
        "[protocols/https] failed to generate certificate for",
        hostname,
        err
      );
      clientSocket.write("HTTP/1.1 502 Bad Gateway\r\n\r\n");
      clientSocket.end();
      return;
    }

    clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");

    const tlsSocket = new tls.TLSSocket(clientSocket, {
      isServer: true,
      key: cert.keyPem,
      cert: cert.certPem,
    });

    if (head && head.length > 0) tlsSocket.unshift(head);

    // Track where this TLS connection was when it failed, so the failure log
    // can distinguish real problems (handshake never completed — likely a CA
    // trust issue, the kind that actually breaks clients) from harmless noise
    // (idle keep-alive sockets the client opened and closed without sending
    // anything). See PROJECT_HISTORY.md, bug-hunting notes for #14.
    const tlsState = { phase: "handshake" };
    tlsSocket.on("secureConnect", () => {
      tlsState.phase = "idle";
    });

    tlsSocket.on("error", (err) => {
      console.error(`[protocols/https] TLS error for ${hostname}:`, err.message);
      logFailedRequest({
        source: "tls-socket",
        url: `https://${hostname}`,
        origin: `client:${req.socket.remoteAddress}:${req.socket.remotePort}`,
        reason: err.message,
        phase: tlsState.phase,
      });
    });

    handleDecryptedHttpStream(
      tlsSocket,
      hostname,
      clientSocket,
      relayToExtension,
      tlsState
    );
  });
}

function handleDecryptedHttpStream(
  tlsSocket,
  hostname,
  clientSocket,
  relayToExtension,
  tlsState
) {
  let buffer = Buffer.alloc(0);
  const clientOrigin = `${clientSocket.remoteAddress}:${clientSocket.remotePort}`;

  tlsSocket.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    tryParseAndHandleRequest();
  });

  function tryParseAndHandleRequest() {
    const headerEnd = buffer.indexOf("\r\n\r\n");
    if (headerEnd === -1) return;

    const headerText = buffer.subarray(0, headerEnd).toString("utf8");
    const lines = headerText.split("\r\n");
    const requestLine = lines[0];
    const [method, path] = requestLine.split(" ");
    const headers = {};

    // The TLS handshake completed and the client sent a real request. Any
    // error from this point on is a mid-stream failure, not idle churn.
    if (tlsState) tlsState.phase = "request";

    for (let i = 1; i < lines.length; i++) {
      const idx = lines[i].indexOf(":");
      if (idx === -1) continue;
      const key = lines[i].slice(0, idx).trim().toLowerCase();
      const value = lines[i].slice(idx + 1).trim();
      headers[key] = value;
    }

    const contentLength = parseInt(headers["content-length"] || "0", 10);
    const bodyStart = headerEnd + 4;
    if (buffer.length - bodyStart < contentLength) return;

    const bodyBuffer = buffer.subarray(bodyStart, bodyStart + contentLength);
    buffer = buffer.subarray(bodyStart + contentLength);

    const targetUrl = `https://${hostname}${
      path.startsWith("/") ? path : "/" + path
    }`;
    const context = {
      method,
      url: targetUrl,
      origin: clientOrigin,
      requestHeaders: headers,
    };

    if (isCorsPreflight(targetUrl, method, headers)) {
      const responseHeaders = buildCorsPreflightHeaders(headers);
      responseHeaders["connection"] = "close";
      const headerLines = Object.entries(responseHeaders)
        .map(([k, v]) => `${k}: ${v}`)
        .join("\r\n");
      tlsSocket.end(
        `HTTP/1.1 204 No Content\r\n${headerLines}\r\n\r\n`
      );
      return;
    }

    const streamState = {
      started: false,
      ended: false,
      noBody: false,
    };

    relayToExtension({
      url: targetUrl,
      method,
      headers,
      bodyBuffer,
      onResponseStart: (start) => {
        streamState.started = true;
        writeTlsStreamStart(tlsSocket, start, context, streamState);
      },
      onResponseChunk: (chunk) => {
        writeTlsStreamChunk(tlsSocket, chunk, streamState);
      },
      onResponseEnd: () => {
        finishTlsStream(tlsSocket, streamState);
      },
    })
      .then((result) => {
        if (!streamState.started && !result.streamed) {
          writeTlsResult(tlsSocket, result, context);
        }
      })
      .catch((err) => {
        console.error("[protocols/https] MITM request failed:", err.message);
        logFailedRequest({
          source: "proxy-server",
          method,
          url: targetUrl,
          origin: clientOrigin,
          reason: err.message,
          phase: tlsState.phase,
        });

        if (streamState.started) {
          // A streamed HTTP response has already begun, so its status cannot
          // be replaced with a 502. End without the terminating HTTP chunk so
          // the client can detect the truncated transfer.
          if (!streamState.ended && !tlsSocket.destroyed) {
            streamState.ended = true;
            tlsSocket.end();
          }
        } else {
          writeTlsResult(
            tlsSocket,
            { status: 502, error: err.message, alreadyLogged: true },
            context
          );
        }
      });

    // Existing behavior is connection-close oriented, so pipelining is not
    // useful for the streamed response itself. Keep the old defensive parser
    // behavior for already-buffered input.
    if (buffer.length > 0) tryParseAndHandleRequest();
  }
}

function responseHasNoBody(method, status) {
  if (String(method || "").toUpperCase() === "HEAD") return true;
  if (status >= 100 && status < 200) return true;
  return status === 204 || status === 304;
}

function sanitizeHeaders(headers, context) {
  const result = { ...(headers || {}) };
  delete result["content-length"];
  delete result["content-encoding"]; // browser fetch already decoded it
  delete result["transfer-encoding"];
  delete result["connection"];
  applyCorsResponseHeaders(result, context);
  return result;
}

function logHttpStatus(result, context) {
  if (result.status >= 400) {
    logFailedRequest({
      source: "http-status",
      id: result.id,
      method: context.method,
      url: context.url,
      origin: context.origin,
      status: result.status,
      reason: result.statusText,
      phase: "request",
    });
  }
}

function writeTlsStreamStart(tlsSocket, start, context, state) {
  logHttpStatus(start, context);

  const status = start.status || 502;
  const headers = sanitizeHeaders(start.headers, context);
  state.noBody = responseHasNoBody(context.method, status);

  if (!state.noBody) {
    headers["transfer-encoding"] = "chunked";
  }
  headers["connection"] = "close";

  const statusLine = `HTTP/1.1 ${status} ${start.statusText || ""}`.trimEnd();
  const headerLines = Object.entries(headers)
    .map(([k, v]) => `${k}: ${v}`)
    .join("\r\n");

  tlsSocket.write(`${statusLine}\r\n${headerLines}\r\n\r\n`);
}

function writeTlsStreamChunk(tlsSocket, chunk, state) {
  if (
    state.ended ||
    state.noBody ||
    tlsSocket.destroyed ||
    !chunk ||
    chunk.length === 0
  ) {
    return;
  }

  tlsSocket.write(`${chunk.length.toString(16)}\r\n`);
  tlsSocket.write(chunk);
  tlsSocket.write("\r\n");
}

function finishTlsStream(tlsSocket, state) {
  if (state.ended || tlsSocket.destroyed) return;
  state.ended = true;

  if (!state.noBody) tlsSocket.write("0\r\n\r\n");
  tlsSocket.end();
}

// Backward compatibility with the original single-message response format.
function writeTlsResult(tlsSocket, result, context) {
  if (result.error) {
    if (!result.alreadyLogged) {
      logFailedRequest({
        source: "extension",
        id: result.id,
        method: context.method,
        url: context.url,
        phase: "request",
        origin: context.origin,
        reason: result.error,
      });
    }

    const body = `Extension fetch failed: ${result.error}\n`;
    tlsSocket.write(
      `HTTP/1.1 502 Bad Gateway\r\nContent-Length: ${Buffer.byteLength(
        body
      )}\r\nConnection: close\r\n\r\n${body}`
    );
    tlsSocket.end();
    return;
  }

  logHttpStatus(result, context);

  const responseBody = result.body
    ? Buffer.from(result.body, "base64")
    : Buffer.alloc(0);
  const headers = sanitizeHeaders(result.headers, context);
  headers["content-length"] = String(responseBody.length);
  headers["connection"] = "close";

  const statusLine = `HTTP/1.1 ${result.status || 502} ${
    result.statusText || ""
  }`.trimEnd();
  const headerLines = Object.entries(headers)
    .map(([k, v]) => `${k}: ${v}`)
    .join("\r\n");

  tlsSocket.write(`${statusLine}\r\n${headerLines}\r\n\r\n`);
  tlsSocket.end(responseBody);
}

module.exports = { attach };
