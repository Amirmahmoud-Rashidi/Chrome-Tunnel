// CONNECT TLS termination with bounded HTTP/1 request framing and WSS routing.
const tls = require("tls");
const { EventEmitter } = require("events");
const { getCertificateForHost } = require("./tls-mitm");
const wsProtocol = require("../ws");
const { logFailedRequest } = require("../../failure-logger");
const { isCorsPreflight, buildCorsPreflightHeaders, applyCorsResponseHeaders } = require("../../core/cors");
const MAX_HEADER_BYTES = 64 * 1024;
const MAX_BODY_BYTES = 64 * 1024 * 1024;

function parseAuthority(value) {
  // CONNECT uses authority-form, not an arbitrary URL or path.
  if (!/^(?:\[[0-9a-f:.]+\]|[^\s:/?#@]+):\d+$/i.test(value || "")) throw new Error("Invalid CONNECT authority");
  const target = new URL(`https://${value}`);
  const port = Number(value.slice(value.lastIndexOf(":") + 1));
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("Invalid CONNECT port");
  return { hostname: target.hostname.replace(/^\[|\]$/g, ""), authority: target.host };
}

function attach(server, options) {
  const { relayToExtension } = options;
  const upgrades = new EventEmitter();
  if (options.relayWsOpen) wsProtocol.attach(upgrades, options);
  server.on("connect", (req, clientSocket, head) => {
    let target, cert;
    try {
      target = parseAuthority(req.url);
    } catch {
      clientSocket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
      return;
    }
    try { cert = getCertificateForHost(target.hostname); }
    catch (err) {
      console.error("[protocols/https] certificate generation failed:", err.message);
      clientSocket.end("HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
      return;
    }
    clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
    // CONNECT head is encrypted TLS input, not decrypted HTTP data.
    if (head && head.length) clientSocket.unshift(head);
    let tlsSocket;
    try { tlsSocket = new tls.TLSSocket(clientSocket, { isServer: true, key: cert.keyPem, cert: cert.certPem }); }
    catch { clientSocket.destroy(); return; }
    const state = { phase: "handshake" };
    tlsSocket.on("secure", () => { state.phase = "idle"; });
    tlsSocket.on("error", err => {
      logFailedRequest({ source: "tls-socket", url: `https://${target.authority}`,
        reason: err.message, phase: state.phase });
    });
    handleDecryptedHttpStream(tlsSocket, target.authority, clientSocket, relayToExtension, state, upgrades);
  });
}

// Returns null for incomplete data, or a decoded body and consumed byte count.
// Trailers are consumed but not forwarded as regular request headers.
function readChunked(buffer, offset) {
  const parts = []; let size = 0;
  for (;;) {
    const lineEnd = buffer.indexOf("\r\n", offset);
    if (lineEnd === -1) {
      if (buffer.length - offset > MAX_HEADER_BYTES) throw new Error("Chunk size line too long");
      return null;
    }
    if (lineEnd - offset > MAX_HEADER_BYTES) throw new Error("Chunk size line too long");
    const line = buffer.subarray(offset, lineEnd).toString("latin1");
    if (!/^[0-9a-f]+(?:;[^\r\n]*)?$/i.test(line)) throw new Error("Invalid chunk size");
    const length = Number.parseInt(line.split(";", 1)[0], 16);
    if (!Number.isSafeInteger(length) || size + length > MAX_BODY_BYTES) throw new Error("Request body exceeds limit");
    offset = lineEnd + 2;
    if (length === 0) {
      const trailerStart = offset;
      for (;;) {
        const end = buffer.indexOf("\r\n", offset);
        if (end === -1) {
          if (buffer.length - trailerStart > MAX_HEADER_BYTES) throw new Error("Trailers too large");
          return null;
        }
        if (end - trailerStart > MAX_HEADER_BYTES) throw new Error("Trailers too large");
        if (end === offset) return { bodyBuffer: Buffer.concat(parts, size), consumed: end + 2 };
        const trailer = buffer.subarray(offset, end).toString("latin1");
        if (!/^[!#$%&'*+.^_`|~0-9a-z-]+:[\t\x20-\x7e\x80-\xff]*$/i.test(trailer)) throw new Error("Invalid trailer");
        offset = end + 2;
      }
    }
    if (buffer.length < offset + length + 2) return null;
    if (buffer.toString("latin1", offset + length, offset + length + 2) !== "\r\n") throw new Error("Invalid chunk terminator");
    parts.push(buffer.subarray(offset, offset + length)); size += length;
    offset += length + 2;
  }
}

function parseRequest(buffer) {
  const headerEnd = buffer.indexOf("\r\n\r\n");
  if (headerEnd === -1) {
    if (buffer.length > MAX_HEADER_BYTES) throw new Error("Request headers too large");
    return null;
  }
  if (headerEnd > MAX_HEADER_BYTES) throw new Error("Request headers too large");
  const lines = buffer.subarray(0, headerEnd).toString("latin1").split("\r\n");
  const match = /^([!#$%&'*+.^_`|~0-9a-z-]+) ([^\x00-\x20\x7f]+) HTTP\/1\.[01]$/i.exec(lines.shift());
  if (!match) throw new Error("Malformed HTTP request line");
  const headers = Object.create(null);
  for (const line of lines) {
    const field = /^([!#$%&'*+.^_`|~0-9a-z-]+):([\t\x20-\x7e\x80-\xff]*)$/i.exec(line);
    if (!field) throw new Error("Malformed HTTP header");
    const name = field[1].toLowerCase(), value = field[2].trim();
    if (headers[name] !== undefined && ["content-length", "transfer-encoding", "host"].includes(name)) throw new Error("Duplicate framing/authority header");
    headers[name] = headers[name] === undefined ? value : `${headers[name]}, ${value}`;
  }
  const transfer = headers["transfer-encoding"], lengthText = headers["content-length"];
  if (transfer !== undefined && (transfer.toLowerCase() !== "chunked" || lengthText !== undefined)) throw new Error("Unsupported or ambiguous transfer framing");
  if (lengthText !== undefined && !/^\d+$/.test(lengthText)) throw new Error("Invalid Content-Length");
  const length = Number(lengthText || 0), bodyStart = headerEnd + 4;
  if (!Number.isSafeInteger(length) || length > MAX_BODY_BYTES) throw new Error("Request body exceeds limit");
  let parsed;
  if (transfer) parsed = readChunked(buffer, bodyStart);
  else if (buffer.length >= bodyStart + length) parsed = { bodyBuffer: buffer.subarray(bodyStart, bodyStart + length), consumed: bodyStart + length };
  if (!parsed) return { waiting: true, expectContinue: (headers.expect || "").toLowerCase() === "100-continue" };
  return { method: match[1], path: match[2], headers, ...parsed };
}

function handleDecryptedHttpStream(socket, authority, clientSocket, relayToExtension, tlsState, upgrades) {
  let buffer = Buffer.alloc(0), handled = false, continued = false;
  const clientOrigin = `${clientSocket.remoteAddress}:${clientSocket.remotePort}`;
  function fail(err) {
    handled = true; buffer = Buffer.alloc(0); socket.removeListener("data", onData);
    logFailedRequest({ source: "https-request", url: `https://${authority}`, reason: err.message, phase: "request" });
    if (!socket.destroyed) socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
  }
  function onData(chunk) {
    if (handled) return;
    try {
      if (buffer.length + chunk.length > MAX_BODY_BYTES + MAX_HEADER_BYTES) throw new Error("Request wire size exceeds limit");
      buffer = Buffer.concat([buffer, chunk]);
      const request = parseRequest(buffer);
      if (!request) return;
      if (request.waiting) {
        if (request.expectContinue && !continued) { continued = true; socket.write("HTTP/1.1 100 Continue\r\n\r\n"); }
        return;
      }
      const { method, path, headers, bodyBuffer, consumed } = request;
      const target = new URL(path === "*" ? "/" : path, `https://${authority}`);
      if (target.origin !== new URL(`https://${authority}`).origin || target.username || target.password || target.hash) throw new Error("Request target differs from CONNECT authority");
      const url = target.href;
      handled = true; tlsState.phase = "request";
      socket.removeListener("data", onData);
      const head = Buffer.from(buffer.subarray(consumed)); buffer = Buffer.alloc(0);
      if (String(headers.upgrade || "").toLowerCase() === "websocket") {
        if (!upgrades || !upgrades.listenerCount("upgrade")) throw new Error("WebSocket relay unavailable");
        upgrades.emit("upgrade", { method, url: target.pathname + target.search, headers, socket,
          tunnelAuthority: authority }, socket, head);
        return;
      }
      const context = { method, url, origin: clientOrigin, requestHeaders: headers };
      if (isCorsPreflight(url, method, headers)) {
        const fields = { ...buildCorsPreflightHeaders(headers), connection: "close" };
        socket.end(`HTTP/1.1 204 No Content\r\n${Object.entries(fields).map(([k,v]) => `${k}: ${v}`).join("\r\n")}\r\n\r\n`);
        return;
      }
      const state = { started: false, ended: false, noBody: false };
      Promise.resolve(relayToExtension({ url, method, headers, bodyBuffer,
        onResponseStart(start) { state.started = true; writeTlsStreamStart(socket, start, context, state); },
        onResponseChunk(chunk) { writeTlsStreamChunk(socket, chunk, state); },
        onResponseEnd() { finishTlsStream(socket, state); },
      })).then(result => {
        if (!state.started && !result.streamed) writeTlsResult(socket, result, context);
      }).catch(err => {
        logFailedRequest({ source: "proxy-server", method, url, reason: err.message, phase: "request" });
        if (state.started) {
          if (!state.ended && !socket.destroyed) { state.ended = true; socket.end(); }
        } else if (!socket.destroyed) writeTlsResult(socket, { error: err.message, alreadyLogged: true }, context);
      });
      // HTTP replies advertise Connection: close. Do not execute a pipelined
      // second request whose response cannot be delivered on this connection.
    } catch (err) { fail(err); }
  }
  socket.on("data", onData);
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
