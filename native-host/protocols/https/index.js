// protocols/https/index.js — CONNECT requests, e.g.
// "CONNECT example.com:443 HTTP/1.1". This is what curl, git, npm, and
// pip actually send for HTTPS targets (confirmed by direct testing, not
// assumed — see PROJECT_HISTORY.md). We terminate TLS ourselves here
// using a certificate signed by our local CA (./tls-mitm.js), read the
// plaintext HTTP request the client sends inside that tunnel, forward it
// to the extension via native messaging exactly like protocols/http/,
// and write the extension's response back over the same TLS connection.
//
// This requires the client machine to trust our local CA — see
// tls-mitm.js and the install docs. Nothing is intercepted anywhere
// except this 127.0.0.1 process itself.
//
// attach(server, { relayToExtension }) registers this handler's 'connect'
// listener on the shared http.Server owned by core/dispatcher.js.

const tls = require("tls");
const { getCertificateForHost } = require("./tls-mitm");
const { logFailedRequest } = require("../../failure-logger");
const {
  isMarketplaceCorsPreflight,
  buildCorsPreflightHeaders,
  applyMarketplaceCorsResponseHeaders,
} = require("../../core/cors");

function attach(server, { relayToExtension }) {
  server.on("connect", (req, clientSocket, head) => {
    // req.url is like "example.com:443"
    const [hostname] = req.url.split(":");
    console.error(`[protocols/https] CONNECT ${req.url}`);
    let cert;
    try {
      cert = getCertificateForHost(hostname);
    } catch (err) {
      console.error("[protocols/https] failed to generate certificate for", hostname, err);
      clientSocket.write("HTTP/1.1 502 Bad Gateway\r\n\r\n");
      clientSocket.end();
      return;
    }
    // Tell the client the tunnel is established, THEN start TLS on top of
    // this same socket. Some clients send `head` bytes already read by
    // Node's HTTP parser before the upgrade — most callers get an empty
    // buffer here for CONNECT, but we handle it just in case.
    clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");

    const tlsSocket = new tls.TLSSocket(clientSocket, {
      isServer: true,
      key: cert.keyPem,
      cert: cert.certPem,
    });
    if (head && head.length > 0) {
      tlsSocket.unshift(head);
    }
    tlsSocket.on("error", (err) => {
      // This listener covers the entire lifetime of the TLS socket, not
      // only the handshake. A client may reset an already-established TLS
      // connection after an HTTP response, so classify it as a socket error.
      console.error(`[protocols/https] TLS error for ${hostname}:`, err.message);
      logFailedRequest({
        source: "tls-socket",
        url: `https://${hostname}`,
        origin: `client:${req.socket.remoteAddress}:${req.socket.remotePort}`,
        reason: err.message,
      });
    });
    // Parse the plaintext HTTP request(s) the client sends inside the
    // now-decrypted TLS stream. We do this manually with a tiny buffer
    // parser rather than spinning up a second http.Server per connection,
    // to keep this self-contained and easy to reason about.
    handleDecryptedHttpStream(tlsSocket, hostname, clientSocket, relayToExtension);
  });
}

function handleDecryptedHttpStream(tlsSocket, hostname, clientSocket, relayToExtension) {
  let buffer = Buffer.alloc(0);
  const clientOrigin = `${clientSocket.remoteAddress}:${clientSocket.remotePort}`;

  tlsSocket.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    tryParseAndHandleRequest();
  });

  function tryParseAndHandleRequest() {
    const headerEnd = buffer.indexOf("\r\n\r\n");
    if (headerEnd === -1) return; // headers not fully received yet
    const headerText = buffer.subarray(0, headerEnd).toString("utf8");
    const lines = headerText.split("\r\n");
    const requestLine = lines[0];
    const [method, path] = requestLine.split(" ");
    const headers = {};
    for (let i = 1; i < lines.length; i++) {
      const idx = lines[i].indexOf(":");
      if (idx === -1) continue;
      const key = lines[i].slice(0, idx).trim().toLowerCase();
      const value = lines[i].slice(idx + 1).trim();
      headers[key] = value;
    }

    const contentLength = parseInt(headers["content-length"] || "0", 10);
    const bodyStart = headerEnd + 4;
    if (buffer.length - bodyStart < contentLength) {
      return; // body not fully received yet
    }

    const bodyBuffer = buffer.subarray(bodyStart, bodyStart + contentLength);
    // Remove this parsed request from the buffer in case of keep-alive
    // pipelining (rare for these CLI tools, but handled defensively).
    buffer = buffer.subarray(bodyStart + contentLength);

    const targetUrl = `https://${hostname}${path.startsWith("/") ? path : "/" + path}`;
    const context = { method, url: targetUrl, origin: clientOrigin, requestHeaders: headers };

    if (isMarketplaceCorsPreflight(targetUrl, method, headers)) {
      const responseHeaders = buildCorsPreflightHeaders(headers);
      responseHeaders["connection"] = "close";
      const headerLines = Object.entries(responseHeaders)
        .map(([k, v]) => `${k}: ${v}`)
        .join("\r\n");
      tlsSocket.end(`HTTP/1.1 204 No Content\r\n${headerLines}\r\n\r\n`);
      return;
    }

    relayToExtension({ url: targetUrl, method, headers, bodyBuffer })
      .then((result) => writeTlsResult(tlsSocket, result, context))
      .catch((err) => {
        console.error("[protocols/https] MITM request failed:", err.message);
        logFailedRequest({
          source: "proxy-server",
          method,
          url: targetUrl,
          origin: clientOrigin,
          reason: err.message,
        });
        writeTlsResult(
          tlsSocket,
          { status: 502, error: err.message, alreadyLogged: true },
          context
        );
      });
    // If there's more pipelined data left in the buffer, try again.
    if (buffer.length > 0) {
      tryParseAndHandleRequest();
    }
  }
}

function writeTlsResult(tlsSocket, result, context) {
  if (result.error) {
    // Already logged by the .catch() above when this came from a
    // relayToExtension() rejection; but if the extension itself
    // returned {error: ...} without going through the catch path,
    // log it here so it's never missed.
    if (!result.alreadyLogged) {
      logFailedRequest({
        source: "extension",
        id: result.id,
        method: context.method,
        url: context.url,
        origin: context.origin,
        reason: result.error,
      });
    }
    const body = `Extension fetch failed: ${result.error}\n`;
    tlsSocket.write(
      `HTTP/1.1 502 Bad Gateway\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`
    );
    tlsSocket.end();
    return;
  }
  if (result.status >= 400) {
    logFailedRequest({
      source: "http-status",
      id: result.id,
      method: context.method,
      url: context.url,
      origin: context.origin,
      status: result.status,
      reason: result.statusText,
    });
  }
  const responseBody = result.body ? Buffer.from(result.body, "base64") : Buffer.alloc(0);
  const headers = { ...(result.headers || {}) };
  delete headers["content-length"];
  delete headers["content-encoding"];
  delete headers["transfer-encoding"];
  applyMarketplaceCorsResponseHeaders(headers, context);
  headers["content-length"] = String(responseBody.length);
  headers["connection"] = "close"; // keep this per-connection logic simple
  const statusLine = `HTTP/1.1 ${result.status || 502} ${result.statusText || ""}`.trimEnd();
  const headerLines = Object.entries(headers)
    .map(([k, v]) => `${k}: ${v}`)
    .join("\r\n");

  tlsSocket.write(`${statusLine}\r\n${headerLines}\r\n\r\n`);
  tlsSocket.end(responseBody);
}

module.exports = { attach };
