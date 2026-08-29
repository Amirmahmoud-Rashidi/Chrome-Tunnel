// proxy-server.js — Step 2+3: local HTTP proxy that relays requests through
// the Chrome extension (and therefore through DotVPN), now with CONNECT
// tunneling for HTTPS via a local MITM using tls-mitm.js.
//
// Two request shapes are handled:
//
// 1) Absolute-form plain HTTP requests — e.g. "GET http://example.com/foo".
//    Rare in practice (curl/git/npm/pip all issue CONNECT even for
//    http.proxy-style config), but kept since it's the simplest case and
//    some clients/environments do use it for plain http:// targets.
//
// 2) CONNECT requests — e.g. "CONNECT example.com:443 HTTP/1.1". This is
//    what curl, git, npm, and pip actually send for HTTPS targets (this
//    was confirmed by direct testing, not assumed). We terminate TLS
//    ourselves here using a certificate signed by our local CA
//    (tls-mitm.js), read the plaintext HTTP request the client sends
//    inside that tunnel, forward it to the extension via native
//    messaging exactly like case 1, and write the extension's response
//    back over the same TLS connection.
//
//    This requires the client machine to trust our local CA — see
//    tls-mitm.js and the install docs. Nothing is intercepted anywhere
//    except this 127.0.0.1 process itself.
//
// This module owns the id -> pending response map, since it's the piece
// that both sends jobs into native messaging and needs to resolve them
// when a matching response comes back.

const http = require("http");
const tls = require("tls");
const crypto = require("crypto");
const { getCertificateForHost } = require("./tls-mitm");

/**
 * @param {object} opts
 * @param {number} opts.port - local port to listen on
 * @param {(job: object) => void} opts.sendToExtension - called with
 *        {id, url, method, headers, body} to forward to the extension.
 * @returns {{ server: http.Server, handleExtensionResponse: (msg: object) => void }}
 */
function createProxyServer({ port, sendToExtension }) {
  // Map of request id -> { resolve, reject } for in-flight requests.
  // Shared between the plain-HTTP path and the CONNECT/MITM path.
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
   * error/timeout. Shared by both the plain-HTTP and MITM/CONNECT paths.
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
      }, 30000);
    });

    sendToExtension(job);
    return responsePromise;
  }

  // ---- Case 1: absolute-form plain HTTP ------------------------------------

  const server = http.createServer((req, res) => {
    const targetUrl = req.url;

    if (!/^https?:\/\//i.test(targetUrl)) {
      res.writeHead(400, { "Content-Type": "text/plain" });
      res.end(
        "This proxy expects absolute-form HTTP requests. If you're seeing " +
          "this for an https:// target, your client should be using CONNECT " +
          "instead, which is handled separately.\n"
      );
      return;
    }

    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("error", (err) => {
      console.error("[proxy-server] request stream error:", err);
    });

    req.on("end", async () => {
      const bodyBuffer = Buffer.concat(chunks);
      try {
        const result = await relayToExtension({
          url: targetUrl,
          method: req.method,
          headers: req.headers,
          bodyBuffer,
        });
        writeHttpResult(res, result);
      } catch (err) {
        console.error("[proxy-server] request failed:", err.message);
        res.writeHead(504, { "Content-Type": "text/plain" });
        res.end(`Proxy error: ${err.message}\n`);
      }
    });
  });

  function writeHttpResult(res, result) {
    if (result.error) {
      res.writeHead(502, { "Content-Type": "text/plain" });
      res.end(`Extension fetch failed: ${result.error}\n`);
      return;
    }
    const responseBody = result.body ? Buffer.from(result.body, "base64") : Buffer.alloc(0);
    const headersToSend = { ...(result.headers || {}) };
    delete headersToSend["content-length"];
    delete headersToSend["content-encoding"]; // fetch() already decoded the body
    res.writeHead(result.status || 502, headersToSend);
    res.end(responseBody);
  }

  // ---- Case 2: CONNECT -> local TLS termination (MITM) ---------------------

  server.on("connect", (req, clientSocket, head) => {
    // req.url is like "example.com:443"
    const [hostname] = req.url.split(":");
    console.error(`[proxy-server] CONNECT ${req.url}`);

    let cert;
    try {
      cert = getCertificateForHost(hostname);
    } catch (err) {
      console.error("[proxy-server] failed to generate certificate for", hostname, err);
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
      // Very common/benign: client aborts, or doesn't trust our CA yet.
      console.error(`[proxy-server] TLS error for ${hostname}:`, err.message);
    });

    // Parse the plaintext HTTP request(s) the client sends inside the
    // now-decrypted TLS stream. We do this manually with a tiny buffer
    // parser rather than spinning up a second http.Server per connection,
    // to keep this self-contained and easy to reason about.
    handleDecryptedHttpStream(tlsSocket, hostname);
  });

  function handleDecryptedHttpStream(tlsSocket, hostname) {
    let buffer = Buffer.alloc(0);

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


      relayToExtension({ url: targetUrl, method, headers, bodyBuffer })
        .then((result) => writeTlsResult(tlsSocket, result))
        .catch((err) => {
          console.error("[proxy-server] MITM request failed:", err.message);
          writeTlsResult(tlsSocket, { status: 502, error: err.message });
        });

      // If there's more pipelined data left in the buffer, try again.
      if (buffer.length > 0) {
        tryParseAndHandleRequest();
      }
    }
  }

  function writeTlsResult(tlsSocket, result) {
    if (result.error) {
      const body = `Extension fetch failed: ${result.error}\n`;
      tlsSocket.write(
        `HTTP/1.1 502 Bad Gateway\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`
      );
      tlsSocket.end();
      return;
    }

    const responseBody = result.body ? Buffer.from(result.body, "base64") : Buffer.alloc(0);
    const headers = { ...(result.headers || {}) };
    delete headers["content-length"];
    delete headers["content-encoding"];
    delete headers["transfer-encoding"];
    headers["content-length"] = String(responseBody.length);
    headers["connection"] = "close"; // keep this per-connection logic simple

    const statusLine = `HTTP/1.1 ${result.status || 502} ${result.statusText || ""}`.trimEnd();
    const headerLines = Object.entries(headers)
      .map(([k, v]) => `${k}: ${v}`)
      .join("\r\n");

    tlsSocket.write(`${statusLine}\r\n${headerLines}\r\n\r\n`);
    tlsSocket.end(responseBody);
  }

  // If a previous host.js instance just exited (e.g. after Chrome's
  // service worker died and we detected the broken pipe), Windows/Node
  // may take a brief moment to fully release the port. Retry a few times
  // with backoff instead of giving up immediately on the first
  // EADDRINUSE, which previously caused a hard crash-loop.
  let listenAttempts = 0;
  const MAX_LISTEN_ATTEMPTS = 5;

  function tryListen() {
    listenAttempts++;
    server.listen(port, "127.0.0.1");
  }

  server.on("error", (err) => {
    if (err.code === "EADDRINUSE" && listenAttempts < MAX_LISTEN_ATTEMPTS) {
      const delayMs = 500 * listenAttempts;
      console.error(
        `[proxy-server] port ${port} still in use (attempt ${listenAttempts}/${MAX_LISTEN_ATTEMPTS}), ` +
          `retrying in ${delayMs}ms...`
      );
      setTimeout(tryListen, delayMs);
    }
    // If retries are exhausted or it's some other error, let it propagate
    // to whatever error listener the caller (host.js) attaches, which
    // logs it and exits — that's still the right behavior for anything
    // that isn't a transient post-exit port hold.
  });

  server.once("listening", () => {
    console.error(`[proxy-server] listening on http://127.0.0.1:${port}`);
  });

  tryListen();

  return { server, handleExtensionResponse };
}

module.exports = { createProxyServer };
