// protocols/ws/index.js — HTTP Upgrade: websocket handler.
//
// Flow:
//   1. client opens a TCP connection through the local proxy and sends
//      "GET /chat HTTP/1.1\r\nUpgrade: websocket\r\n...".
//   2. http.Server emits an 'upgrade' event. We compute the
//      Sec-WebSocket-Accept, and ask the extension to open a real
//      WebSocket to the upstream server (this is the only thing that
//      will pick up the user's VPN/proxy extension in Chrome).
//   3. If the extension's WebSocket opens, we send "HTTP/1.1 101
//      Switching Protocols" + the extension-provided handshake headers
//      back to the client, then start relaying bytes both ways via
//      WebSocket frames.
//   4. If the extension rejects (network error, non-101 response,
//      etc.), we synthesize a 502 to the client and close the socket.
//   5. Either side can close. We map WebSocket close codes through.
//   6. Ping/pong: we transparently forward pings (extension→client and
//      client→extension) so liveness checks still work end-to-end.
//
// All client→extension traffic is masked (RFC 6455 §5.1: client→server
// frames MUST be masked). All extension→client traffic is unmasked
// (server→client MUST NOT be masked). Our parser/encoder handle that
// automatically based on which side produced the data.

const {
  createFrameParser,
  encodeFrame,
  buildClosePayload,
  computeAcceptKey,
  OP_TEXT,
  OP_BINARY,
  OP_CLOSE,
  OP_PING,
  OP_PONG,
} = require("./ws-frames");
const { logFailedRequest } = require("../../failure-logger");

// Close code used when we synthesize a 502 (the proxy never saw a real
// WebSocket close, so there is no code from the server side).
const LOCAL_CLOSE_CODE = 1011; // "server error"
const LOCAL_CLOSE_REASON = "Proxy error";

function attach(server, { relayWsOpen, relayWsControl, relayWsMessage, wsTargets }) {
  const targets = wsTargets || new Map();

  server.on("upgrade", (req, clientSocket, head) => {
    const upgradeHeader = String(req.headers.upgrade || "").toLowerCase();
    if (upgradeHeader !== "websocket") {
      // Not for us (could be HTTP/2, another protocol). Hand back.
      return;
    }

    const targetUrl = buildTargetUrl(req);
    if (!targetUrl) {
      // No usable Host header → can't construct an absolute URL.
      sendHttpErrorAndClose(clientSocket, 400, "Bad Request: missing Host");
      logFailedRequest({
        source: "ws-upgrade",
        method: "GET",
        url: "(unknown)",
        reason: "Missing Host header on WebSocket upgrade",
        phase: "handshake",
      });
      return;
    }

    // Compute the response accept key BEFORE we touch the network.
    const acceptKey = computeAcceptKey(req.headers["sec-websocket-key"]);

    // Ask the extension to open a real WebSocket to upstream.
    // relayWsOpen resolves to either { accepted, headers } or { error }.
    let settled = false;
    const cleanup = () => {
      if (settled) return;
      settled = true;
    };

    const logOrigin = `client:${clientSocket.remoteAddress}:${clientSocket.remotePort}`;

    relayWsOpen({
      url: targetUrl,
      headers: stripHopByHopHeaders(req.headers),
    })
      .then((result) => {
        if (settled) return;
        if (result.error) {
          logFailedRequest({
            source: "ws-upgrade",
            method: "GET",
            url: targetUrl,
            origin: logOrigin,
            reason: result.error,
            phase: "handshake",
          });
          sendHttpErrorAndClose(clientSocket, 502, `Bad Gateway: ${result.error}`);
          cleanup();
          return;
        }

        // Success: tell the client "101 Switching Protocols" and the
        // Sec-WebSocket-Accept so the handshake completes. After this
        // line, the TCP socket IS the WebSocket — there is no more HTTP.
        const responseHeaders = {
          Upgrade: "websocket",
          Connection: "Upgrade",
          "Sec-WebSocket-Accept": acceptKey,
          ...filterExtensionHeaders(result.headers || {}),
        };
        const headerLines = Object.entries(responseHeaders)
          .map(([k, v]) => `${k}: ${v}`)
          .join("\r\n");
        clientSocket.write(
          `HTTP/1.1 101 Switching Protocols\r\n${headerLines}\r\n\r\n`
        );

        // Any bytes the client already pushed after the request line
        // (the 'head' buffer) belong to the WebSocket stream now.
        const clientParser = createFrameParser();
        const ws = {
          remoteClosed: false,
          clientClosed: false,
          closeSent: false,
        };

        // Register the client socket so inbound (extension → server →
        // client) frames get routed here.
        targets.set(result.id, {
          socket: clientSocket,
          closeSent: false,
        });

        const closeFromRemote = (code, reason) => {
          if (ws.closeSent) return;
          ws.closeSent = true;
          if (!clientSocket.destroyed) {
            clientSocket.end(
              encodeFrame({
                opcode: OP_CLOSE,
                payload: buildClosePayload(code, reason),
              })
            );
          }
        };

        // --- Client → Extension ---
        clientSocket.on("data", (chunk) => {
          if (ws.clientClosed) return;
          const frames = clientParser.push(chunk);
          for (const frame of frames) {
            if (frame.opcode === OP_CLOSE) {
              ws.clientClosed = true;
              const { code, reason } = parseClosePayload(frame.payload);
              relayWsControl({
                id: result.id,
                wsClose: { code, reason },
              }).catch(() => {
                // best-effort
              });
              closeFromRemote(code, reason);
              return;
            }
            if (frame.opcode === OP_PING) {
              // Reply with a pong carrying the same payload.
              if (!clientSocket.destroyed) {
                clientSocket.write(
                  encodeFrame({ opcode: OP_PONG, payload: frame.payload })
                );
              }
              // Also forward the ping upstream so it still serves as a
              // liveness probe end-to-end.
              relayWsMessage({
                id: result.id,
                wsSend: {
                  payload: frame.payload.toString("base64"),
                  isBinary: true,
                  kind: "ping",
                },
              }).catch(() => {});
              continue;
            }
            if (frame.opcode === OP_PONG) {
              // Nothing to do (we don't initiate pings at this layer).
              continue;
            }
            if (frame.opcode === OP_TEXT || frame.opcode === OP_BINARY) {
              relayWsMessage({
                id: result.id,
                wsSend: {
                  payload: frame.payload.toString("base64"),
                  isBinary: frame.opcode === OP_BINARY,
                  kind: "message",
                },
              }).catch(() => {});
              continue;
            }
            // Continuation / reserved opcodes are not generated by us
            // (we set FIN=1 on every send). Silently drop.
          }
        });

        const onClientError = (err) => {
          console.error(`[protocols/ws] client socket error for ${targetUrl}:`, err.message);
          logFailedRequest({
            source: "ws-socket",
            url: targetUrl,
            origin: logOrigin,
            reason: err.message,
            phase: ws.clientClosed ? "closed" : "open",
          });
          relayWsControl({
            id: result.id,
            wsClose: { code: 1011, reason: "Client socket error" },
          }).catch(() => {});
        };
        clientSocket.on("error", onClientError);

        const onClientEnd = () => {
          if (ws.closeSent) return;
          ws.closeSent = true;
          if (!ws.clientClosed) {
            // TCP half-close from the client without a WS close frame.
            // Tell the extension to close so the upstream sees a clean
            // shutdown.
            relayWsControl({
              id: result.id,
              wsClose: { code: 1006, reason: "Client disconnected" },
            }).catch(() => {});
          }
        };
        clientSocket.on("end", onClientEnd);
        clientSocket.on("close", () => {
          targets.delete(result.id);
          cleanup();
        });

        // Initial bytes the client had already pre-sent (if any).
        if (head && head.length > 0 && !clientSocket.destroyed) {
          clientSocket.emit("data", head);
        }
      })
      .catch((err) => {
        if (settled) return;
        console.error(`[protocols/ws] extension handshake failed for ${targetUrl}:`, err.message);
        logFailedRequest({
          source: "ws-upgrade",
          method: "GET",
          url: targetUrl,
          origin: logOrigin,
          reason: err.message,
          phase: "handshake",
        });
        sendHttpErrorAndClose(clientSocket, 504, `Gateway Timeout: ${err.message}`);
        cleanup();
      });

    // If the client closes before the extension responds, propagate.
    clientSocket.once("close", () => {
      if (!settled) {
        // We never wrote a 101; nothing to clean up. The relay entry
        // will time out on its own and clear the pending response.
        settled = true;
      }
    });
  });
}

// --- Inbound messages from the extension (extension → client) ----------------
//
// The dispatcher calls these once the extension has finished its
// WebSocket lifecycle. We look up the registered client socket by id
// and write the framed bytes back to it (or close it).

function handleExtensionWsMessage(targets, id, payload, isBinary) {
  const t = targets && targets.get(id);
  if (!t || t.socket.destroyed) return;
  const opcode = isBinary ? OP_BINARY : OP_TEXT;
  t.socket.write(encodeFrame({ opcode, payload }));
}

function handleExtensionWsClose(targets, id, code, reason) {
  const t = targets && targets.get(id);
  if (!t) return;
  if (!t.closeSent) {
    t.closeSent = true;
    if (!t.socket.destroyed) {
      t.socket.end(
        encodeFrame({
          opcode: OP_CLOSE,
          payload: buildClosePayload(code, reason),
        })
      );
    }
  }
  if (targets) targets.delete(id);
}

// --- helpers ----------------------------------------------------------------

function buildTargetUrl(req) {
  // req.url is the path+query ("/chat?room=42"). req.headers.host is
  // "host:port" (no scheme). We default to ws:// because this proxy
  // receives plain HTTP-upgrade requests; the extension's WebSocket
  // can be opened as ws:// or wss:// depending on what the extension
  // decides, but the typical case is HTTPS targets (clients configured
  // HTTPS_PROXY will hit us via CONNECT-tunneled wss://).
  //
  // To keep things simple and consistent with the rest of chrometunnel,
  // we treat the inbound Host as if it were already HTTPS — because in
  // practice, WebSocket clients (browsers, VS Code, copilot chat, etc.)
  // all open wss:// against the public server. The TLS handshake (if
  // any) happens entirely inside the extension's WebSocket, so the
  // client never has to know.
  const host = String(req.headers.host || "").trim();
  if (!host) return null;
  const pathAndQuery = req.url && req.url.startsWith("/") ? req.url : `/${req.url || ""}`;
  return `wss://${host}${pathAndQuery}`;
}

function stripHopByHopHeaders(headers) {
  // Per RFC 7230 §6.1, hop-by-hop headers must not be forwarded by a
  // proxy. We forward everything else verbatim.
  const out = { ...headers };
  const hopByHop = [
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
  ];
  for (const k of hopByHop) delete out[k];
  return out;
}

function filterExtensionHeaders(headers) {
  // The extension returns the response headers the upstream WebSocket
  // handshake sent back. Some of these are hop-by-hop or otherwise
  // unsafe to forward to a different client (different Sec-WebSocket-*
  // values, etc). We keep only the ones that make sense to relay.
  const out = {};
  const allow = new Set([
    "sec-websocket-protocol",
    "sec-websocket-extensions",
    "set-cookie",
  ]);
  for (const [k, v] of Object.entries(headers || {})) {
    if (allow.has(k.toLowerCase())) out[k] = v;
  }
  return out;
}

function parseClosePayload(payload) {
  if (!payload || payload.length < 2) {
    return { code: 1005, reason: "" }; // "no status received"
  }
  const code = payload.readUInt16BE(0);
  const reason = payload.subarray(2).toString("utf8");
  return { code, reason };
}

function sendHttpErrorAndClose(socket, status, message) {
  if (socket.destroyed) return;
  const body = `${message}\n`;
  const headers = [
    `HTTP/1.1 ${status} ${statusText(status)}`,
    "Content-Type: text/plain; charset=utf-8",
    `Content-Length: ${Buffer.byteLength(body)}`,
    "Connection: close",
    "",
    "",
  ].join("\r\n");
  socket.end(headers + body);
}

function statusText(status) {
  return (
    {
      400: "Bad Request",
      502: "Bad Gateway",
      504: "Gateway Timeout",
    }[status] || "Error"
  );
}

module.exports = {
  attach,
  handleExtensionWsMessage,
  handleExtensionWsClose,
  LOCAL_CLOSE_CODE,
  LOCAL_CLOSE_REASON,
};
