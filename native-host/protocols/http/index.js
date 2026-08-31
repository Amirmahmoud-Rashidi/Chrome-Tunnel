// protocols/http/index.js — absolute-form plain HTTP requests.
const { logFailedRequest } = require("../../failure-logger");
const {
  isMarketplaceCorsPreflight,
  buildCorsPreflightHeaders,
  applyMarketplaceCorsResponseHeaders,
} = require("../../core/cors");

function attach(server, { relayToExtension }) {
  server.on("request", (req, res) => {
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

    if (isMarketplaceCorsPreflight(targetUrl, req.method, req.headers)) {
      req.resume();
      res.writeHead(204, buildCorsPreflightHeaders(req.headers));
      res.end();
      return;
    }

    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("error", (err) => {
      console.error("[protocols/http] request stream error:", err);
    });

    req.on("end", async () => {
      const bodyBuffer = Buffer.concat(chunks);
      const clientOrigin = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
      const context = {
        method: req.method,
        url: targetUrl,
        origin: clientOrigin,
        requestHeaders: req.headers,
      };

      let streamed = false;

      try {
        const result = await relayToExtension({
          url: targetUrl,
          method: req.method,
          headers: req.headers,
          bodyBuffer,
          onResponseStart: (start) => {
            streamed = true;
            writeHttpStreamStart(res, start, context);
          },
          onResponseChunk: (chunk) => {
            if (!res.destroyed && !res.writableEnded && chunk.length > 0) {
              res.write(chunk);
            }
          },
          onResponseEnd: () => {
            if (!res.destroyed && !res.writableEnded) res.end();
          },
        });

        // Backward compatibility with the original one-message protocol.
        if (!streamed && !result.streamed) {
          writeHttpResult(res, result, context);
        }
      } catch (err) {
        console.error("[protocols/http] request failed:", err.message);
        logFailedRequest({
          source: "proxy-server",
          method: req.method,
          url: targetUrl,
          origin: clientOrigin,
          reason: err.message,
        });

        if (!res.headersSent) {
          res.writeHead(504, { "Content-Type": "text/plain" });
          res.end(`Proxy error: ${err.message}\n`);
        } else if (!res.destroyed) {
          // Headers/body may already have been streamed. We cannot replace the
          // HTTP status now; closing the incomplete stream correctly tells the
          // client that the transfer did not finish successfully.
          res.destroy(err);
        }
      }
    });
  });
}

function sanitizeStreamHeaders(headers, context) {
  const headersToSend = { ...(headers || {}) };
  delete headersToSend["content-length"];
  delete headersToSend["content-encoding"]; // browser fetch already decoded it
  delete headersToSend["transfer-encoding"];
  delete headersToSend["connection"];
  applyMarketplaceCorsResponseHeaders(headersToSend, context);
  return headersToSend;
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
    });
  }
}

function writeHttpStreamStart(res, start, context) {
  logHttpStatus(start, context);
  const headersToSend = sanitizeStreamHeaders(start.headers, context);
  res.writeHead(start.status || 502, headersToSend);
  // Push response headers immediately so streaming clients do not wait for
  // the first/body-final chunk before they consider the response started.
  if (typeof res.flushHeaders === "function") res.flushHeaders();
  if (res.socket && typeof res.socket.setNoDelay === "function") {
    res.socket.setNoDelay(true);
  }
}

function writeHttpResult(res, result, context) {
  if (result.error) {
    logFailedRequest({
      source: "extension",
      id: result.id,
      method: context.method,
      url: context.url,
      origin: context.origin,
      reason: result.error,
    });
    res.writeHead(502, { "Content-Type": "text/plain" });
    res.end(`Extension fetch failed: ${result.error}\n`);
    return;
  }

  logHttpStatus(result, context);
  const responseBody = result.body
    ? Buffer.from(result.body, "base64")
    : Buffer.alloc(0);
  const headersToSend = sanitizeStreamHeaders(result.headers, context);
  res.writeHead(result.status || 502, headersToSend);
  res.end(responseBody);
}

module.exports = { attach };
