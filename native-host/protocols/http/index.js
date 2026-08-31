// protocols/http/index.js — absolute-form plain HTTP requests, e.g.
// "GET http://example.com/foo".
//
// Rare in practice (curl/git/npm/pip all issue CONNECT even for
// http.proxy-style config — see protocols/https/), but kept since it's
// the simplest case and some clients/environments do use it for plain
// http:// targets.
//
// attach(server, { relayToExtension }) registers this handler's request
// listener on the shared http.Server owned by core/dispatcher.js. It
// only claims requests that look like absolute-form HTTP; anything else
// (CONNECT) is left for protocols/https/ to handle via its own 'connect'
// listener on the same server.

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

    // VS Code's browser-side Marketplace client performs CORS preflights.
    // The Marketplace endpoint itself returns 404 for those OPTIONS calls,
    // so terminate only genuine Marketplace preflights locally. This lets
    // VS Code proceed to the real GET/POST, which is then relayed normally.
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
      try {
        const result = await relayToExtension({
          url: targetUrl,
          method: req.method,
          headers: req.headers,
          bodyBuffer,
        });
        writeHttpResult(res, result, context);
      } catch (err) {
        console.error("[protocols/http] request failed:", err.message);
        logFailedRequest({
          source: "proxy-server",
          method: req.method,
          url: targetUrl,
          origin: clientOrigin,
          reason: err.message,
        });
        res.writeHead(504, { "Content-Type": "text/plain" });
        res.end(`Proxy error: ${err.message}\n`);
      }
    });
  });
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
  const headersToSend = { ...(result.headers || {}) };
  delete headersToSend["content-length"];
  delete headersToSend["content-encoding"]; // fetch() already decoded the body
  applyMarketplaceCorsResponseHeaders(headersToSend, context);
  res.writeHead(result.status || 502, headersToSend);
  res.end(responseBody);
}

module.exports = { attach };
