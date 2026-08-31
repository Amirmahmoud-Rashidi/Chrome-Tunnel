// core/dispatcher.js — the single entry point clients connect to.
//
// This module owns the listening socket and the port-retry/backoff
// housekeeping, and nothing about any specific protocol. Its only job is:
//   1. Accept the connection.
//   2. Work out what kind of request this is (today: only "http", which
//      covers both plain absolute-form HTTP and CONNECT/HTTPS — both
//      arrive over the same http.Server; tomorrow: something else, like
//      a raw WebSocket upgrade, detected the same way).
//   3. Hand it to the matching protocol module in protocols/<name>/.
//
// Adding support for a new request type later means adding a new
// protocols/<name>/ folder and registering it below — this file and
// everything in extension/ stay untouched.

const http = require("http");
const { createRelay } = require("./relay");
const httpProtocol = require("../protocols/http");
const httpsProtocol = require("../protocols/https");

/**
 * @param {object} opts
 * @param {number} opts.port - local port to listen on
 * @param {(job: object) => void} opts.sendToExtension - called with
 *        {id, url, method, headers, body} to forward to the extension.
 * @returns {{ server: http.Server, handleExtensionResponse: (msg: object) => void }}
 */
function createDispatcher({ port, sendToExtension }) {
  const { relayToExtension, handleExtensionResponse } = createRelay({ sendToExtension });

  // The underlying transport is always a plain http.Server: this is what
  // gives us both "regular" request events (absolute-form HTTP) and
  // "connect" events (CONNECT, i.e. what curl/git/npm/pip/VS Code send
  // for HTTPS targets) on the same listening socket, with no protocol
  // detection of our own required at the TCP level. Each protocol module
  // registers the event(s) it cares about.
  const server = http.createServer();

  httpProtocol.attach(server, { relayToExtension });
  httpsProtocol.attach(server, { relayToExtension });

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
        `[dispatcher] port ${port} still in use (attempt ${listenAttempts}/${MAX_LISTEN_ATTEMPTS}), ` +
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
    console.error(`[dispatcher] listening on http://127.0.0.1:${port}`);
  });

  tryListen();

  return { server, handleExtensionResponse };
}

module.exports = { createDispatcher };
