#!/usr/bin/env node
// host.js — Step 2 of the rollout plan: native messaging + local HTTP proxy.
//
// Two jobs running in this one process:
//   1. Native messaging host (unchanged from step 1) — talks to the Chrome
//      extension over stdin/stdout.
//   2. HTTP proxy server (proxy-server.js) — listens on 127.0.0.1:PORT for
//      curl/git/npm/pip/VS Code, forwards each request to the extension via
//      native messaging, and returns the extension's fetch() result.
//
// The {ping: true} test message from step 1 still works, for regression
// testing the native-messaging link on its own.
//
// Manual test for THIS step (after loading extension + running install.ps1):
//   curl -x http://127.0.0.1:8765 https://example.com
//
// PORT can be overridden: PROXY_PORT=9999 node host.js
//
// IMPORTANT — logging: when Chrome launches this process via native
// messaging, its stdout/stdin are the binary-framed channel to the
// extension and stderr is invisible (Chrome doesn't show it anywhere).
// So any crash here is otherwise silent — the extension just sees
// "Native host has exited" with no explanation. To make debugging
// possible, every startup error and uncaught exception is also written
// to host-error.log next to this script.

const fs = require("fs");
const path = require("path");

const LOG_PATH = path.join(__dirname, "host-error.log");

function logToFile(label, err) {
  const line = `[${new Date().toISOString()}] ${label}: ${err && err.stack ? err.stack : err}\n`;
  try {
    fs.appendFileSync(LOG_PATH, line);
  } catch (writeErr) {
    // Nothing more we can do if even the log file can't be written.
  }
}

process.on("uncaughtException", (err) => {
  logToFile("uncaughtException", err);
  process.exit(1);
});

const { createNativeMessagingHost } = require("./native-messaging");
const { createProxyServer } = require("./proxy-server");

const PORT = parseInt(process.env.PROXY_PORT || "8765", 10);

const host = createNativeMessagingHost();

let proxyHandle;
try {
  proxyHandle = createProxyServer({
    port: PORT,
    sendToExtension: (job) => {
      console.error("[host.js] forwarding job to extension:", job.id, job.method, job.url);
      host.send(job);
    },
  });
} catch (err) {
  logToFile("createProxyServer threw synchronously", err);
  throw err;
}

// createProxyServer's server.listen() can fail asynchronously (e.g. the
// port is already in use by a leftover process that hasn't fully exited
// yet). proxy-server.js retries a few times internally with backoff for
// exactly this case. This handler logs every attempt's error and only
// gives up (exits) once retries are exhausted — determined by checking
// whether the server ever successfully starts listening.
let hasStartedListening = false;
proxyHandle.server.once("listening", () => {
  hasStartedListening = true;
});

proxyHandle.server.on("error", (err) => {
  logToFile(`proxy server error (port ${PORT})`, err);
  if (err.code === "EADDRINUSE") {
    console.error(`[host.js] port ${PORT} in use, proxy-server.js will retry automatically.`);
    // Give proxy-server.js's internal retry loop a chance; only exit if,
    // after a generous window, we still never reached "listening".
    setTimeout(() => {
      if (!hasStartedListening) {
        logToFile(`proxy server error (port ${PORT})`, new Error("Exhausted retries, giving up."));
        console.error(`[host.js] giving up on port ${PORT} after retries exhausted.`);
        process.exit(1);
      }
    }, 4000);
    return;
  }
  // Any other error is not something we know how to recover from.
  process.exit(1);
});

const { handleExtensionResponse } = proxyHandle;

console.error("[host.js] native messaging host started, waiting for messages...");

host.onMessage((message) => {
  if (message && message.ping) {
    console.error("[host.js] received ping:", message.id);
    host.send({ id: message.id, pong: true, time: Date.now() });
    return;
  }

  // Everything else is assumed to be a proxy job response
  // ({id, status, headers, body} or {id, error}) coming back from the
  // extension's fetch() call.
  handleExtensionResponse(message);
});
