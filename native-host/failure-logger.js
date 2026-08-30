// failure-logger.js — Logs every FAILED request (never successful ones)
// with full detail: where it came from, where it was going, what kind of
// failure, and when. Written to requests-failed.log next to this file.
//
// "Failure" here means any of:
//   - the local proxy couldn't relay the request at all (e.g. malformed
//     CONNECT, native messaging timeout, extension unreachable)
//   - the extension's fetch() itself threw (network error, DNS failure,
//     CORS-style rejection, etc — anything that never got an HTTP
//     response at all)
//   - an HTTP response WAS received, but with a 4xx or 5xx status
//
// Deliberately NOT filtered: every failure is logged, including ones
// that are "expected" or not chrometunnel's fault (e.g. a request to an
// internal cloud metadata address, a 404 from a real server) — the
// point of this log is a complete record, not a curated one.
//
// Successful requests (2xx/3xx) are never logged here — this file is
// failure-only by design, to keep it small and relevant.

const fs = require("fs");
const path = require("path");

const LOG_PATH = path.join(__dirname, "requests-failed.log");

/**
 * @param {object} entry
 * @param {string} entry.source - where the failure was detected, e.g.
 *        "proxy-server" (couldn't relay), "extension" (fetch() itself
 *        failed), "http-status" (got a response but 4xx/5xx).
 * @param {string} [entry.method] - HTTP method of the original request.
 * @param {string} [entry.url] - the target URL that was being requested.
 * @param {string} [entry.origin] - where the request came from, e.g.
 *        "client:127.0.0.1:54321" for an incoming proxy connection, or
 *        left out if not applicable.
 * @param {number} [entry.status] - HTTP status code, if one was received.
 * @param {string} [entry.reason] - human-readable failure reason/message.
 * @param {string} [entry.id] - the internal request id, for cross-
 *        referencing with other logs (host-error.log, console output).
 */
function logFailedRequest(entry) {
  const timestamp = new Date().toISOString();
  const parts = [
    `[${timestamp}]`,
    `source=${entry.source || "unknown"}`,
  ];

  if (entry.id) parts.push(`id=${entry.id}`);
  if (entry.method) parts.push(`method=${entry.method}`);
  if (entry.url) parts.push(`url=${entry.url}`);
  if (entry.origin) parts.push(`from=${entry.origin}`);
  if (entry.status !== undefined) parts.push(`status=${entry.status}`);
  if (entry.reason) parts.push(`reason=${JSON.stringify(entry.reason)}`);

  const line = parts.join(" ") + "\n";

  try {
    fs.appendFileSync(LOG_PATH, line);
  } catch (err) {
    // If we can't even write the failure log, there's nothing further
    // to do — don't let logging itself crash the proxy.
    console.error("[failure-logger] could not write to requests-failed.log:", err.message);
  }
}

module.exports = { logFailedRequest, LOG_PATH };
