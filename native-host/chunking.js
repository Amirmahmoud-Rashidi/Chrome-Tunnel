// chunking.js — Splits/reassembles large native-messaging payloads.
//
// Chrome enforces an undocumented ~1MB cap on a single native-messaging
// message. Sending (or receiving) anything larger causes the port to
// silently disconnect ("Error when communicating with the native
// messaging host"). This showed up in practice whenever VS Code /
// Marketplace / Copilot needed a response bigger than that (a VSIX
// package, a large API response, etc) — the extension's fetch() would
// succeed, but forwarding the result back through native messaging would
// kill the connection before the client ever saw it.
//
// The fix: any message larger than CHUNK_THRESHOLD_BYTES is split into
// multiple {chunkId, seq, total, data} parts and reassembled on the
// other end. This mirrors the identical logic in extension/background.js
// — the two sides can't share a literal file (one runs in Node, the
// other in a Chrome service worker), so the protocol is kept deliberately
// simple and duplicated rather than shared.

const CHUNK_THRESHOLD_BYTES = 800 * 1024;
const CHUNK_SIZE_BYTES = 700 * 1024;

/**
 * Sends `message` via `send(msg)`, transparently splitting it into
 * chunks first if it's too large for a single native-messaging payload.
 */
function sendChunked(send, message, chunkIdPrefix = "host") {
  const json = JSON.stringify(message);

  if (json.length <= CHUNK_THRESHOLD_BYTES) {
    send(message);
    return;
  }

  const chunkId = `${chunkIdPrefix}-${message.id || Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const total = Math.ceil(json.length / CHUNK_SIZE_BYTES);
  console.error(`[chunking] message for ${chunkId} is ${json.length} bytes, splitting into ${total} chunks`);

  for (let seq = 0; seq < total; seq++) {
    const data = json.slice(seq * CHUNK_SIZE_BYTES, (seq + 1) * CHUNK_SIZE_BYTES);
    send({ chunkId, seq, total, data });
  }
}

/**
 * Creates a reassembler: call `.handle(message)` for every incoming
 * message. Returns the reassembled JSON-parsed object once all chunks of
 * a chunked message have arrived, or the message itself unchanged if it
 * wasn't chunked. Returns null while still waiting for more chunks.
 */
function createChunkReassembler() {
  const buffers = new Map();
  // Defensive cap: if chunks for some chunkId never complete (a bug, a
  // dropped connection mid-transfer, etc), the buffer for it stays in
  // this Map forever. Capping the number of concurrent in-progress
  // reassemblies bounds worst-case memory growth from that scenario.
  const MAX_INCOMPLETE_BUFFERS = 50;

  function handle(message) {
    if (!message || !message.chunkId) {
      return message; // not chunked, pass through as-is
    }

    const { chunkId, seq, total, data } = message;
    let buf = buffers.get(chunkId);
    if (!buf) {
      if (buffers.size >= MAX_INCOMPLETE_BUFFERS) {
        const oldestKey = buffers.keys().next().value;
        buffers.delete(oldestKey);
        console.error(
          `[chunking] too many incomplete reassembly buffers, dropping oldest (${oldestKey}) to make room for ${chunkId}`
        );
      }
      buf = new Array(total).fill(null);
      buffers.set(chunkId, buf);
    }
    buf[seq] = data;

    if (buf.every((part) => part !== null)) {
      buffers.delete(chunkId);
      try {
        return JSON.parse(buf.join(""));
      } catch (err) {
        console.error("[chunking] failed to reassemble chunked message:", chunkId, err);
        return null;
      }
    }

    return null; // still waiting for more chunks
  }

  return { handle };
}

module.exports = { sendChunked, createChunkReassembler, CHUNK_THRESHOLD_BYTES, CHUNK_SIZE_BYTES };
