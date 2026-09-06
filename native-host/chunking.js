// Native host -> Chrome is limited to 1 MiB per serialized UTF-8 message.
// Chrome -> host permits 64 MiB; both ends use the conservative chunk budget.
// Keep the wire helpers in sync with extension/background.js.
const CHUNK_THRESHOLD_BYTES = 800 * 1024;
const CHUNK_SIZE_BYTES = 700 * 1024;
const MAX_MESSAGE_BYTES = 64 * 1024 * 1024;
const MAX_CHUNKS = 512;

// Budget the serialized string, including JSON escaping. Envelope overhead
// remains well below the headroom between 700 KiB and Chrome's 1 MiB limit.
function splitNativeMessage(message, prefix, byteLength) {
  const json = JSON.stringify(message);
  if (byteLength(json) > MAX_MESSAGE_BYTES) throw new Error("Native message exceeds 64 MiB reassembly limit");
  if (byteLength(json) <= CHUNK_THRESHOLD_BYTES) return [message];
  const chunkId = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const parts = [];
  for (let start = 0; start < json.length;) {
    let lo = 1, hi = Math.min(CHUNK_SIZE_BYTES, json.length - start), count = 0;
    while (lo <= hi) {
      const mid = Math.floor((lo + hi) / 2);
      if (byteLength(JSON.stringify(json.slice(start, start + mid))) <= CHUNK_SIZE_BYTES) {
        count = mid; lo = mid + 1;
      } else hi = mid - 1;
    }
    parts.push(json.slice(start, start + count));
    start += count;
  }
  if (parts.length > MAX_CHUNKS) throw new Error("Too many native message chunks");
  return parts.map((data, seq) => ({ chunkId, seq, total: parts.length, data }));
}

function createBoundedReassembler(byteLength) {
  const buffers = new Map();
  let bufferedBytes = 0;
  function drop(id) {
    const entry = buffers.get(id);
    if (entry) bufferedBytes -= entry.bytes;
    buffers.delete(id);
  }
  function handle(message) {
    if (!message || !Object.prototype.hasOwnProperty.call(message, "chunkId")) return message;
    const { chunkId, seq, total, data } = message;
    if (typeof chunkId !== "string" || !chunkId || chunkId.length > 200 ||
        !Number.isInteger(total) || total < 1 || total > MAX_CHUNKS ||
        !Number.isInteger(seq) || seq < 0 || seq >= total || typeof data !== "string") {
      drop(chunkId); return null;
    }
    const size = byteLength(data);
    if (size > 1024 * 1024) { drop(chunkId); return null; }
    let entry = buffers.get(chunkId);
    if (entry && entry.parts.length !== total) { drop(chunkId); return null; }
    if (!entry) {
      if (buffers.size >= 50) drop(buffers.keys().next().value);
      entry = { parts: new Array(total).fill(null), count: 0, bytes: 0 };
      buffers.set(chunkId, entry);
    }
    if (entry.parts[seq] !== null) {
      if (entry.parts[seq] !== data) drop(chunkId);
      return null;
    }
    if (bufferedBytes + size > MAX_MESSAGE_BYTES) { drop(chunkId); return null; }
    entry.parts[seq] = data; entry.count++; entry.bytes += size; bufferedBytes += size;
    if (entry.count !== total) return null;
    drop(chunkId);
    try { return JSON.parse(entry.parts.join("")); } catch { return null; }
  }
  return { handle };
}

function sendChunked(send, message, prefix = "host") {
  for (const part of splitNativeMessage(message, prefix, text => Buffer.byteLength(text, "utf8"))) send(part);
}
function createChunkReassembler() {
  return createBoundedReassembler(text => Buffer.byteLength(text, "utf8"));
}
module.exports = { sendChunked, createChunkReassembler, CHUNK_THRESHOLD_BYTES, CHUNK_SIZE_BYTES };
