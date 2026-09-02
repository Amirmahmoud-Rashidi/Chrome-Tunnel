// ws-frames.js — minimal RFC 6455 WebSocket frame parser/encoder.
//
// Scope: just enough to relay WebSocket traffic between a client speaking
// to this proxy and the extension's WebSocket (which speaks to the real
// server through Chrome's network stack). We do NOT implement:
//   - per-message deflate (permessage-deflate) — extension/per-server
//     negotiation only, not in our minimal scope. If the client requests
//     it, we drop that extension from the response and continue.
//   - fragmentation reassembly beyond a single buffered frame
//   - 16 MB > payloads (caller chunks). WebSocket base framing supports
//     up to 2^63, but a 1MB cap is plenty for our use case and keeps
//     the chunked-handshake with the extension simple.
//
// Frame format (RFC 6455 §5):
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//  +-+-+-+-+-------+-+-------------+-------------------------------+
//  |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
//  |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
//  |N|V|V|V|       |S|             |   (if payload len==126/127)   |
//  | |1|2|3|       |K|             |                               |
//  +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
//  |     Extended payload length continued, if payload len == 127  |
//  + - - - - - - - - - - - - - - - +-------------------------------+
//  |                               |Masking-key, if MASK set to 1  |
//  +-------------------------------+-------------------------------+
//  | Masking-key (continued)       |          Payload Data         |
//  +-------------------------------- - - - - - - - - - - - - - - - +
//  :                     Payload Data continued ...                :
//  + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
//  |                     Payload Data continued ...                |
//  +---------------------------------------------------------------+

const crypto = require("crypto");

// Opcodes (RFC 6455 §5.2)
const OP_CONTINUATION = 0x0;
const OP_TEXT = 0x1;
const OP_BINARY = 0x2;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xa;

const MAX_PAYLOAD_BYTES = 1 * 1024 * 1024; // 1 MiB hard cap per frame

/**
 * Stateful parser. Feed raw bytes via push() — it returns an array of
 * complete frames (each as a Buffer) and holds any partial data
 * internally until the rest of the frame arrives.
 */
function createFrameParser({ maxFrameBytes = MAX_PAYLOAD_BYTES } = {}) {
  let buffer = Buffer.alloc(0);

  function push(chunk) {
    if (chunk && chunk.length > 0) {
      buffer = Buffer.concat([buffer, chunk]);
    }
    const frames = [];
    while (tryExtractFrame(frames, maxFrameBytes)) {
      // loop
    }
    return frames;
  }

  function tryExtractFrame(frames, limit) {
    if (buffer.length < 2) return false;

    const b0 = buffer[0];
    const b1 = buffer[1];
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let payloadLen = b1 & 0x7f;
    let headerLen = 2;

    if (payloadLen === 126) {
      if (buffer.length < 4) return false;
      payloadLen = buffer.readUInt16BE(2);
      headerLen = 4;
    } else if (payloadLen === 127) {
      if (buffer.length < 10) return false;
      // High bit must be 0 (RFC 6455 §5.2)
      if (buffer[2] !== 0 || buffer[3] !== 0) {
        throw new Error("WebSocket frame payload length overflow (MSB set).");
      }
      const lo = buffer.readUInt32BE(6);
      payloadLen = lo;
      headerLen = 10;
    }

    if (payloadLen > limit) {
      throw new Error(
        `WebSocket frame payload ${payloadLen} exceeds limit ${limit}.`
      );
    }

    let maskStart = headerLen;
    let payloadStart = headerLen;
    if (masked) {
      payloadStart = headerLen + 4;
    }
    const total = payloadStart + payloadLen;
    if (buffer.length < total) return false;

    let payload = buffer.subarray(payloadStart, total);
    if (masked) {
      const mask = buffer.subarray(maskStart, maskStart + 4);
      // XOR each byte in place
      const out = Buffer.alloc(payload.length);
      for (let i = 0; i < payload.length; i++) {
        out[i] = payload[i] ^ mask[i & 3];
      }
      payload = out;
    }

    frames.push({ opcode, payload });
    buffer = buffer.subarray(total);
    return true;
  }

  function reset() {
    buffer = Buffer.alloc(0);
  }

  return { push, reset };
}

/**
 * Encode a single WebSocket frame. Server→client frames MUST NOT be
 * masked per RFC 6455 §5.1; we follow that.
 *
 * @param {object} frame
 * @param {number} frame.opcode - one of OP_* constants.
 * @param {Buffer} frame.payload - already-assembled payload bytes.
 * @param {boolean} [frame.mask] - mask the frame (used for client→server
 *        test cases; real traffic to clients must leave this false).
 * @param {Buffer} [frame.maskKey] - 4-byte mask; random if omitted.
 */
function encodeFrame({ opcode, payload, mask = false, maskKey }) {
  if (!Buffer.isBuffer(payload)) {
    payload = Buffer.from(payload || "");
  }

  let header;
  const b0 = 0x80 | (opcode & 0x0f); // FIN=1 (we never fragment)
  let b1 = 0;

  if (mask) {
    b1 |= 0x80;
    maskKey = maskKey || crypto.randomBytes(4);
  }

  const len = payload.length;
  let extra;
  if (len < 126) {
    b1 |= len;
    extra = Buffer.alloc(0);
  } else if (len < 0x10000) {
    b1 |= 126;
    extra = Buffer.alloc(2);
    extra.writeUInt16BE(len, 0);
  } else {
    b1 |= 127;
    extra = Buffer.alloc(8);
    extra.writeUInt32BE(0, 0); // high 32 bits (must be 0)
    extra.writeUInt32BE(len, 4);
  }

  header = Buffer.from([b0, b1]);
  const all = [header, extra];
  if (mask) all.push(maskKey);
  if (mask && payload.length > 0) {
    const masked = Buffer.alloc(payload.length);
    for (let i = 0; i < payload.length; i++) {
      masked[i] = payload[i] ^ maskKey[i & 3];
    }
    all.push(masked);
  } else {
    all.push(payload);
  }
  return Buffer.concat(all);
}

/**
 * Build a server-side close frame payload (2-byte code + UTF-8 reason).
 */
function buildClosePayload(code, reason) {
  if (typeof code !== "number" || !Number.isInteger(code)) code = 1000;
  const reasonBuf = Buffer.from(String(reason || ""), "utf8");
  const out = Buffer.alloc(2 + reasonBuf.length);
  out.writeUInt16BE(code, 0);
  reasonBuf.copy(out, 2);
  return out;
}

/**
 * Compute the Sec-WebSocket-Accept value for a given
 * Sec-WebSocket-Key. RFC 6455 §1.3.
 */
function computeAcceptKey(secWebSocketKey) {
  const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
  return crypto
    .createHash("sha1")
    .update(String(secWebSocketKey || "") + GUID)
    .digest("base64");
}

module.exports = {
  OP_CONTINUATION,
  OP_TEXT,
  OP_BINARY,
  OP_CLOSE,
  OP_PING,
  OP_PONG,
  MAX_PAYLOAD_BYTES,
  createFrameParser,
  encodeFrame,
  buildClosePayload,
  computeAcceptKey,
};
