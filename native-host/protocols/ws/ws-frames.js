// RFC 6455 framing with bounded fragmented-message reassembly.
// Compression is not negotiated on the local client connection.

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
function createFrameParser({ maxFrameBytes = MAX_PAYLOAD_BYTES, requireMasked = false } = {}) {
  let buffer = Buffer.alloc(0), fragmentOpcode = null, fragments = [], fragmentBytes = 0;
  function push(chunk) {
    if (chunk && chunk.length) buffer = Buffer.concat([buffer, chunk]);
    const frames = [];
    while (buffer.length >= 2) {
      const b0 = buffer[0], b1 = buffer[1], fin = Boolean(b0 & 0x80);
      const opcode = b0 & 0x0f, masked = Boolean(b1 & 0x80);
      if (b0 & 0x70) throw new Error("Unsupported WebSocket RSV bits");
      if (![OP_CONTINUATION, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG].includes(opcode)) throw new Error("Invalid WebSocket opcode");
      if (requireMasked && !masked) throw new Error("Client WebSocket frame must be masked");
      let size = b1 & 0x7f, offset = 2;
      if (size === 126) {
        if (buffer.length < 4) break;
        size = buffer.readUInt16BE(2); offset = 4;
        if (size > maxFrameBytes) throw new Error(`WebSocket frame payload ${size} exceeds limit ${maxFrameBytes}`);
        if (size < 126) throw new Error("Non-minimal WebSocket length");
      } else if (size === 127) {
        if (buffer.length < 10) break;
        if (buffer.readUInt32BE(2) !== 0) throw new Error("WebSocket length overflow / exceeds limit");
        size = buffer.readUInt32BE(6); offset = 10;
        if (size < 65536) throw new Error("Non-minimal WebSocket length");
      }
      if (size > maxFrameBytes) throw new Error(`WebSocket frame payload ${size} exceeds limit ${maxFrameBytes}`);
      if (opcode >= 8 && (!fin || size > 125)) throw new Error("Invalid WebSocket control frame");
      const payloadStart = offset + (masked ? 4 : 0), end = payloadStart + size;
      if (buffer.length < end) break;
      let payload = Buffer.from(buffer.subarray(payloadStart, end));
      if (masked) for (let i = 0; i < size; i++) payload[i] ^= buffer[offset + (i & 3)];
      buffer = buffer.subarray(end);
      if (opcode >= 8) {
        if (opcode === OP_CLOSE) {
          if (size === 1) throw new Error("Invalid WebSocket close payload");
          if (size >= 2 && (!validCloseCode(payload.readUInt16BE(0)) || !require("buffer").isUtf8(payload.subarray(2)))) throw new Error("Invalid WebSocket close status/reason");
        }
        frames.push({ opcode, payload });
        continue;
      }
      if (opcode === OP_CONTINUATION) {
        if (fragmentOpcode === null) throw new Error("Unexpected WebSocket continuation");
      } else {
        if (fragmentOpcode !== null) throw new Error("Missing WebSocket continuation");
        fragmentOpcode = opcode;
      }
      fragmentBytes += size;
      if (fragmentBytes > maxFrameBytes) throw new Error("WebSocket fragmented message exceeds limit");
      fragments.push(payload);
      if (fin) {
        payload = Buffer.concat(fragments, fragmentBytes);
        if (fragmentOpcode === OP_TEXT && !require("buffer").isUtf8(payload)) throw new Error("Invalid WebSocket UTF-8 text");
        frames.push({ opcode: fragmentOpcode, payload });
        fragments = []; fragmentBytes = 0; fragmentOpcode = null;
      }
    }
    return frames;
  }
  function reset() { buffer = Buffer.alloc(0); fragments = []; fragmentBytes = 0; fragmentOpcode = null; }
  return { push, reset };
}

function validCloseCode(code) {
  return Number.isInteger(code) && ((code >= 1000 && code <= 1014 && ![1004, 1005, 1006].includes(code)) || (code >= 3000 && code <= 4999));
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
  if (!validCloseCode(code)) code = 1000;
  let text = "";
  for (const char of String(reason || "")) {
    if (Buffer.byteLength(text + char, "utf8") > 123) break;
    text += char;
  }
  const bytes = Buffer.from(text, "utf8"), out = Buffer.alloc(2 + bytes.length);
  out.writeUInt16BE(code); bytes.copy(out, 2);
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
