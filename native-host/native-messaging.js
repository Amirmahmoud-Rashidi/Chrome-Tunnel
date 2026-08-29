// native-messaging.js — Chrome Native Messaging protocol implementation.
//
// Chrome's protocol: each message is a 4-byte little-endian length prefix,
// followed by that many bytes of UTF-8 JSON. This applies both to what
// Chrome sends us on stdin and what we send back on stdout.
//
// This module is a self-contained, custom implementation (not a library) —
// there is no built-in Node API for this framing, we build it from
// process.stdin/stdout streams ourselves.

const HEADER_LENGTH = 4;

/**
 * Wraps stdin/stdout with a simple event-based API:
 *   host.onMessage(msg => { ... })
 *   host.send(msg)
 *
 * Designed to not crash if launched without Chrome's native-messaging
 * argv (e.g. running `node host.js` by hand for testing) — reading simply
 * waits for stdin data instead of assuming Chrome-specific behavior.
 */
function createNativeMessagingHost() {
  const listeners = [];
  let inputBuffer = Buffer.alloc(0);
  let stdoutBroken = false;

  // A broken pipe (Chrome closed its end, e.g. because the service worker
  // was killed) can surface as an 'error' event on stdout asynchronously,
  // separately from any exception thrown directly by write(). Without
  // this listener, Node treats an unhandled stream error as fatal and
  // kills the whole process — which causes a crash loop: EPIPE -> process
  // dies -> Chrome relaunches host.js -> repeats, dropping any in-flight
  // responses along the way.
  process.stdout.on("error", (err) => {
    if (err && err.code === "EPIPE") {
      stdoutBroken = true;
      console.error(
        "[native-host] stdout pipe broken (Chrome disconnected); shutting down " +
          "cleanly so the port is free for the next instance Chrome launches."
      );
      process.exit(0);
    } else {
      console.error("[native-host] unexpected stdout error:", err);
    }
  });

  function onMessage(callback) {
    listeners.push(callback);
  }

  function send(messageObj) {
    if (stdoutBroken) {
      console.error("[native-host] dropping message, stdout is broken:", messageObj.id);
      return;
    }
    try {
      const json = JSON.stringify(messageObj);
      const jsonBuffer = Buffer.from(json, "utf8");
      const header = Buffer.alloc(HEADER_LENGTH);
      header.writeUInt32LE(jsonBuffer.length, 0);
      process.stdout.write(Buffer.concat([header, jsonBuffer]));
    } catch (err) {
      // Synchronous throw path — some Node versions/platforms surface
      // EPIPE here instead of via the async 'error' event above.
      console.error("[native-host] failed to write to stdout:", err.message);
      if (err && err.code === "EPIPE") {
        stdoutBroken = true;
      }
    }
  }

  function tryParseBuffer() {
    // There may be multiple complete messages queued up; drain all of them.
    while (inputBuffer.length >= HEADER_LENGTH) {
      const messageLength = inputBuffer.readUInt32LE(0);
      const totalLength = HEADER_LENGTH + messageLength;

      if (inputBuffer.length < totalLength) {
        // Haven't received the full message body yet; wait for more data.
        break;
      }

      const jsonBuffer = inputBuffer.subarray(HEADER_LENGTH, totalLength);
      inputBuffer = inputBuffer.subarray(totalLength);

      let parsed;
      try {
        parsed = JSON.parse(jsonBuffer.toString("utf8"));
      } catch (err) {
        console.error("[native-host] failed to parse incoming message:", err);
        continue;
      }

      for (const listener of listeners) {
        try {
          listener(parsed);
        } catch (err) {
          console.error("[native-host] listener threw:", err);
        }
      }
    }
  }

  process.stdin.on("data", (chunk) => {
    inputBuffer = Buffer.concat([inputBuffer, chunk]);
    tryParseBuffer();
  });

  process.stdin.on("end", () => {
    // Chrome closes stdin when the extension disconnects/browser closes.
    console.error("[native-host] stdin closed (Chrome disconnected).");
  });

  return { onMessage, send };
}

module.exports = { createNativeMessagingHost };
