// Remove hop-by-hop fields before crossing the native boundary. Input is immutable.
function stripHopByHop(headers = {}) {
  const hop = new Set(["connection", "proxy-connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailer", "trailers", "transfer-encoding", "upgrade", "host", "content-length"]);
  for (const [name, value] of Object.entries(headers)) {
    if (name.toLowerCase() === "connection") {
      for (const token of String(value).split(",")) hop.add(token.trim().toLowerCase());
    }
  }
  return Object.fromEntries(Object.entries(headers).filter(([name]) => !hop.has(name.toLowerCase())));
}
module.exports = { stripHopByHop };
