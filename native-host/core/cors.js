// core/cors.js — CORS preflight short-circuit, shared by the plain-HTTP
// and HTTPS/MITM protocol handlers.
//
// Background: VS Code's Marketplace client (and, it turns out, related
// asset hosts it talks to — see below) issues an OPTIONS preflight
// before its real GET/POST. These endpoints return 404/400 for a bare
// OPTIONS (they don't implement CORS preflight handling the way a
// browser expects), so we answer preflights locally instead of relaying
// them through the full MITM -> native-messaging -> fetch() -> DotVPN
// pipeline. See PROJECT_HISTORY.md bug #14.
//
// This was originally hardcoded to exactly "marketplace.visualstudio.com",
// but Marketplace's real asset downloads (extension icons, manifests,
// VSIX packages) come from separate per-publisher hosts like
// "<publisher>.gallerycdn.vsassets.io" and "<publisher>.gallery.vsassets.io"
// — those were still failing with the same OPTIONS/400/404 pattern
// because they didn't match the single hardcoded hostname. Rather than
// hardcode every such host individually (more will likely surface over
// time), this now recognizes ANY request carrying the actual CORS
// preflight signature: method=OPTIONS plus both the `Origin` and
// `Access-Control-Request-Method` headers. That combination is not
// something a normal GET/POST client sends — only a browser's own CORS
// preflight machinery produces it — so treating it as "answer locally"
// regardless of hostname is safe and doesn't risk misclassifying a real
// request.
//
// This lives in core/ (not inside protocols/http or protocols/https)
// because both protocol handlers need the identical logic — duplicating
// it per-protocol would risk the two copies drifting apart.

function isCorsPreflight(url, method, headers) {
  if (String(method || "").toUpperCase() !== "OPTIONS") return false;
  if (!headers || !headers["origin"]) return false;
  if (!headers["access-control-request-method"]) return false;
  try {
    // eslint-disable-next-line no-new
    new URL(url); // just validate the URL is well-formed; hostname no longer matters
    return true;
  } catch {
    return false;
  }
}

function buildCorsPreflightHeaders(requestHeaders) {
  const responseHeaders = {
    "access-control-allow-origin": requestHeaders["origin"],
    "access-control-allow-methods": requestHeaders["access-control-request-method"],
    "access-control-allow-credentials": "true",
    "access-control-max-age": "600",
    "content-length": "0",
    "vary": "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
  };

  if (requestHeaders["access-control-request-headers"]) {
    responseHeaders["access-control-allow-headers"] = requestHeaders["access-control-request-headers"];
  }

  return responseHeaders;
}

function applyCorsResponseHeaders(responseHeaders, context) {
  const requestOrigin = context.requestHeaders && context.requestHeaders["origin"];
  if (!requestOrigin) return;

  responseHeaders["access-control-allow-origin"] = requestOrigin;
  responseHeaders["access-control-allow-credentials"] = "true";

  const existingVary = responseHeaders["vary"];
  if (!existingVary) {
    responseHeaders["vary"] = "Origin";
  } else if (!String(existingVary).split(",").some((value) => value.trim().toLowerCase() === "origin")) {
    responseHeaders["vary"] = `${existingVary}, Origin`;
  }
}

module.exports = {
  isCorsPreflight,
  buildCorsPreflightHeaders,
  applyCorsResponseHeaders,
};
