// core/cors.js — CORS preflight short-circuit for VS Code's Marketplace
// client, shared by the plain-HTTP and HTTPS/MITM protocol handlers.
//
// Background: Marketplace's browser-side client issues an OPTIONS
// preflight before its real GET/POST. The Marketplace endpoint itself
// returns 404 for bare OPTIONS (it doesn't implement CORS preflight
// handling the way a browser expects), so we answer preflights locally
// instead of relaying them through the full MITM -> native-messaging ->
// fetch() -> DotVPN pipeline. See PROJECT_HISTORY.md bug #14.
//
// This lives in core/ (not inside protocols/http or protocols/https)
// because both protocol handlers need the identical logic — duplicating
// it per-protocol would risk the two copies drifting apart.

const MARKETPLACE_HOSTNAME = "marketplace.visualstudio.com";

function isMarketplaceUrl(url) {
  try {
    return new URL(url).hostname.toLowerCase() === MARKETPLACE_HOSTNAME;
  } catch {
    return false;
  }
}

function isMarketplaceCorsPreflight(url, method, headers) {
  return (
    isMarketplaceUrl(url) &&
    String(method || "").toUpperCase() === "OPTIONS" &&
    Boolean(headers && headers["origin"]) &&
    Boolean(headers && headers["access-control-request-method"])
  );
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

function applyMarketplaceCorsResponseHeaders(responseHeaders, context) {
  if (!isMarketplaceUrl(context.url)) return;

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
  isMarketplaceUrl,
  isMarketplaceCorsPreflight,
  buildCorsPreflightHeaders,
  applyMarketplaceCorsResponseHeaders,
};
