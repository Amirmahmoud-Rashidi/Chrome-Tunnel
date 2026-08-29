// tls-mitm.js — Local certificate authority + per-domain leaf certificates
// for terminating TLS locally, so we can decrypt CONNECT-tunneled HTTPS
// requests, relay them to the extension's fetch(), and re-encrypt the
// response back to the client.
//
// IMPORTANT — what this is and isn't:
//   - This CA and its certificates are used ONLY for connections between
//     the client (curl/git/npm/pip/VS Code) and 127.0.0.1 (this process).
//     Nothing leaves the machine signed by this CA.
//   - The client must be told to trust this CA once (imported into the
//     Windows "Trusted Root Certification Authorities" store for the
//     current user), or it will refuse the TLS handshake with an
//     untrusted-certificate error.
//   - We do NOT modify DotVPN, the registry, or any system-wide trust
//     store — only the current user's certificate store.
//
// Uses node-forge because Node's built-in `crypto`/`tls` modules can
// generate keypairs but cannot build or sign X.509 certificate structures
// on their own.

const forge = require("node-forge");
const fs = require("fs");
const path = require("path");

const pki = forge.pki;

const CA_DIR = path.join(__dirname, "ca");
const CA_KEY_PATH = path.join(CA_DIR, "ca-key.pem");
const CA_CERT_PATH = path.join(CA_DIR, "ca-cert.pem");

/**
 * Loads the CA from disk if present, otherwise generates a new one and
 * saves it. The CA is long-lived (10 years) since regenerating it would
 * require the user to re-trust it in Windows every time.
 */
function loadOrCreateCA() {
  if (fs.existsSync(CA_KEY_PATH) && fs.existsSync(CA_CERT_PATH)) {
    const keyPem = fs.readFileSync(CA_KEY_PATH, "utf8");
    const certPem = fs.readFileSync(CA_CERT_PATH, "utf8");
    return {
      key: pki.privateKeyFromPem(keyPem),
      cert: pki.certificateFromPem(certPem),
      certPem,
    };
  }

  console.error("[tls-mitm] No existing CA found, generating a new one...");

  const keys = pki.rsa.generateKeyPair(2048);
  const cert = pki.createCertificate();

  cert.publicKey = keys.publicKey;
  cert.serialNumber = "01";
  cert.validity.notBefore = new Date();
  cert.validity.notAfter = new Date();
  cert.validity.notAfter.setFullYear(cert.validity.notBefore.getFullYear() + 10);

  const attrs = [
    { name: "commonName", value: "chrometunnel Local CA" },
    { name: "organizationName", value: "chrometunnel (local only)" },
  ];
  cert.setSubject(attrs);
  cert.setIssuer(attrs); // self-signed: issuer == subject

  cert.setExtensions([
    { name: "basicConstraints", cA: true },
    {
      name: "keyUsage",
      keyCertSign: true,
      digitalSignature: true,
      cRLSign: true,
    },
  ]);

  cert.sign(keys.privateKey, forge.md.sha256.create());

  const keyPem = pki.privateKeyToPem(keys.privateKey);
  const certPem = pki.certificateToPem(cert);

  fs.mkdirSync(CA_DIR, { recursive: true });
  fs.writeFileSync(CA_KEY_PATH, keyPem, { mode: 0o600 });
  fs.writeFileSync(CA_CERT_PATH, certPem, { mode: 0o644 });

  console.error(`[tls-mitm] New CA written to ${CA_CERT_PATH}`);
  console.error(
    "[tls-mitm] Import this file into 'Trusted Root Certification " +
      "Authorities' (Current User) before using HTTPS through the proxy."
  );

  return { key: keys.privateKey, cert, certPem };
}

const ca = loadOrCreateCA();

// Cache of per-domain leaf certs so we don't regenerate one on every
// single request to the same host.
const leafCertCache = new Map();

/**
 * Returns { keyPem, certPem } for the given hostname, signed by our local
 * CA. Generated once per hostname per process run and cached in memory.
 */
function getCertificateForHost(hostname) {
  if (leafCertCache.has(hostname)) {
    return leafCertCache.get(hostname);
  }

  const keys = pki.rsa.generateKeyPair(2048);
  const cert = pki.createCertificate();

  cert.publicKey = keys.publicKey;
  cert.serialNumber = Date.now().toString(); // unique enough for local use
  cert.validity.notBefore = new Date();
  cert.validity.notAfter = new Date();
  cert.validity.notAfter.setFullYear(cert.validity.notBefore.getFullYear() + 1);

  cert.setSubject([{ name: "commonName", value: hostname }]);
  cert.setIssuer(ca.cert.subject.attributes);

  cert.setExtensions([
    { name: "basicConstraints", cA: false },
    {
      name: "keyUsage",
      digitalSignature: true,
      keyEncipherment: true,
    },
    { name: "extKeyUsage", serverAuth: true },
    {
      name: "subjectAltName",
      altNames: [{ type: 2, value: hostname }], // type 2 = DNS name
    },
  ]);

  cert.sign(ca.key, forge.md.sha256.create());

  const result = {
    keyPem: pki.privateKeyToPem(keys.privateKey),
    certPem: pki.certificateToPem(cert),
  };

  leafCertCache.set(hostname, result);
  return result;
}

module.exports = {
  caCertPem: ca.certPem,
  caCertPath: CA_CERT_PATH,
  getCertificateForHost,
};
