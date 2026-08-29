# chrometunnel
(Please read the README carefully and completely)  
Routes HTTP/HTTPS traffic from CLI tools (git, npm, pip) and other apps
(like VS Code) through Chrome's own network stack — so it passes through
whatever proxy extension (a VPN extension, etc.) Chrome is configured to
use, without needing a system-wide VPN or a separate proxy config.

## How it works

A Chrome extension can only control Chrome's own traffic
(`chrome.proxy` API) — it has no access to the OS network stack. Many
proxy/VPN browser extensions only tunnel traffic that goes through
Chrome itself.

chrometunnel bridges that gap for plain HTTP/HTTPS traffic (not raw
TCP/UDP — no games, VoIP, etc.) using three pieces:

```
[VS Code / git / npm / pip / etc]
        |  proxy set to 127.0.0.1:PORT
        v
[Native host — Node.js]
   - local HTTP proxy server
   - terminates HTTPS locally with a self-signed CA (MITM), since
     fetch() can't open a raw CONNECT tunnel
   - native messaging bridge to Chrome
        |  Chrome native messaging protocol
        v
[Chrome extension — background service worker, no UI]
   - receives {url, method, headers, body} from the native host
   - calls fetch(url, ...) — this goes through Chrome's own network
     stack, so your proxy/VPN extension applies to it
   - returns the response to the native host
        |
        v
[Native host returns the response to the original client]
```

## Setup

### 1. Load the extension

`chrome://extensions` → enable Developer mode → **Load unpacked** →
select the `extension/` folder. Copy the extension ID Chrome assigns.

### 2. Install the native host
First open terminal in the native-host folder. Then :
```powershell
npm install
powershell -ExecutionPolicy Bypass -File .\install.ps1 -ExtensionId "<your-extension-id>"
```

If your system blocks script execution entirely, run once:
```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

### 3. Trust the local CA (required for HTTPS)

Restart Chrome, then make sure the extension is enabled so Chrome
launches the native host and it generates `native-host/ca/ca-cert.pem`.

Import that file into Windows: `certmgr.msc` → Certificates (Current
User) → Trusted Root Certification Authorities → Certificates →
right-click → All Tasks → Import → select `ca-cert.pem`.

This only affects TLS connections made *to 127.0.0.1* by clients you
configure to trust it — nothing is installed system-wide beyond the
current user's certificate store, and no other traffic is intercepted.

### 4. Point your tools at the proxy(optional, For who do not know how to set proxy)

```powershell
# npm
npm config set proxy http://127.0.0.1:8765
npm config set https-proxy http://127.0.0.1:8765
npm config set cafile "C:\path\to\native-host\ca\ca-cert.pem"

# git
git config --global http.proxy http://127.0.0.1:8765
git config --global https.proxy http://127.0.0.1:8765
git config --global http.sslCAInfo "C:\path\to\native-host\ca\ca-cert.pem"

# pip (per-session; use setx for a permanent env var)
$env:HTTP_PROXY = "http://127.0.0.1:8765"
$env:HTTPS_PROXY = "http://127.0.0.1:8765"
$env:REQUESTS_CA_BUNDLE = "C:\path\to\native-host\ca\ca-cert.pem"
```

```jsonc
// VS Code settings.json
"http.proxy": "http://127.0.0.1:8765",
"http.proxySupport": "on",
"http.proxyStrictSSL": false
```

## Known limitations

- Only plain request/response HTTP(S) works — no WebSockets, no raw
  TCP protocols (e.g. SSH-based git remotes).
- Not every VS Code extension respects `http.proxy` — it depends on
  whether the extension author wired their networking through VS
  Code's proxy settings at all. Extensions that spawn their own
  external process for networking (e.g. some language servers) may
  bypass it entirely.
- Requires trusting a locally-generated CA for HTTPS to work, since
  Chrome extensions cannot open a raw CONNECT tunnel.
- Adds latency compared to a direct proxy, due to the extra hops
  (client → native host → native messaging → extension → fetch →
  your VPN extension → internet).
- Depends on Chrome staying open with the extension enabled — the
  native host process only runs while Chrome keeps it alive.
extension/      Chrome extension (MV3 service worker, no UI)
native-host/    Node.js native messaging host + local HTTP/HTTPS proxy

## Important note

### Please toggle off the extension after using it and terminate the listetning on its port with command to prevent of bieng stucked in loop to make a listening on specified port
1. First find the PID
```powershell
netstat -ano | findstr :8765
```
2. Then with PID and this command, terminate it
```powershell
taskkill /PID <number> /F
```
### Also consider this tool can not tunnel all request correctly and some request may be failed.
## Uninstall the tool

### 1. Remove the extension from Chrome
`chrome://extensions` → click the chrometunnel extension → **Remove**

### 2. Remove the native messaging registry key
```powershell
Remove-Item -Path "HKCU:\Software\Google\Chrome\NativeMessagingHosts\local.chrometunnel.host" -Recurse
```

### 3. Remove the CA certificate from the Windows Certificate Store
- Run `certmgr.msc`
- Certificates - Current User → Trusted Root Certification Authorities → Certificates
- Find the certificate named **"chrometunnel Local CA"**, right-click → **Delete**

### 4. Remove the proxy/CA settings from your tools(Optional, For who do not know how to restore settings)
```powershell
# npm
npm config delete proxy
npm config delete https-proxy
npm config delete cafile

# git
git config --global --unset http.proxy
git config --global --unset https.proxy
git config --global --unset http.sslCAInfo

# pip / env vars (current session only; see below if set permanently with setx)
Remove-Item Env:\HTTP_PROXY -ErrorAction SilentlyContinue
Remove-Item Env:\HTTPS_PROXY -ErrorAction SilentlyContinue
Remove-Item Env:\REQUESTS_CA_BUNDLE -ErrorAction SilentlyContinue
```

If you made them permanent with `setx`:
```powershell
[Environment]::SetEnvironmentVariable("HTTP_PROXY", $null, "User")
[Environment]::SetEnvironmentVariable("HTTPS_PROXY", $null, "User")
[Environment]::SetEnvironmentVariable("REQUESTS_CA_BUNDLE", $null, "User")
```

### 5. Remove the VS Code settings(Optional, For who do not know how to restore settings)
Delete these lines from `settings.json`:
```json
"http.proxy": "http://127.0.0.1:8765",
"http.proxySupport": "on",
"http.proxyStrictSSL": false
```

### 6. Kill any remaining node processes
1. First find the PID
```powershell
netstat -ano | findstr :8765
```
2. Then with PID and this command, terminate it
```powershell
taskkill /PID <number> /F
```

### 7. Delete the project folder
Delete the `chrometunnel` folder (including `native-host/ca/`, which holds the CA's private key).
