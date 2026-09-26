> *Ops guide. Last reviewed 2026-09-19 (documentation reconciliation at `master` `2718bc16`); no architectural claims — content not re-validated against a live deployment.*

# Container Image Testing Guide

This guide describes how to run and test the `naust` container image locally with authentication, OCI compliance checks, and API workflows.

---

## 1. Running the Registry Container

Launch the container with environment variables configuring authentication credentials and persistent storage:

```bash
podman run -d --name my-registry \
  -p 5000:5000 \
  -e REGISTRY_USERNAME=admin \
  -e REGISTRY_PASSWORD=adminpassword \
  -v ./registry-data:/data:Z \
  localhost/naust:latest
```

> **Note:** If using Docker instead of Podman, replace `podman` with `docker` and omit the `:Z` volume flag if SELinux is not in enforcing mode.

---

## 2. Testing Methods

### Method A: Docker / Podman CLI (End-to-End Image Workflow)

#### 1. Log in
```bash
podman login --tls-verify=false 127.0.0.1:5000 -u admin -p adminpassword
```

#### 2. Tag and Push an Image
```bash
# Pull or create a minimal local image
podman pull alpine:latest
podman tag alpine:latest 127.0.0.1:5000/my-alpine:latest

# Push to local registry
podman push --tls-verify=false 127.0.0.1:5000/my-alpine:latest
```

#### 3. Pull the Image Back
```bash
podman rmi 127.0.0.1:5000/my-alpine:latest
podman pull --tls-verify=false 127.0.0.1:5000/my-alpine:latest
```

---

### Method B: Testing via `curl`

#### 1. Health & Base Protocol Check
```bash
curl -i http://127.0.0.1:5000/v2/
```
*Expected response: `200 OK` with header `docker-distribution-api-version: registry/2.0`.*

#### 2. Request a Bearer Token (using Basic Auth)
```bash
TOKEN=$(curl -s -u admin:adminpassword \
  "http://127.0.0.1:5000/token?service=naust&scope=repository:testrepo:pull,push" \
  | jq -r .token)

echo "Acquired Token: ${TOKEN:0:20}..."
```

#### 3. Initiate an Upload Session
```bash
UPLOAD_URL=$(curl -s -i -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:5000/v2/testrepo/blobs/uploads/" \
  | grep -i "^location:" | awk '{print $2}' | tr -d '\r')

echo "Upload location: $UPLOAD_URL"
```

#### 4. Upload a Blob
```bash
DATA="hello world"
DIGEST="sha256:$(echo -n "$DATA" | sha256sum | awk '{print $1}')"

curl -i -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/octet-stream" \
  --data "$DATA" \
  "http://127.0.0.1:5000${UPLOAD_URL}?digest=${DIGEST}"
```
*Expected response: `201 Created` with `Docker-Content-Digest: sha256:...`.*

#### 5. Read the Blob Back
```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:5000/v2/testrepo/blobs/${DIGEST}"
```

#### 6. Extension & Catalog Discovery
```bash
# OCI standard extension discovery
curl -s http://127.0.0.1:5000/v2/_oci/ext/discover | jq .

# Catalog listing
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:5000/v2/_catalog | jq .
```

---

### Method C: Automated Python Test Script

You can execute this self-contained Python script to verify the entire Bearer token auth, blob upload session, digest verification, and download lifecycle:

```bash
python3 -c "
import urllib.request, hashlib, json, base64

HOST = 'http://127.0.0.1:5000'
USER = 'admin'
PASS = 'adminpassword'
REPO = 'testrepo'
DATA = b'automated test payload\n'

digest = 'sha256:' + hashlib.sha256(DATA).hexdigest()

# 1. Fetch Bearer Token
token_url = f'{HOST}/token?service=naust&scope=repository:{REPO}:pull,push'
req = urllib.request.Request(token_url)
basic_b64 = base64.b64encode(f'{USER}:{PASS}'.encode()).decode()
req.add_header('Authorization', f'Basic {basic_b64}')

with urllib.request.urlopen(req) as resp:
    token = json.loads(resp.read().decode())['token']
    print('✔ Token acquired')

# 2. Start Upload
req = urllib.request.Request(f'{HOST}/v2/{REPO}/blobs/uploads/', method='POST')
req.add_header('Authorization', f'Bearer {token}')
with urllib.request.urlopen(req) as resp:
    location = resp.headers.get('Location')
    print(f'✔ Upload initiated: {location}')

# 3. Put Data
req = urllib.request.Request(f'{HOST}{location}?digest={digest}', data=DATA, method='PUT')
req.add_header('Authorization', f'Bearer {token}')
req.add_header('Content-Type', 'application/octet-stream')
with urllib.request.urlopen(req) as resp:
    print(f'✔ Upload committed: {resp.status}')

# 4. Fetch & Verify
req = urllib.request.Request(f'{HOST}/v2/{REPO}/blobs/{digest}')
req.add_header('Authorization', f'Bearer {token}')
with urllib.request.urlopen(req) as resp:
    body = resp.read()
    assert body == DATA
    print(f'✔ Blob verified successfully: {body.decode().strip()}')
"
```

---

## 3. Stopping and Cleaning Up

```bash
podman stop my-registry && podman rm my-registry
```
