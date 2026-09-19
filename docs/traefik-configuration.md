> *Ops guide. Last reviewed 2026-09-19 (documentation reconciliation at `master` `2718bc16`); no architectural claims — content not re-validated against a live deployment.*

# Traefik configuration for registry-rust

This document captures Traefik settings that are commonly required for reliable Docker/OCI pushes (especially large blobs) through Traefik.

## Symptoms this addresses

When Traefik timeouts are too low for long-running uploads, clients and the registry typically show errors like:

- Client (`docker`, `skopeo`, `buildx`):
  - `connection reset by peer`
  - `unexpected status: 502 Bad Gateway`
  - stalled uploads that fail around a fixed time (often ~60s)
- Registry logs:
  - `failed to read request body: error reading a body from connection`

These almost always indicate a reverse-proxy timeout or connection handling issue during request-body streaming.

## Two different timeout “legs” (both matter)

There are two separate connections to consider:

1. **Client → Traefik** (frontend)
   - Controlled by **entryPoint** settings.
   - If the entryPoint read timeout is too low, Traefik may close the connection while the client is still uploading the request body.
   - Result: client sees a reset; the registry sees request-body read failures.

2. **Traefik → registry-rust** (backend)
   - Controlled by **serversTransport** (can be configured per-service in dynamic config).
   - If backend forwarding timeouts are too low, Traefik may return 502/504 while waiting for response headers or keeping backend connections.

## Recommended dynamic config (registry-only, safe)

This is the preferred first step because it is **scoped to the registry service only** and does not affect other routers/services.

Add a dedicated `serversTransport` and reference it from the registry service.

```yaml
http:
  # Dedicated backend transport for Docker Registry uploads.
  #
  # Why this exists:
  # - Upload requests (PATCH/PUT to /v2/*/blobs/uploads*) can legitimately take minutes.
  # - Default proxy/backend timeouts often assume “normal web apps” and can cut the
  #   connection, causing client errors and registry request-body read failures.
  #
  # Scope:
  # - This affects ONLY Traefik -> registry (backend) traffic for services referencing it.
  serversTransports:
    registryTransport:
      forwardingTimeouts:
        # Time to establish a TCP connection to the backend.
        dialTimeout: 30s

        # Max time Traefik waits for the *response headers* from the backend.
        # During uploads/finalization, the backend may not send response headers until it
        # processed received bytes. Too low => 502/504-like failures.
        responseHeaderTimeout: 7200s

        # Keep idle backend connections around (helps reuse; avoids connection churn).
        idleConnTimeout: 7200s

  services:
    registry-service:
      loadBalancer:
        # Bind the dedicated upload-safe transport to the registry only.
        serversTransport: registryTransport
        servers:
          - url: "http://127.0.0.1:8082"
```

## Recommended static config (entryPoint, global per port)

This controls **client → Traefik** timeouts.

Important limitation: entryPoint transport timeouts are configured **per entryPoint**, so changing `entryPoints.https` affects all routers using that entryPoint.

Example for `entryPoints.https` (port 443):

```yaml
entryPoints:
  https:
    address: ":443"

    # Transport timeouts between client and Traefik.
    #
    # Why this exists:
    # - Docker/skopeo uploads stream request bodies for a long time.
    # - If readTimeout is ~60s (a common default), Traefik can close the client
    #   connection mid-upload => client sees “connection reset by peer”.
    #
    # WARNING:
    # - Applies to *all* routers on this entryPoint.
    # - Usually safe, but it allows slow clients to keep connections open longer.
    transport:
      respondingTimeouts:
        # Max duration Traefik will spend reading the request (client upload).
        readTimeout: 7200s

        # Max duration Traefik will spend writing the response back to the client.
        writeTimeout: 7200s

        # Max idle time for keep-alive connections.
        idleTimeout: 7200s
```

## If you need full isolation (no impact to other services)

If you cannot change the shared `https` entryPoint, you have two common options:

- **Dedicated entryPoint on a different port** (e.g. `userhttps` on `:8443`) and move only the registry router to that entryPoint.
  - Clients must use `registry.example.com:8443`.
- **Dedicated IP + entryPoint on `:443`** for registry only.
  - Requires an additional IP address on the host.

## Kubernetes (no file edits; configure via Helm/CRDs/annotations)

In Kubernetes you typically cannot edit on-disk Traefik config files. Instead, you configure:

- **Static settings** (entryPoints, timeouts) via Traefik deployment args / Helm values.
- **Dynamic settings** (per-service `serversTransport`) via Traefik CRDs (recommended) or
  (depending on your setup) Ingress annotations.

The goal is the same as above:

1. Allow long request bodies (uploads) on the entryPoint (**client → Traefik**).
2. Use a dedicated `ServersTransport` for the registry service (**Traefik → backend**).

### 1) Static entryPoint timeouts via Helm (Traefik chart)

If you install Traefik via the official Helm chart, add these as `additionalArguments`.

This is global *per entryPoint* (e.g. `websecure`), so consider using a dedicated entryPoint if you need strict isolation.

```yaml
# values.yaml
additionalArguments:
  # Client -> Traefik timeouts for long uploads (Docker Registry blobs).
  - "--entrypoints.websecure.transport.respondingTimeouts.readTimeout=7200s"
  - "--entrypoints.websecure.transport.respondingTimeouts.writeTimeout=7200s"
  - "--entrypoints.websecure.transport.respondingTimeouts.idleTimeout=7200s"
```

If you need isolation, create a dedicated entryPoint (example: `registry`) and bind only the registry router to it:

```yaml
# values.yaml
additionalArguments:
  - "--entrypoints.registry.address=:8443"
  - "--entrypoints.registry.transport.respondingTimeouts.readTimeout=7200s"
  - "--entrypoints.registry.transport.respondingTimeouts.writeTimeout=7200s"
  - "--entrypoints.registry.transport.respondingTimeouts.idleTimeout=7200s"
```

### 2) Dynamic per-service timeouts via a `ServersTransport` CRD (recommended)

Create a `ServersTransport` object and reference it from your router/service.

```yaml
apiVersion: traefik.io/v1alpha1
kind: ServersTransport
metadata:
  name: registry-transport
  namespace: traefik
spec:
  forwardingTimeouts:
    dialTimeout: 30s
    responseHeaderTimeout: 7200s
    idleConnTimeout: 7200s
```

### 3) Wire it up: IngressRoute (Traefik CRD)

If you use `IngressRoute`, you can attach the transport directly to the service reference.

```yaml
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: registry
  namespace: registry
spec:
  entryPoints:
    - websecure
  routes:
    - match: Host(`registry.example.com`)
      kind: Rule
      services:
        - name: registry-service
          port: 8082
          # Per-service backend timeouts (Traefik -> registry).
          serversTransport: registry-transport@kubernetescrd
  tls:
    certResolver: letsencrypt
```

### 4) Alternative: Kubernetes Ingress annotations

If you use plain Kubernetes `Ingress` (provider: *Kubernetes Ingress*), Traefik supports annotations on both the **Ingress** and the **Service**.

Key point: the `serversTransport` reference is a **Service annotation**:

- `traefik.ingress.kubernetes.io/service.serverstransport: registry-transport@kubernetescrd`

Example `Service` for the registry (note the transport reference):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: registry
  namespace: registry
  annotations:
    # Per-service backend timeouts (Traefik -> registry).
    # Reference the ServersTransport CRD using provider namespace syntax.
    traefik.ingress.kubernetes.io/service.serverstransport: registry-transport@kubernetescrd
spec:
  selector:
    app: registry
  ports:
    - name: http
      port: 8082
      targetPort: 8082
```

Example `Ingress` for the registry (router-level settings):

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: registry
  namespace: registry
  annotations:
    # Choose the entryPoint (typically websecure for :443).
    traefik.ingress.kubernetes.io/router.entrypoints: websecure

    # Enable TLS on the router.
    traefik.ingress.kubernetes.io/router.tls: "true"

    # If you use ACME certResolver in Traefik, you can reference it here.
    # (Optional if TLS is enabled globally on the entryPoint.)
    traefik.ingress.kubernetes.io/router.tls.certresolver: letsencrypt
spec:
  rules:
    - host: registry.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: registry
                port:
                  number: 8082
```

Notes:

- These annotations cover **routing and backend transport selection**.
- They do **not** replace entryPoint (static) `respondingTimeouts` for long client uploads; that part still must be set via Helm args / static config.

## Suggested starting values

- For uploads: values like `3600s` to `7200s` are typical for home-lab / WAN conditions.
- Tune down if you have strict resource constraints, but keep in mind:
  - long-lived uploads are normal
  - too-low timeouts often fail around predictable fixed intervals

## Debugging tips

- Compare timestamps:
  - client error time
  - Traefik access log / Traefik main log
  - registry log line `failed to read request body`

If Traefik is the component closing the connection, its logs usually contain a clue (timeout/canceled request/i/o timeout) around the same time.
