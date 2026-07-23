# AgentMesh public deployment quickstart

Expose a Hub on the public Internet **only with TLS**. The Hub refuses non-loopback binds without `MESH_TLS_CERT`/`MESH_TLS_KEY` or an explicit `MESH_INSECURE=true` (error `INSECURE_REFUSED`).

## Option A — Cloudflare Tunnel (recommended)

1. Run Hub on loopback (TLS optional behind Tunnel):

```bash
export MESH_HOST=127.0.0.1 MESH_PORT=8080
export MESH_API_KEYS='a:ka:alice:default'
meshd serve
```

2. Create a Tunnel that routes `https://mesh.example.com` → `http://127.0.0.1:8080`.
   See `deploy/tunnel-example.yml` for a minimal `cloudflared` config.

3. Clients:

```bash
mesh agent --hub https://mesh.example.com --token ka --agent-id alice-laptop --caps echo
mesh call  --hub https://mesh.example.com --token ka --to alice-laptop --payload '{"hello":"public"}'
```

## Option B — nginx reverse proxy TLS

Terminate TLS at nginx; Hub can stay on loopback HTTP:

```nginx
server {
  listen 443 ssl;
  server_name mesh.example.com;
  ssl_certificate     /etc/letsencrypt/live/mesh.example.com/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/mesh.example.com/privkey.pem;

  location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 3600s;
  }
}
```

## Option C — Hub process TLS

```bash
# generate dev cert (LAN / lab only)
go run ./internal/hub/ -  # or use GenerateSelfSigned helper / openssl
export MESH_HOST=0.0.0.0 MESH_PORT=8443
export MESH_TLS_CERT=/etc/mesh/cert.pem MESH_TLS_KEY=/etc/mesh/key.pem
export MESH_API_KEYS='a:ka:alice:default'
meshd serve
```

## Security checklist

- [ ] Non-loopback without TLS refused (`INSECURE_REFUSED`)
- [ ] API keys only over TLS
- [ ] Distinct keys per principal / tenant
- [ ] Prefer Tunnel or reverse-proxy for certificate lifecycle
