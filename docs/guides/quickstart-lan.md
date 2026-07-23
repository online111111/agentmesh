# AgentMesh LAN quickstart

## Three commands (plain HTTP on loopback)

```bash
# 1) Hub
export MESH_HOST=127.0.0.1 MESH_PORT=8080
export MESH_API_KEYS='a:ka:alice:default\nb:kb:bob:default'
meshd serve

# 2) Echo agent
mesh agent --hub http://127.0.0.1:8080 --token ka --agent-id alice-echo --caps echo

# 3) Call
mesh call --hub http://127.0.0.1:8080 --token ka --to alice-echo --payload '{"hello":"lan"}'
```

Or run the automated smoke:

```bash
./scripts/smoke.sh
```

## LAN with process TLS

```bash
# Generate a self-signed cert for 192.168.1.10 (example)
# (use hub.GenerateSelfSigned or openssl)
export MESH_HOST=0.0.0.0 MESH_PORT=8443
export MESH_TLS_CERT=./cert.pem MESH_TLS_KEY=./key.pem
export MESH_API_KEYS='a:ka:alice:default'
meshd serve

mesh agent --hub https://192.168.1.10:8443 --token ka --agent-id alice-echo
# Clients must trust the cert (or use -k / InsecureSkipVerify only in lab).
```

Non-loopback binds **require** TLS or `MESH_INSECURE=true`.
