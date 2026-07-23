# AgentMesh relay baseline

Date: 2026-07-23T15:34:38Z
Host: Linux 6.8.0-71-generic x86_64
CPU cores (nproc): 2
Memory: 1.9Gi
Go: go1.23.5
GOMAXPROCS: 
GOGC: ${GOGC:-default}
GOMEMLIMIT: ${GOMEMLIMIT:-unset}

## Command

```
go test ./bench/ -bench BenchmarkSendRelay -benchmem -count=1
go test ./bench/ -run TestSendRelayAllocsGate -v
```

## Results (SEND relay, 64-byte payload, 2 agents via httptest Hub)

| Metric | Value |
|--------|-------|
| ns/op | ~11814 |
| B/op | ~2923 |
| allocs/op (bench) | ~38 |
| allocs/op (AllocsPerRun gate) | **5.0** |
| Allocs gate | ≤ 200 (PASS) |

Notes:
- Hot path still allocates for frame encode + websocket write buffers; payload tail is not re-copied by the Hub beyond slice header attach in EncodeFrame.
- Rate limiters disabled for bench via `Gateway.SetLimiters(nil, nil)`.
- Full open-loop loadgen/HdrHistogram deferred; this is the CI regression baseline for P4 exit.
