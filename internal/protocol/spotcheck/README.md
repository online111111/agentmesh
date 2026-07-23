# Cross-language wire parity spot-check (Task 0.9)

These minimal scripts prove — before the full P6 SDKs exist — that the Go
golden `testvectors.json` envelope bytes are reproducible by the Python and
TypeScript msgpack libraries. They are an early guard for the frozen wire
constraints:

- **B2**: envelope is a fixed 11-slot positional msgpack **array** (never a
  map, never `omitempty`).
- **B3**: the `hdr` sub-map (slot 10) is encoded with keys sorted ascending.

Each script reads `../testvectors.json`, takes a representative subset
(`request` full envelope, `hdr_multi_unsorted` for hdr sorting, `stream_data`
compact shape, `utf8_multibyte`), decodes the golden `envHex` as an 11-element
array, re-encodes it, and asserts the bytes are byte-identical to the Go golden.
Exit code is 0 on parity, non-zero on any mismatch.

## Run

Python (needs `msgpack`):

```bash
python3 decode_check.py
```

Node / TypeScript (needs `@msgpack/msgpack` resolvable):

```bash
# if installed locally next to this file:
node decode_check.mjs
# or point NODE_PATH at any dir containing node_modules/@msgpack/msgpack:
NODE_PATH=/path/to/node_modules node decode_check.mjs
```

If either script fails, **do not proceed** — fix the Go encoder
(`internal/protocol/frame.go`) and regenerate the vectors
(`go run ./internal/protocol/internal/vecgen`) before the wire is depended on by
more code. This is the cheap moment (B2/B3 early guard).

The full three-language byte-parity assertions live in the P6 SDK test suites
(`sdk/python/tests/test_vectors.py`, `sdk/typescript/test/`); these scripts are
the P0 pre-flight.
