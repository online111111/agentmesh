// Command vecgen regenerates internal/protocol/testvectors.json from the frozen
// GenerateVectors set. Run from the repo root:
//
//	go run ./internal/protocol/internal/vecgen
//
// Equivalent to: go test ./internal/protocol/ -run TestGoldenFileUpToDate -update
package main

import (
	"fmt"
	"os"

	"github.com/online111111/agentmesh/internal/protocol"
)

func main() {
	b, err := protocol.MarshalGolden()
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("internal/protocol/testvectors.json", b, 0o644); err != nil {
		panic(err)
	}
	fmt.Println("wrote internal/protocol/testvectors.json")
}
