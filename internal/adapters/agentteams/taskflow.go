package agentteams

import (
	"crypto/sha256"
	"encoding/hex"
)

func stableTaskID(invocationRef string) string {
	sum := sha256.Sum256([]byte(invocationRef))
	return "threadmill-" + hex.EncodeToString(sum[:16])
}
