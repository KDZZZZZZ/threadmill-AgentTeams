package agentteams

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

func stableTaskID(invocationRef string) string {
	return attemptedTaskID(invocationRef, 1)
}

func attemptedTaskID(invocationRef string, attempt int) string {
	sum := sha256.Sum256([]byte(invocationRef))
	base := "threadmill-" + hex.EncodeToString(sum[:16])
	if attempt <= 1 {
		return base
	}
	return base + "-attempt-" + strconv.Itoa(attempt)
}
