package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
)

func NewOpaqueSecret() (string, []byte, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", nil, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw[:])
	hash := HashOpaqueSecret(encoded)
	return encoded, hash, nil
}

func HashOpaqueSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}
