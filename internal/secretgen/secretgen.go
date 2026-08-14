package secretgen

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

const (
	alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	hexChars     = "0123456789abcdef"
)

// Random returns a cryptographically-random string of length n from the named charset.
// - alphanumeric avoids URL-breaking symbols,
// - hex for tokens.
func Random(n int, charset string) (string, error) {
	if n <= 0 {
		n = 24
	}
	set := alphanumeric
	if charset == "hex" {
		set = hexChars
	}

	b := make([]byte, n)
	max := big.NewInt(int64(len(set)))
	for i := range b {
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", fmt.Errorf("random: %w", err)
		}
		b[i] = set[idx.Int64()]
	}
	return string(b), nil
}
