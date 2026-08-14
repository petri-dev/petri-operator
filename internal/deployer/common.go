package deployer

import (
	"crypto/sha256"
	"encoding/hex"
)

const (
	// k8s object names must be a DNS-1123 label: at most 63 chars.
	MaxJobNameLen = 63

	// hash suffix length when a name is too long
	JobNameHashLen = 8
)

// TruncateName returns name unchanged if it fits in MaxJobNameLen, otherwise truncates and appends an 8-char sha256 suffix to keep it unique and DNS-1123 safe.
func TruncateName(name string) string {
	if len(name) <= MaxJobNameLen {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:JobNameHashLen]
	prefix := name[:MaxJobNameLen-1-JobNameHashLen] // reserve one char for dash
	return prefix + "-" + suffix
}
