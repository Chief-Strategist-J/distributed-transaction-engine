package adapters

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/base64"
	"encoding/hex"
	"github.com/google/uuid"
	"strings"
	"time"
)

const (
	MinimumKeyBytes     = 32
	publicIDRandomBytes = 13
)

type RuntimeKeys struct {
	TokenSigningKey []byte
	BlindIndexKey   []byte
	ExportKey       []byte
}

type SystemRuntimeAdapter struct {
	keys RuntimeKeys
	aead cipher.AEAD
}

func NewSystemRuntimeAdapter(keys RuntimeKeys) (*SystemRuntimeAdapter, error) {
	for _, key := range [][]byte{keys.TokenSigningKey, keys.BlindIndexKey} {
		if len(key) < MinimumKeyBytes {
			return nil, appErrors.NewInternalError("runtime keys must be at least 32 bytes")
		}
	}
	if len(keys.ExportKey) != MinimumKeyBytes {
		return nil, appErrors.NewInternalError("export key must be exactly 32 bytes")
	}
	block, err := aes.NewCipher(keys.ExportKey)
	if err != nil {
		return nil, appErrors.NewInternalError("export cipher setup failed")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, appErrors.NewInternalError("export cipher setup failed")
	}
	return &SystemRuntimeAdapter{keys: keys, aead: aead}, nil
}

func (a *SystemRuntimeAdapter) Now() time.Time {
	defer observability.TraceEnter(nil)()
	return time.Now().UTC()
}

func (a *SystemRuntimeAdapter) Sleep(ctx context.Context, duration time.Duration) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (a *SystemRuntimeAdapter) NewID() (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, appErrors.NewInternalError("identifier generation failed")
	}
	return id, nil
}

func (a *SystemRuntimeAdapter) NewPublicID(prefix string) (string, error) {
	random := make([]byte, publicIDRandomBytes)
	if _, err := rand.Read(random); err != nil {
		return "", appErrors.NewInternalError("identifier generation failed")
	}
	return prefix + hex.EncodeToString(random), nil
}

func (a *SystemRuntimeAdapter) SignToken(purpose string, payload []byte) string {
	defer observability.TraceEnter(nil)()
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	return encoded + "." + base64.RawURLEncoding.EncodeToString(a.mac(purpose, encoded))
}

func (a *SystemRuntimeAdapter) VerifyToken(purpose string, token string) (spanResult0 []byte, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	encoded, signature, found := strings.Cut(token, ".")
	if !found {
		return nil, appErrors.NewTypedError(400, constants.TQErrCursorInvalid, "token is malformed")
	}
	provided, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil || !hmac.Equal(provided, a.mac(purpose, encoded)) {
		return nil, appErrors.NewTypedError(400, constants.TQErrCursorInvalid, "token signature is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, appErrors.NewTypedError(400, constants.TQErrCursorInvalid, "token is malformed")
	}
	return payload, nil
}

func (a *SystemRuntimeAdapter) mac(purpose string, encoded string) []byte {
	hasher := hmac.New(sha256.New, a.keys.TokenSigningKey)
	hasher.Write([]byte(purpose + "|" + encoded))
	return hasher.Sum(nil)
}

func (a *SystemRuntimeAdapter) Encrypt(plaintext []byte) (spanResult0 []byte, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	nonce := make([]byte, a.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, appErrors.NewInternalError("encryption failed")
	}
	return a.aead.Seal(nonce, nonce, plaintext, nil), nil
}

func (a *SystemRuntimeAdapter) Decrypt(ciphertext []byte) (spanResult0 []byte, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	size := a.aead.NonceSize()
	if len(ciphertext) < size {
		return nil, appErrors.NewInternalError("ciphertext is malformed")
	}
	plaintext, err := a.aead.Open(nil, ciphertext[:size], ciphertext[size:], nil)
	if err != nil {
		return nil, appErrors.NewInternalError("ciphertext failed authentication")
	}
	return plaintext, nil
}

func (a *SystemRuntimeAdapter) Digest(text string) string {
	defer observability.TraceEnter(nil)()
	hasher := hmac.New(sha256.New, a.keys.BlindIndexKey)
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
