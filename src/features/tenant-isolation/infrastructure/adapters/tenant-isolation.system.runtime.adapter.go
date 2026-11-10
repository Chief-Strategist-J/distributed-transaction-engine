package adapters

import (
	"crypto/rand"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/hex"
	"github.com/google/uuid"
	"time"
)

const publicIDRandomBytes = 13

type SystemRuntimeAdapter struct{}

func NewSystemRuntimeAdapter() *SystemRuntimeAdapter {
	return &SystemRuntimeAdapter{}
}

func (a *SystemRuntimeAdapter) Now() time.Time {
	defer observability.TraceEnter(nil)()
	return time.Now().UTC()
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
