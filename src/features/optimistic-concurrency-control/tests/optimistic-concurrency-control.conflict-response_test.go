package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	occ "distributed-transaction-engine/src/features/optimistic-concurrency-control"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/machines"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/rules"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/adapters"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/service"
	"github.com/google/uuid"
)

type alwaysConflictingRepository struct {
	*adapters.MemoryOCCRepositoryAdapter
}

func (r alwaysConflictingRepository) ExecuteCAS(_ context.Context, _ contracts.CASMutationInput) (*contracts.CASResult, error) {
	return &contracts.CASResult{Succeeded: false, Cause: contracts.CauseVersionConflict, CurrentVersion: 7, CurrentEpoch: 1, CurrentState: "RUNNING"}, nil
}

func TestMutateConflictReturnsTheLatestVersionAndStateForSmartRetries(t *testing.T) {
	repo := adapters.NewMemoryOCCRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{Repo: alwaysConflictingRepository{repo}, Rules: rules.NewOCCRules(), Machine: machines.NewOCCStrategyMachine()})
	sagaID := uuid.New()
	repo.SeedSaga(contracts.SagaRootState{
		ID: sagaID, PublicID: "sag_11111111111111111111111111", TenantID: "tenant-occ-1", CurrentState: "RUNNING",
		Version: 7, OwnerEpoch: 1, UpdatedAt: time.Now().UTC(),
	})
	handler := handlers.NewOCCHandler((*occ.OCCService)(svc))

	body := `{"tenantId":"tenant-occ-1","sagaId":"` + sagaID.String() + `","expectedVersion":3,"expectedEpoch":1,"targetState":"RUNNING","workerId":"w1","inputId":"in-1","inputType":"` + string(contracts.InputTypeOperatorCommand) + `","inputPayload":"{}"}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/occ/mutate?maxRetries=1", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.Mutate(recorder, request)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("a stale expected version must be a 409, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var envelope struct {
		Data struct {
			CurrentVersion int64  `json:"currentVersion"`
			CurrentState   string `json:"currentState"`
			CurrentEpoch   int64  `json:"currentEpoch"`
		} `json:"data"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != "CONFLICT" || envelope.Data.CurrentVersion != 7 || envelope.Data.CurrentState != "RUNNING" {
		t.Fatalf("the 409 body must carry the latest version and state, got %s", recorder.Body.String())
	}
}
