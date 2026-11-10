package tests

import (
	"context"
	"testing"
	"time"

	audittrail "distributed-transaction-engine/src/features/audit-trail"
	"distributed-transaction-engine/src/features/audit-trail/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

func TestAuditTrail_TracingSpans(t *testing.T) {
	ctx := context.Background()
	tracer := observability.NewTracer("audit-test-tracer")
	repo := audittrail.NewMemoryRepository()
	svc := audittrail.NewService(audittrail.ServiceDeps{
		Repo:   repo,
		Tracer: tracer,
	})

	tenantID := "tenant-tracer"
	segmentID := uuid.New()
	keyID := "key-tracer"
	secret := []byte("secret-32-bytes-tracer-key-test!")

	captureCmd := contracts.CaptureOutboxEventCommand{
		TenantID:      tenantID,
		Source:        "TRACER_SERVICE",
		ExternalID:    "ext-trace-1",
		CorrelationID: uuid.New(),
		AuditID:       "audit-trace-1",
		OccurredAt:    time.Now().UTC(),
		Action:        "TRACE_ACTION",
		ActorID:       "trace-actor",
		Payload:       map[string]interface{}{"traced": true},
	}

	_, err := svc.CaptureOutboxEvent(ctx, captureCmd)
	if err != nil {
		t.Fatalf("failed capture: %v", err)
	}

	_, err = svc.ProcessChainerBatch(ctx, contracts.ProcessChainerBatchCommand{
		TenantID:  tenantID,
		SegmentID: segmentID,
		KeyID:     keyID,
		SecretKey: secret,
		BatchSize: 10,
	})
	if err != nil {
		t.Fatalf("failed chainer: %v", err)
	}

	_, err = svc.CreateCheckpoint(ctx, contracts.CreateCheckpointCommand{
		TenantID:   tenantID,
		SegmentID:  segmentID,
		KeyID:      keyID,
		SigningKey: secret,
	})
	if err != nil {
		t.Fatalf("failed checkpoint: %v", err)
	}

	_, err = svc.VerifyChain(ctx, contracts.VerifyChainQuery{
		TenantID:   tenantID,
		SegmentID:  segmentID,
		KeyID:      keyID,
		SecretKey:  secret,
		VerifiedBy: "verifier-tracer",
	})
	if err != nil {
		t.Fatalf("failed verify: %v", err)
	}
}

