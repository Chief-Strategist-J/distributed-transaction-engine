package tests

import (
	"context"
	"testing"
	"time"

	audittrail "distributed-transaction-engine/src/features/audit-trail"
	"distributed-transaction-engine/src/features/audit-trail/contracts"
	"github.com/google/uuid"
)

func TestAuditTrail_EndToEndLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := audittrail.NewMemoryRepository()
	svc := audittrail.NewDefaultService(repo)

	tenantID := "tenant-alpha"
	segmentID := uuid.New()
	keyID := "kms-key-v1"
	secretKey := []byte("engine-hmac-secret-key-32-bytes!")

	captureCmd := contracts.CaptureOutboxEventCommand{
		TenantID:       tenantID,
		Source:         "PAYMENT_SERVICE",
		ExternalID:     "ext-pay-001",
		CorrelationID:  uuid.New(),
		AuditID:        "audit-tx-001",
		OccurredAt:     time.Now().UTC(),
		Action:         "PAYMENT_AUTHORIZED",
		ActorID:        "user-operator-42",
		ActorIP:        "192.168.1.100",
		TargetResource: "accounts/acc-9988",
		Payload: map[string]interface{}{
			"amount":   250.75,
			"currency": "USD",
			"password": "confidential-secret",
		},
	}

	outboxRec, err := svc.CaptureOutboxEvent(ctx, captureCmd)
	if err != nil {
		t.Fatalf("failed to capture outbox event: %v", err)
	}
	if outboxRec.Status != contracts.OutboxStatusPending {
		t.Fatalf("expected outbox status PENDING, got %s", outboxRec.Status)
	}

	chainerCmd := contracts.ProcessChainerBatchCommand{
		TenantID:  tenantID,
		SegmentID: segmentID,
		KeyID:     keyID,
		SecretKey: secretKey,
		BatchSize: 10,
	}

	batchRes, err := svc.ProcessChainerBatch(ctx, chainerCmd)
	if err != nil {
		t.Fatalf("failed to process chainer batch: %v", err)
	}
	if batchRes.EventsChained != 1 {
		t.Fatalf("expected 1 event chained, got %d", batchRes.EventsChained)
	}
	if batchRes.LastSequenceID != 1 {
		t.Fatalf("expected last sequence 1, got %d", batchRes.LastSequenceID)
	}

	dedupeCmd := contracts.CaptureOutboxEventCommand{
		TenantID:       tenantID,
		Source:         "PAYMENT_SERVICE",
		ExternalID:     "ext-pay-001-retry",
		CorrelationID:  uuid.New(),
		AuditID:        "audit-tx-001",
		OccurredAt:     time.Now().UTC(),
		Action:         "PAYMENT_AUTHORIZED",
		ActorID:        "user-operator-42",
		ActorIP:        "192.168.1.100",
		TargetResource: "accounts/acc-9988",
		Payload: map[string]interface{}{
			"amount": 250.75,
		},
	}
	_, err = svc.CaptureOutboxEvent(ctx, dedupeCmd)
	if err != nil {
		t.Fatalf("failed to capture duplicate outbox: %v", err)
	}

	batchRes2, err := svc.ProcessChainerBatch(ctx, chainerCmd)
	if err != nil {
		t.Fatalf("failed second chainer batch: %v", err)
	}
	if batchRes2.EventsChained != 0 {
		t.Fatalf("expected 0 events chained due to dedupe, got %d", batchRes2.EventsChained)
	}

	ckptCmd := contracts.CreateCheckpointCommand{
		TenantID:   tenantID,
		SegmentID:  segmentID,
		KeyID:      keyID,
		SigningKey: secretKey,
	}
	ckpt, err := svc.CreateCheckpoint(ctx, ckptCmd)
	if err != nil {
		t.Fatalf("failed to create checkpoint: %v", err)
	}
	if ckpt.LastSequenceID != 1 {
		t.Fatalf("expected checkpoint last sequence 1, got %d", ckpt.LastSequenceID)
	}

	verifyQuery := contracts.VerifyChainQuery{
		TenantID:   tenantID,
		SegmentID:  segmentID,
		KeyID:      keyID,
		SecretKey:  secretKey,
		VerifiedBy: "auditor-external",
	}
	verifyRes, err := svc.VerifyChain(ctx, verifyQuery)
	if err != nil {
		t.Fatalf("failed to verify chain: %v", err)
	}
	if verifyRes.Status != contracts.VerificationStatusValid {
		t.Fatalf("expected verification VALID, got %s (reason: %s)", verifyRes.Status, verifyRes.FailureReason)
	}

	listEvents, err := svc.ListEvents(ctx, contracts.ListAuditEventsQuery{
		TenantID: tenantID,
		Limit:    10,
		MaskPII:  true,
	})
	if err != nil {
		t.Fatalf("failed to list events: %v", err)
	}
	if len(listEvents) != 1 {
		t.Fatalf("expected 1 listed event, got %d", len(listEvents))
	}
	if listEvents[0].RawPayload["password"] != "******" {
		t.Fatalf("expected masked password in listed event, got %v", listEvents[0].RawPayload["password"])
	}
}

func TestAuditTrail_QuarantinesMissingAttribution(t *testing.T) {
	ctx := context.Background()
	repo := audittrail.NewMemoryRepository()
	svc := audittrail.NewDefaultService(repo)

	cmd := contracts.CaptureOutboxEventCommand{
		TenantID:      "tenant-beta",
		Source:        "ORDER_SERVICE",
		ExternalID:    "ext-ord-002",
		CorrelationID: uuid.New(),
		AuditID:       "audit-ord-002",
		OccurredAt:    time.Now().UTC(),
		Action:        "ORDER_PLACED",
		ActorID:       "",
		Payload:       map[string]interface{}{"order_id": "ord-123"},
	}

	_, err := svc.CaptureOutboxEvent(ctx, cmd)
	if err == nil {
		t.Fatal("expected error due to missing actor attribution")
	}

	quarantined, err := repo.GetQuarantinedEvents(ctx, "tenant-beta", 10)
	if err != nil {
		t.Fatalf("failed to get quarantined events: %v", err)
	}
	if len(quarantined) != 1 {
		t.Fatalf("expected 1 quarantined event, got %d", len(quarantined))
	}
	if quarantined[0].QuarantineReason != contracts.QuarantineReasonAttributionMissing {
		t.Fatalf("expected reason ATTRIBUTION_MISSING, got %s", quarantined[0].QuarantineReason)
	}
}

func TestAuditTrail_TamperDetection(t *testing.T) {
	ctx := context.Background()
	repo := audittrail.NewMemoryRepository()
	svc := audittrail.NewDefaultService(repo)

	tenantID := "tenant-gamma"
	segmentID := uuid.New()
	keyID := "kms-key-v1"
	secretKey := []byte("engine-hmac-secret-key-32-bytes!")

	cmd := contracts.CaptureOutboxEventCommand{
		TenantID:      tenantID,
		Source:        "CORE_SERVICE",
		ExternalID:    "ext-003",
		CorrelationID: uuid.New(),
		AuditID:       "audit-003",
		OccurredAt:    time.Now().UTC(),
		Action:        "RECORD_UPDATE",
		ActorID:       "admin-1",
		Payload:       map[string]interface{}{"val": 10},
	}
	_, err := svc.CaptureOutboxEvent(ctx, cmd)
	if err != nil {
		t.Fatalf("failed capture: %v", err)
	}

	_, err = svc.ProcessChainerBatch(ctx, contracts.ProcessChainerBatchCommand{
		TenantID:  tenantID,
		SegmentID: segmentID,
		KeyID:     keyID,
		SecretKey: secretKey,
		BatchSize: 10,
	})
	if err != nil {
		t.Fatalf("failed chainer: %v", err)
	}

	events, _ := repo.GetEventsBySegment(ctx, tenantID, segmentID, 1)
	if len(events) > 0 {
		tampered := events[0]
		tampered.PrevHash = "0000000000000000000000000000000000000000000000000000000000000000"
		_ = repo.AppendChainedEvent(ctx, &tampered)
	}

	verifyRes, err := svc.VerifyChain(ctx, contracts.VerifyChainQuery{
		TenantID:   tenantID,
		SegmentID:  segmentID,
		KeyID:      keyID,
		SecretKey:  secretKey,
		VerifiedBy: "security-auditor",
	})
	if err != nil {
		t.Fatalf("unexpected error during verify: %v", err)
	}
	if verifyRes.Status != contracts.VerificationStatusCompromised {
		t.Fatalf("expected verification COMPROMISED upon tampering, got %s", verifyRes.Status)
	}
}

