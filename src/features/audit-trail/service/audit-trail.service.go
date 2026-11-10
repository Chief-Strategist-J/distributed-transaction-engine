package service

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"distributed-transaction-engine/src/features/audit-trail/contracts"
	"distributed-transaction-engine/src/features/audit-trail/domain/machines"
	"distributed-transaction-engine/src/features/audit-trail/domain/rules"
	"distributed-transaction-engine/src/features/audit-trail/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo   ports.AuditTrailRepositoryPort
	Tracer *observability.DefaultTracer
}

type AuditTrailService struct {
	repo    ports.AuditTrailRepositoryPort
	tracer  *observability.DefaultTracer
	machine *machines.AuditChainMachine
}

func NewAuditTrailService(deps ServiceDeps) *AuditTrailService {
	tracer := deps.Tracer
	if tracer == nil {
		tracer = observability.NewTracer("audit-trail-service")
	}
	return &AuditTrailService{
		repo:    deps.Repo,
		tracer:  tracer,
		machine: machines.NewAuditChainMachine(),
	}
}

func (s *AuditTrailService) CaptureOutboxEvent(ctx context.Context, cmd contracts.CaptureOutboxEventCommand) (spanResult0 *contracts.OutboxEventRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "audit.service_capture_outbox")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateCaptureOutboxEvent(&inputCmd); err != nil {
		return nil, err
	}

	validAttribution, reason := rules.ValidateAttribution(inputCmd.ActorID, inputCmd.TenantID)
	if !validAttribution {
		quarantineRecord := &contracts.AuditQuarantineRecord{
			ID:               uuid.New(),
			PublicID:         s.generatePublicID("quar"),
			TenantID:         inputCmd.TenantID,
			Source:           inputCmd.Source,
			ExternalID:       inputCmd.ExternalID,
			CorrelationID:    inputCmd.CorrelationID,
			AuditID:          inputCmd.AuditID,
			QuarantineReason: contracts.QuarantineReasonAttributionMissing,
			RawRecord:        inputCmd.Payload,
			ErrorDetails:     reason,
			Resolved:         false,
			CreatedAt:        time.Now().UTC(),
		}
		observability.NoteSwallowed(s.repo.AppendQuarantine(ctx, quarantineRecord))
		return nil, appErrors.NewValidationError("audit event quarantined due to missing attribution: " + reason)
	}

	record := &contracts.OutboxEventRecord{
		ID:             uuid.New(),
		PublicID:       s.generatePublicID("aob"),
		TenantID:       inputCmd.TenantID,
		Source:         inputCmd.Source,
		ExternalID:     inputCmd.ExternalID,
		CorrelationID:  inputCmd.CorrelationID,
		AuditID:        inputCmd.AuditID,
		OccurredAt:     inputCmd.OccurredAt.UTC(),
		Action:         inputCmd.Action,
		ActorID:        inputCmd.ActorID,
		ActorIP:        inputCmd.ActorIP,
		OnBehalfOf:     inputCmd.OnBehalfOf,
		TargetResource: inputCmd.TargetResource,
		Payload:        inputCmd.Payload,
		Status:         contracts.OutboxStatusPending,
		CreatedAt:      time.Now().UTC(),
	}

	if err := s.repo.AppendOutbox(ctx, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *AuditTrailService) ProcessChainerBatch(ctx context.Context, cmd contracts.ProcessChainerBatchCommand) (spanResult0 *contracts.ChainerBatchResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "audit.service_process_chainer_batch")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateProcessChainerBatch(&inputCmd); err != nil {
		return nil, err
	}

	pending, err := s.repo.GetPendingOutbox(ctx, inputCmd.TenantID, inputCmd.BatchSize)
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		return &contracts.ChainerBatchResult{
			TenantID:      inputCmd.TenantID,
			SegmentID:     inputCmd.SegmentID,
			EventsChained: 0,
		}, nil
	}

	tail, err := s.repo.GetTailEvent(ctx, inputCmd.TenantID, inputCmd.SegmentID)
	if err != nil {
		return nil, err
	}

	prevHash := rules.ComputeGenesisHash(inputCmd.TenantID, inputCmd.SegmentID, inputCmd.SecretKey)
	seqID := int64(1)
	if tail != nil {
		prevHash = tail.CurrHash
		seqID = tail.SequenceID + 1
	}

	processedIDs := make([]uuid.UUID, 0, len(pending))
	chainedCount := 0
	headHash := prevHash

	for _, item := range pending {
		alreadyChained, err := s.repo.IsAuditIDChained(ctx, inputCmd.TenantID, item.AuditID)
		if err != nil {
			return nil, err
		}
		if alreadyChained {
			processedIDs = append(processedIDs, item.ID)
			continue
		}

		currHash, err := rules.ComputeKeyedEventHash(
			prevHash, inputCmd.SegmentID, seqID, inputCmd.KeyID, item.OccurredAt,
			inputCmd.TenantID, item.ActorID, item.Action, item.Payload, inputCmd.SecretKey,
		)
		if err != nil {
			return nil, err
		}

		auditRecord := &contracts.AuditEventRecord{
			ID:            uuid.New(),
			PublicID:      s.generatePublicID("aud"),
			TenantID:      inputCmd.TenantID,
			Source:        item.Source,
			ExternalID:    item.ExternalID,
			CorrelationID: item.CorrelationID,
			AuditID:       item.AuditID,
			SegmentID:     inputCmd.SegmentID,
			SequenceID:    seqID,
			KeyID:         inputCmd.KeyID,
			ActorID:       item.ActorID,
			ActorIP:       item.ActorIP,
			Action:        item.Action,
			PrevHash:      prevHash,
			CurrHash:      currHash,
			RawPayload:    item.Payload,
			OccurredAt:    item.OccurredAt,
			CreatedAt:     time.Now().UTC(),
		}

		if err := s.repo.AppendChainedEvent(ctx, auditRecord); err != nil {
			return nil, err
		}

		prevHash = currHash
		headHash = currHash
		seqID++
		chainedCount++
		processedIDs = append(processedIDs, item.ID)
	}

	if len(processedIDs) > 0 {
		if err := s.repo.MarkOutboxChained(ctx, inputCmd.TenantID, processedIDs, time.Now().UTC()); err != nil {
			return nil, err
		}
	}

	return &contracts.ChainerBatchResult{
		TenantID:       inputCmd.TenantID,
		SegmentID:      inputCmd.SegmentID,
		EventsChained:  chainedCount,
		HeadHash:       headHash,
		LastSequenceID: seqID - 1,
	}, nil
}

func (s *AuditTrailService) CreateCheckpoint(ctx context.Context, cmd contracts.CreateCheckpointCommand) (spanResult0 *contracts.AuditCheckpointRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "audit.service_create_checkpoint")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputCmd := cmd
	if err := contracts.ValidateCreateCheckpoint(&inputCmd); err != nil {
		return nil, err
	}

	tail, err := s.repo.GetTailEvent(ctx, inputCmd.TenantID, inputCmd.SegmentID)
	if err != nil {
		return nil, err
	}
	if tail == nil {
		return nil, appErrors.NewNotFoundError("no chained events found in segment to checkpoint")
	}

	mac := hmac.New(sha256.New, inputCmd.SigningKey)
	sigPayload := fmt.Sprintf("%s:%s:%d:%s", inputCmd.TenantID, inputCmd.SegmentID, tail.SequenceID, tail.CurrHash)
	mac.Write([]byte(sigPayload))
	signature := hex.EncodeToString(mac.Sum(nil))

	checkpoint := &contracts.AuditCheckpointRecord{
		ID:             uuid.New(),
		PublicID:       s.generatePublicID("ack"),
		TenantID:       inputCmd.TenantID,
		Source:         "AUDIT_CHECKPOINT_WORKER",
		ExternalID:     uuid.New().String(),
		CorrelationID:  uuid.New(),
		SegmentID:      inputCmd.SegmentID,
		LastSequenceID: tail.SequenceID,
		HeadHash:       tail.CurrHash,
		Signature:      signature,
		KeyID:          inputCmd.KeyID,
		EventCount:     int(tail.SequenceID),
		CreatedAt:      time.Now().UTC(),
	}

	if err := s.repo.SaveCheckpoint(ctx, checkpoint); err != nil {
		return nil, err
	}
	return checkpoint, nil
}

func (s *AuditTrailService) VerifyChain(ctx context.Context, query contracts.VerifyChainQuery) (spanResult0 *contracts.VerifyChainResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "audit.service_verify_chain")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputQuery := query
	if err := contracts.ValidateVerifyChain(&inputQuery); err != nil {
		return nil, err
	}

	events, err := s.repo.GetEventsBySegment(ctx, inputQuery.TenantID, inputQuery.SegmentID, 5000)
	if err != nil {
		return nil, err
	}

	result := s.traverseAndValidateChain(inputQuery, events)
	if err := s.persistVerificationResult(ctx, inputQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *AuditTrailService) traverseAndValidateChain(query contracts.VerifyChainQuery, events []contracts.AuditEventRecord) *contracts.VerifyChainResult {
	expectedPrev := rules.ComputeGenesisHash(query.TenantID, query.SegmentID, query.SecretKey)
	headHash := expectedPrev

	for i, e := range events {
		expectedSeq := int64(i + 1)
		if e.SequenceID != expectedSeq {
			return s.failVerification(query.TenantID, query.SegmentID, i, e.SequenceID, fmt.Sprintf("sequence gap: expected %d got %d", expectedSeq, e.SequenceID), headHash)
		}
		if e.PrevHash != expectedPrev {
			return s.failVerification(query.TenantID, query.SegmentID, i, e.SequenceID, "prev_hash linkage broken", headHash)
		}
		valid, err := rules.VerifyEventNode(e.PrevHash, e.CurrHash, e.SegmentID, e.SequenceID, query.KeyID, e.OccurredAt, e.TenantID, e.ActorID, e.Action, e.RawPayload, query.SecretKey)
		if err != nil || !valid {
			return s.failVerification(query.TenantID, query.SegmentID, i, e.SequenceID, "computed curr_hash mismatch", headHash)
		}
		expectedPrev = e.CurrHash
		headHash = e.CurrHash
	}

	return &contracts.VerifyChainResult{
		TenantID:      query.TenantID,
		SegmentID:     query.SegmentID,
		Status:        contracts.VerificationStatusValid,
		TotalVerified: len(events),
		HeadHash:      headHash,
	}
}

func (s *AuditTrailService) failVerification(tenantID string, segmentID uuid.UUID, checked int, failSeq int64, reason string, headHash string) *contracts.VerifyChainResult {
	seq := failSeq
	return &contracts.VerifyChainResult{
		TenantID:          tenantID,
		SegmentID:         segmentID,
		Status:            contracts.VerificationStatusCompromised,
		TotalVerified:     checked,
		HeadHash:          headHash,
		FailureSequenceID: &seq,
		FailureReason:     reason,
	}
}

func (s *AuditTrailService) persistVerificationResult(ctx context.Context, query contracts.VerifyChainQuery, res *contracts.VerifyChainResult) error {
	now := time.Now().UTC()
	return s.repo.RecordVerification(ctx, &contracts.AuditVerificationRecord{
		ID:                   uuid.New(),
		PublicID:             s.generatePublicID("acv"),
		TenantID:             query.TenantID,
		Source:               "AUDIT_VERIFIER",
		ExternalID:           uuid.New().String(),
		CorrelationID:        uuid.New(),
		SegmentID:            query.SegmentID,
		VerifiedUpToSequence: int64(res.TotalVerified),
		TotalEventsChecked:   res.TotalVerified,
		Status:               res.Status,
		FailureSequenceID:    res.FailureSequenceID,
		FailureReason:        res.FailureReason,
		VerifiedBy:           query.VerifiedBy,
		CreatedAt:            now,
	})
}

func (s *AuditTrailService) ListEvents(ctx context.Context, query contracts.ListAuditEventsQuery) (spanResult0 []contracts.AuditEventRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "audit.service_list_events")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	inputQuery := query
	if err := contracts.ValidateListAuditEvents(&inputQuery); err != nil {
		return nil, err
	}
	return s.repo.GetEventsByTenant(ctx, inputQuery)
}

func (s *AuditTrailService) generatePublicID(prefix string) string {
	b := make([]byte, 13)
	_, swallowedErr1 := rand.Read(b)
	observability.NoteSwallowed(swallowedErr1)
	return prefix + "_" + hex.EncodeToString(b)
}
