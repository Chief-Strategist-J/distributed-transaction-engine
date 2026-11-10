package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/machines"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/rules"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"

	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo    ports.RepositoryPort
	Rules   *rules.DeadLetterRules
	Machine *machines.DeadLetterMachine
	Tracer  *observability.DefaultTracer
}

type DeadLetterService struct {
	repo    ports.RepositoryPort
	rules   *rules.DeadLetterRules
	machine *machines.DeadLetterMachine
	tracer  *observability.DefaultTracer
}

func NewService(deps ServiceDeps) *DeadLetterService {
	r := deps.Rules
	if r == nil {
		r = rules.NewDeadLetterRules()
	}
	m := deps.Machine
	if m == nil {
		m = machines.NewDeadLetterMachine()
	}
	tr := deps.Tracer
	if tr == nil {
		tr = observability.NewTracer("dead-letter-queue-service")
	}

	return &DeadLetterService{
		repo:    deps.Repo,
		rules:   r,
		machine: m,
		tracer:  tr,
	}
}

func (s *DeadLetterService) AdmitRecord(ctx context.Context, input contracts.AdmitDLQInput) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_admit_record")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateAdmitInput(input); err != nil {
		return nil, err
	}

	if !s.rules.CanAdmit(input.FailureClass) {
		return nil, appErrors.NewValidationError("failure class is not eligible for DLQ admission: " + string(input.FailureClass))
	}

	recentCount, err := s.repo.CountRecentByFingerprint(ctx, input.TenantID, input.ErrorFingerprint, 5*time.Minute)
	if err == nil && s.rules.IsSystemicBurst(recentCount+1, 20) {
		incUUID := uuid.New()
		incPubID := fmt.Sprintf("inc_%s", strings.ReplaceAll(incUUID.String(), "-", "")[:26])
		now := time.Now().UTC()
		incident := contracts.SystemicIncident{
			ID:             incUUID,
			PublicID:       incPubID,
			TenantID:       input.TenantID,
			Source:         "ENGINE_DLQ",
			ExternalID:     input.ErrorFingerprint,
			CorrelationID:  input.CorrelationID,
			Fingerprint:    input.ErrorFingerprint,
			StepType:       input.StepType,
			IncidentStatus: "ACTIVE",
			AffectedCount:  recentCount + 1,
			PausedAt:       now,
			OwnerTeam:      "platform-team",
			ReviewDeadline: now.Add(2 * time.Hour),
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		pause := contracts.StepTypePause{
			TenantID:       input.TenantID,
			StepType:       input.StepType,
			IncidentID:     incUUID,
			PausedAt:       now,
			ReviewDeadline: now.Add(2 * time.Hour),
			OwnerTeam:      "platform-team",
		}
		observability.NoteSwallowed(s.repo.CreateSystemicIncident(ctx, incident, pause))
	}

	rawUUID := uuid.New()
	pubID := fmt.Sprintf("dlq_%s", strings.ReplaceAll(rawUUID.String(), "-", "")[:26])
	sanitized := contracts.SanitizeDiagnosticMessage(input.RawMessage)

	rec := contracts.DeadLetterRecord{
		ID:                rawUUID,
		PublicID:          pubID,
		TenantID:          input.TenantID,
		Source:            "ENGINE_SAGA",
		ExternalID:        input.AttemptID.String(),
		CorrelationID:     input.CorrelationID,
		SagaID:            input.SagaID,
		StepIndex:         input.StepIndex,
		StepType:          input.StepType,
		AttemptID:         input.AttemptID,
		IdempotencyKey:    input.IdempotencyKey,
		OutcomeClass:      input.OutcomeClass,
		FailureClass:      input.FailureClass,
		ErrorFingerprint:  input.ErrorFingerprint,
		SanitizedMessage:  sanitized,
		PayloadRef:        input.PayloadRef,
		SchemaVersion:     input.SchemaVersion,
		MoneyAtStakeCents: input.MoneyAtStakeCents,
		Currency:          input.Currency,
		BusinessDeadline:  input.BusinessDeadline,
		Status:            contracts.StatusQuarantined,
	}

	outboxPayloadBytes, _ := json.Marshal(map[string]any{
		"recordId":     rec.ID.String(),
		"publicId":     rec.PublicID,
		"sagaId":       rec.SagaID.String(),
		"stepIndex":    rec.StepIndex,
		"stepType":     rec.StepType,
		"failureClass": string(rec.FailureClass),
		"fingerprint":  rec.ErrorFingerprint,
	})

	return s.repo.AdmitAtomic(ctx, rec, string(outboxPayloadBytes))
}

func (s *DeadLetterService) TriageRecord(ctx context.Context, input contracts.TriageInput) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_triage_record")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateTriageInput(input); err != nil {
		return nil, err
	}

	rec, err := s.repo.GetRecordByID(ctx, input.TenantID, input.RecordID)
	if err != nil {
		return nil, err
	}

	nextState, err := s.machine.Transition(rec.Status, machines.EventTriage)
	if err != nil {
		return nil, err
	}

	return s.repo.UpdateRecordStatus(ctx, input.TenantID, input.RecordID, rec.Version, nextState)
}

func (s *DeadLetterService) EvaluateReplay(
	ctx context.Context,
	tenantID string,
	recordID uuid.UUID,
	sagaState string,
	sagaStep int,
) (spanResult0 *contracts.ReplayEvaluation, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_evaluate_replay")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	rec, err := s.repo.GetRecordByID(ctx, tenantID, recordID)
	if err != nil {
		return nil, err
	}

	isPaused, err := s.repo.IsStepTypePaused(ctx, tenantID, rec.StepType)
	if err != nil {
		return nil, err
	}

	eval := s.rules.EvaluateReplaySafety(*rec, sagaState, sagaStep, isPaused, time.Now().UTC())
	return &eval, nil
}

func (s *DeadLetterService) RetryRecord(
	ctx context.Context,
	input contracts.RetryActionInput,
	sagaState string,
	sagaStep int,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_retry_record")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateRetryInput(input); err != nil {
		return nil, err
	}

	rec, err := s.repo.GetRecordByID(ctx, input.TenantID, input.RecordID)
	if err != nil {
		return nil, err
	}

	isPaused, err := s.repo.IsStepTypePaused(ctx, input.TenantID, rec.StepType)
	if err != nil {
		return nil, err
	}

	eval := s.rules.EvaluateReplaySafety(*rec, sagaState, sagaStep, isPaused, time.Now().UTC())
	if !eval.Allowed {
		return nil, appErrors.NewConflictError("replay precondition failed: " + eval.Reason)
	}

	nextState, err := s.machine.Transition(rec.Status, machines.EventStartAction)
	if err != nil {
		return nil, err
	}

	return s.repo.UpdateRecordStatus(ctx, input.TenantID, input.RecordID, rec.Version, nextState)
}

func (s *DeadLetterService) ResolveEvidence(ctx context.Context, input contracts.ResolveEvidenceInput) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_resolve_evidence")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateResolveEvidenceInput(input); err != nil {
		return nil, err
	}

	rec, err := s.repo.GetRecordByID(ctx, input.TenantID, input.RecordID)
	if err != nil {
		return nil, err
	}

	disp := contracts.DispositionResolvedAsExecuted
	if !input.Executed {
		disp = contracts.DispositionResolvedAsNotExecuted
	}

	return s.repo.ResolveRecord(
		ctx, input.TenantID, input.RecordID, rec.Version,
		disp, input.ActorID, "", input.Reason, input.EvidenceRef,
		nil, "",
	)
}

func (s *DeadLetterService) ApplyCorrectiveAction(ctx context.Context, input contracts.CorrectiveActionInput) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_corrective_action")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateCorrectiveActionInput(input); err != nil {
		return nil, err
	}

	rec, err := s.repo.GetRecordByID(ctx, input.TenantID, input.RecordID)
	if err != nil {
		return nil, err
	}

	if s.rules.RequiresSecondApprover(rec.MoneyAtStakeCents, contracts.DispositionCorrected) {
		if strings.TrimSpace(input.SecondApproverID) == "" || input.SecondApproverID == input.ActorID {
			return nil, appErrors.NewForbiddenError("money-impacting corrective action requires a distinct second approver")
		}
	}

	return s.repo.ResolveRecord(
		ctx, input.TenantID, input.RecordID, rec.Version,
		contracts.DispositionCorrected, input.ActorID, input.SecondApproverID,
		input.Reason, "", &input.NewAttemptID, input.NewIdempotencyKey,
	)
}

func (s *DeadLetterService) WriteOffRecord(ctx context.Context, input contracts.WriteOffInput) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_write_off")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateWriteOffInput(input); err != nil {
		return nil, err
	}

	rec, err := s.repo.GetRecordByID(ctx, input.TenantID, input.RecordID)
	if err != nil {
		return nil, err
	}

	if s.rules.RequiresSecondApprover(rec.MoneyAtStakeCents, contracts.DispositionWrittenOff) {
		if strings.TrimSpace(input.SecondApproverID) == "" || input.SecondApproverID == input.ActorID {
			return nil, appErrors.NewForbiddenError("monetary write-off requires a distinct second approver")
		}
	}

	return s.repo.ResolveRecord(
		ctx, input.TenantID, input.RecordID, rec.Version,
		contracts.DispositionWrittenOff, input.ActorID, input.SecondApproverID,
		input.Reason, "", nil, "",
	)
}

func (s *DeadLetterService) ResolveSagaAction(ctx context.Context, input contracts.ResolveSagaActionInput) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_resolve_saga_action")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateResolveSagaActionInput(input); err != nil {
		return nil, err
	}

	rec, err := s.repo.GetRecordByID(ctx, input.TenantID, input.RecordID)
	if err != nil {
		return nil, err
	}

	return s.repo.ResolveRecord(
		ctx, input.TenantID, input.RecordID, rec.Version,
		input.Disposition, input.ActorID, "",
		input.Reason, "", nil, "",
	)
}

func (s *DeadLetterService) GetRecord(ctx context.Context, tenantID string, id uuid.UUID) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_get_record")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	return s.repo.GetRecordByID(ctx, tenantID, id)
}

func (s *DeadLetterService) ListRecords(ctx context.Context, tenantID string, status *contracts.RecordStatus, limit int) (spanResult0 []contracts.DeadLetterRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "dlq.service_list_records")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	return s.repo.ListRecords(ctx, tenantID, status, limit)
}
