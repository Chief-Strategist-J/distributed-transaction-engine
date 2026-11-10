package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/machines"
	"distributed-transaction-engine/src/features/fencing-tokens/domain/rules"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"

	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo    ports.RepositoryPort
	Rules   *rules.FencingRules
	Machine *machines.FencingLeaseMachine
	Tracer  *observability.DefaultTracer
}

type FencingTokensService struct {
	repo    ports.RepositoryPort
	rules   *rules.FencingRules
	machine *machines.FencingLeaseMachine
	tracer  *observability.DefaultTracer
}

func NewService(deps ServiceDeps) *FencingTokensService {
	r := deps.Rules
	if r == nil {
		r = rules.NewFencingRules()
	}
	m := deps.Machine
	if m == nil {
		m = machines.NewFencingLeaseMachine()
	}
	tr := deps.Tracer
	if tr == nil {
		tr = observability.NewTracer("fencing-tokens-service")
	}

	return &FencingTokensService{
		repo:    deps.Repo,
		rules:   r,
		machine: m,
		tracer:  tr,
	}
}

func (s *FencingTokensService) AcquireLease(ctx context.Context, input contracts.AcquireLeaseInput) (spanResult0 *contracts.FencingLease, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "fencing.service_acquire_lease")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateAcquireInput(input); err != nil {
		return nil, err
	}

	return s.repo.AcquireOrTakeover(ctx, input.TenantID, input.ResourceID, input.HolderID, input.LeaseDurationMs)
}

func (s *FencingTokensService) RenewLease(ctx context.Context, input contracts.RenewLeaseInput) (spanResult0 *contracts.FencingLease, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "fencing.service_renew_lease")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateRenewInput(input); err != nil {
		return nil, err
	}

	return s.repo.Renew(ctx, input.TenantID, input.ResourceID, input.HolderID, input.Epoch, input.LeaseDurationMs)
}

func (s *FencingTokensService) ReleaseLease(ctx context.Context, input contracts.ReleaseLeaseInput) (spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "fencing.service_release_lease")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateReleaseInput(input); err != nil {
		return err
	}

	return s.repo.Release(ctx, input.TenantID, input.ResourceID, input.HolderID, input.Epoch)
}

func (s *FencingTokensService) GetLease(ctx context.Context, tenantID, resourceID string) (spanResult0 *contracts.FencingLease, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "fencing.service_get_lease")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	return s.repo.GetLease(ctx, tenantID, resourceID)
}

func (s *FencingTokensService) ValidateAndFence(ctx context.Context, input contracts.FencedBarrierInput) (spanResult0 *contracts.FencingDecision, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "fencing.service_validate_and_fence")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateBarrierInput(input); err != nil {
		return nil, err
	}

	decision, err := s.repo.ExecuteFencedBarrier(ctx, input.TenantID, input.ResourceID, input.Token)
	if err != nil {
		return nil, err
	}

	if !decision.Allowed {
		auditUUID := uuid.New()
		audit := contracts.FencingAudit{
			ID:             auditUUID,
			PublicID:       fmt.Sprintf("fnv_%s", strings.ReplaceAll(auditUUID.String(), "-", "")[:26]),
			TenantID:       input.TenantID,
			ResourceID:     input.ResourceID,
			AttemptedToken: decision.AttemptedToken,
			CurrentToken:   decision.StoredToken,
			RejectedHolder: "unknown_zombie",
			ViolationType:  decision.ViolationType,
			RecordedAt:     time.Now().UTC(),
		}
		observability.NoteSwallowed(s.repo.RecordAudit(ctx, audit))
	}

	return decision, nil
}

func (s *FencingTokensService) GetAudits(ctx context.Context, tenantID, resourceID string, limit int) (spanResult0 []contracts.FencingAudit, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "fencing.service_get_audits")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if limit <= 0 {
		limit = 10
	}
	return s.repo.GetAudits(ctx, tenantID, resourceID, limit)
}
