package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/features/workflow-versioning/domain/machines"
	"distributed-transaction-engine/src/features/workflow-versioning/domain/rules"
	"distributed-transaction-engine/src/features/workflow-versioning/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo   ports.WorkflowVersioningRepositoryPort
	Tracer *observability.DefaultTracer
}

type WorkflowVersioningService struct {
	repo          ports.WorkflowVersioningRepositoryPort
	tracer        *observability.DefaultTracer
	bundleMachine *machines.BundleMachine
	promoMachine  *machines.PromotionMachine
}

func NewWorkflowVersioningService(deps ServiceDeps) *WorkflowVersioningService {
	tracer := deps.Tracer
	if tracer == nil {
		tracer = observability.NewTracer("workflow-versioning-service")
	}
	return &WorkflowVersioningService{
		repo:          deps.Repo,
		tracer:        tracer,
		bundleMachine: machines.NewBundleMachine(),
		promoMachine:  machines.NewPromotionMachine(),
	}
}

func (s *WorkflowVersioningService) RegisterBundle(ctx context.Context, cmd contracts.RegisterBundleCommand) (spanResult0 *contracts.WorkflowBundleRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "wv.service_register_bundle")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateRegisterBundle(&cmd); err != nil {
		return nil, err
	}
	hash, err := rules.ComputeBundleHash(cmd.BundlePayload)
	if err != nil {
		return nil, err
	}
	record := s.buildBundleRecord(cmd, hash)
	if err := s.repo.InsertBundle(ctx, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *WorkflowVersioningService) buildBundleRecord(cmd contracts.RegisterBundleCommand, hash string) *contracts.WorkflowBundleRecord {
	now := time.Now().UTC()
	return &contracts.WorkflowBundleRecord{
		ID:                   uuid.New(),
		PublicID:             s.generatePublicID("wvb"),
		TenantID:             cmd.TenantID,
		Source:               s.defaultString(cmd.Source, "SYSTEM"),
		ExternalID:           s.defaultString(cmd.ExternalID, uuid.New().String()),
		CorrelationID:        s.defaultUUID(cmd.CorrelationID),
		WorkflowName:         cmd.WorkflowName,
		VersionTag:           cmd.VersionTag,
		ContentHash:          hash,
		Signature:            cmd.Signature,
		Status:               contracts.BundleStatusDraft,
		BundlePayload:        cmd.BundlePayload,
		CapabilitiesRequired: cmd.CapabilitiesRequired,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
}

func (s *WorkflowVersioningService) ProposePromotion(ctx context.Context, cmd contracts.ProposePromotionCommand) (spanResult0 *contracts.WorkflowVersionPromotionRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "wv.service_propose_promotion")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateProposePromotion(&cmd); err != nil {
		return nil, err
	}
	bundle, err := s.repo.GetBundleByID(ctx, cmd.TenantID, cmd.BundleID)
	if err != nil {
		return nil, err
	}
	if _, err := s.bundleMachine.Transition(bundle.Status, cmd.Action); err != nil {
		return nil, err
	}
	promo := s.buildPromotionRecord(cmd)
	if err := s.repo.InsertPromotion(ctx, promo); err != nil {
		return nil, err
	}
	return promo, nil
}

func (s *WorkflowVersioningService) buildPromotionRecord(cmd contracts.ProposePromotionCommand) *contracts.WorkflowVersionPromotionRecord {
	now := time.Now().UTC()
	return &contracts.WorkflowVersionPromotionRecord{
		ID:            uuid.New(),
		PublicID:      s.generatePublicID("wvp"),
		TenantID:      cmd.TenantID,
		Source:        s.defaultString(cmd.Source, "SYSTEM"),
		ExternalID:    s.defaultString(cmd.ExternalID, uuid.New().String()),
		CorrelationID: s.defaultUUID(cmd.CorrelationID),
		BundleID:      cmd.BundleID,
		Action:        cmd.Action,
		ProposerID:    cmd.ProposerID,
		Status:        contracts.PromotionStatusPending,
		Reason:        cmd.Reason,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func (s *WorkflowVersioningService) DecidePromotion(ctx context.Context, cmd contracts.DecidePromotionCommand) (spanResult0 *contracts.WorkflowVersionPromotionRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "wv.service_decide_promotion")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateDecidePromotion(&cmd); err != nil {
		return nil, err
	}
	promo, err := s.repo.GetPromotionByID(ctx, cmd.TenantID, cmd.PromotionID)
	if err != nil {
		return nil, err
	}
	if err := rules.ValidateMakerChecker(promo.ProposerID, cmd.ApproverID); err != nil {
		return nil, err
	}
	nextStatus, err := s.promoMachine.Decide(promo.Status, cmd.Approved)
	if err != nil {
		return nil, err
	}
	if err := s.applyPromotionSideEffects(ctx, promo, nextStatus); err != nil {
		return nil, err
	}
	decidedAt := time.Now().UTC()
	input := contracts.UpdatePromotionDecisionInput{
		TenantID:   cmd.TenantID,
		ID:         cmd.PromotionID,
		Status:     nextStatus,
		ApproverID: cmd.ApproverID,
		Reason:     cmd.Reason,
		DecidedAt:  decidedAt,
	}
	if err := s.repo.UpdatePromotionDecision(ctx, input); err != nil {
		return nil, err
	}
	promo.Status = nextStatus
	promo.ApproverID = cmd.ApproverID
	promo.DecidedAt = &decidedAt
	return promo, nil
}

func (s *WorkflowVersioningService) applyPromotionSideEffects(ctx context.Context, promo *contracts.WorkflowVersionPromotionRecord, nextStatus contracts.PromotionStatus) error {
	if nextStatus != contracts.PromotionStatusApproved {
		return nil
	}
	bundle, err := s.repo.GetBundleByID(ctx, promo.TenantID, promo.BundleID)
	if err != nil {
		return err
	}
	targetStatus, err := s.bundleMachine.Transition(bundle.Status, promo.Action)
	if err != nil {
		return err
	}
	return s.repo.UpdateBundleStatus(ctx, promo.TenantID, promo.BundleID, targetStatus)
}

func (s *WorkflowVersioningService) ResolveVersion(ctx context.Context, query contracts.ResolveVersionQuery) (spanResult0 *contracts.ResolveVersionResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "wv.service_resolve_version")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateResolveVersion(&query); err != nil {
		return nil, err
	}
	snapshot := rules.SnapshotLiveInputs(query.LiveInputs)
	existing, err := s.repo.GetEntityCohort(ctx, query.TenantID, query.WorkflowName, query.EntityID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		bundle, err := s.repo.GetBundleByID(ctx, query.TenantID, existing.BundleID)
		if err != nil {
			return nil, err
		}
		return &contracts.ResolveVersionResult{
			Bundle:             bundle,
			IsStickyCohort:     true,
			LiveInputsSnapshot: snapshot,
		}, nil
	}
	return s.resolveAndAssignCohort(ctx, query, snapshot)
}

func (s *WorkflowVersioningService) resolveAndAssignCohort(ctx context.Context, query contracts.ResolveVersionQuery, snapshot map[string]interface{}) (*contracts.ResolveVersionResult, error) {
	activeBundle, err := s.repo.GetActiveBundle(ctx, query.TenantID, query.WorkflowName)
	if err != nil {
		return nil, err
	}
	bucket := rules.ComputeEntityCohortBucket(query.EntityID)
	cohort := s.buildCohortRecord(query, activeBundle.ID, bucket)
	if err := s.repo.InsertEntityCohort(ctx, cohort); err != nil {
		return nil, err
	}
	return &contracts.ResolveVersionResult{
		Bundle:             activeBundle,
		IsStickyCohort:     false,
		LiveInputsSnapshot: snapshot,
	}, nil
}

func (s *WorkflowVersioningService) buildCohortRecord(query contracts.ResolveVersionQuery, bundleID uuid.UUID, bucket int) *contracts.WorkflowEntityCohortRecord {
	return &contracts.WorkflowEntityCohortRecord{
		ID:            uuid.New(),
		PublicID:      s.generatePublicID("wvc"),
		TenantID:      query.TenantID,
		Source:        "SYSTEM",
		ExternalID:    uuid.New().String(),
		CorrelationID: uuid.New(),
		WorkflowName:  query.WorkflowName,
		EntityID:      query.EntityID,
		CohortBucket:  bucket,
		BundleID:      bundleID,
		AssignedAt:    time.Now().UTC(),
	}
}

func (s *WorkflowVersioningService) VerifyBundleIntegrity(ctx context.Context, query contracts.VerifyIntegrityQuery) (spanResult0 *contracts.VerifyIntegrityResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "wv.service_verify_integrity")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	bundle, err := s.repo.GetBundleByID(ctx, query.TenantID, query.BundleID)
	if err != nil {
		return nil, err
	}
	computed, err := rules.ComputeBundleHash(bundle.BundlePayload)
	if err != nil {
		return nil, err
	}
	match := computed == bundle.ContentHash
	if query.ExpectedHash != "" && computed != query.ExpectedHash {
		match = false
	}
	return &contracts.VerifyIntegrityResult{
		Valid:          match,
		ComputedHash:   computed,
		IntegrityMatch: match,
	}, nil
}

func (s *WorkflowVersioningService) MigrateSaga(ctx context.Context, cmd contracts.MigrateSagaCommand) (spanResult0 *contracts.WorkflowVersionMigrationRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "wv.service_migrate_saga")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateMigrateSaga(&cmd); err != nil {
		return nil, err
	}
	target, err := s.repo.GetBundleByVersion(ctx, cmd.TenantID, cmd.Source, cmd.TargetVersionTag)
	if err != nil {
		return nil, err
	}
	if err := rules.CanMigrateAtSafePoint(cmd.SafePointStepID, cmd.SafePointStepID, target.Status); err != nil {
		return nil, err
	}
	migration := s.buildMigrationRecord(cmd, target.ID)
	if err := s.repo.InsertMigration(ctx, migration); err != nil {
		return nil, err
	}
	return migration, nil
}

func (s *WorkflowVersioningService) buildMigrationRecord(cmd contracts.MigrateSagaCommand, targetBundleID uuid.UUID) *contracts.WorkflowVersionMigrationRecord {
	now := time.Now().UTC()
	return &contracts.WorkflowVersionMigrationRecord{
		ID:                  uuid.New(),
		PublicID:            s.generatePublicID("wvm"),
		TenantID:            cmd.TenantID,
		Source:              s.defaultString(cmd.Source, "SYSTEM"),
		ExternalID:          s.defaultString(cmd.ExternalID, uuid.New().String()),
		CorrelationID:       s.defaultUUID(cmd.CorrelationID),
		SagaID:              cmd.SagaID,
		FromBundleID:        targetBundleID,
		ToBundleID:          targetBundleID,
		Status:              contracts.MigrationStatusExecuted,
		SafePointStepID:     cmd.SafePointStepID,
		StateMappingApplied: cmd.StateMappingApplied,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

func (s *WorkflowVersioningService) generatePublicID(prefix string) string {
	b := make([]byte, 13)
	_, swallowedErr1 := rand.Read(b)
	observability.NoteSwallowed(swallowedErr1)
	return prefix + "_" + hex.EncodeToString(b)
}

func (s *WorkflowVersioningService) defaultString(val string, def string) string {
	if val == "" {
		return def
	}
	return val
}

func (s *WorkflowVersioningService) defaultUUID(val uuid.UUID) uuid.UUID {
	if val == uuid.Nil {
		return uuid.New()
	}
	return val
}
