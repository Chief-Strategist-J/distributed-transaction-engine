package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/features/transaction-query-api/domain/rules"
	"distributed-transaction-engine/src/features/transaction-query-api/domain/workflows"
	"distributed-transaction-engine/src/features/transaction-query-api/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

const (
	cursorPurpose        = "tql-cursor"
	auditPublicIDPrefix  = "tqa_"
	principalBucketKey   = "principal:"
	tenantBucketKey      = "tenant:"
	aggregateExtraTokens = 5
)

type PipelineServiceDeps struct {
	View     ports.TransactionViewRepositoryPort
	Search   ports.TransactionSearchRepositoryPort
	Audit    ports.AuditRepositoryPort
	Limiter  ports.RateLimiterPort
	Clock    ports.ClockPort
	IDs      ports.IDGeneratorPort
	Signer   ports.TokenSignerPort
	BlindIdx ports.BlindIndexPort
	Tracer   *observability.DefaultTracer
}

type PipelineService struct {
	view     ports.TransactionViewRepositoryPort
	search   ports.TransactionSearchRepositoryPort
	audit    ports.AuditRepositoryPort
	limiter  ports.RateLimiterPort
	clock    ports.ClockPort
	ids      ports.IDGeneratorPort
	signer   ports.TokenSignerPort
	blindIdx ports.BlindIndexPort
	tracer   *observability.DefaultTracer
}

func NewPipelineService(deps PipelineServiceDeps) *PipelineService {
	return &PipelineService{
		view: deps.View, search: deps.Search, audit: deps.Audit, limiter: deps.Limiter, clock: deps.Clock,
		ids: deps.IDs, signer: deps.Signer, blindIdx: deps.BlindIdx, tracer: deps.Tracer,
	}
}

func (s *PipelineService) PrepareQuery(ctx context.Context, principal contracts.Principal, input contracts.PrepareInput) (prepared contracts.PreparedQuery, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.prepare_query")
	defer func() { observability.RecordError(span, err); span.End() }()
	steps, err := workflows.OrderSteps(workflows.SearchSteps())
	if err != nil {
		return contracts.PreparedQuery{}, err
	}
	state := &prepareState{principal: principal, input: input, prepared: contracts.PreparedQuery{Principal: principal}}
	for _, step := range steps {
		if err = s.runStep(spanCtx, step, state); err != nil {
			return contracts.PreparedQuery{}, err
		}
	}
	return state.prepared, nil
}

type prepareState struct {
	principal contracts.Principal
	input     contracts.PrepareInput
	prepared  contracts.PreparedQuery
	query     contracts.Query
	now       time.Time
}

func (s *PipelineService) runStep(ctx context.Context, step string, state *prepareState) error {
	switch step {
	case workflows.StepParse:
		return s.stepParse(state)
	case workflows.StepValidate:
		if err := s.checkNoOrderOrLimit(state); err != nil {
			return err
		}
		return s.stepValidate(state)
	case workflows.StepEntitle:
		return s.stepEntitle(state)
	case workflows.StepInject:
		return s.stepInject(state)
	case workflows.StepNormalize:
		return s.stepNormalize(ctx, state)
	case workflows.StepCost:
		return s.stepCost(ctx, state)
	case workflows.StepRateLimit:
		return s.stepRateLimit(ctx, state)
	case workflows.StepPlan:
		return s.stepPlan(ctx, state)
	}
	return nil
}

func (s *PipelineService) stepParse(state *prepareState) error {
	registry, err := rules.ResolveRegistry(state.input.TQLVersion)
	if err != nil {
		return err
	}
	state.prepared.Registry, state.prepared.TQLVersion = registry, state.input.TQLVersion
	if state.input.Text != nil {
		state.query, err = contracts.ParseTQLText(*state.input.Text)
		return err
	}
	state.query = *state.input.AST
	return contracts.ValidateQueryStructure(state.query)
}

func (s *PipelineService) checkNoOrderOrLimit(state *prepareState) error {
	if state.input.ForbidOrderAndLimit && (len(state.query.OrderBy) > 0 || state.query.Limit > 0) {
		return appErrors.NewTypedError(400, constants.TQErrInvalidSyntax, "aggregate queries take no ORDER BY or LIMIT")
	}
	return nil
}

func (s *PipelineService) stepValidate(state *prepareState) error {
	normalized, err := rules.ValidateQuery(state.prepared.Registry, state.principal, state.query, state.input.Params)
	if err != nil {
		return err
	}
	state.prepared.Normalized = normalized
	state.prepared.Unresolved = contracts.Query{Filter: &normalized.Filter, OrderBy: normalized.OrderBy, Limit: normalized.Limit}
	state.prepared.SourceParams = state.input.Params
	return nil
}

func (s *PipelineService) stepEntitle(state *prepareState) error {
	fields, uses, err := rules.ValidateProjection(state.prepared.Registry, state.principal, state.input.Fields)
	if err != nil {
		return err
	}
	state.prepared.Fields = fields
	state.prepared.FieldUses = append(append([]contracts.FieldUse(nil), state.prepared.Normalized.Uses...), uses...)
	return nil
}

func (s *PipelineService) stepInject(state *prepareState) error {
	state.now = s.clock.Now()
	tenantID, err := rules.EvaluateQueryTenant(state.principal, state.input.TenantID, state.now)
	if err != nil {
		return err
	}
	state.prepared.TenantID = tenantID
	state.prepared.Injected = rules.BuildInjectedPredicates(state.principal, tenantID)
	state.prepared.Purpose = state.input.Purpose
	return nil
}

func (s *PipelineService) stepNormalize(ctx context.Context, state *prepareState) error {
	prepared := &state.prepared
	prepared.Store = rules.RouteStore(prepared.Normalized)
	if err := rules.CheckStoreCoverage(prepared.Registry, prepared.Store, prepared.FieldUses); err != nil {
		return err
	}
	prepared.Normalized.Filter = s.applyBlindIndex(prepared.Registry, prepared.Normalized.Filter)
	prepared.QueryHash = rules.HashQuery(prepared.TQLVersion, prepared.Registry.RegistryVersion, prepared.Normalized, prepared.Injected)
	prepared.Consistency = state.input.Consistency
	if prepared.Consistency == "" {
		prepared.Consistency = contracts.ConsistencyLive
	}
	if err := s.applyCursor(state); err != nil {
		return err
	}
	if err := s.applyConsistencyToken(state); err != nil {
		return err
	}
	return s.resolveTimesAndSnapshot(ctx, state)
}

func (s *PipelineService) resolveTimesAndSnapshot(ctx context.Context, state *prepareState) error {
	prepared := &state.prepared
	prepared.ResolvedNow = state.now
	if prepared.CursorClaims != nil {
		prepared.ResolvedNow = prepared.CursorClaims.ResolvedNow
	}
	resolved, err := rules.ResolveQueryTimes(prepared.Normalized, prepared.ResolvedNow)
	if err != nil {
		return err
	}
	prepared.Normalized = resolved
	return s.applySnapshot(ctx, state)
}

func (s *PipelineService) applyBlindIndex(registry contracts.Registry, node contracts.Node) contracts.Node {
	switch node.Kind {
	case contracts.NodePredicate:
		if registry.Fields[node.Predicate.Field].Type != contracts.FieldTypeSensitive {
			return node
		}
		values := make([]contracts.Value, 0, len(node.Predicate.Values))
		for _, value := range node.Predicate.Values {
			values = append(values, contracts.Value{Kind: contracts.ValueString, Text: s.blindIdx.Digest(strings.ToLower(strings.TrimSpace(value.Text)))})
		}
		return contracts.Node{Kind: contracts.NodePredicate, Predicate: &contracts.Predicate{Field: node.Predicate.Field, Operator: node.Predicate.Operator, Values: values}}
	case contracts.NodeQuantified:
		body := s.applyBlindIndex(registry, *node.Quantified.Body)
		return contracts.Node{Kind: contracts.NodeQuantified, Quantified: &contracts.QuantifiedNode{Collection: node.Quantified.Collection, Quantifier: node.Quantified.Quantifier, Body: &body}}
	}
	children := make([]contracts.Node, 0, len(node.Children))
	for _, child := range node.Children {
		children = append(children, s.applyBlindIndex(registry, child))
	}
	return contracts.Node{Kind: node.Kind, Children: children}
}

func (s *PipelineService) applyCursor(state *prepareState) error {
	if state.input.Cursor == nil {
		return nil
	}
	payload, err := s.signer.VerifyToken(cursorPurpose, *state.input.Cursor)
	if err != nil {
		return err
	}
	var claims contracts.CursorClaims
	if json.Unmarshal(payload, &claims) != nil {
		return appErrors.NewTypedError(400, constants.TQErrCursorInvalid, "cursor payload is malformed")
	}
	prepared := &state.prepared
	expected := contracts.CursorClaims{
		TQLVersion: prepared.TQLVersion, RegistryVersion: prepared.Registry.RegistryVersion, QueryHash: prepared.QueryHash,
		PrincipalID: state.principal.PrincipalID, TenantID: prepared.TenantID, SortFields: rules.SortFieldNames(prepared.Normalized.OrderBy),
	}
	if err = rules.ValidateCursorClaims(claims, expected, state.now); err != nil {
		return err
	}
	prepared.CursorClaims, prepared.Consistency = &claims, claims.Consistency
	return nil
}

func (s *PipelineService) applyConsistencyToken(state *prepareState) error {
	if state.input.ConsistencyToken == nil {
		return nil
	}
	payload, err := s.signer.VerifyToken(consistencyPurpose, *state.input.ConsistencyToken)
	if err != nil {
		return appErrors.NewTypedError(400, constants.TQErrConsistencyTokenInvalid, "consistency token is invalid")
	}
	var claims contracts.ConsistencyClaims
	if json.Unmarshal(payload, &claims) != nil {
		return appErrors.NewTypedError(400, constants.TQErrConsistencyTokenInvalid, "consistency token is malformed")
	}
	if err = rules.ValidateConsistencyClaims(claims, state.prepared.TenantID, state.now); err != nil {
		return err
	}
	state.prepared.ConsistencyClaims = &claims
	return nil
}

func (s *PipelineService) applySnapshot(ctx context.Context, state *prepareState) error {
	prepared := &state.prepared
	switch {
	case state.input.AsOfSequenceOverride != nil:
		prepared.AsOfSequence = state.input.AsOfSequenceOverride
	case prepared.CursorClaims != nil && prepared.Consistency == contracts.ConsistencySnapshot:
		sequence := prepared.CursorClaims.AsOfSequence
		prepared.AsOfSequence = &sequence
	case prepared.Consistency == contracts.ConsistencySnapshot:
		watermark, err := s.findWatermark(ctx, prepared.Store)
		if err != nil {
			return err
		}
		prepared.AsOfSequence = &watermark.Sequence
	}
	return nil
}

func (s *PipelineService) findWatermark(ctx context.Context, store contracts.StoreName) (contracts.Watermark, error) {
	if store == contracts.StoreSearch {
		watermark, err := s.search.FindSearchWatermark(ctx)
		if err == nil && watermark.Rebuilding {
			return contracts.Watermark{}, appErrors.NewTypedError(503, constants.TQErrStoreUnavailable, "the search index is rebuilding")
		}
		return watermark, err
	}
	return s.view.FindWatermark(ctx, store)
}

func (s *PipelineService) stepCost(ctx context.Context, state *prepareState) error {
	prepared := &state.prepared
	if prepared.Normalized.Contradiction {
		prepared.CostTokens = 1
		return nil
	}
	estimate, err := s.view.FindTenantRowEstimate(ctx, prepared.TenantID)
	if err != nil {
		return err
	}
	prepared.TenantRowEstimate = estimate
	decision, err := rules.EvaluateQueryCost(contracts.CostInput{Store: prepared.Store, Query: prepared.Normalized, TenantRowEstimate: estimate, Now: prepared.ResolvedNow})
	prepared.CostTokens = decision.Tokens
	return err
}

func (s *PipelineService) stepRateLimit(ctx context.Context, state *prepareState) error {
	if state.input.SkipRateLimit {
		return nil
	}
	return s.ConsumeRateTokens(ctx, state.principal, state.prepared.TenantID, state.prepared.CostTokens)
}

func (s *PipelineService) stepPlan(_ context.Context, _ *prepareState) error {
	return nil
}

func (s *PipelineService) ConsumeRateTokens(ctx context.Context, principal contracts.Principal, tenantID string, tokens int) (err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.consume_rate_tokens")
	defer func() { observability.RecordError(span, err); span.End() }()
	buckets := []struct {
		key      string
		capacity int
		refill   int
	}{
		{principalBucketKey + principal.PrincipalID, rules.PrincipalBucketCapacity, rules.PrincipalRefillPerSec},
		{tenantBucketKey + tenantID, rules.TenantBucketCapacity, rules.TenantRefillPerSec},
	}
	for _, bucket := range buckets {
		decision, limitErr := s.limiter.UpdateRateBucketTokens(spanCtx, tenantID, bucket.key, tokens, bucket.capacity, bucket.refill)
		if limitErr != nil {
			return limitErr
		}
		if !decision.Allowed {
			return appErrors.NewRateLimitedError("query rate limit exceeded", decision.RetryAfterSeconds)
		}
	}
	return nil
}

func (s *PipelineService) PrepareAggregate(ctx context.Context, principal contracts.Principal, request contracts.AggregateRequest) (prepared contracts.PreparedAggregate, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.prepare_aggregate")
	defer func() { observability.RecordError(span, err); span.End() }()
	if _, err = workflows.OrderSteps(workflows.AggregateSteps()); err != nil {
		return contracts.PreparedAggregate{}, err
	}
	base, err := s.PrepareQuery(spanCtx, principal, contracts.PrepareInput{
		TQLVersion: request.TQLVersion, Text: request.Text, AST: request.AST, Params: request.Params, TenantID: request.TenantID,
		Purpose: request.Purpose, SkipRateLimit: true, ForbidOrderAndLimit: true,
	})
	if err != nil {
		return contracts.PreparedAggregate{}, err
	}
	return s.finishAggregate(spanCtx, principal, request, base)
}

func (s *PipelineService) finishAggregate(ctx context.Context, principal contracts.Principal, request contracts.AggregateRequest, base contracts.PreparedQuery) (contracts.PreparedAggregate, error) {
	uses, err := rules.ValidateAggregates(base.Registry, principal, request, base.Normalized.Filter, base.ResolvedNow)
	if err != nil {
		return contracts.PreparedAggregate{}, err
	}
	base.FieldUses = append(base.FieldUses, uses...)
	exactness := request.Exactness
	if exactness == "" {
		exactness = contracts.ExactnessExact
	}
	if err = s.checkAggregateStore(&base, request, exactness); err != nil {
		return contracts.PreparedAggregate{}, err
	}
	if err = s.ConsumeRateTokens(ctx, principal, base.TenantID, base.CostTokens+aggregateExtraTokens); err != nil {
		return contracts.PreparedAggregate{}, err
	}
	return contracts.PreparedAggregate{Prepared: base, Request: request, Exactness: exactness}, nil
}

func (s *PipelineService) checkAggregateStore(prepared *contracts.PreparedQuery, request contracts.AggregateRequest, exactness contracts.Exactness) error {
	if prepared.Store == contracts.StoreSearch && exactness == contracts.ExactnessExact {
		return appErrors.NewTypedError(422, constants.TQErrQueryTooExpensive, "MATCH aggregates are approximate, pass exactness APPROXIMATE",
			appErrors.ErrorDetail{Field: "exactness", Issue: "use APPROXIMATE with MATCH", Rule: "exactness"})
	}
	for _, expression := range request.Aggregates {
		if prepared.Store == contracts.StoreSearch && expression.Function != contracts.AggCount {
			return appErrors.NewTypedError(422, constants.TQErrQueryTooExpensive, "only COUNT is available for MATCH aggregates")
		}
	}
	return rules.CheckStoreCoverage(prepared.Registry, prepared.Store, prepared.FieldUses)
}

func (s *PipelineService) CreateAuditEntry(ctx context.Context, prepared contracts.PreparedQuery, action contracts.AuditAction, resultCount *int64) (err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.create_audit_entry")
	defer func() { observability.RecordError(span, err); span.End() }()
	exporting := action == contracts.AuditExportCreate || action == contracts.AuditExportDownload
	if !rules.RequiresAudit(prepared.Principal, prepared.FieldUses, prepared.Registry, exporting) {
		return nil
	}
	record, err := s.buildAuditRecord(prepared, action, resultCount)
	if err != nil {
		return err
	}
	if err = s.audit.CreateAuditRecord(spanCtx, record); err != nil {
		return appErrors.NewTypedError(503, constants.TQErrStoreUnavailable, "the audit trail is unavailable")
	}
	return nil
}

func (s *PipelineService) buildAuditRecord(prepared contracts.PreparedQuery, action contracts.AuditAction, resultCount *int64) (contracts.AuditRecord, error) {
	id, err := s.ids.NewID()
	if err != nil {
		return contracts.AuditRecord{}, err
	}
	publicID, err := s.ids.NewPublicID(auditPublicIDPrefix)
	if err != nil {
		return contracts.AuditRecord{}, err
	}
	redacted := rules.RedactNodeForAudit(prepared.Registry, prepared.Unresolved.FilterOrEmpty(), s.blindIdx.Digest)
	encoded, err := json.Marshal(map[string]any{"tql_version": prepared.TQLVersion, "filter": redacted, "order_by": prepared.Unresolved.OrderBy, "store": prepared.Store})
	if err != nil {
		return contracts.AuditRecord{}, appErrors.NewInternalError("audit encoding failed")
	}
	return contracts.AuditRecord{
		ID: id, PublicID: publicID, TenantID: prepared.TenantID, PrincipalID: prepared.Principal.PrincipalID, Audience: prepared.Principal.Audience,
		Action: action, NormalizedAST: encoded, ResultCount: resultCount, Purpose: prepared.Purpose,
		ClassesTouched: rules.ClassesTouched(prepared.FieldUses, prepared.Registry), Impersonated: prepared.Principal.Impersonation != nil,
		CorrelationID: id, OccurredAt: s.clock.Now(),
	}, nil
}

func (s *PipelineService) CreateCursor(prepared contracts.PreparedQuery, last contracts.Row) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	base := contracts.CursorClaims{
		TQLVersion: prepared.TQLVersion, RegistryVersion: prepared.Registry.RegistryVersion, QueryHash: prepared.QueryHash,
		PrincipalID: prepared.Principal.PrincipalID, TenantID: prepared.TenantID, ResolvedNow: prepared.ResolvedNow,
		Store: prepared.Store, Consistency: prepared.Consistency,
	}
	if prepared.AsOfSequence != nil {
		base.AsOfSequence = *prepared.AsOfSequence
	}
	claims := rules.BuildCursorClaims(base, prepared.Normalized.OrderBy, last, s.clock.Now())
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", appErrors.NewInternalError("cursor encoding failed")
	}
	return s.signer.SignToken(cursorPurpose, payload), nil
}

func (s *PipelineService) GetFreshness(ctx context.Context, prepared contracts.PreparedQuery) (meta contracts.ResponseMeta, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.get_freshness")
	defer func() { observability.RecordError(span, err); span.End() }()
	watermark, err := s.findWatermark(spanCtx, prepared.Store)
	if err != nil {
		return contracts.ResponseMeta{}, err
	}
	now := s.clock.Now()
	meta = contracts.ResponseMeta{
		Store: prepared.Store, AsOf: watermark.AsOf, AsOfSequence: watermark.Sequence, TQLVersion: prepared.TQLVersion,
		RegistryVersion: prepared.Registry.RegistryVersion, Consistency: prepared.Consistency, Completeness: contracts.CompletenessComplete,
		FreshnessLagSeconds: maxInt64(0, int64(now.Sub(watermark.AsOf).Seconds())), ResolvedTimes: prepared.Normalized.ResolvedTimes,
		Warnings: prepared.Normalized.Warnings, ResultsEmptyByProof: prepared.Normalized.Contradiction,
	}
	if prepared.ConsistencyClaims != nil && watermark.Sequence < prepared.ConsistencyClaims.MinSequence {
		meta.Completeness = contracts.CompletenessIncomplete
		meta.PendingHints = []string{"recent writes are not yet visible in the " + string(prepared.Store) + " store"}
		if prepared.ConsistencyClaims.TransactionID != "" {
			meta.PendingHints = append(meta.PendingHints, "pending transaction "+prepared.ConsistencyClaims.TransactionID)
		}
	}
	return meta, nil
}

func maxInt64(first int64, second int64) int64 {
	if first > second {
		return first
	}
	return second
}

func (s *PipelineService) buildPlan(prepared contracts.PreparedQuery, keyset *contracts.KeysetPosition, limit int) contracts.QueryPlan {
	return contracts.QueryPlan{
		Store: prepared.Store, Injected: prepared.Injected, Filter: prepared.Normalized.Filter, OrderBy: prepared.Normalized.OrderBy,
		Limit: limit, Keyset: keyset, Fields: prepared.Fields, AsOfSequence: prepared.AsOfSequence,
	}
}

func (s *PipelineService) EvaluatePreparedEstimate(ctx context.Context, prepared contracts.PreparedQuery) (estimate int64, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.evaluate_prepared_estimate")
	defer func() { observability.RecordError(span, err); span.End() }()
	if prepared.Store == contracts.StoreSearch || prepared.Normalized.Contradiction {
		return 0, nil
	}
	if estimate, err = s.view.EstimatePlanRows(spanCtx, s.buildPlan(prepared, nil, prepared.Normalized.Limit)); err != nil {
		return 0, err
	}
	return estimate, rules.EvaluatePlanEstimate(estimate)
}

func (s *PipelineService) ListPage(ctx context.Context, prepared contracts.PreparedQuery, keyset *contracts.KeysetPosition, limit int) (rows []contracts.Row, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.list_page")
	defer func() { observability.RecordError(span, err); span.End() }()
	plan := s.buildPlan(prepared, keyset, limit)
	if prepared.Store == contracts.StorePrimary {
		return s.view.ListTransactions(spanCtx, plan)
	}
	matches, err := s.search.ListSearchMatches(spanCtx, plan)
	if err != nil || len(matches) == 0 {
		return matches, err
	}
	return s.hydrateMatches(spanCtx, prepared, matches)
}

func (s *PipelineService) hydrateMatches(ctx context.Context, prepared contracts.PreparedQuery, matches []contracts.Row) ([]contracts.Row, error) {
	identifiers := make([]string, 0, len(matches))
	for _, match := range matches {
		identifiers = append(identifiers, match["id"].(string))
	}
	fields := append(append([]string(nil), prepared.Fields...), rules.SortFieldNames(prepared.Normalized.OrderBy)...)
	hydrated, err := s.view.ListTransactionsByIDs(ctx, prepared.TenantID, identifiers, prepared.Injected, fields)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]contracts.Row, len(hydrated))
	for _, row := range hydrated {
		byID[row["id"].(string)] = row
	}
	ordered := make([]contracts.Row, 0, len(matches))
	for _, identifier := range identifiers {
		if row, present := byID[identifier]; present {
			ordered = append(ordered, row)
		}
	}
	return ordered, nil
}

const consistencyPurpose = "tql-consistency"
