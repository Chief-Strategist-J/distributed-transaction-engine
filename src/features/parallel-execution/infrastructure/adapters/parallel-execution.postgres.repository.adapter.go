package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	queries "distributed-transaction-engine/src/features/parallel-execution/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"encoding/json"
)

type PostgresParallelExecutionRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresParallelExecutionRepositoryAdapter(db *sql.DB) *PostgresParallelExecutionRepositoryAdapter {
	return &PostgresParallelExecutionRepositoryAdapter{db: db}
}

func (a *PostgresParallelExecutionRepositoryAdapter) CreateGroupAtomic(
	ctx context.Context,
	group *contracts.ParallelGroupInstance,
	branches []contracts.ParallelBranchInstance,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewInternalError("failed to begin transaction")
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		queries.FLOW_CREATE_PARALLEL_GROUP,
		group.ID, group.PublicID, group.TenantID, group.SagaID, string(group.JoinPolicy),
		group.QuorumCount, string(group.CurrentState), len(branches), group.CorrelationID, group.ID,
	)
	if err != nil {
		return errors.NewConflictError("duplicate or invalid parallel group")
	}

	for _, b := range branches {
		inBytes, _ := json.Marshal(b.BranchInput)
		outBytes, _ := json.Marshal(b.BranchOutput)
		_, err = tx.ExecContext(
			ctx,
			queries.FLOW_CREATE_PARALLEL_BRANCH,
			b.ID, b.PublicID, b.TenantID, b.GroupID, b.SagaID, b.BranchID, b.BranchName,
			string(b.Role), string(b.EffectType), string(b.CurrentState), b.IdempotencyKey,
			string(inBytes), string(outBytes), b.CorrelationID, b.ID,
		)
		if err != nil {
			return errors.NewConflictError("duplicate or invalid branch")
		}
	}

	return tx.Commit()
}

func (a *PostgresParallelExecutionRepositoryAdapter) GetGroup(ctx context.Context, tenantID string, groupID string) (spanResult0 *contracts.ParallelGroupInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_PARALLEL_GROUP, tenantID, groupID)

	var g contracts.ParallelGroupInstance
	var policyStr, stateStr string
	err := row.Scan(
		&g.ID, &g.PublicID, &g.TenantID, &g.SagaID, &policyStr, &g.QuorumCount,
		&stateStr, &g.TotalBranches, &g.SucceededBranches, &g.FailedBranches,
		&g.Version, &g.CorrelationID, &g.CreatedAt, &g.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("parallel group not found")
		}
		return nil, errors.NewInternalError("database query failed")
	}
	g.JoinPolicy = contracts.JoinPolicy(policyStr)
	g.CurrentState = contracts.GroupState(stateStr)

	bRows, err := a.db.QueryContext(ctx, queries.FLOW_GET_GROUP_BRANCHES, tenantID, groupID)
	if err != nil {
		return nil, errors.NewInternalError("failed to query group branches")
	}
	defer bRows.Close()

	for bRows.Next() {
		var b contracts.ParallelBranchInstance
		var roleStr, effectStr, bStateStr string
		var inBytes, outBytes []byte
		if scanErr := bRows.Scan(
			&b.ID, &b.PublicID, &b.TenantID, &b.GroupID, &b.SagaID, &b.BranchID, &b.BranchName,
			&roleStr, &effectStr, &bStateStr, &b.IdempotencyKey, &inBytes, &outBytes,
			&b.ErrorMessage, &b.Version, &b.CreatedAt, &b.UpdatedAt,
		); scanErr != nil {
			return nil, errors.NewInternalError("failed scanning branch")
		}
		b.Role = contracts.BranchRole(roleStr)
		b.EffectType = contracts.BranchEffectType(effectStr)
		b.CurrentState = contracts.BranchState(bStateStr)
		observability.NoteSwallowed(json.Unmarshal(inBytes, &b.BranchInput))
		observability.NoteSwallowed(json.Unmarshal(outBytes, &b.BranchOutput))
		g.Branches = append(g.Branches, b)
	}

	return &g, nil
}

func (a *PostgresParallelExecutionRepositoryAdapter) UpdateBranchOutcome(
	ctx context.Context,
	tenantID string,
	groupID string,
	branchID string,
	state contracts.BranchState,
	output map[string]interface{},
	errMsg string,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	outBytes, _ := json.Marshal(output)
	res, err := a.db.ExecContext(
		ctx,
		queries.FLOW_UPDATE_BRANCH_OUTCOME,
		string(state), string(outBytes), errMsg, tenantID, groupID, branchID,
	)
	if err != nil {
		return errors.NewInternalError("failed updating branch outcome")
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return errors.NewNotFoundError("branch not found")
	}
	return nil
}

func (a *PostgresParallelExecutionRepositoryAdapter) UpdateGroupCAS(
	ctx context.Context,
	tenantID string,
	groupID string,
	expectedVersion int64,
	newState contracts.GroupState,
	succeededCount int,
	failedCount int,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	res, err := a.db.ExecContext(
		ctx,
		queries.FLOW_UPDATE_GROUP_CAS,
		string(newState), succeededCount, failedCount,
		tenantID, groupID, expectedVersion,
	)
	if err != nil {
		return errors.NewInternalError("failed to update parallel group")
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return errors.NewConflictError("concurrency conflict or group not found")
	}
	return nil
}
