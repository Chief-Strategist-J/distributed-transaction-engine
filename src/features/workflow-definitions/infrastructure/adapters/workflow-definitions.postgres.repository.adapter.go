package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	ports "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/features/workflow-definitions/infrastructure/queries"
	"distributed-transaction-engine/src/shared/errors"
)

type PostgresWorkflowDefinitionsRepositoryAdapter struct {
	db *sql.DB
}

var _ ports.WorkflowDefinitionsRepositoryPort = (*PostgresWorkflowDefinitionsRepositoryAdapter)(nil)

func NewPostgresWorkflowDefinitionsRepositoryAdapter(db *sql.DB) *PostgresWorkflowDefinitionsRepositoryAdapter {
	return &PostgresWorkflowDefinitionsRepositoryAdapter{db: db}
}

func (a *PostgresWorkflowDefinitionsRepositoryAdapter) Save(ctx context.Context, def *contracts.WorkflowDefinitionInstance) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewInternalError(err.Error())
	}
	defer tx.Rollback()

	rawSpecJSON, err := json.Marshal(map[string]interface{}{
		"name":  def.Name,
		"steps": def.Steps,
	})
	if err != nil {
		return errors.NewValidationError("failed to serialize steps to JSON")
	}

	_, err = tx.ExecContext(
		ctx,
		queries.FlowInsertWorkflowDefinition,
		def.ID,
		def.PublicID,
		def.TenantID,
		def.Name,
		def.Version,
		def.Source,
		def.ExternalID,
		def.CorrelationID,
		def.SpecHash,
		def.Status,
		rawSpecJSON,
		def.TotalSteps,
		def.CreatedAt,
		def.UpdatedAt,
	)
	if err != nil {
		if strings.Contains(err.Error(), "unique") || strings.Contains(err.Error(), "duplicate") {
			return errors.NewConflictError(fmt.Sprintf("workflow definition conflict: %v", err))
		}
		return errors.NewInternalError(err.Error())
	}

	for _, step := range def.Steps {
		depJSON, err := json.Marshal(step.Dependencies)
		if err != nil {
			return errors.NewValidationError("failed to serialize step dependencies")
		}
		stepPublicID := fmt.Sprintf("stp_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:26])

		_, err = tx.ExecContext(
			ctx,
			queries.FlowInsertWorkflowDefinitionStep,
			uuid.New().String(),
			stepPublicID,
			def.TenantID,
			def.ID,
			step.StepID,
			step.StepName,
			step.IsPivot,
			step.ActionType,
			step.ActionEndpoint,
			step.CompensationAction,
			step.CompensationEndpoint,
			step.TimeoutSeconds,
			step.MaxRetries,
			depJSON,
			def.CreatedAt,
			def.UpdatedAt,
		)
		if err != nil {
			return errors.NewInternalError(err.Error())
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresWorkflowDefinitionsRepositoryAdapter) GetByID(ctx context.Context, tenantID string, id string) (*contracts.WorkflowDefinitionInstance, error) {
	row := a.db.QueryRowContext(ctx, queries.FlowSelectWorkflowDefinitionByID, tenantID, id)
	return a.scanWorkflowWithSteps(ctx, row)
}

func (a *PostgresWorkflowDefinitionsRepositoryAdapter) GetByNameAndVersion(ctx context.Context, tenantID string, name string, version int64) (*contracts.WorkflowDefinitionInstance, error) {
	row := a.db.QueryRowContext(ctx, queries.FlowSelectWorkflowDefinitionByNameAndVersion, tenantID, name, version)
	return a.scanWorkflowWithSteps(ctx, row)
}

func (a *PostgresWorkflowDefinitionsRepositoryAdapter) GetBySpecHash(ctx context.Context, tenantID string, name string, hash string) (*contracts.WorkflowDefinitionInstance, error) {
	row := a.db.QueryRowContext(ctx, queries.FlowSelectWorkflowDefinitionByHash, tenantID, name, hash)
	return a.scanWorkflowWithSteps(ctx, row)
}

func (a *PostgresWorkflowDefinitionsRepositoryAdapter) ListByTenant(ctx context.Context, tenantID string, limit int, offset int) ([]*contracts.WorkflowDefinitionInstance, error) {
	rows, err := a.db.QueryContext(ctx, queries.FlowListWorkflowDefinitionsByTenant, tenantID, limit, offset)
	if err != nil {
		return nil, errors.NewInternalError(err.Error())
	}
	defer rows.Close()

	var defs []*contracts.WorkflowDefinitionInstance
	for rows.Next() {
		var (
			d           contracts.WorkflowDefinitionInstance
			rawSpecJSON []byte
		)
		if err := rows.Scan(
			&d.ID,
			&d.PublicID,
			&d.TenantID,
			&d.Name,
			&d.Version,
			&d.Source,
			&d.ExternalID,
			&d.CorrelationID,
			&d.SpecHash,
			&d.Status,
			&rawSpecJSON,
			&d.TotalSteps,
			&d.CreatedAt,
			&d.UpdatedAt,
		); err != nil {
			return nil, errors.NewInternalError(err.Error())
		}
		defs = append(defs, &d)
	}
	return defs, nil
}

func (a *PostgresWorkflowDefinitionsRepositoryAdapter) scanWorkflowWithSteps(ctx context.Context, row *sql.Row) (*contracts.WorkflowDefinitionInstance, error) {
	var (
		d           contracts.WorkflowDefinitionInstance
		rawSpecJSON []byte
	)
	if err := row.Scan(
		&d.ID,
		&d.PublicID,
		&d.TenantID,
		&d.Name,
		&d.Version,
		&d.Source,
		&d.ExternalID,
		&d.CorrelationID,
		&d.SpecHash,
		&d.Status,
		&rawSpecJSON,
		&d.TotalSteps,
		&d.CreatedAt,
		&d.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("workflow definition not found")
		}
		return nil, errors.NewInternalError(err.Error())
	}

	stepRows, err := a.db.QueryContext(ctx, queries.FlowSelectWorkflowDefinitionSteps, d.TenantID, d.ID)
	if err != nil {
		return nil, errors.NewInternalError(err.Error())
	}
	defer stepRows.Close()

	for stepRows.Next() {
		var (
			step         contracts.StepDefinition
			stepID       string
			publicID     string
			tenantID     string
			wfID         string
			depJSON      []byte
			createdAtStr string
			updatedAtStr string
		)
		if err := stepRows.Scan(
			&stepID,
			&publicID,
			&tenantID,
			&wfID,
			&step.StepID,
			&step.StepName,
			&step.IsPivot,
			&step.ActionType,
			&step.ActionEndpoint,
			&step.CompensationAction,
			&step.CompensationEndpoint,
			&step.TimeoutSeconds,
			&step.MaxRetries,
			&depJSON,
			&createdAtStr,
			&updatedAtStr,
		); err != nil {
			return nil, errors.NewInternalError(err.Error())
		}
		if err := json.Unmarshal(depJSON, &step.Dependencies); err != nil {
			return nil, errors.NewInternalError("failed to unmarshal dependencies")
		}
		d.Steps = append(d.Steps, step)
	}

	return &d, nil
}

