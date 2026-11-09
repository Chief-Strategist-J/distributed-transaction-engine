package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	queries "distributed-transaction-engine/src/features/transactional-outbox/infrastructure/queries"
	"distributed-transaction-engine/src/shared/errors"
)

type PostgresOutboxRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresOutboxRepositoryAdapter(db *sql.DB) *PostgresOutboxRepositoryAdapter {
	return &PostgresOutboxRepositoryAdapter{db: db}
}

func (a *PostgresOutboxRepositoryAdapter) InsertOutboxEventTx(ctx context.Context, tx *sql.Tx, event *contracts.OutboxEventInstance) error {
	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		return errors.NewInternalError("failed to serialize outbox payload")
	}

	headersBytes, err := json.Marshal(event.Headers)
	if err != nil {
		return errors.NewInternalError("failed to serialize outbox headers")
	}

	_, err = tx.ExecContext(
		ctx,
		queries.FLOW_INSERT_OUTBOX_EVENT,
		event.ID,
		event.PublicID,
		event.TenantID,
		event.SagaID,
		event.AggregateType,
		event.AggregateID,
		event.AggregateSequence,
		event.DestinationTopic,
		event.PartitionKey,
		event.EventType,
		string(payloadBytes),
		string(headersBytes),
	)
	if err != nil {
		return errors.NewConflictError("failed to insert outbox event: duplicate or constraint violation")
	}
	return nil
}

func (a *PostgresOutboxRepositoryAdapter) ClaimPendingEvents(ctx context.Context, input *contracts.ClaimOutboxEventsInput) ([]contracts.OutboxEventInstance, error) {
	leaseExpiry := time.Now().UTC().Add(input.LeaseDuration)

	rows, err := a.db.QueryContext(
		ctx,
		queries.FLOW_CLAIM_PENDING_OUTBOX_EVENTS,
		input.WorkerID,
		leaseExpiry,
		input.TenantID,
		input.Topic,
		input.BatchLimit,
	)
	if err != nil {
		return nil, errors.NewInternalError("failed to claim pending outbox events: " + err.Error())
	}
	defer rows.Close()

	var events []contracts.OutboxEventInstance
	for rows.Next() {
		var e contracts.OutboxEventInstance
		var payloadBytes, headersBytes []byte
		var leaseOwner, lastError sql.NullString
		var leaseExpiresAt, publishedAt sql.NullTime

		err := rows.Scan(
			&e.ID,
			&e.PublicID,
			&e.TenantID,
			&e.SagaID,
			&e.AggregateType,
			&e.AggregateID,
			&e.AggregateSequence,
			&e.DestinationTopic,
			&e.PartitionKey,
			&e.EventType,
			&payloadBytes,
			&headersBytes,
			&e.Status,
			&leaseOwner,
			&leaseExpiresAt,
			&e.AttemptCount,
			&lastError,
			&e.Version,
			&e.CreatedAt,
			&e.UpdatedAt,
			&publishedAt,
		)
		if err != nil {
			return nil, errors.NewInternalError("failed to scan claimed outbox event: " + err.Error())
		}

		_ = json.Unmarshal(payloadBytes, &e.Payload)
		_ = json.Unmarshal(headersBytes, &e.Headers)
		if leaseOwner.Valid {
			e.LeaseOwner = leaseOwner.String
		}
		if leaseExpiresAt.Valid {
			e.LeaseExpiresAt = &leaseExpiresAt.Time
		}
		if lastError.Valid {
			e.LastError = lastError.String
		}
		if publishedAt.Valid {
			e.PublishedAt = &publishedAt.Time
		}

		events = append(events, e)
	}

	return events, nil
}

func (a *PostgresOutboxRepositoryAdapter) ConfirmPublished(ctx context.Context, input *contracts.ConfirmOutboxPublishInput) error {
	res, err := a.db.ExecContext(
		ctx,
		queries.FLOW_CONFIRM_OUTBOX_PUBLISHED,
		input.TenantID,
		input.EventID,
		input.WorkerID,
	)
	if err != nil {
		return errors.NewInternalError("failed to confirm outbox published: " + err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError("failed to inspect rows affected")
	}
	if rows == 0 {
		return errors.NewConflictError("stale lease or invalid status on outbox confirm")
	}
	return nil
}

func (a *PostgresOutboxRepositoryAdapter) ReleaseInFlightToPending(ctx context.Context, tenantID string, eventID string, reason string) error {
	_, err := a.db.ExecContext(ctx, queries.FLOW_RELEASE_IN_FLIGHT_TO_PENDING, reason, tenantID, eventID)
	if err != nil {
		return errors.NewInternalError("failed to release outbox event: " + err.Error())
	}
	return nil
}

func (a *PostgresOutboxRepositoryAdapter) ParkEvent(ctx context.Context, input *contracts.ParkOutboxEventInput) error {
	_, err := a.db.ExecContext(ctx, queries.FLOW_PARK_OUTBOX_EVENT, input.Reason, input.TenantID, input.EventID, input.WorkerID)
	if err != nil {
		return errors.NewInternalError("failed to park outbox event: " + err.Error())
	}
	return nil
}

func (a *PostgresOutboxRepositoryAdapter) GetEvent(ctx context.Context, tenantID string, eventID string) (*contracts.OutboxEventInstance, error) {
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_OUTBOX_EVENT, tenantID, eventID)

	var e contracts.OutboxEventInstance
	var payloadBytes, headersBytes []byte
	var leaseOwner, lastError sql.NullString
	var leaseExpiresAt, publishedAt sql.NullTime

	err := row.Scan(
		&e.ID,
		&e.PublicID,
		&e.TenantID,
		&e.SagaID,
		&e.AggregateType,
		&e.AggregateID,
		&e.AggregateSequence,
		&e.DestinationTopic,
		&e.PartitionKey,
		&e.EventType,
		&payloadBytes,
		&headersBytes,
		&e.Status,
		&leaseOwner,
		&leaseExpiresAt,
		&e.AttemptCount,
		&lastError,
		&e.Version,
		&e.CreatedAt,
		&e.UpdatedAt,
		&publishedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("outbox event not found")
		}
		return nil, errors.NewInternalError("failed to fetch outbox event: " + err.Error())
	}

	_ = json.Unmarshal(payloadBytes, &e.Payload)
	_ = json.Unmarshal(headersBytes, &e.Headers)
	if leaseOwner.Valid {
		e.LeaseOwner = leaseOwner.String
	}
	if leaseExpiresAt.Valid {
		e.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if lastError.Valid {
		e.LastError = lastError.String
	}
	if publishedAt.Valid {
		e.PublishedAt = &publishedAt.Time
	}

	return &e, nil
}
