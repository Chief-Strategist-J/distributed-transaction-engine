package adapters

import (
	"context"
	"database/sql"
	"encoding/json"

	contracts "distributed-transaction-engine/src/features/inbox-deduplication/contracts"
	queries "distributed-transaction-engine/src/features/inbox-deduplication/infrastructure/queries"
	"distributed-transaction-engine/src/shared/errors"
)

type PostgresInboxDeduplicationRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresInboxDeduplicationRepositoryAdapter(db *sql.DB) *PostgresInboxDeduplicationRepositoryAdapter {
	return &PostgresInboxDeduplicationRepositoryAdapter{db: db}
}

func (a *PostgresInboxDeduplicationRepositoryAdapter) InsertInboxRecord(ctx context.Context, msg *contracts.InboxMessage) (bool, error) {
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return false, errors.NewInternalError("failed to serialize payload")
	}

	res, err := a.db.ExecContext(
		ctx,
		queries.FLOW_INSERT_INBOX_RECORD,
		msg.ID, msg.PublicID, msg.TenantID, msg.Source, msg.ExternalID, msg.CorrelationID,
		msg.HandlerName, msg.HandlerGeneration, msg.ContentHash, string(msg.Status), msg.AttemptCount,
		msg.ProducerEventTime, string(payloadBytes), msg.Version, msg.CreatedAt, msg.UpdatedAt,
	)
	if err != nil {
		return false, errors.NewInternalError("database insert failed")
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, errors.NewInternalError("failed to verify rows affected")
	}

	return rows > 0, nil
}

func (a *PostgresInboxDeduplicationRepositoryAdapter) GetInboxRecord(
	ctx context.Context,
	tenantID string,
	handlerName string,
	handlerGeneration int,
	externalID string,
) (*contracts.InboxMessage, error) {
	row := a.db.QueryRowContext(
		ctx,
		queries.FLOW_GET_INBOX_RECORD,
		tenantID, handlerName, handlerGeneration, externalID,
	)

	var msg contracts.InboxMessage
	var statusStr string
	var payloadBytes []byte

	err := row.Scan(
		&msg.ID, &msg.PublicID, &msg.TenantID, &msg.Source, &msg.ExternalID, &msg.CorrelationID,
		&msg.HandlerName, &msg.HandlerGeneration, &msg.ContentHash, &statusStr, &msg.AttemptCount,
		&msg.ProducerEventTime, &payloadBytes, &msg.Version, &msg.CreatedAt, &msg.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("inbox message not found")
		}
		return nil, errors.NewInternalError("database query failed")
	}

	msg.Status = contracts.InboxMessageStatus(statusStr)
	msg.Payload = make(map[string]interface{})
	_ = json.Unmarshal(payloadBytes, &msg.Payload)

	return &msg, nil
}

func (a *PostgresInboxDeduplicationRepositoryAdapter) MarkQuarantined(ctx context.Context, tenantID string, id string, reason string) error {
	res, err := a.db.ExecContext(ctx, queries.FLOW_MARK_QUARANTINED, tenantID, id)
	if err != nil {
		return errors.NewInternalError("failed to mark record quarantined")
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return errors.NewInternalError("failed to inspect rows affected")
	}
	if rows == 0 {
		return errors.NewNotFoundError("inbox record to quarantine not found")
	}
	return nil
}

