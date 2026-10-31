package queries

const (
	FlowInsertSaga = "INSERT INTO sagas (id, tenant_id, state, version, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)"
	FlowSelectSagaByID = "SELECT id, tenant_id, state, version, created_at, updated_at FROM sagas WHERE tenant_id = $1 AND id = $2"
)
