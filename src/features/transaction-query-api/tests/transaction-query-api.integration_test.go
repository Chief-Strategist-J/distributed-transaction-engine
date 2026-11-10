package tests

import (
	"context"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	queryapi "distributed-transaction-engine/src/features/transaction-query-api"
	"github.com/google/uuid"
)

func TestTransactionQueryAPI_SearchAndDetailIntegration(t *testing.T) {
	ctx := context.Background()
	memRepo := queryapi.NewMemoryAdapter()
	svc := queryapi.NewDefaultService(memRepo)

	now := time.Now().UTC()
	sagaID := uuid.New()
	txnPubID := "txn_11111111111111111111111111"

	record := contracts.ProjectionRecord{
		TenantID:           "tenant-1",
		PublicID:           txnPubID,
		SagaID:             sagaID,
		InternalStatus:     "COMPLETED",
		BusinessStatus:     "COMPLETED",
		CompensationStatus: "NONE",
		WorkflowType:       "order_fulfillment",
		WorkflowVersion:    "1.0.0",
		AmountMinor:        50000,
		Currency:           "INR",
		CustomerRef:        "cust_101",
		CreatedAt:          now,
		EffectiveAt:        now,
		CorrelationID:      uuid.New(),
	}

	seq, err := svc.UpdateTransactionProjection(ctx, record)
	if err != nil || seq <= 0 {
		t.Fatalf("failed to update transaction projection: %v", err)
	}

	principal := contracts.Principal{
		PrincipalID: "user-test",
		TenantID:    "tenant-1",
		Audience:    contracts.AudienceTenantUser,
	}

	searchResp, err := svc.SearchTransactions(ctx, principal, contracts.SearchRequest{})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(searchResp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(searchResp.Items))
	}

	detail, err := svc.GetTransaction(ctx, principal, contracts.GetTransactionRequest{
		TransactionID: txnPubID,
	})
	if err != nil {
		t.Fatalf("get transaction detail failed: %v", err)
	}
	if detail.Item["id"] != txnPubID {
		t.Fatalf("expected id %s, got %v", txnPubID, detail.Item["id"])
	}

	aggResp, err := svc.AggregateTransactions(ctx, principal, contracts.AggregateRequest{
		Exactness: contracts.ExactnessExact,
	})
	if err != nil {
		t.Fatalf("aggregate transactions failed: %v", err)
	}
	if len(aggResp.Groups) == 0 {
		t.Fatal("expected at least 1 aggregation group")
	}

	sq, err := svc.CreateSavedQuery(ctx, principal, contracts.CreateSavedQueryRequest{
		Name:       "completed_sagas",
		Visibility: contracts.VisibilityTeam,
	})
	if err != nil {
		t.Fatalf("failed to create saved query: %v", err)
	}
	if sq.PublicID == "" {
		t.Fatal("expected non-empty saved query public id")
	}

	savedList, err := svc.ListSavedQueries(ctx, principal)
	if err != nil || len(savedList) != 1 {
		t.Fatalf("expected 1 saved query, got %d err: %v", len(savedList), err)
	}

	queryText := "status = 'COMPLETED'"
	exportJob, err := svc.CreateExport(ctx, principal, contracts.ExportRequest{
		Text: &queryText,
	})
	if err != nil {
		t.Fatalf("failed to create export: %v", err)
	}
	if exportJob.PublicID == "" {
		t.Fatal("expected non-empty export job public id")
	}

	downloadLink, err := svc.CreateExportLink(ctx, principal, exportJob.PublicID)
	if err != nil || downloadLink.Token == "" {
		t.Fatalf("expected download link token, got %v err: %v", downloadLink, err)
	}
}
