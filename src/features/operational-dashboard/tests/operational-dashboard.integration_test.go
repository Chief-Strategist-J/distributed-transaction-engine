package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	dashboard "distributed-transaction-engine/src/features/operational-dashboard"
	"distributed-transaction-engine/src/features/operational-dashboard/contracts"
	"distributed-transaction-engine/src/features/operational-dashboard/infrastructure/adapters"
	"github.com/google/uuid"
)

func TestOperationalDashboard_FullLifecycleIntegration(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewDashboardMemoryRepositoryAdapter()
	svc := dashboard.NewService(dashboard.DashboardServiceDeps{Repo: repo})

	repo.SetSagaState("tenant-1", "saga-100", 3, contracts.VisualStateRunning)

	tileSnap := &contracts.DashboardTileSnapshot{
		ID:              uuid.New(),
		PublicID:        "dts_11111111111111111111111111",
		TenantID:        "tenant-1",
		Source:          "METRICS_ENGINE",
		ExternalID:      "ext_tile_1",
		CorrelationID:   uuid.New(),
		TileKey:         "sagas_by_state",
		MetricsPayload:  json.RawMessage(`{"RUNNING": 12, "COMPLETED": 450}`),
		DataAgeMs:       10,
		IsStale:         false,
		AsOfVersion:     1,
		SourceTimestamp: time.Now().UTC(),
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
		Version:         1,
	}
	if err := repo.UpsertTileSnapshot(ctx, tileSnap); err != nil {
		t.Fatalf("failed to upsert tile snapshot: %v", err)
	}

	streamReq := &contracts.ConnectStreamRequest{
		PrincipalID: "admin-user",
		ScopeType:   contracts.ScopeTenantAdmin,
	}

	overview, err := svc.GetOverview(ctx, "tenant-1", streamReq, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("failed to get overview: %v", err)
	}
	if len(overview.Tiles) != 1 {
		t.Fatalf("expected 1 tile, got %d", len(overview.Tiles))
	}
	if overview.TenantScope != "tenant-1" {
		t.Fatalf("expected scope tenant-1, got %s", overview.TenantScope)
	}

	session, eventCh, err := svc.RegisterStreamSession(ctx, "tenant-1", streamReq, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("failed to register stream session: %v", err)
	}
	defer svc.UnregisterStreamSession(ctx, session.TenantID, session.PublicID)

	deltaEvent := contracts.StreamEvent{
		ID:           "evt-1",
		Event:        "SAGA_TRANSITION",
		Data:         json.RawMessage(`{"saga_id":"saga-100","state":"COMPLETED"}`),
		TimestampUTC: time.Now().UTC(),
	}
	svc.BroadcastDelta("tenant-1", deltaEvent)

	select {
	case received := <-eventCh:
		if received.ID != "evt-1" {
			t.Fatalf("expected evt-1, got %s", received.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for broadcast event")
	}

	previewReq := &contracts.PreviewActionRequest{
		SagaID:             "saga-100",
		ActionType:         contracts.ActionPause,
		CurrentSagaVersion: 3,
		OperatorID:         "operator-alice",
	}

	previewResp, err := svc.PreviewAction(ctx, "tenant-1", previewReq, "127.0.0.1")
	if err != nil {
		t.Fatalf("failed to preview action: %v", err)
	}
	if previewResp.SnapshotVersion != 3 {
		t.Fatalf("expected snapshot version 3, got %d", previewResp.SnapshotVersion)
	}

	execReq := &contracts.ExecuteActionRequest{
		PreviewID:       previewResp.PreviewID,
		InterventionID:  "intv-001",
		SnapshotVersion: 3,
		CSRFToken:       "valid-csrf-token",
		OperatorID:      "operator-alice",
	}

	execResp, err := svc.ExecuteAction(ctx, "tenant-1", execReq, "127.0.0.1")
	if err != nil {
		t.Fatalf("failed to execute action: %v", err)
	}
	if !execResp.Success {
		t.Fatal("expected execution success")
	}

	repo.SetSagaState("tenant-1", "saga-100", 4, contracts.VisualStateRunning)

	previewReq2 := &contracts.PreviewActionRequest{
		SagaID:             "saga-100",
		ActionType:         contracts.ActionResume,
		CurrentSagaVersion: 4,
		OperatorID:         "operator-alice",
	}
	previewResp2, err := svc.PreviewAction(ctx, "tenant-1", previewReq2, "127.0.0.1")
	if err != nil {
		t.Fatalf("failed to preview action 2: %v", err)
	}

	repo.SetSagaState("tenant-1", "saga-100", 5, contracts.VisualStateRunning)

	execReqMismatch := &contracts.ExecuteActionRequest{
		PreviewID:       previewResp2.PreviewID,
		InterventionID:  "intv-002",
		SnapshotVersion: 4,
		CSRFToken:       "valid-csrf-token",
		OperatorID:      "operator-alice",
	}
	_, err = svc.ExecuteAction(ctx, "tenant-1", execReqMismatch, "127.0.0.1")
	if err == nil {
		t.Fatal("expected version mismatch error when executing action on mutated saga")
	}
}

func TestOperationalDashboard_DualApproverForceCompensate(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewDashboardMemoryRepositoryAdapter()
	svc := dashboard.NewService(dashboard.DashboardServiceDeps{Repo: repo})

	repo.SetSagaState("tenant-1", "saga-failed-1", 1, contracts.VisualStateFailed)

	previewReq := &contracts.PreviewActionRequest{
		SagaID:             "saga-failed-1",
		ActionType:         contracts.ActionForceCompensate,
		CurrentSagaVersion: 1,
		OperatorID:         "operator-alice",
	}

	previewResp, err := svc.PreviewAction(ctx, "tenant-1", previewReq, "127.0.0.1")
	if err != nil {
		t.Fatalf("failed to preview force compensate: %v", err)
	}
	if !previewResp.RequiresSecondApprover {
		t.Fatal("expected force compensate to require second approver")
	}

	selfApproval := "operator-alice"
	execSelf := &contracts.ExecuteActionRequest{
		PreviewID:        previewResp.PreviewID,
		InterventionID:   "intv-self",
		SnapshotVersion:  1,
		CSRFToken:        "valid-csrf",
		OperatorID:       "operator-alice",
		SecondApproverID: &selfApproval,
	}
	_, err = svc.ExecuteAction(ctx, "tenant-1", execSelf, "127.0.0.1")
	if err == nil {
		t.Fatal("expected error preventing self second approval")
	}

	secondApprover := "operator-bob"
	execValid := &contracts.ExecuteActionRequest{
		PreviewID:        previewResp.PreviewID,
		InterventionID:   "intv-dual",
		SnapshotVersion:  1,
		CSRFToken:        "valid-csrf",
		OperatorID:       "operator-alice",
		SecondApproverID: &secondApprover,
	}
	execResp, err := svc.ExecuteAction(ctx, "tenant-1", execValid, "127.0.0.1")
	if err != nil {
		t.Fatalf("expected success with dual approver: %v", err)
	}
	if !execResp.Success {
		t.Fatal("expected successful execution")
	}
}

