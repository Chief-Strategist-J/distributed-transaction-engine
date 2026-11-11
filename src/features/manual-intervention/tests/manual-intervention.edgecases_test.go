package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	manualintervention "distributed-transaction-engine/src/features/manual-intervention"
	"distributed-transaction-engine/src/features/manual-intervention/domain/rules"
	"distributed-transaction-engine/src/infra/auth"
	"github.com/google/uuid"
)

type operatorCase struct {
	name       string
	principal  *auth.Principal
	operatorID string
	role       string
	tenantID   string
	wantStatus int
}

func newInterventionService() *manualintervention.ManualInterventionService {
	return manualintervention.NewService(manualintervention.ServiceDeps{
		Repo:    manualintervention.NewMemoryAdapter(),
		Rules:   manualintervention.NewManualInterventionRules(),
		Machine: manualintervention.NewManualInterventionMachine(),
	})
}

func interventionBody(sagaID uuid.UUID, tenantID string, operatorID string, role string) string {
	payload := map[string]string{
		"tenantId": tenantID, "sagaId": sagaID.String(), "operatorId": operatorID, "operatorRole": role,
		"actionType": "FORCE_COMPLETE", "currentSagaState": "STUCK",
		"reason": "Bank confirmed out-of-band payment clearance", "approvalTicketId": "TICKET-1",
	}
	encoded, _ := json.Marshal(payload)
	return string(encoded)
}

func callIntervene(svc *manualintervention.ManualInterventionService, body string, principal *auth.Principal) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/manual-interventions", strings.NewReader(body))
	if principal != nil {
		request = request.WithContext(auth.WithPrincipal(request.Context(), *principal))
	}
	recorder := httptest.NewRecorder()
	handlers.NewManualInterventionHandler(svc).Intervene(recorder, request)
	return recorder
}

func TestEdgeCases_OperatorIdentityComesFromAuthenticatedPrincipal(t *testing.T) {
	granted := &auth.Principal{Subject: "usr_real", TenantID: "tenant-alpha", Roles: []string{rules.RequiredInterventionRole}}
	plain := &auth.Principal{Subject: "usr_real", TenantID: "tenant-alpha", Roles: []string{"standard_user"}}
	cases := []operatorCase{
		{"authorised_empty_operator_fields", granted, "", "", "", http.StatusOK},
		{"authorised_matching_fields", granted, "usr_real", rules.RequiredInterventionRole, "tenant-alpha", http.StatusOK},
		{"role_missing_from_token", plain, "", "", "", http.StatusForbidden},
		{"claims_role_not_in_token", plain, "usr_real", rules.RequiredInterventionRole, "", http.StatusForbidden},
		{"claims_other_operator", granted, "usr_someone_else", "", "", http.StatusForbidden},
		{"claims_other_tenant", granted, "", "", "tenant-victim", http.StatusForbidden},
		{"claims_unknown_role", granted, "", "root", "", http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := interventionBody(uuid.New(), tc.tenantID, tc.operatorID, tc.role)
			recorder := callIntervene(newInterventionService(), body, tc.principal)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("expected %d got %d: %s", tc.wantStatus, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestEdgeCases_AuditRecordsVerifiedOperator(t *testing.T) {
	svc := newInterventionService()
	sagaID := uuid.New()
	principal := &auth.Principal{Subject: "usr_real", TenantID: "tenant-alpha", Roles: []string{rules.RequiredInterventionRole}}
	if recorder := callIntervene(svc, interventionBody(sagaID, "", "", ""), principal); recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	audits, err := svc.ListAuditBySaga(context.Background(), "tenant-alpha", sagaID)
	if err != nil || len(audits) != 1 {
		t.Fatalf("expected one audit record, got %d err=%v", len(audits), err)
	}
	if audits[0].OperatorID != "usr_real" || audits[0].OperatorRole != rules.RequiredInterventionRole {
		t.Fatalf("audit must record the verified identity, got %q/%q", audits[0].OperatorID, audits[0].OperatorRole)
	}
}

func TestEdgeCases_UnauthenticatedModeKeepsBodyIdentity(t *testing.T) {
	body := interventionBody(uuid.New(), "tenant-alpha", "operator-42", rules.RequiredInterventionRole)
	if recorder := callIntervene(newInterventionService(), body, nil); recorder.Code != http.StatusOK {
		t.Fatalf("auth-disabled mode must keep working, got %d: %s", recorder.Code, recorder.Body.String())
	}
	body = interventionBody(uuid.New(), "tenant-alpha", "operator-42", "root")
	if recorder := callIntervene(newInterventionService(), body, nil); recorder.Code == http.StatusOK {
		t.Fatal("an unknown role must still be refused by the domain rules")
	}
}
