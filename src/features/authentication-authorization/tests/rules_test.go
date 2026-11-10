package tests

import (
	"net/http"
	"testing"
	"time"

	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/features/authentication-authorization/domain/rules"
)

func TestEvaluate5LayerAuthorization_PermissionDenied(t *testing.T) {
	cmd := contracts.AuthorizeRequestCommand{
		Auth: contracts.AuthContext{
			PrincipalID: "user-1",
			TenantID:    "tenant-1",
			Scopes:      []string{"tx:view"},
			Roles:       []string{"viewer"},
		},
		ResourceTenantID: "tenant-1",
		ResourceID:       "saga-123",
		ResourceOwnerID:  "user-1",
		Action:           "tx:cancel",
	}

	res := rules.Evaluate5LayerAuthorization(cmd)
	if res.Decision != contracts.AuthDecisionDeny || res.HTTPStatus != http.StatusForbidden {
		t.Fatalf("expected 403 DENY, got %d %s", res.HTTPStatus, res.Decision)
	}
	if res.ReasonCode != "PERMISSION_DENIED" {
		t.Fatalf("expected PERMISSION_DENIED, got %s", res.ReasonCode)
	}
}

func TestEvaluate5LayerAuthorization_CrossTenantReturns404(t *testing.T) {
	cmd := contracts.AuthorizeRequestCommand{
		Auth: contracts.AuthContext{
			PrincipalID: "user-1",
			TenantID:    "tenant-alpha",
			Scopes:      []string{"tx:cancel"},
			Roles:       []string{"standard_user"},
		},
		ResourceTenantID: "tenant-bravo",
		ResourceID:       "saga-999",
		ResourceOwnerID:  "user-1",
		Action:           "tx:cancel",
	}

	res := rules.Evaluate5LayerAuthorization(cmd)
	if res.HTTPStatus != http.StatusNotFound {
		t.Fatalf("expected anti-leakage 404, got %d", res.HTTPStatus)
	}
	if res.ReasonCode != "RESOURCE_NOT_FOUND" {
		t.Fatalf("expected RESOURCE_NOT_FOUND, got %s", res.ReasonCode)
	}
}

func TestEvaluate5LayerAuthorization_IntraTenantIDORReturns404(t *testing.T) {
	cmd := contracts.AuthorizeRequestCommand{
		Auth: contracts.AuthContext{
			PrincipalID: "user-attacker",
			TenantID:    "tenant-alpha",
			Scopes:      []string{"tx:cancel"},
			Roles:       []string{"standard_user"},
		},
		ResourceTenantID: "tenant-alpha",
		ResourceID:       "saga-colleague",
		ResourceOwnerID:  "user-victim",
		Action:           "tx:cancel",
	}

	res := rules.Evaluate5LayerAuthorization(cmd)
	if res.HTTPStatus != http.StatusNotFound {
		t.Fatalf("expected 404 on intra-tenant IDOR attempt, got %d", res.HTTPStatus)
	}
}

func TestEvaluate5LayerAuthorization_PostPivotCancelReturns409(t *testing.T) {
	cmd := contracts.AuthorizeRequestCommand{
		Auth: contracts.AuthContext{
			PrincipalID: "user-1",
			TenantID:    "tenant-alpha",
			Scopes:      []string{"tx:cancel"},
			Roles:       []string{"standard_user"},
		},
		ResourceTenantID: "tenant-alpha",
		ResourceID:       "saga-123",
		ResourceOwnerID:  "user-1",
		Action:           "tx:cancel",
		IsPostPivot:      true,
	}

	res := rules.Evaluate5LayerAuthorization(cmd)
	if res.HTTPStatus != http.StatusConflict {
		t.Fatalf("expected 409 Conflict post-pivot, got %d", res.HTTPStatus)
	}
	if res.ReasonCode != "POST_PIVOT_CANCEL_FORBIDDEN" {
		t.Fatalf("expected POST_PIVOT_CANCEL_FORBIDDEN, got %s", res.ReasonCode)
	}
}

func TestEvaluate5LayerAuthorization_DualApprovalControls(t *testing.T) {
	cmd := contracts.AuthorizeRequestCommand{
		Auth: contracts.AuthContext{
			PrincipalID: "operator-1",
			TenantID:    "tenant-alpha",
			Scopes:      []string{"*"},
			Roles:       []string{"sre_operator"},
		},
		ResourceTenantID: "tenant-alpha",
		ResourceID:       "saga-high-val",
		Action:           "tx:force_complete",
		Amount:           75000.0,
		ApproverID:       "",
	}

	res := rules.Evaluate5LayerAuthorization(cmd)
	if res.HTTPStatus != http.StatusForbidden || res.ReasonCode != "DUAL_APPROVAL_REQUIRED" {
		t.Fatalf("expected DUAL_APPROVAL_REQUIRED, got %d %s", res.HTTPStatus, res.ReasonCode)
	}

	cmd.ApproverID = "operator-1"
	res = rules.Evaluate5LayerAuthorization(cmd)
	if res.HTTPStatus != http.StatusForbidden || res.ReasonCode != "SELF_APPROVAL_FORBIDDEN" {
		t.Fatalf("expected SELF_APPROVAL_FORBIDDEN, got %d %s", res.HTTPStatus, res.ReasonCode)
	}

	cmd.ApproverID = "operator-2"
	res = rules.Evaluate5LayerAuthorization(cmd)
	if res.Decision != contracts.AuthDecisionAllow || res.HTTPStatus != http.StatusOK {
		t.Fatalf("expected ALLOW with distinct approver, got %d %s", res.HTTPStatus, res.Decision)
	}
}

func TestEvaluate5LayerAuthorization_StepUpRequired(t *testing.T) {
	cmd := contracts.AuthorizeRequestCommand{
		Auth: contracts.AuthContext{
			PrincipalID:    "operator-1",
			TenantID:       "tenant-alpha",
			Scopes:         []string{"*"},
			Roles:          []string{"sre_operator"},
			StepUpVerified: false,
		},
		ResourceTenantID: "tenant-alpha",
		ResourceID:       "dlq-record-1",
		Action:           "tx:redrive",
	}

	res := rules.Evaluate5LayerAuthorization(cmd)
	if res.HTTPStatus != http.StatusUnauthorized || res.ReasonCode != "STEP_UP_MFA_REQUIRED" {
		t.Fatalf("expected STEP_UP_MFA_REQUIRED, got %d %s", res.HTTPStatus, res.ReasonCode)
	}

	cmd.Auth.StepUpVerified = true
	res = rules.Evaluate5LayerAuthorization(cmd)
	if res.Decision != contracts.AuthDecisionAllow {
		t.Fatalf("expected ALLOW with step-up verified, got %s", res.Decision)
	}
}

func TestAPIKeySecretValidation(t *testing.T) {
	secret := "raw-secret-key-token-value-12345"
	hash := rules.HashAPIKeySecret(secret)

	if !rules.ValidateAPIKeySecret(secret, hash) {
		t.Fatal("expected valid secret to match stored hash")
	}

	if rules.ValidateAPIKeySecret("wrong-secret", hash) {
		t.Fatal("expected wrong secret to fail comparison")
	}
}

func TestWebhookSignatureValidation(t *testing.T) {
	secret := []byte("provider-secret-hmac-key")
	payload := []byte(`{"event":"payment_confirmed","amount":100}`)
	now := time.Now().UTC()
	nonce := "nonce-998877"

	sig := rules.ComputeWebhookSignature(secret, payload, now, nonce)
	if !rules.ValidateWebhookSignature(secret, payload, now, nonce, sig) {
		t.Fatal("expected valid signature to match")
	}

	tamperedPayload := []byte(`{"event":"payment_confirmed","amount":999}`)
	if rules.ValidateWebhookSignature(secret, tamperedPayload, now, nonce, sig) {
		t.Fatal("expected tampered payload signature to fail")
	}

	skewedTime := now.Add(-10 * time.Minute)
	if rules.ValidateWebhookTimestampTolerance(skewedTime, now, 5*time.Minute) {
		t.Fatal("expected skewed timestamp beyond 5m to fail tolerance check")
	}
}

func TestSPIFFEMethodPolicyValidation(t *testing.T) {
	spiffe := "spiffe://prod.internal/ns/default/sa/payment-worker"
	methods := []string{"Redrive", "Cancel"}

	if !rules.ValidateSPIFFEMethodPolicy(spiffe, "Redrive", methods) {
		t.Fatal("expected Redrive to be allowed")
	}
	if rules.ValidateSPIFFEMethodPolicy(spiffe, "ForceComplete", methods) {
		t.Fatal("expected ForceComplete to be denied by default")
	}
}

