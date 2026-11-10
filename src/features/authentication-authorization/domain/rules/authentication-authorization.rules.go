package rules

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"distributed-transaction-engine/src/features/authentication-authorization/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"
)

func Evaluate5LayerAuthorization(cmd contracts.AuthorizeRequestCommand) contracts.AuthorizeResult {
	defer observability.TraceEnter(nil)()
	if !hasRequiredPermission(cmd.Auth.Scopes, cmd.Auth.Roles, cmd.Action) {
		return contracts.AuthorizeResult{
			Decision:   contracts.AuthDecisionDeny,
			HTTPStatus: http.StatusForbidden,
			ReasonCode: "PERMISSION_DENIED",
			Message:    "caller lacks required scope or role for action",
		}
	}

	if cmd.Auth.TenantID != cmd.ResourceTenantID {
		return contracts.AuthorizeResult{
			Decision:   contracts.AuthDecisionDeny,
			HTTPStatus: http.StatusNotFound,
			ReasonCode: "RESOURCE_NOT_FOUND",
			Message:    "resource not found",
		}
	}

	if !isPlatformOperator(cmd.Auth.Roles) {
		if cmd.ResourceOwnerID != "" && cmd.Auth.PrincipalID != cmd.ResourceOwnerID {
			return contracts.AuthorizeResult{
				Decision:   contracts.AuthDecisionDeny,
				HTTPStatus: http.StatusNotFound,
				ReasonCode: "RESOURCE_NOT_FOUND",
				Message:    "resource not found",
			}
		}
	}

	if strings.EqualFold(cmd.Action, "tx:cancel") && cmd.IsPostPivot {
		return contracts.AuthorizeResult{
			Decision:   contracts.AuthDecisionDeny,
			HTTPStatus: http.StatusConflict,
			ReasonCode: "POST_PIVOT_CANCEL_FORBIDDEN",
			Message:    "transaction has passed its pivot point and cannot be cancelled",
		}
	}

	if cmd.Amount >= 10000.0 || strings.EqualFold(cmd.Action, "tx:force_complete") {
		if strings.TrimSpace(cmd.ApproverID) == "" {
			return contracts.AuthorizeResult{
				Decision:   contracts.AuthDecisionDeny,
				HTTPStatus: http.StatusForbidden,
				ReasonCode: "DUAL_APPROVAL_REQUIRED",
				Message:    "action requires a designated second approver",
			}
		}
		if cmd.ApproverID == cmd.Auth.PrincipalID {
			return contracts.AuthorizeResult{
				Decision:   contracts.AuthDecisionDeny,
				HTTPStatus: http.StatusForbidden,
				ReasonCode: "SELF_APPROVAL_FORBIDDEN",
				Message:    "requester cannot act as own approver",
			}
		}
	}

	if strings.EqualFold(cmd.Action, "tx:redrive") || strings.EqualFold(cmd.Action, "tx:manual_intervention") {
		if !cmd.Auth.StepUpVerified {
			return contracts.AuthorizeResult{
				Decision:   contracts.AuthDecisionDeny,
				HTTPStatus: http.StatusUnauthorized,
				ReasonCode: "STEP_UP_MFA_REQUIRED",
				Message:    "privileged operator action requires recent step-up authentication",
			}
		}
	}

	return contracts.AuthorizeResult{
		Decision:   contracts.AuthDecisionAllow,
		HTTPStatus: http.StatusOK,
		ReasonCode: "AUTHORIZED",
		Message:    "access granted",
	}
}

func hasRequiredPermission(scopes []string, roles []string, action string) bool {
	for _, r := range roles {
		if strings.EqualFold(r, "platform_admin") || strings.EqualFold(r, "sre_operator") {
			return true
		}
	}
	for _, s := range scopes {
		if strings.EqualFold(s, "*") || strings.EqualFold(s, action) {
			return true
		}
	}
	return false
}

func isPlatformOperator(roles []string) bool {
	for _, r := range roles {
		if strings.EqualFold(r, "platform_admin") || strings.EqualFold(r, "sre_operator") || strings.EqualFold(r, "auditor") {
			return true
		}
	}
	return false
}

func HashAPIKeySecret(secret string) string {
	defer observability.TraceEnter(nil)()
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func ValidateAPIKeySecret(providedSecret string, storedHash string) bool {
	defer observability.TraceEnter(nil)()
	computed := HashAPIKeySecret(providedSecret)
	return subtle.ConstantTimeCompare([]byte(computed), []byte(storedHash)) == 1
}

func ValidateSPIFFEMethodPolicy(spiffeID string, method string, allowedMethods []string) bool {
	defer observability.TraceEnter(nil)()
	if strings.TrimSpace(spiffeID) == "" || strings.TrimSpace(method) == "" {
		return false
	}
	for _, m := range allowedMethods {
		if m == "*" || strings.EqualFold(m, method) {
			return true
		}
	}
	return false
}

func ValidateWebhookTimestampTolerance(ts time.Time, now time.Time, maxTolerance time.Duration) bool {
	defer observability.TraceEnter(nil)()
	diff := math.Abs(now.Sub(ts).Seconds())
	return diff <= maxTolerance.Seconds()
}

func ComputeWebhookSignature(providerSecret []byte, payload []byte, timestamp time.Time, nonce string) string {
	defer observability.TraceEnter(nil)()
	mac := hmac.New(sha256.New, providerSecret)
	tsStr := fmt.Sprintf("%d", timestamp.UTC().Unix())
	msg := fmt.Sprintf("%s.%s.%s", tsStr, nonce, string(payload))
	mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}

func ValidateWebhookSignature(providerSecret []byte, payload []byte, timestamp time.Time, nonce string, providedSig string) bool {
	defer observability.TraceEnter(nil)()
	expected := ComputeWebhookSignature(providerSecret, payload, timestamp, nonce)
	return hmac.Equal([]byte(expected), []byte(providedSig))
}
