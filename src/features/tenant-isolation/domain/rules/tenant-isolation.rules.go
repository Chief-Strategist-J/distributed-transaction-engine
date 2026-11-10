package rules

import (
	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	MaxStoredTenantIDLength = 64
	MaxWebhookURLLength     = 2048
	WebhookRequiredPort     = "443"
	WebhookRequiredHTTP     = "https"
)

type reasonKey string

const (
	reasonKeyNone    reasonKey = "NONE"
	reasonKeyBilling reasonKey = "BILLING"
	reasonKeyAbuse   reasonKey = "ABUSE"
)

type admissionScope struct {
	status contracts.TenantStatus
	reason reasonKey
}

var admitAll = map[contracts.CommandKind]contracts.AdmissionDecision{
	contracts.CommandKindNewCommand:          {Action: contracts.AdmissionActionAdmit},
	contracts.CommandKindInFlightContinue:    {Action: contracts.AdmissionActionAdmit},
	contracts.CommandKindMoneyOwedToTenant:   {Action: contracts.AdmissionActionAdmit},
	contracts.CommandKindScheduledJob:        {Action: contracts.AdmissionActionAdmit},
	contracts.CommandKindPartnerInboundEvent: {Action: contracts.AdmissionActionAdmit},
}

var admissionTable = map[admissionScope]map[contracts.CommandKind]contracts.AdmissionDecision{
	{status: contracts.TenantStatusActive, reason: reasonKeyNone}: admitAll,
	{status: contracts.TenantStatusSuspended, reason: reasonKeyBilling}: {
		contracts.CommandKindNewCommand:          {Action: contracts.AdmissionActionReject, ReasonCode: constants.ErrCodeTenantSuspendedBilling},
		contracts.CommandKindInFlightContinue:    {Action: contracts.AdmissionActionAdmit},
		contracts.CommandKindMoneyOwedToTenant:   {Action: contracts.AdmissionActionAdmit},
		contracts.CommandKindScheduledJob:        {Action: contracts.AdmissionActionPause, ReasonCode: constants.ErrCodeTenantSuspendedBilling},
		contracts.CommandKindPartnerInboundEvent: {Action: contracts.AdmissionActionAdmit},
	},
	{status: contracts.TenantStatusSuspended, reason: reasonKeyAbuse}: {
		contracts.CommandKindNewCommand:          {Action: contracts.AdmissionActionReject, ReasonCode: constants.ErrCodeTenantSuspendedAbuse},
		contracts.CommandKindInFlightContinue:    {Action: contracts.AdmissionActionPause, ReasonCode: constants.ErrCodeTenantSuspendedAbuse},
		contracts.CommandKindMoneyOwedToTenant:   {Action: contracts.AdmissionActionAdmit},
		contracts.CommandKindScheduledJob:        {Action: contracts.AdmissionActionPause, ReasonCode: constants.ErrCodeTenantSuspendedAbuse},
		contracts.CommandKindPartnerInboundEvent: {Action: contracts.AdmissionActionPark, ReasonCode: constants.ErrCodeTenantSuspendedAbuse},
	},
	{status: contracts.TenantStatusOffboarding, reason: reasonKeyNone}: {
		contracts.CommandKindNewCommand:          {Action: contracts.AdmissionActionReject, ReasonCode: constants.ErrCodeTenantOffboarding},
		contracts.CommandKindInFlightContinue:    {Action: contracts.AdmissionActionAdmit},
		contracts.CommandKindMoneyOwedToTenant:   {Action: contracts.AdmissionActionAdmit},
		contracts.CommandKindScheduledJob:        {Action: contracts.AdmissionActionPause, ReasonCode: constants.ErrCodeTenantOffboarding},
		contracts.CommandKindPartnerInboundEvent: {Action: contracts.AdmissionActionPark, ReasonCode: constants.ErrCodeTenantOffboarding},
	},
	{status: contracts.TenantStatusOffboarded, reason: reasonKeyNone}: {
		contracts.CommandKindNewCommand:          {Action: contracts.AdmissionActionReject, ReasonCode: constants.ErrCodeTenantOffboarded},
		contracts.CommandKindInFlightContinue:    {Action: contracts.AdmissionActionReject, ReasonCode: constants.ErrCodeTenantOffboarded},
		contracts.CommandKindMoneyOwedToTenant:   {Action: contracts.AdmissionActionReject, ReasonCode: constants.ErrCodeTenantOffboarded},
		contracts.CommandKindScheduledJob:        {Action: contracts.AdmissionActionReject, ReasonCode: constants.ErrCodeTenantOffboarded},
		contracts.CommandKindPartnerInboundEvent: {Action: contracts.AdmissionActionQuarantine, ReasonCode: constants.ErrCodeTenantOffboarded},
	},
}

var webhookDeniedHostSuffixes = []string{
	".localhost", ".local", ".internal", ".localdomain", ".lan", ".home.arpa", ".intranet", ".corp",
}

var webhookBlockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

var legacyNumericHostLabel = regexp.MustCompile(`^(0x[0-9a-f]+|[0-9]+)$`)

func ResolveTenant(input contracts.ResolveTenantInput) (spanResult0 contracts.ResolvedTenant, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if input.PrincipalTenantID == "" {
		return contracts.ResolvedTenant{}, appErrors.NewForbiddenError("authenticated principal carries no tenant")
	}
	if input.RequestedTenantID == nil {
		return contracts.ResolvedTenant{TenantID: input.PrincipalTenantID}, nil
	}
	memberships := buildMembershipSet(input.PrincipalTenantID, input.VerifiedMemberships)
	if _, verified := memberships[*input.RequestedTenantID]; verified {
		return contracts.ResolvedTenant{TenantID: *input.RequestedTenantID}, nil
	}
	return contracts.ResolvedTenant{}, appErrors.NewNotFoundError("tenant not found")
}

func IsCrossTenantReference(actorTenantID string, referencedTenantID string) bool {
	defer observability.TraceEnter(nil)()
	return actorTenantID != referencedTenantID
}

func EvaluateAdmission(status contracts.TenantStatus, reason *contracts.SuspensionReason, kind contracts.CommandKind) (spanResult0 contracts.AdmissionDecision, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	byKind, scopeDefined := admissionTable[admissionScope{status: status, reason: toReasonKey(reason)}]
	if !scopeDefined {
		return contracts.AdmissionDecision{}, appErrors.NewInternalError("no admission policy for tenant status " + string(status))
	}
	decision, kindDefined := byKind[kind]
	if !kindDefined {
		return contracts.AdmissionDecision{}, appErrors.NewInternalError("no admission policy for command kind " + string(kind))
	}
	return decision, nil
}

func EvaluateMessageTenant(envelope contracts.MessageTenantEnvelope, status *contracts.TenantStatus) contracts.MessageTenantDecision {
	defer observability.TraceEnter(nil)()
	if !isMessageTenantConsistent(envelope) {
		return quarantineMessage(contracts.QuarantineReasonIntegrityMismatch)
	}
	if status == nil {
		return quarantineMessage(contracts.QuarantineReasonUnknownTenant)
	}
	if *status == contracts.TenantStatusOffboarded {
		return quarantineMessage(contracts.QuarantineReasonOffboardedTenant)
	}
	return contracts.MessageTenantDecision{Proceed: true, TenantID: envelope.EnvelopeTenantID}
}

func EvaluateOperatorTenantAccess(grant contracts.OperatorAccessGrant, tenantID string, now time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(grant.OperatorID) == "" || strings.TrimSpace(grant.Reason) == "" || strings.TrimSpace(grant.TicketID) == "" {
		return appErrors.NewForbiddenError("operator access grant requires operator, reason and ticket")
	}
	if now.Before(grant.IssuedAt) || !now.Before(grant.ExpiresAt) {
		return appErrors.NewForbiddenError("operator access grant is outside its validity window")
	}
	for _, scoped := range grant.TenantScope {
		if scoped == tenantID {
			return nil
		}
	}
	return appErrors.NewForbiddenError("operator access grant does not cover the requested tenant")
}

func RejectionError(decision contracts.AdmissionDecision) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if decision.Action != contracts.AdmissionActionReject {
		return nil
	}
	return appErrors.NewTenantRejectedError(decision.ReasonCode, "tenant is not accepting this request")
}

func BoundTenantIDForStorage(tenantID *string) *string {
	defer observability.TraceEnter(nil)()
	if tenantID == nil || *tenantID == "" || len(*tenantID) > MaxStoredTenantIDLength || *tenantID != strings.TrimSpace(*tenantID) {
		return nil
	}
	return tenantID
}

func ValidateWebhookEndpoint(raw string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	parsed, err := parseWebhookURL(raw)
	if err != nil {
		return err
	}
	host := parsed.Hostname()
	if address, isAddress := parseStrictAddress(host); isAddress {
		return rejectBlockedAddress(address)
	}
	return rejectInternalHostname(host)
}

func parseWebhookURL(raw string) (*url.URL, error) {
	if raw == "" || len(raw) > MaxWebhookURLLength {
		return nil, appErrors.NewValidationError("webhook url must be 1..2048 characters")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != WebhookRequiredHTTP || parsed.Host == "" {
		return nil, appErrors.NewValidationError("webhook url must be an absolute https url")
	}
	if parsed.User != nil || (parsed.Port() != "" && parsed.Port() != WebhookRequiredPort) {
		return nil, appErrors.NewValidationError("webhook url must not carry credentials or a non-443 port")
	}
	return parsed, nil
}

func parseStrictAddress(host string) (netip.Addr, bool) {
	address, err := netip.ParseAddr(host)
	if err == nil && address.Zone() == "" {
		return address.Unmap(), true
	}
	return netip.Addr{}, false
}

func rejectBlockedAddress(address netip.Addr) error {
	for _, blocked := range webhookBlockedPrefixes {
		if blocked.Contains(address) {
			return appErrors.NewValidationError("webhook url must not target an internal address")
		}
	}
	return nil
}

func rejectInternalHostname(host string) error {
	lowered := strings.ToLower(host)
	if lowered == "" || strings.HasSuffix(lowered, ".") || lowered == "localhost" || !strings.Contains(lowered, ".") {
		return appErrors.NewValidationError("webhook url must target a fully qualified public hostname")
	}
	if isLegacyNumericHost(lowered) || hasDeniedHostSuffix(lowered) {
		return appErrors.NewValidationError("webhook url must not target an internal or ambiguous host")
	}
	return nil
}

func isLegacyNumericHost(host string) bool {
	for _, label := range strings.Split(host, ".") {
		if !legacyNumericHostLabel.MatchString(label) {
			return false
		}
	}
	return true
}

func hasDeniedHostSuffix(host string) bool {
	for _, suffix := range webhookDeniedHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

func isMessageTenantConsistent(envelope contracts.MessageTenantEnvelope) bool {
	if envelope.EnvelopeTenantID == nil || envelope.KeyTenantID == nil || envelope.PayloadTenantID == nil {
		return false
	}
	if contracts.ValidateTenantID("envelopeTenantId", *envelope.EnvelopeTenantID) != nil {
		return false
	}
	if *envelope.KeyTenantID != *envelope.EnvelopeTenantID || *envelope.PayloadTenantID != *envelope.EnvelopeTenantID {
		return false
	}
	return envelope.AggregateTenantID == nil || *envelope.AggregateTenantID == *envelope.EnvelopeTenantID
}

func quarantineMessage(reason contracts.QuarantineReason) contracts.MessageTenantDecision {
	return contracts.MessageTenantDecision{Proceed: false, QuarantineReason: &reason}
}

func buildMembershipSet(principalTenantID string, verifiedMemberships []string) map[string]struct{} {
	memberships := make(map[string]struct{}, len(verifiedMemberships)+1)
	memberships[principalTenantID] = struct{}{}
	for _, membership := range verifiedMemberships {
		memberships[membership] = struct{}{}
	}
	return memberships
}

func toReasonKey(reason *contracts.SuspensionReason) reasonKey {
	if reason == nil {
		return reasonKeyNone
	}
	return reasonKey(*reason)
}
