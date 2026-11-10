package rules

import (
	tenantcontracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	tenantrules "distributed-transaction-engine/src/features/tenant-isolation/domain/rules"
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"strings"
	"time"
)

const (
	scopeView           = "tx:view"
	scopeViewTenantWide = "tx:view:tenant"
	scopeExport         = "tx:export"
	roleOperator        = "operator"
	roleFinanceAuditor  = "finance_auditor"
	roleTenantAdmin     = "tenant_admin"
	callerTypeOperator  = "OPERATOR"
	callerTypeInternal  = "INTERNAL_SERVICE"
	callerTypeAPIKey    = "API_KEY"
)

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func entitlementsFor(audience contracts.Audience) []contracts.EntitlementClass {
	switch audience {
	case contracts.AudienceOperator:
		return []contracts.EntitlementClass{contracts.EntitlementPublic, contracts.EntitlementInternal, contracts.EntitlementFinancial, contracts.EntitlementPII, contracts.EntitlementRestricted}
	case contracts.AudienceFinanceAuditor:
		return []contracts.EntitlementClass{contracts.EntitlementPublic, contracts.EntitlementFinancial, contracts.EntitlementInternal}
	case contracts.AudienceInternalService:
		return []contracts.EntitlementClass{contracts.EntitlementPublic, contracts.EntitlementInternal}
	}
	return []contracts.EntitlementClass{contracts.EntitlementPublic, contracts.EntitlementFinancial}
}

func deriveAudience(caller contracts.AuthenticatedCaller) contracts.Audience {
	switch {
	case caller.CallerType == callerTypeOperator || containsString(caller.Roles, roleOperator):
		return contracts.AudienceOperator
	case containsString(caller.Roles, roleFinanceAuditor):
		return contracts.AudienceFinanceAuditor
	case caller.CallerType == callerTypeInternal:
		return contracts.AudienceInternalService
	case caller.CallerType == callerTypeAPIKey:
		return contracts.AudienceTenantAPIClient
	}
	return contracts.AudienceTenantUser
}

func DerivePrincipal(caller contracts.AuthenticatedCaller) (spanResult0 contracts.Principal, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	audience := deriveAudience(caller)
	privileged := audience == contracts.AudienceOperator || audience == contracts.AudienceFinanceAuditor || audience == contracts.AudienceInternalService
	if strings.TrimSpace(caller.PrincipalID) == "" || (!privileged && !containsString(caller.Scopes, scopeView)) {
		return contracts.Principal{}, appErrors.NewForbiddenError("caller is not permitted to query transactions")
	}
	if len(caller.TeamIDs) > contracts.MaxTeamIDs {
		return contracts.Principal{}, appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "caller belongs to more than 50 teams")
	}
	scope := contracts.ScopeOwnedOrAssigned
	if privileged || containsString(caller.Scopes, scopeViewTenantWide) || containsString(caller.Roles, roleTenantAdmin) {
		scope = contracts.ScopeTenantWide
	}
	return contracts.Principal{
		PrincipalID: caller.PrincipalID, TenantID: caller.TenantID, Audience: audience, Entitlements: entitlementsFor(audience),
		RelationshipScope: scope, TeamIDs: caller.TeamIDs, Impersonation: caller.Impersonation,
	}, nil
}

func CanExport(caller contracts.AuthenticatedCaller) bool {
	defer observability.TraceEnter(nil)()
	return containsString(caller.Scopes, scopeExport) || containsString(caller.Roles, roleOperator) || containsString(caller.Roles, roleFinanceAuditor)
}

func HasEntitlement(principal contracts.Principal, class contracts.EntitlementClass) bool {
	defer observability.TraceEnter(nil)()
	if class == contracts.EntitlementPublic {
		return true
	}
	for _, granted := range principal.Entitlements {
		if granted == class {
			return true
		}
	}
	return false
}

func IsFieldEntitled(definition contracts.FieldDefinition, principal contracts.Principal) bool {
	defer observability.TraceEnter(nil)()
	if definition.OperatorOnly && principal.Audience != contracts.AudienceOperator {
		return false
	}
	return HasEntitlement(principal, definition.Entitlement)
}

func IsEnumerationSensitive(principal contracts.Principal) bool {
	defer observability.TraceEnter(nil)()
	return !HasEntitlement(principal, contracts.EntitlementInternal)
}

func EvaluateQueryTenant(principal contracts.Principal, requested *string, now time.Time) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if principal.Audience != contracts.AudienceOperator {
		if requested != nil && *requested != principal.TenantID {
			return "", appErrors.NewNotFoundError("tenant not found")
		}
		return principal.TenantID, nil
	}
	if requested == nil || principal.Impersonation == nil {
		return "", appErrors.NewForbiddenError("operator access to tenant data requires an impersonation session")
	}
	grant := tenantcontracts.OperatorAccessGrant{
		OperatorID: principal.Impersonation.OperatorID, TenantScope: principal.Impersonation.TenantScope, Reason: principal.Impersonation.Reason,
		TicketID: principal.Impersonation.TicketID, IssuedAt: principal.Impersonation.IssuedAt, ExpiresAt: principal.Impersonation.ExpiresAt,
	}
	if err := tenantrules.EvaluateOperatorTenantAccess(grant, *requested, now); err != nil {
		return "", err
	}
	return *requested, nil
}

func BuildInjectedPredicates(principal contracts.Principal, tenantID string) contracts.InjectedPredicates {
	defer observability.TraceEnter(nil)()
	elevated := principal.Audience == contracts.AudienceOperator || principal.Audience == contracts.AudienceInternalService
	injected := contracts.InjectedPredicates{
		TenantID:            tenantID,
		ExcludeInternalOnly: !elevated,
		ExcludeArchived:     !(elevated || principal.Audience == contracts.AudienceFinanceAuditor),
	}
	if principal.RelationshipScope == contracts.ScopeOwnedOrAssigned {
		injected.RestrictToOwner, injected.OwnerRef = true, principal.PrincipalID
		injected.TeamRefs = append([]string(nil), principal.TeamIDs...)
	}
	return injected
}

func RequiresAudit(principal contracts.Principal, uses []contracts.FieldUse, registry contracts.Registry, exporting bool) bool {
	defer observability.TraceEnter(nil)()
	if exporting || principal.Impersonation != nil || principal.Audience == contracts.AudienceOperator || principal.Audience == contracts.AudienceFinanceAuditor {
		return true
	}
	return len(ClassesTouched(uses, registry)) > 0
}

func ClassesTouched(uses []contracts.FieldUse, registry contracts.Registry) []string {
	defer observability.TraceEnter(nil)()
	seen := make(map[string]struct{})
	for _, use := range uses {
		definition, known := registry.Fields[use.Field]
		if !known {
			continue
		}
		if isAuditedClass(definition.Entitlement, use.Usage) {
			seen[string(definition.Entitlement)] = struct{}{}
		}
	}
	classes := make([]string, 0, len(seen))
	for class := range seen {
		classes = append(classes, class)
	}
	return sortedStrings(classes)
}

func isAuditedClass(class contracts.EntitlementClass, usage contracts.FieldUsageKind) bool {
	switch class {
	case contracts.EntitlementPII, contracts.EntitlementRestricted:
		return true
	case contracts.EntitlementFinancial:
		return usage != contracts.UsageProject
	}
	return false
}
