package utils

import "context"

type tenantScopeKey struct{}

type TenantScope struct {
	TenantID string
	Platform bool
}

func WithTenant(ctx context.Context, tenantID string) context.Context {
	scope, _ := ctx.Value(tenantScopeKey{}).(TenantScope)
	scope.TenantID = tenantID
	return context.WithValue(ctx, tenantScopeKey{}, scope)
}

func WithPlatform(ctx context.Context) context.Context {
	scope, _ := ctx.Value(tenantScopeKey{}).(TenantScope)
	scope.Platform = true
	return context.WithValue(ctx, tenantScopeKey{}, scope)
}

func TenantScopeFrom(ctx context.Context) TenantScope {
	scope, _ := ctx.Value(tenantScopeKey{}).(TenantScope)
	return scope
}
