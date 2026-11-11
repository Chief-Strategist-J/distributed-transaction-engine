package utils

import "context"

type dispatchIdentityKey struct{}

type DispatchIdentity struct {
	SagaID        string
	FencingToken  int64
	IncarnationID string
}

func WithDispatchIdentity(ctx context.Context, identity DispatchIdentity) context.Context {
	return context.WithValue(ctx, dispatchIdentityKey{}, identity)
}

func DispatchIdentityFrom(ctx context.Context) (DispatchIdentity, bool) {
	identity, ok := ctx.Value(dispatchIdentityKey{}).(DispatchIdentity)
	return identity, ok
}
