package chassis

import (
	"context"
	"database/sql"
	"net/http"

	"distributed-transaction-engine/src/infra/config"
)

type Identity struct {
	Name string
	Team string
	Tier string
}

type Needs struct {
	Database bool
	Cache    bool
	Broker   bool
}

type DownstreamDependency struct {
	ServiceName string
	Criticality string
}

type Contracts struct {
	APISchemas     []string
	ProducedTopics []string
	ConsumedTopics []string
}

type Dependencies struct {
	Context context.Context
	Config  *config.AppConfig
	DB      *sql.DB
}

type Services struct {
	Instances map[string]any
}

type JobSpec struct {
	Name  string
	Start func(ctx context.Context) error
	Drain func(ctx context.Context) error
}

type MigrationSet struct {
	Apply func(ctx context.Context, db *sql.DB, dir string) error
}

type ServiceSpec struct {
	Identity          Identity
	Needs             Needs
	DependsOn         []DownstreamDependency
	Contracts         Contracts
	BuildServices     func(deps Dependencies) (Services, error)
	RegisterAPIRoutes func(router *http.ServeMux, services Services)
	RegisterConsumers func(group any, services Services)
	Jobs              func(services Services) []JobSpec
	Migrations        func() MigrationSet
}
