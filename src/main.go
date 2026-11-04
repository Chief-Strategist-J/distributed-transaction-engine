package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	"distributed-transaction-engine/src/api/rest/v1/routers"
	crashRecovery "distributed-transaction-engine/src/features/crash-recovery"
	crashRecoveryContracts "distributed-transaction-engine/src/features/crash-recovery/contracts"
	crashRecoveryRules "distributed-transaction-engine/src/features/crash-recovery/domain/rules"
	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	sagaPorts "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	workflowDefinitions "distributed-transaction-engine/src/features/workflow-definitions"
	workflowPorts "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/infra/chassis"
)

func main() {
	os.Exit(chassis.Run(describeService(), os.Args[1:], os.Environ()))
}

func describeService() chassis.ServiceSpec {
	return chassis.ServiceSpec{
		Identity: chassis.Identity{
			Name: "distributed-transaction-engine",
			Team: "core-platform",
			Tier: "tier-1",
		},
		Needs: chassis.Needs{
			Database: true,
			Cache:    false,
			Broker:   false,
		},
		DependsOn: []chassis.DownstreamDependency{},
		Contracts: chassis.Contracts{
			APISchemas:     []string{"contracts/openapi/v1.yml"},
			ProducedTopics: []string{},
			ConsumedTopics: []string{},
		},
		BuildServices: func(deps chassis.Dependencies) (chassis.Services, error) {
			var sagaRepo sagaPorts.SagaRepositoryPort = sagaAdapters.NewMemorySagaRepositoryAdapter()
			if deps.DB != nil {
				sagaRepo = sagaAdapters.NewPostgresSagaRepositoryAdapter(deps.DB)
			}

			var workflowRepo workflowPorts.WorkflowDefinitionsRepositoryPort = workflowDefinitions.NewMemoryAdapter()
			if deps.DB != nil {
				workflowRepo = workflowDefinitions.NewPostgresAdapter(deps.DB)
			}
			workflowSvc := workflowDefinitions.NewService(workflowRepo)
			workflowHandler := handlers.NewWorkflowDefinitionsHandler(workflowSvc)
			workflowRouter := routers.NewWorkflowDefinitionsRouter(workflowHandler)

			var stateRepo transactionStateMachine.RepositoryPort = transactionStateMachine.NewMemoryAdapter()
			if deps.DB != nil {
				stateRepo = transactionStateMachine.NewPostgresAdapter(deps.DB)
			}
			stateSvc := transactionStateMachine.NewService(stateRepo)
			stateHandler := handlers.NewTransactionStateMachineHandler(stateSvc)
			stateRouter := routers.NewTransactionStateMachineRouter(stateHandler)

			dispatcher := sagaAdapters.NewHttpStepDispatcherAdapter()
			sagaSvc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)
			sagaHandler := handlers.NewSagaHandler(sagaSvc)
			sagaRouter := routers.NewSagaRouter(sagaHandler)

			bootUUID := uuid.New().String()
			recoveryCfg := crashRecoveryContracts.CrashRecoveryConfig{
				IncarnationID:      crashRecoveryRules.BuildIncarnationID(deps.Config.Poller.WorkerID, bootUUID),
				ScanInterval:       2 * time.Second,
				BatchSize:          50,
				LeaseDuration:      deps.Config.LeaseDuration(),
				MaxTakeovers:       5,
				HeartbeatInterval:  10 * time.Second,
				SelfFenceMargin:    5 * time.Second,
				MaxConcurrentOwned: 100,
			}
			var recoveryRepo crashRecovery.CrashRecoveryRepositoryPort
			if deps.DB != nil {
				recoveryRepo = crashRecovery.NewPostgresAdapter(deps.DB)
			}
			recoverySvc := crashRecovery.NewService(recoveryCfg, recoveryRepo, sagaSvc)
			recoveryHandler := handlers.NewCrashRecoveryHandler(recoverySvc)
			recoveryRouter := routers.NewCrashRecoveryRouter(recoveryHandler)

			poller := sagaService.NewSagaPoller(sagaRepo, sagaSvc, deps.Config.Poller.WorkerID, deps.Config.LeaseDuration())

			return chassis.Services{
				Instances: map[string]any{
					"workflowRouter": workflowRouter,
					"stateRouter":    stateRouter,
					"sagaRouter":     sagaRouter,
					"recoveryRouter": recoveryRouter,
					"poller":         poller,
					"recoverySvc":    recoverySvc,
					"pollInterval":   deps.Config.PollInterval(),
					"hasDB":          deps.DB != nil,
				},
			}, nil
		},
		RegisterAPIRoutes: func(router *http.ServeMux, services chassis.Services) {
			if wr, ok := services.Instances["workflowRouter"].(*routers.WorkflowDefinitionsRouter); ok && wr != nil {
				wr.RegisterRoutes(router)
			}
			if sr, ok := services.Instances["stateRouter"].(*routers.TransactionStateMachineRouter); ok && sr != nil {
				sr.RegisterRoutes(router)
			}
			if sgr, ok := services.Instances["sagaRouter"].(*routers.SagaRouter); ok && sgr != nil {
				sgr.RegisterRoutes(router)
			}
			if rr, ok := services.Instances["recoveryRouter"].(*routers.CrashRecoveryRouter); ok && rr != nil {
				rr.RegisterRoutes(router)
			}
		},
		Jobs: func(services chassis.Services) []chassis.JobSpec {
			var jobs []chassis.JobSpec
			poller, hasPoller := services.Instances["poller"].(*sagaService.SagaPoller)
			interval, hasInterval := services.Instances["pollInterval"].(time.Duration)
			if hasPoller && hasInterval {
				jobs = append(jobs, chassis.JobSpec{
					Name: "saga-poller",
					Start: func(ctx context.Context) error {
						poller.Start(ctx, interval)
						return nil
					},
					Drain: func(_ context.Context) error {
						return nil
					},
				})
			}

			hasDB, _ := services.Instances["hasDB"].(bool)
			recoverySvc, hasRecovery := services.Instances["recoverySvc"].(*crashRecovery.CrashRecoveryService)
			if hasDB && hasRecovery && recoverySvc != nil {
				jobs = append(jobs, chassis.JobSpec{
					Name: "crash-recovery",
					Start: func(ctx context.Context) error {
						recoverySvc.Start(ctx)
						return nil
					},
					Drain: func(_ context.Context) error {
						return nil
					},
				})
			}
			return jobs
		},
	}
}
