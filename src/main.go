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
	sequentialExecution "distributed-transaction-engine/src/features/sequential-execution"
	sequentialMachines "distributed-transaction-engine/src/features/sequential-execution/domain/machines"
	parallelExecution "distributed-transaction-engine/src/features/parallel-execution"
	parallelMachines "distributed-transaction-engine/src/features/parallel-execution/domain/machines"
	inboxDeduplication "distributed-transaction-engine/src/features/inbox-deduplication"
	inboxAdapters "distributed-transaction-engine/src/features/inbox-deduplication/infrastructure/adapters"
	idempotencyKeys "distributed-transaction-engine/src/features/idempotency-keys"
	retryPolicies "distributed-transaction-engine/src/features/retry-policies"
	fencingTokens "distributed-transaction-engine/src/features/fencing-tokens"
	compensationRetry "distributed-transaction-engine/src/features/compensation-retry"
	transactionalOutbox "distributed-transaction-engine/src/features/transactional-outbox"
	outboxMachines "distributed-transaction-engine/src/features/transactional-outbox/domain/machines"
	compensationEngine "distributed-transaction-engine/src/features/compensation-engine"
	compensationMachines "distributed-transaction-engine/src/features/compensation-engine/domain/machines"
	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	workflowDefinitions "distributed-transaction-engine/src/features/workflow-definitions"
	workflowPorts "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/infra/chassis"
	"distributed-transaction-engine/src/infra/observability"
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

			var seqRepo sequentialExecution.RepositoryPort = sequentialExecution.NewMemoryAdapter()
			if deps.DB != nil {
				seqRepo = sequentialExecution.NewPostgresAdapter(deps.DB)
			}
			seqDispatcher := sequentialExecution.NewMemoryDispatcher()
			seqSvc := sequentialExecution.NewService(sequentialExecution.ServiceDeps{
				Repo:         seqRepo,
				WorkflowRepo: workflowRepo,
				Dispatcher:   seqDispatcher,
				Machine:      sequentialMachines.NewSequentialExecutionMachine(),
				Tracer:       observability.NewTracer("sequential-execution"),
			})
			seqHandler := handlers.NewSequentialExecutionHandler(seqSvc)
			seqRouter := routers.NewSequentialExecutionRouter(seqHandler)

			var parRepo parallelExecution.RepositoryPort = parallelExecution.NewMemoryAdapter()
			if deps.DB != nil {
				parRepo = parallelExecution.NewPostgresAdapter(deps.DB)
			}
			parDispatcher := parallelExecution.NewMemoryDispatcher()
			parSvc := parallelExecution.NewService(parallelExecution.ServiceDeps{
				Repo:       parRepo,
				Dispatcher: parDispatcher,
				Machine:    parallelMachines.NewParallelExecutionMachine(),
				Tracer:     observability.NewTracer("parallel-execution"),
			})
			parHandler := handlers.NewParallelExecutionHandler(parSvc)
			parRouter := routers.NewParallelExecutionRouter(parHandler)

			var inbRepo inboxDeduplication.RepositoryPort = inboxDeduplication.NewMemoryAdapter()
			if deps.DB != nil {
				inbRepo = inboxAdapters.NewPostgresInboxDeduplicationRepositoryAdapter(deps.DB)
			}
			inbSvc := inboxDeduplication.NewService(inboxDeduplication.ServiceDeps{
				Repo:   inbRepo,
				Tracer: observability.NewTracer("inbox-deduplication"),
			})
			inbHandler := handlers.NewInboxDeduplicationHandler(inbSvc)
			inbRouter := routers.NewInboxDeduplicationRouter(inbHandler)

			var idkRepo idempotencyKeys.RepositoryPort = idempotencyKeys.NewMemoryAdapter()
			if deps.DB != nil {
				idkRepo = idempotencyKeys.NewPostgresAdapter(deps.DB)
			}
			idkSvc := idempotencyKeys.NewService(idempotencyKeys.ServiceDeps{
				Repo:   idkRepo,
				Tracer: observability.NewTracer("idempotency-keys"),
			})
			idkHandler := handlers.NewIdempotencyKeysHandler(idkSvc)
			idkRouter := routers.NewIdempotencyKeysRouter(idkHandler)

			var retryRepo retryPolicies.RepositoryPort = retryPolicies.NewMemoryAdapter()
			if deps.DB != nil {
				retryRepo = retryPolicies.NewPostgresAdapter(deps.DB)
			}
			retrySvc := retryPolicies.NewService(retryPolicies.ServiceDeps{
				Repo:    retryRepo,
				Breaker: retryPolicies.NewCircuitBreakerManager(),
				Tracer:  observability.NewTracer("retry-policies"),
			})
			retryHandler := handlers.NewRetryPoliciesHandler(retrySvc)
			retryRouter := routers.NewRetryPoliciesRouter(retryHandler)

			var outboxRepo transactionalOutbox.RepositoryPort = transactionalOutbox.NewMemoryAdapter()
			if deps.DB != nil {
				outboxRepo = transactionalOutbox.NewPostgresAdapter(deps.DB)
			}
			var outboxPublisher transactionalOutbox.PublisherPort = transactionalOutbox.NewMemoryPublisher()
			kafkaBrokers := deps.Config.Kafka.Brokers
			if kafkaBrokers == "" {
				kafkaBrokers = os.Getenv("KAFKA_BROKERS")
			}
			if kafkaBrokers != "" {
				outboxPublisher = transactionalOutbox.NewKafkaPublisher(kafkaBrokers)
			}
			outboxSvc := transactionalOutbox.NewService(transactionalOutbox.ServiceDeps{
				Repo:      outboxRepo,
				Publisher: outboxPublisher,
				Machine:   outboxMachines.NewOutboxStateMachine(),
				Tracer:    observability.NewTracer("transactional-outbox"),
			})
			outboxHandler := handlers.NewOutboxHandler(outboxSvc)
			outboxRouter := routers.NewOutboxRouter(outboxHandler)

			var compRepo compensationEngine.RepositoryPort = compensationEngine.NewMemoryAdapter()
			if deps.DB != nil {
				compRepo = compensationEngine.NewPostgresAdapter(deps.DB)
			}
			compDispatcher := compensationEngine.NewMemoryDispatcher()
			compSvc := compensationEngine.NewService(compensationEngine.ServiceDeps{
				Repo:       compRepo,
				Dispatcher: compDispatcher,
				Machine:    compensationMachines.NewCompensationStateMachine(),
				Tracer:     observability.NewTracer("compensation-engine"),
			})
			compHandler := handlers.NewCompensationHandler(compSvc)
			compRouter := routers.NewCompensationRouter(compHandler)

			var fencingRepo fencingTokens.RepositoryPort = fencingTokens.NewMemoryAdapter()
			if deps.DB != nil {
				fencingRepo = fencingTokens.NewPostgresAdapter(deps.DB)
			}
			fencingSvc := fencingTokens.NewService(fencingTokens.ServiceDeps{
				Repo:    fencingRepo,
				Rules:   fencingTokens.NewFencingRules(),
				Machine: fencingTokens.NewFencingLeaseMachine(),
				Tracer:  observability.NewTracer("fencing-tokens"),
			})
			fencingHandler := handlers.NewFencingTokensHandler(fencingSvc)
			fencingRouter := routers.NewFencingTokensRouter(fencingHandler)

			var compRetryRepo compensationRetry.RepositoryPort = compensationRetry.NewMemoryAdapter()
			if deps.DB != nil {
				compRetryRepo = compensationRetry.NewPostgresAdapter(deps.DB)
			}
			compRetrySvc := compensationRetry.NewService(compensationRetry.ServiceDeps{
				Repo:    compRetryRepo,
				Rules:   compensationRetry.NewCompensationRetryRules(),
				Machine: compensationRetry.NewCompensationRetryMachine(),
				Tracer:  observability.NewTracer("compensation-retry"),
			})
			compRetryHandler := handlers.NewCompensationRetryHandler(compRetrySvc)
			compRetryRouter := routers.NewCompensationRetryRouter(compRetryHandler)

			poller := sagaService.NewSagaPoller(sagaRepo, sagaSvc, deps.Config.Poller.WorkerID, deps.Config.LeaseDuration())

			return chassis.Services{
				Instances: map[string]any{
					"workflowRouter":          workflowRouter,
					"stateRouter":             stateRouter,
					"sagaRouter":              sagaRouter,
					"recoveryRouter":          recoveryRouter,
					"sequentialRouter":        seqRouter,
					"parallelRouter":          parRouter,
					"inboxRouter":             inbRouter,
					"idempotencyRouter":       idkRouter,
					"retryRouter":             retryRouter,
					"outboxRouter":            outboxRouter,
					"compensationRouter":      compRouter,
					"fencingRouter":           fencingRouter,
					"compensationRetryRouter": compRetryRouter,
					"poller":                  poller,
					"recoverySvc":             recoverySvc,
					"pollInterval":            deps.Config.PollInterval(),
					"hasDB":                   deps.DB != nil,
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
			if sqr, ok := services.Instances["sequentialRouter"].(*routers.SequentialExecutionRouter); ok && sqr != nil {
				sqr.RegisterRoutes(router)
			}
			if parR, ok := services.Instances["parallelRouter"].(*routers.ParallelExecutionRouter); ok && parR != nil {
				parR.RegisterRoutes(router)
			}
			if inbR, ok := services.Instances["inboxRouter"].(*routers.InboxDeduplicationRouter); ok && inbR != nil {
				inbR.RegisterRoutes(router)
			}
			if idkR, ok := services.Instances["idempotencyRouter"].(*routers.IdempotencyKeysRouter); ok && idkR != nil {
				idkR.RegisterRoutes(router)
			}
			if rtr, ok := services.Instances["retryRouter"].(*routers.RetryPoliciesRouter); ok && rtr != nil {
				rtr.RegisterRoutes(router)
			}
			if oxR, ok := services.Instances["outboxRouter"].(*routers.OutboxRouter); ok && oxR != nil {
				oxR.RegisterRoutes(router)
			}
			if cmpR, ok := services.Instances["compensationRouter"].(*routers.CompensationRouter); ok && cmpR != nil {
				cmpR.RegisterRoutes(router)
			}
			if fnR, ok := services.Instances["fencingRouter"].(*routers.FencingTokensRouter); ok && fnR != nil {
				fnR.RegisterRoutes(router)
			}
			if crR, ok := services.Instances["compensationRetryRouter"].(*routers.CompensationRetryRouter); ok && crR != nil {
				crR.RegisterRoutes(router)
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
