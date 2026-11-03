package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	"distributed-transaction-engine/src/api/rest/v1/routers"
	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	sagaPorts "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	workflowDefinitions "distributed-transaction-engine/src/features/workflow-definitions"
	workflowPorts "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/infra/config"
	"distributed-transaction-engine/src/infra/database"
	"distributed-transaction-engine/src/infra/observability"
)

func parseFlags() (string, bool) {
	var configPath string
	var validateOnly bool
	flag.StringVar(&configPath, "config", "config/default.yaml", "path to configuration file")
	flag.BoolVar(&validateOnly, "validate-only", false, "validate configuration and desired state without starting")
	flag.Parse()
	return configPath, validateOnly
}

func initDatabase(ctx context.Context, cfg *config.AppConfig) (*sql.DB, sagaPorts.SagaRepositoryPort) {
	if cfg.Database.Host == "" {
		return nil, sagaAdapters.NewMemorySagaRepositoryAdapter()
	}

	db, err := sql.Open("postgres", cfg.ConnString())
	if err != nil {
		log.Printf("PostgreSQL open failed: %v\n", err)
		return nil, sagaAdapters.NewMemorySagaRepositoryAdapter()
	}

	db.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	db.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime())

	if err := db.Ping(); err != nil {
		log.Printf("PostgreSQL ping failed: %v\n", err)
		return nil, sagaAdapters.NewMemorySagaRepositoryAdapter()
	}

	if err := database.RunMigrations(ctx, db, cfg.Database.MigrationsDir); err != nil {
		log.Printf("Migration error: %v\n", err)
	}

	return db, sagaAdapters.NewPostgresSagaRepositoryAdapter(db)
}

func initWorkflowFeature() (workflowPorts.WorkflowDefinitionsRepositoryPort, *routers.WorkflowDefinitionsRouter) {
	repo := workflowDefinitions.NewMemoryAdapter()
	svc := workflowDefinitions.NewService(repo)
	handler := handlers.NewWorkflowDefinitionsHandler(svc)
	router := routers.NewWorkflowDefinitionsRouter(handler)
	return repo, router
}

func initStateFeature() *routers.TransactionStateMachineRouter {
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)
	handler := handlers.NewTransactionStateMachineHandler(svc)
	return routers.NewTransactionStateMachineRouter(handler)
}

func initSagaFeature(
	ctx context.Context,
	cfg *config.AppConfig,
	repo sagaPorts.SagaRepositoryPort,
	workflowRepo workflowPorts.WorkflowDefinitionsRepositoryPort,
) *routers.SagaRouter {
	dispatcher := sagaAdapters.NewHttpStepDispatcherAdapter()
	svc := sagaService.NewSagaOrchestrationService(repo, workflowRepo, dispatcher)
	handler := handlers.NewSagaHandler(svc)
	router := routers.NewSagaRouter(handler)

	poller := sagaService.NewSagaPoller(repo, svc, cfg.Poller.WorkerID, cfg.LeaseDuration())
	go poller.Start(ctx, cfg.PollInterval())

	return router
}

func buildAdminServer(cfg *config.AppConfig, db *sql.DB) *http.Server {
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"alive"}`))
	})
	adminMux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if db != nil {
			pingCtx, pingCancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer pingCancel()
			if err := db.PingContext(pingCtx); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"status":"database unreachable"}`))
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})

	return &http.Server{
		Addr:         cfg.Server.AdminBindAddress,
		Handler:      adminMux,
		ReadTimeout:  cfg.ReadTimeout(),
		WriteTimeout: cfg.WriteTimeout(),
	}
}

func buildPublicServer(
	cfg *config.AppConfig,
	sagaRouter *routers.SagaRouter,
	stateRouter *routers.TransactionStateMachineRouter,
	workflowRouter *routers.WorkflowDefinitionsRouter,
) *http.Server {
	publicMux := http.NewServeMux()
	sagaRouter.RegisterRoutes(publicMux)
	stateRouter.RegisterRoutes(publicMux)
	workflowRouter.RegisterRoutes(publicMux)

	publicHandler := observability.ObservabilityMiddleware(cfg.Server.Name, publicMux)

	return &http.Server{
		Addr:         cfg.Server.BindAddress,
		Handler:      publicHandler,
		ReadTimeout:  cfg.ReadTimeout(),
		WriteTimeout: cfg.WriteTimeout(),
		IdleTimeout:  cfg.IdleTimeout(),
	}
}

func startListeners(admin *http.Server, public *http.Server) <-chan error {
	fatalChan := make(chan error, 2)
	go func() {
		if err := admin.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatalChan <- fmt.Errorf("admin listener failed: %w", err)
		}
	}()
	go func() {
		if err := public.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatalChan <- fmt.Errorf("public listener failed: %w", err)
		}
	}()
	return fatalChan
}

func gracefulShutdown(deadline time.Duration, public *http.Server, admin *http.Server, db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	if err := public.Shutdown(ctx); err != nil {
		log.Printf("Public server shutdown error: %v\n", err)
	}
	if err := admin.Shutdown(ctx); err != nil {
		log.Printf("Admin server shutdown error: %v\n", err)
	}
	if db != nil {
		_ = db.Close()
	}
}

func main() {
	configPath, validateOnly := parseFlags()
	if validateOnly {
		fmt.Println("Configuration and desired state valid")
		os.Exit(0)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, sagaRepo := initDatabase(rootCtx, cfg)
	workflowRepo, workflowRouter := initWorkflowFeature()
	stateRouter := initStateFeature()
	sagaRouter := initSagaFeature(rootCtx, cfg, sagaRepo, workflowRepo)

	adminServer := buildAdminServer(cfg, db)
	publicServer := buildPublicServer(cfg, sagaRouter, stateRouter, workflowRouter)

	fatalChan := startListeners(adminServer, publicServer)
	log.Println("Server running (Phase 7: Run)")

	select {
	case <-rootCtx.Done():
		log.Println("Termination signal received, beginning Phase 8: Shutdown")
	case err := <-fatalChan:
		log.Printf("Fatal runtime error: %v, shutting down\n", err)
	}

	gracefulShutdown(cfg.ShutdownDeadline(), publicServer, adminServer, db)
	log.Println("Shutdown complete, exiting with code 0")
}
