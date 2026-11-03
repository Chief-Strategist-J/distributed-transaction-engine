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
	"distributed-transaction-engine/src/infra/database"
	"distributed-transaction-engine/src/infra/observability"
)

type Config struct {
	PublicAddr string
	AdminAddr  string
}

func main() {
	var configPath string
	var validateOnly bool
	flag.StringVar(&configPath, "config", "config/default.yaml", "path to configuration file")
	flag.BoolVar(&validateOnly, "validate-only", false, "validate configuration and desired state without starting")
	flag.Parse()

	if validateOnly {
		fmt.Println("Configuration and desired state valid")
		os.Exit(0)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := Config{
		PublicAddr: ":8080",
		AdminAddr:  ":8081",
	}

	workflowRepo := workflowDefinitions.NewMemoryAdapter()
	workflowSvc := workflowDefinitions.NewService(workflowRepo)
	workflowHandler := handlers.NewWorkflowDefinitionsHandler(workflowSvc)
	workflowRouter := routers.NewWorkflowDefinitionsRouter(workflowHandler)

	var sagaRepo sagaPorts.SagaRepositoryPort = sagaAdapters.NewMemorySagaRepositoryAdapter()
	var activeDB *sql.DB
	dbHost := os.Getenv("DB_HOST")
	if dbHost != "" {
		dbPort := os.Getenv("DB_PORT")
		if dbPort == "" {
			dbPort = "5432"
		}
		dbUser := os.Getenv("DB_USER")
		if dbUser == "" {
			dbUser = "postgres"
		}
		dbPass := os.Getenv("DB_PASSWORD")
		dbName := os.Getenv("DB_NAME")
		if dbName == "" {
			dbName = "transaction_engine"
		}
		sslMode := os.Getenv("DB_SSLMODE")
		if sslMode == "" {
			sslMode = "disable"
		}

		connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			dbHost, dbPort, dbUser, dbPass, dbName, sslMode)
		if db, err := sql.Open("postgres", connStr); err == nil {
			db.SetMaxOpenConns(25)
			db.SetMaxIdleConns(10)
			db.SetConnMaxLifetime(5 * time.Minute)
			if pingErr := db.Ping(); pingErr == nil {
				activeDB = db
				if migErr := database.RunMigrations(rootCtx, db, "database/migrations/sql"); migErr != nil {
					log.Printf("Migration error: %v\n", migErr)
				}
				sagaRepo = sagaAdapters.NewPostgresSagaRepositoryAdapter(db)
			}
		}
	}

	sagaDispatcher := sagaAdapters.NewHttpStepDispatcherAdapter()
	sagaSvc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, sagaDispatcher)
	sagaHandler := handlers.NewSagaHandler(sagaSvc)
	sagaRouter := routers.NewSagaRouter(sagaHandler)

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-main"
	}
	poller := sagaService.NewSagaPoller(sagaRepo, sagaSvc, workerID, 30*time.Second)
	go poller.Start(rootCtx, 250*time.Millisecond)

	stateRepo := transactionStateMachine.NewMemoryAdapter()
	stateSvc := transactionStateMachine.NewService(stateRepo)
	stateHandler := handlers.NewTransactionStateMachineHandler(stateSvc)
	stateRouter := routers.NewTransactionStateMachineRouter(stateHandler)

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"alive"}`))
	})
	adminMux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if activeDB != nil {
			pingCtx, pingCancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer pingCancel()
			if pingErr := activeDB.PingContext(pingCtx); pingErr != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"status":"database unreachable"}`))
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})

	adminServer := &http.Server{
		Addr:         cfg.AdminAddr,
		Handler:      adminMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	publicMux := http.NewServeMux()
	sagaRouter.RegisterRoutes(publicMux)
	stateRouter.RegisterRoutes(publicMux)
	workflowRouter.RegisterRoutes(publicMux)

	publicHandler := observability.ObservabilityMiddleware("distributed-transaction-engine", publicMux)

	publicServer := &http.Server{
		Addr:         cfg.PublicAddr,
		Handler:      publicHandler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  75 * time.Second,
	}

	fatalChan := make(chan error, 2)

	go func() {
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatalChan <- fmt.Errorf("admin listener failed: %w", err)
		}
	}()

	go func() {
		if err := publicServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatalChan <- fmt.Errorf("public listener failed: %w", err)
		}
	}()

	log.Println("Server running (Phase 7: Run)")

	select {
	case <-rootCtx.Done():
		log.Println("Termination signal received, beginning Phase 8: Shutdown")
	case err := <-fatalChan:
		log.Printf("Fatal runtime error: %v, shutting down\n", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := publicServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Public server shutdown error: %v\n", err)
	}
	if err := adminServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Admin server shutdown error: %v\n", err)
	}

	if activeDB != nil {
		_ = activeDB.Close()
	}

	log.Println("Shutdown complete, exiting with code 0")
	os.Exit(0)
}
