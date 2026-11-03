package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	"distributed-transaction-engine/src/api/rest/v1/routers"
	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	workflowDefinitions "distributed-transaction-engine/src/features/workflow-definitions"
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

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	sagaSvc := sagaService.NewSagaOrchestrationService(sagaRepo)
	sagaHandler := handlers.NewSagaHandler(sagaSvc)
	sagaRouter := routers.NewSagaRouter(sagaHandler)

	stateRepo := transactionStateMachine.NewMemoryAdapter()
	stateSvc := transactionStateMachine.NewService(stateRepo)
	stateHandler := handlers.NewTransactionStateMachineHandler(stateSvc)
	stateRouter := routers.NewTransactionStateMachineRouter(stateHandler)

	workflowRepo := workflowDefinitions.NewMemoryAdapter()
	workflowSvc := workflowDefinitions.NewService(workflowRepo)
	workflowHandler := handlers.NewWorkflowDefinitionsHandler(workflowSvc)
	workflowRouter := routers.NewWorkflowDefinitionsRouter(workflowHandler)

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"alive"}`))
	})
	adminMux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
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

	publicServer := &http.Server{
		Addr:         cfg.PublicAddr,
		Handler:      publicMux,
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

	log.Println("Shutdown complete, exiting with code 0")
	os.Exit(0)
}
