package chassis

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"distributed-transaction-engine/src/infra/config"
	"distributed-transaction-engine/src/infra/database"
	"distributed-transaction-engine/src/infra/observability"
)

type App struct {
	Config   *config.AppConfig
	DB       *sql.DB
	Services Services
	Health   *HealthCoordinator
	Spec     ServiceSpec
}

func Run(spec ServiceSpec, args []string, env []string) int {
	bootCtx, exitCode, err := Preflight(args, env, os.Stdout)
	if err != nil {
		log.Printf("Preflight error: %v\n", err)
		return exitCode
	}

	if bootCtx.ValidateOnly {
		fmt.Println("Configuration and desired state valid")
		return ExitSuccess
	}

	cfg, err := config.Load(bootCtx.ConfigPath)
	if err != nil {
		log.Printf("Configuration error: %v\n", err)
		return ExitConfig
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var db *sql.DB
	if spec.Needs.Database && cfg.Database.Host != "" {
		openedDB, dbErr := observability.OpenTracedPostgres(cfg.ConnString())
		if dbErr != nil {
			log.Printf("PostgreSQL open failed: %v\n", dbErr)
		} else {
			openedDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
			openedDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
			openedDB.SetConnMaxLifetime(cfg.ConnMaxLifetime())
			if pingErr := openedDB.Ping(); pingErr != nil {
				log.Printf("PostgreSQL ping failed: %v\n", pingErr)
			} else {
				db = openedDB
				if spec.Migrations != nil {
					mig := spec.Migrations()
					if mig.Apply != nil {
						if err := mig.Apply(rootCtx, db, cfg.Database.MigrationsDir); err != nil {
							log.Printf("Migration error: %v\n", err)
						}
					}
				} else {
					if err := database.RunMigrations(rootCtx, db, cfg.Database.MigrationsDir); err != nil {
						log.Printf("Migration error: %v\n", err)
					}
				}
			}
		}
	}

	shutdownTracer, err := observability.InitTracer(rootCtx, cfg.Server.Name)
	if err != nil {
		log.Printf("Warning: failed to initialize distributed tracing: %v\n", err)
	}

	health := NewHealthCoordinator(db, ParseBrokers(firstNonEmpty(cfg.Kafka.Brokers, os.Getenv("KAFKA_BROKERS")))...)
	deps := Dependencies{
		Context: rootCtx,
		Config:  cfg,
		DB:      db,
	}

	services, err := spec.BuildServices(deps)
	if err != nil {
		log.Printf("Dependency composition failed: %v\n", err)
		if db != nil {
			_ = db.Close()
		}
		return ExitDependency
	}

	app := &App{
		Config:   cfg,
		DB:       db,
		Services: services,
		Health:   health,
		Spec:     spec,
	}

	lifecycle := NewLifecycle()

	switch bootCtx.Command {
	case "serve":
		wireServe(app, lifecycle)
	case "migrate":
		if db != nil && spec.Migrations != nil {
			mig := spec.Migrations()
			if mig.Apply != nil {
				if err := mig.Apply(rootCtx, db, cfg.Database.MigrationsDir); err != nil {
					log.Printf("Migration failed: %v\n", err)
					_ = db.Close()
					return ExitDependency
				}
			}
		}
		if db != nil {
			_ = db.Close()
		}
		return ExitSuccess
	default:
		log.Printf("Unsupported command mode: %s\n", bootCtx.Command)
		if db != nil {
			_ = db.Close()
		}
		return ExitUsage
	}

	if spec.Jobs != nil {
		for _, job := range spec.Jobs(services) {
			j := job
			lifecycle.AddComponent(j.Name, j.Start, j.Drain)
		}
	}

	exitCode = runLongLived(app, lifecycle, rootCtx)

	if shutdownTracer != nil {
		tCtx, tCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = shutdownTracer(tCtx)
		tCancel()
	}

	if db != nil {
		_ = db.Close()
	}

	return exitCode
}

func wireServe(app *App, lifecycle *Lifecycle) {
	adminMux := http.NewServeMux()
	adminLivePath := app.Config.Endpoints.HealthLive
	if adminLivePath == "" {
		adminLivePath = "/health/live"
	}
	adminReadyPath := app.Config.Endpoints.HealthReady
	if adminReadyPath == "" {
		adminReadyPath = "/health/ready"
	}

	adminMux.HandleFunc(adminLivePath, app.Health.LiveHandler())
	adminMux.HandleFunc(adminReadyPath, app.Health.ReadyHandler())

	adminServer := &http.Server{
		Addr:         app.Config.Server.AdminBindAddress,
		Handler:      adminMux,
		ReadTimeout:  app.Config.ReadTimeout(),
		WriteTimeout: app.Config.WriteTimeout(),
	}

	lifecycle.AddComponent("admin-server", func(_ context.Context) error {
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("admin listener failed: %w", err)
		}
		return nil
	}, func(ctx context.Context) error {
		return adminServer.Shutdown(ctx)
	})

	publicMux := http.NewServeMux()
	if app.Spec.RegisterAPIRoutes != nil {
		app.Spec.RegisterAPIRoutes(publicMux, app.Services)
	}

	hideServerDetails := !app.Config.Server.ExposeErrorDetails && strings.EqualFold(os.Getenv("ENV"), "production")
	guarded := ErrorSanitizer(hideServerDetails, RequestSizeLimit(app.Config.MaxBodyLimit(), publicMux))
	publicHandler := observability.ObservabilityMiddleware(app.Config.Server.Name, guarded)

	handlerContext, cancelHandlers := context.WithCancel(context.Background())
	publicServer := &http.Server{
		Addr:         app.Config.Server.BindAddress,
		Handler:      publicHandler,
		BaseContext:  func(net.Listener) context.Context { return handlerContext },
		ReadTimeout:  app.Config.ReadTimeout(),
		WriteTimeout: app.Config.WriteTimeout(),
		IdleTimeout:  app.Config.IdleTimeout(),
	}

	lifecycle.AddComponent("public-server", func(_ context.Context) error {
		if err := publicServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("public listener failed: %w", err)
		}
		return nil
	}, func(ctx context.Context) error {
		return drainServer(ctx, publicServer, cancelHandlers)
	})
}

func drainServer(ctx context.Context, server *http.Server, cancelHandlers context.CancelFunc) error {
	err := server.Shutdown(ctx)
	if err != nil {
		cancelHandlers()
		_ = server.Close()
	}
	return err
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func runLongLived(app *App, lifecycle *Lifecycle, rootCtx context.Context) int {
	fatalChan := make(chan error, 4)
	lifecycle.StartAll(rootCtx, fatalChan)

	app.Health.SetReady(true)
	log.Printf("Server running (service: %s, tier: %s)\n", app.Spec.Identity.Name, app.Spec.Identity.Tier)

	var exitCode = ExitSuccess
	select {
	case <-rootCtx.Done():
		log.Println("Termination signal received, beginning shutdown")
	case err := <-fatalChan:
		log.Printf("Fatal runtime error: %v, shutting down\n", err)
		exitCode = ExitRuntime
	}

	app.Health.SetReady(false)

	if err := lifecycle.StopAll(app.Config.ShutdownDeadline()); err != nil {
		log.Printf("Shutdown finished with errors: %v\n", err)
	} else {
		log.Println("Shutdown complete, exiting with code 0")
	}

	return exitCode
}
