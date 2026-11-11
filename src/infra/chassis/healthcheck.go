package chassis

import (
	"context"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"distributed-transaction-engine/src/infra/config"
)

const healthcheckTimeout = 3 * time.Second

func healthcheckURL(cfg *config.AppConfig) string {
	_, port, err := net.SplitHostPort(cfg.Server.AdminBindAddress)
	if err != nil {
		port = strings.TrimPrefix(cfg.Server.AdminBindAddress, ":")
	}
	path := cfg.Endpoints.HealthLive
	if path == "" {
		path = "/health/live"
	}
	return "http://" + net.JoinHostPort("127.0.0.1", port) + path
}

func runHealthcheck(cfg *config.AppConfig) int {
	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, healthcheckURL(cfg), nil)
	if err != nil {
		log.Printf("Healthcheck request invalid: %v\n", err)
		return ExitRuntime
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Printf("Healthcheck failed: %v\n", err)
		return ExitRuntime
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		log.Printf("Healthcheck returned HTTP %d\n", response.StatusCode)
		return ExitRuntime
	}
	return ExitSuccess
}
