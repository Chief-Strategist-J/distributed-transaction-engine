package database

import (
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	"distributed-transaction-engine/src/infra/config"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestDSNConnectsWithAPasswordThatWouldBreakAnUnquotedConnectionString(t *testing.T) {
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL is not set")
	}
	admin, err := sql.Open("postgres", base)
	if err != nil || admin.Ping() != nil {
		t.Skip("PostgreSQL is offline")
	}
	defer admin.Close()
	parsed, _ := url.Parse(base)
	role := "dsn_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	hostile := `p w'd\x sslmode=disable host=evil`
	if _, err = admin.Exec("CREATE ROLE " + role + " LOGIN PASSWORD '" + strings.ReplaceAll(hostile, "'", "''") + "'"); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec("DROP ROLE IF EXISTS " + role)
	cfg := config.DatabaseConfig{Host: parsed.Hostname(), Port: parsed.Port(), User: role, Password: hostile, Name: strings.TrimPrefix(parsed.Path, "/"), SSLMode: "disable"}
	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		t.Fatalf("a quoted DSN must authenticate with an awkward password: %v", err)
	}
}
