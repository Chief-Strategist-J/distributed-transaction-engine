package database

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	rollbackSuffix   = ".rollback.sql"
	migrationLockKey = 727002
)

func RunMigrations(ctx context.Context, db *sql.DB, migrationsDir string) error {
	if migrationsDir == "" {
		migrationsDir = "database/migrations/sql"
	}
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		return nil
	}

	lockConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer lockConn.Close()
	if _, err = lockConn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return err
	}
	defer func() {
		_, _ = lockConn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockKey)
	}()

	createTableQuery := `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(255) PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`
	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		return err
	}

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") && !strings.HasSuffix(entry.Name(), rollbackSuffix) {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)

	for _, file := range files {
		var exists bool
		err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", file).Scan(&exists)
		if err != nil {
			return err
		}
		if exists {
			continue
		}

		content, err := os.ReadFile(filepath.Join(migrationsDir, file))
		if err != nil {
			return err
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			_ = tx.Rollback()
			return err
		}

		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", file); err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
