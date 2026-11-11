package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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

	if err := ensureMigrationsTable(ctx, db); err != nil {
		return err
	}
	files, err := MigrationFiles(migrationsDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		content, err := os.ReadFile(filepath.Join(migrationsDir, file))
		if err != nil {
			return err
		}
		if err = applyMigration(ctx, db, file, content); err != nil {
			return err
		}
	}
	return nil
}

func ensureMigrationsTable(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(255) PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`,
		`ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum VARCHAR(64)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, db *sql.DB, file string, content []byte) error {
	checksum := FileChecksum(content)
	var recorded sql.NullString
	err := db.QueryRowContext(ctx, "SELECT checksum FROM schema_migrations WHERE version = $1", file).Scan(&recorded)
	if err == nil {
		return verifyApplied(ctx, db, file, checksum, recorded)
	}
	if err != sql.ErrNoRows {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, string(content)); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("apply %s: %w", file, err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)", file, checksum); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func verifyApplied(ctx context.Context, db *sql.DB, file string, checksum string, recorded sql.NullString) error {
	if !recorded.Valid {
		_, err := db.ExecContext(ctx, "UPDATE schema_migrations SET checksum = $1 WHERE version = $2 AND checksum IS NULL", checksum, file)
		return err
	}
	if recorded.String != checksum {
		return fmt.Errorf("migration %s was modified after it was applied (recorded %s, file %s); add a new migration instead of editing an applied one", file, recorded.String, checksum)
	}
	return nil
}
