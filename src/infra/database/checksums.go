package database

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const lockVersion = "1.0.0"

type SchemaLock struct {
	Version    string            `json:"version"`
	Migrations map[string]string `json:"migrations"`
}

func FileChecksum(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

func MigrationFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") && !strings.HasSuffix(entry.Name(), rollbackSuffix) {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}

func BuildSchemaLock(dir string) (SchemaLock, error) {
	files, err := MigrationFiles(dir)
	if err != nil {
		return SchemaLock{}, err
	}
	lock := SchemaLock{Version: lockVersion, Migrations: make(map[string]string, len(files))}
	for _, file := range files {
		content, readErr := os.ReadFile(filepath.Join(dir, file))
		if readErr != nil {
			return SchemaLock{}, readErr
		}
		lock.Migrations[file] = FileChecksum(content)
	}
	return lock, nil
}

func (l SchemaLock) Render() ([]byte, error) {
	rendered, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(rendered, '\n'), nil
}

func VerifySchemaLock(dir string, lockPath string) error {
	expected, err := BuildSchemaLock(dir)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(lockPath)
	if err != nil {
		return fmt.Errorf("read schema lock: %w", err)
	}
	var actual SchemaLock
	if err = json.Unmarshal(raw, &actual); err != nil {
		return fmt.Errorf("parse schema lock: %w", err)
	}
	return compareLocks(expected, actual)
}

func compareLocks(expected SchemaLock, actual SchemaLock) error {
	for file, checksum := range expected.Migrations {
		recorded, found := actual.Migrations[file]
		switch {
		case !found:
			return fmt.Errorf("migration %s is missing from schema.lock", file)
		case recorded != checksum:
			return fmt.Errorf("migration %s changed after it was locked", file)
		}
	}
	for file := range actual.Migrations {
		if _, found := expected.Migrations[file]; !found {
			return fmt.Errorf("schema.lock references %s which no longer exists", file)
		}
	}
	return nil
}
