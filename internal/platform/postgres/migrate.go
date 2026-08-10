package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
)

type Migration struct {
	Version string
	Name    string
	UpSQL   string
	DownSQL string
}

func LoadMigrations(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migration dir: %w", err)
	}

	byVersion := make(map[string]*Migration)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()
		direction := ""
		switch {
		case strings.HasSuffix(filename, ".up.sql"):
			direction = "up"
		case strings.HasSuffix(filename, ".down.sql"):
			direction = "down"
		default:
			continue
		}

		stem := strings.TrimSuffix(strings.TrimSuffix(filename, ".up.sql"), ".down.sql")
		parts := strings.SplitN(stem, "_", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid migration filename %q", filename)
		}
		version, name := parts[0], parts[1]
		migration := byVersion[version]
		if migration == nil {
			migration = &Migration{Version: version, Name: name}
			byVersion[version] = migration
		}
		if migration.Name != name {
			return nil, fmt.Errorf("migration version %s has conflicting names %q and %q", version, migration.Name, name)
		}

		sqlBytes, err := fs.ReadFile(fsys, path.Join(dir, filename))
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", filename, err)
		}
		if direction == "up" {
			migration.UpSQL = string(sqlBytes)
		} else {
			migration.DownSQL = string(sqlBytes)
		}
	}

	migrations := make([]Migration, 0, len(byVersion))
	for _, migration := range byVersion {
		if migration.UpSQL == "" || migration.DownSQL == "" {
			return nil, fmt.Errorf("migration %s_%s must include up and down SQL", migration.Version, migration.Name)
		}
		migrations = append(migrations, *migration)
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
	return migrations, nil
}

type Migrator struct {
	db *sql.DB
}

func NewMigrator(db *sql.DB) *Migrator {
	return &Migrator{db: db}
}

func (m *Migrator) Apply(ctx context.Context, migrations []Migration) error {
	if m.db == nil {
		return errors.New("database is required")
	}
	if _, err := m.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version text PRIMARY KEY,
	name text NOT NULL,
	applied_at timestamptz NOT NULL DEFAULT now()
)`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	for _, migration := range migrations {
		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", migration.Version, err)
		}
		var applied string
		err = tx.QueryRowContext(ctx, `SELECT version FROM schema_migrations WHERE version = $1`, migration.Version).Scan(&applied)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			tx.Rollback()
			return fmt.Errorf("check migration %s: %w", migration.Version, err)
		}
		if applied != "" {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit skipped migration %s: %w", migration.Version, err)
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, migration.UpSQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", migration.Version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version, name) VALUES ($1, $2)`, migration.Version, migration.Name); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %s: %w", migration.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", migration.Version, err)
		}
	}
	return nil
}

func (m *Migrator) Rollback(ctx context.Context, migrations []Migration, version string) error {
	if m.db == nil {
		return errors.New("database is required")
	}
	for i := len(migrations) - 1; i >= 0; i-- {
		migration := migrations[i]
		if migration.Version != version {
			continue
		}
		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin rollback %s: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, migration.DownSQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("rollback migration %s: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version); err != nil {
			tx.Rollback()
			return fmt.Errorf("delete migration %s: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit rollback %s: %w", version, err)
		}
		return nil
	}
	return fmt.Errorf("migration %s not found", version)
}
