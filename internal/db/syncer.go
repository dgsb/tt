package db

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type SyncerConfig struct {
	Login        string
	Password     string
	Hostname     string
	Port         int
	DatabaseName string
}

func (cfg SyncerConfig) String() string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s",
		cfg.Login,
		cfg.Password,
		cfg.Hostname,
		cfg.Port,
		cfg.DatabaseName)
}

func setupSyncerDB(cfg SyncerConfig) (*sql.DB, error) {
	db, err := sql.Open("pgx", cfg.String())
	if err != nil {
		return nil, fmt.Errorf("cannot open syncer database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot validate syncer database connection: %w", err)
	}
	if err := runPostgresMigrations(db); err != nil {
		return nil, fmt.Errorf("cannot run schema migration on syncer database: %w", err)
	}

	return db, nil
}
