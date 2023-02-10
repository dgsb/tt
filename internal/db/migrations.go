package db

import (
	"database/sql"
	_ "embed"

	"github.com/GuiaBolso/darwin"
)

//go:embed migrations/sqlite/01_base.sql
var sqliteBaseMigration string

//go:embed migrations/sqlite/02_add_timestamp.sql
var sqliteAddTimestamp string

//go:embed migrations/sqlite/03_add_uuid_key.sql
var sqliteAddUUIDKey string

//go:embed migrations/sqlite/04_sync_meta.sql
var sqliteAddSyncMeta string

//go:embed migrations/sqlite/05_immutable_interval_object.sql
var sqliteAddImmutableInterval string

func runSqliteMigrations(db *sql.DB) error {
	return darwin.Migrate(
		darwin.NewGenericDriver(db, darwin.SqliteDialect{}),
		[]darwin.Migration{
			{
				Version:     1,
				Description: "base table definition to hold configuration variable",
				Script:      sqliteBaseMigration,
			},
			{
				Version:     2,
				Description: "add timestamp on all tables",
				Script:      sqliteAddTimestamp,
			},
			{
				Version:     3,
				Description: "add uuid unique key as conflict free identifier",
				Script:      sqliteAddUUIDKey,
			},
			{
				Version:     4,
				Description: "add a synchronisation history table",
				Script:      sqliteAddSyncMeta,
			},
			{
				Version:     5,
				Description: "split intervals table in 3 immutable table",
				Script:      sqliteAddImmutableInterval,
			},
		},
		nil)
}

//go:embed migrations/postgres/01_base.sql
var postgresBaseMigration string

func runPostgresMigrations(db *sql.DB) error {
	return darwin.Migrate(
		darwin.NewGenericDriver(db, darwin.PostgresDialect{}),
		[]darwin.Migration{
			{
				Version:     1,
				Description: "base table definition to hold configuration variable",
				Script:      postgresBaseMigration,
			}, // This first migration for postgres encompass sqlite migration 1 to 3
		},
		nil)
}
