package config

import (
	"database/sql"
	_ "embed"

	"github.com/GuiaBolso/darwin"
)

//go:embed migrations/base.sql
var baseMigration string

func runMigrations(db *sql.DB) error {
	return darwin.Migrate(
		darwin.NewGenericDriver(db, darwin.SqliteDialect{}),
		[]darwin.Migration{
			{
				Version:     1,
				Description: "base table definition to hold configuration variable",
				Script:      baseMigration,
			},
		},
		nil)
}
