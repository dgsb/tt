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

type intervalRow struct {
	ID             int
	UUID           string
	StartTimestamp int64
	StopTimestamp  sql.NullInt64
	CreatedAt      int64
	UpdatedAt      sql.NullInt64
	DeletedAt      sql.NullInt64
}

type intervalTagsRow struct {
	UUID      string
	Tag       string
	CreatedAt int64
	DeletedAt sql.NullInt64
}

// getNewLocalTags return all tags created since the last sync operation
func getNewLocalTags(tx *sql.Tx) ([]string, error) {
	var newLocalTags []string

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		)
		SELECT name
		FROM tags
		JOIN last_sync
			ON (last_timestamp IS NULL
				OR created_at >= last_timestamp)
		ORDER BY created_at, name`)
	if err != nil {
		return nil, fmt.Errorf("cannot query local tags table: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("cannot scan local tags row: %w", err)
		}
		newLocalTags = append(newLocalTags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse local tags table: %w", err)
	}

	return newLocalTags, nil
}

func getNewLocalIntervals(tx *sql.Tx) ([]intervalRow, error) {
	var newLocalIntervals []intervalRow

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		) 
		SELECT id, uuid, start_timestamp, stop_timestamp, created_at, updated_at, deleted_at
		FROM intervals
		JOIN last_sync
			ON (last_timestamp IS NULL
				OR created_at >= last_timestamp
				OR updated_at >= last_timestamp
				OR deleted_at >= last_timestamp)
		ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("cannot query local intervals table: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ir intervalRow
		if err := rows.Scan(
			&ir.ID,
			&ir.UUID,
			&ir.StartTimestamp,
			&ir.StopTimestamp,
			&ir.CreatedAt,
			&ir.UpdatedAt,
			&ir.DeletedAt,
		); err != nil {
			return nil, fmt.Errorf("cannot scan local intervals table: %w", err)
		}
		newLocalIntervals = append(newLocalIntervals, ir)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse local intervals table: %w", err)
	}

	return newLocalIntervals, nil
}

func getNewLocalIntervalTags(tx *sql.Tx) ([]intervalTagsRow, error) {
	var newLocalIntervalTags []intervalTagsRow

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		)
		SELECT interval_uuid, tag, created_at, deleted_at
		FROM interval_tags
			JOIN last_sync
				ON (last_timestamp IS NULL
					OR created_at >= last_timestamp
					OR deleted_at >= last_timestamp)
		ORDER BY created_at, deleted_at`)
	if err != nil {
		return nil, fmt.Errorf("cannot query local interval_tags table: %w", err)
	}
	defer rows.Close()

	for i := 0; rows.Next(); i++ {
		var itr intervalTagsRow
		if err := rows.Scan(&itr.UUID, &itr.Tag, &itr.CreatedAt, &itr.DeletedAt); err != nil {
			return nil, fmt.Errorf("cannot scan local interval_tags table: %w", err)
		}
		fmt.Println(i, itr)
		newLocalIntervalTags = append(newLocalIntervalTags, itr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse local interval_tags table: %w", err)
	}

	return newLocalIntervalTags, nil
}

// Sync performs a bidirectional synchronisation with the central database.
func (tt *TimeTracker) Sync() (ret error) {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start a transaction: %w", err)
	}
	defer completeTransaction(tx, &ret)

	// get all new local data which has been created, update or deleted
	// after the last sync timestamp
	newLocalTags, err := getNewLocalTags(tx)
	if err != nil {
		return err
	}

	newLocalIntervals, err := getNewLocalIntervals(tx)
	if err != nil {
		return err
	}

	newLocalIntervalTags, err := getNewLocalIntervalTags(tx)
	if err != nil {
		return err
	}

	fmt.Println(newLocalTags, newLocalIntervals, newLocalIntervalTags)
	return nil
}
