package db

import (
	"database/sql"
	"fmt"

	"github.com/hashicorp/go-multierror"
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

type intervalStartRow struct {
	UUID           string
	StartTimestamp int64
	CreatedAt      int64
}

type intervalStopRow struct {
	UUID          string
	StartUUID     string
	StopTimestamp int64
	CreatedAt     int64
}

type intervalTombstoneRow struct {
	UUID      string
	StartUUID string
	CreatedAt int64
}

type intervalTagsRow struct {
	UUID      string
	Tag       string
	CreatedAt int64
	DeletedAt sql.NullInt64
}

// getNewLocalTags return all tags created since the last sync operation
func getNewLocalTags(tx *sql.Tx) (newLocalTags []string, ret error) {

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
	defer func() {
		if err := rows.Close(); err != nil {
			newLocalTags, ret = nil, multierror.Append(ret, err)
		}
	}()

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

func getNewLocalIntervalStart(tx *sql.Tx) (newLocalIntervals []intervalStartRow, ret error) {

	newLocalIntervals = []intervalStartRow{}

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		) 
		SELECT uuid, start_timestamp, created_at
		FROM interval_start
			JOIN last_sync
				ON (last_timestamp IS NULL OR created_at >= last_timestamp)
		ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("cannot query local interval start table: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			newLocalIntervals, ret = nil, multierror.Append(ret, err)
		}
	}()

	for rows.Next() {
		var ir intervalStartRow
		if err := rows.Scan(
			&ir.UUID,
			&ir.StartTimestamp,
			&ir.CreatedAt,
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

func getNewLocalIntervalStop(tx *sql.Tx) (newLocalIntervalStop []intervalStopRow, ret error) {

	newLocalIntervalStop = []intervalStopRow{}

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		)
		SELECT uuid, start_uuid, stop_timestamp, created_at
		FROM interval_stop
			JOIN last_sync
				ON (last_timestamp IS NULL OR created_at >= last_timestamp)
		ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("cannot query local interval stop table: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			newLocalIntervalStop, ret = nil, multierror.Append(ret, err)
		}
	}()

	for rows.Next() {
		var r intervalStopRow
		if err := rows.Scan(
			&r.UUID,
			&r.StartUUID,
			&r.StopTimestamp,
			&r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("cannot scan a row: %w", err)
		}
		newLocalIntervalStop = append(newLocalIntervalStop, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse interval stop table: %w", err)
	}

	return
}

func getNewLocalIntervalTombstone(tx *sql.Tx) (itr []intervalTombstoneRow, ret error) {

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		)
		SELECT uuid, start_uuid, created_at
		FROM interval_tombstone
			JOIN last_sync
				ON (last_timestamp IS NULL OR created_at >= last_timestamp)
		ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("cannot query interval_tombstone table: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			itr, ret = nil, multierror.Append(ret, err)
		}
	}()

	for rows.Next() {
		var r intervalTombstoneRow
		if err := rows.Scan(
			&r.UUID,
			&r.StartUUID,
			&r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("cannot scan intervalTombstone row: %w", err)
		}
		itr = append(itr, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse interval_tombstone table: %w", err)
	}
	return itr, nil
}

func getNewLocalIntervalTags(tx *sql.Tx) (newLocalIntervalTags []intervalTagsRow, ret error) {

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
	defer func() {
		if err := rows.Close(); err != nil {
			newLocalIntervalTags, ret = nil, multierror.Append(ret, err)
		}
	}()

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
func (tt *TimeTracker) Sync(cfg SyncerConfig) (ret error) {
	syncDB, err := setupSyncerDB(cfg)
	if err != nil {
		return fmt.Errorf("cannot open syncer database: %w", err)
	}
	defer func() { _ = syncDB.Close() }()

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

	newLocalIntervalStart, err := getNewLocalIntervalStart(tx)
	if err != nil {
		return err
	}

	newLocalIntervalStop, err := getNewLocalIntervalStop(tx)
	if err != nil {
		return err
	}

	newLocalIntervalTags, err := getNewLocalIntervalTags(tx)
	if err != nil {
		return err
	}

	fmt.Println(newLocalTags, newLocalIntervalStart, newLocalIntervalStop, newLocalIntervalTags)

	syncTx, err := syncDB.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction on syncer db: %w", err)
	}
	defer completeTransaction(syncTx, &ret)
	return nil
}
