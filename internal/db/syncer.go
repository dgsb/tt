package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
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

func setupSyncerDB(cfg SyncerConfig) (*sqlx.DB, error) {
	db, err := sqlx.Open("pgx", cfg.String())
	if err != nil {
		return nil, fmt.Errorf("cannot open syncer database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot validate syncer database connection: %w", err)
	}
	if err := runPostgresMigrations(db.DB); err != nil {
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
	StartUUID string
	Tag       string
	CreatedAt int64
}

type intervalTagsTombstoneRow struct {
	UUID            string
	IntervalTagUUID string
	CreatedAt       int64
}

// setupLastSyncTimestamp setup a sync_history temporary table on the remote server
// for the queries on the local and remote database to be the same.
func setupLastSyncTimestamp(tx *sqlx.Tx, lastSync time.Time) error {
	if _, err := tx.Exec(`CREATE TEMP TABLE sync_history (sync_timestamp INTEGER)`); err != nil {
		return fmt.Errorf("cannot create sync_timestamp temporary table: %w", err)
	}
	if lastSync.IsZero() {
		return nil
	}
	if _, err := tx.Exec(
		tx.Rebind(`INSERT INTO sync_history (sync_timestamp) VALUES (?)`),
		lastSync.Unix(),
	); err != nil {
		return fmt.Errorf("cannot insert last sync timestamp in temporary table: %w", err)
	}
	return nil
}

// getLastSyncTimestamp returns the last registered sync timestamp.
// If the return time.Time is zero, it means no sync has ever occured.
func getLastSyncTimestamp(tx *sqlx.Tx) (time.Time, error) {

	row := tx.QueryRow(`SELECT max(sync_timestamp) FROM sync_history`)

	var lastSync sql.NullInt64
	if err := row.Scan(&lastSync); err != nil {
		return time.Time{}, fmt.Errorf("cannot scan sync_history table: %w", err)
	}

	if !lastSync.Valid {
		return time.Time{}, nil
	}

	return time.Unix(lastSync.Int64, 0), nil
}

// getNewTags return all tags created since the last sync operation
func getNewTags(tx *sqlx.Tx) (newTags []string, ret error) {

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
		return nil, fmt.Errorf("cannot query tags table: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			newTags, ret = nil, multierror.Append(ret, err)
		}
	}()

	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("cannot scan tags row: %w", err)
		}
		newTags = append(newTags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse tags table: %w", err)
	}

	return newTags, nil
}

func storeNewTags(tx *sqlx.Tx, tags []string, now time.Time) error {
	for _, tag := range tags {
		if _, err := tx.Exec(
			tx.Rebind(`
				INSERT INTO tags (name, created_at)
				VALUES (?, ?)
				ON CONFLICT DO NOTHING`,
			),
			tag,
			now.Unix(),
		); err != nil {
			return fmt.Errorf("cannot insert a row in tags: %w", err)
		}
	}
	return nil
}

func getNewIntervalStart(tx *sqlx.Tx) (newIntervals []intervalStartRow, ret error) {

	newIntervals = []intervalStartRow{}

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		) 
		SELECT uuid, start_timestamp, created_at
		FROM interval_start
			JOIN last_sync
				ON (last_timestamp IS NULL OR created_at >= last_timestamp)
		ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("cannot query interval start table: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			newIntervals, ret = nil, multierror.Append(ret, err)
		}
	}()

	for rows.Next() {
		var ir intervalStartRow
		if err := rows.Scan(
			&ir.UUID,
			&ir.StartTimestamp,
			&ir.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("cannot scan intervals table: %w", err)
		}
		newIntervals = append(newIntervals, ir)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse intervals table: %w", err)
	}

	return newIntervals, nil
}

func storeNewIntervalStart(tx *sqlx.Tx, newIntervals []intervalStartRow, now time.Time) error {
	for _, interval := range newIntervals {
		if _, err := tx.Exec(
			tx.Rebind(`
				INSERT INTO interval_start (uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)
				ON CONFLICT DO NOTHING`,
			),
			interval.UUID,
			interval.StartTimestamp,
			now.Unix(),
		); err != nil {
			return fmt.Errorf("cannot insert a row in interval_start table: %w", err)
		}
	}
	return nil
}

func getNewIntervalStop(tx *sqlx.Tx) (newIntervalStop []intervalStopRow, ret error) {

	newIntervalStop = []intervalStopRow{}

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
		return nil, fmt.Errorf("cannot query interval stop table: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			newIntervalStop, ret = nil, multierror.Append(ret, err)
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
		newIntervalStop = append(newIntervalStop, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse interval stop table: %w", err)
	}

	return
}

func storeNewIntervalStop(tx *sqlx.Tx, newIntervalStop []intervalStopRow, now time.Time) error {
	for _, interval := range newIntervalStop {
		if _, err := tx.Exec(
			tx.Rebind(`
				INSERT INTO interval_stop (uuid, start_uuid, stop_timestamp, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT DO NOTHING`,
			),
			interval.UUID,
			interval.StartUUID,
			interval.StopTimestamp,
			now.Unix(),
		); err != nil {
			return fmt.Errorf("cannot insert a row into inteval_stop table: %w", err)
		}
	}
	return nil
}

func getNewIntervalTombstone(tx *sqlx.Tx) (itr []intervalTombstoneRow, ret error) {

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

func storeNewIntervalTombstone(tx *sqlx.Tx, intervals []intervalTombstoneRow, now time.Time) error {
	for _, i := range intervals {
		if _, err := tx.Exec(
			tx.Rebind(`
				INSERT INTO interval_tombstone (uuid, start_uuid, created_at)
				VALUES (?, ?, ?)
				ON CONFLICT DO NOTHING`,
			),
			i.UUID, i.StartUUID, now.Unix(),
		); err != nil {
			return fmt.Errorf("cannot insert a row in interval_tombstone table: %w", err)
		}
	}
	return nil
}

func getNewIntervalTags(tx *sqlx.Tx) (newIntervalTags []intervalTagsRow, ret error) {

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		)
		SELECT uuid, interval_start_uuid, tag, created_at
		FROM interval_tags
			JOIN last_sync
				ON (last_timestamp IS NULL OR created_at >= last_timestamp)
		ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("cannot query interval_tags table: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			newIntervalTags, ret = nil, multierror.Append(ret, err)
		}
	}()

	for i := 0; rows.Next(); i++ {
		var itr intervalTagsRow
		if err := rows.Scan(&itr.UUID, &itr.StartUUID, &itr.Tag, &itr.CreatedAt); err != nil {
			return nil, fmt.Errorf("cannot scan interval_tags table: %w", err)
		}
		newIntervalTags = append(newIntervalTags, itr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse interval_tags table: %w", err)
	}

	return newIntervalTags, nil
}

func storeNewIntervalTags(tx *sqlx.Tx, newIntervalTags []intervalTagsRow, now time.Time) error {
	for _, i := range newIntervalTags {
		if _, err := tx.Exec(
			tx.Rebind(
				`INSERT INTO interval_tags (uuid, interval_start_uuid, tag, created_at)
				VALUES (?, ?, ?, ?)
				ON CONFLICT DO NOTHING`,
			),
			i.UUID, i.StartUUID, i.Tag, now.Unix(),
		); err != nil {
			return fmt.Errorf("cannot insert row in interval_tags table: %w", err)
		}
	}
	return nil
}

func getNewIntervalTagsTombstone(tx *sqlx.Tx) (val []intervalTagsTombstoneRow, ret error) {

	rows, err := tx.Query(`
		WITH last_sync AS (
			SELECT max(sync_timestamp) last_timestamp
			FROM sync_history
		)
		SELECT uuid, interval_tag_uuid, created_at
		FROM interval_tags_tombstone
			JOIN last_sync
				ON (last_timestamp IS NULL OR created_at >= last_timestamp)
		ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("cannot query interval_tags_tombstone table: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			val, ret = nil, multierror.Append(ret, err)
		}
	}()

	for rows.Next() {
		var r intervalTagsTombstoneRow
		if err := rows.Scan(
			&r.UUID,
			&r.IntervalTagUUID,
			&r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("cannot scan interval_tags_tombsone row: %w", err)
		}
		val = append(val, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse interval_tags_tombstone table: %w", err)
	}

	return
}

func storeNewIntervalTagsTombstone(
	tx *sqlx.Tx,
	newIntervalTagsTombstone []intervalTagsTombstoneRow,
	now time.Time,
) error {
	for _, i := range newIntervalTagsTombstone {
		if _, err := tx.Exec(
			tx.Rebind(
				`INSERT INTO interval_tags_tombstone (uuid, interval_tag_uuid, created_at)
				VALUES (?, ?, ?)
				ON CONFLICT DO NOTHING`,
			),
			i.UUID, i.IntervalTagUUID, now.Unix(),
		); err != nil {
			return fmt.Errorf("cannot insert row in interval_tags_tombstone table: %w", err)
		}
	}
	return nil
}

// Sync performs a bidirectional synchronisation with the central database.
func (tt *TimeTracker) Sync(cfg SyncerConfig) (ret error) {
	syncDB, err := setupSyncerDB(cfg)
	if err != nil {
		return fmt.Errorf("cannot open syncer database: %w", err)
	}
	defer func() {
		if err2 := syncDB.Close(); err2 != nil {
			ret = multierror.Append(ret, fmt.Errorf("cannot close sync db: %w", err2))
		}
	}()

	tx, err := sqlx.NewDb(tt.db, "sqlite3").Beginx()
	if err != nil {
		return fmt.Errorf("cannot start a transaction: %w", err)
	}
	defer completeTransaction(tx.Tx, &ret)

	lastSync, err := getLastSyncTimestamp(tx)
	if err != nil {
		return fmt.Errorf("cannot get last sync timestamp: %w", err)
	}

	syncTx, err := syncDB.Beginx()
	if err != nil {
		return fmt.Errorf("cannot start transaction on syncer db: %w", err)
	}
	defer completeTransaction(syncTx, &ret)

	if err := setupLastSyncTimestamp(syncTx, lastSync); err != nil {
		return fmt.Errorf("cannot setup last sync temp table on remote database: %w", err)
	}

	now := time.Now()

	// get all new local and remote data which has been created, update or deleted
	// after the last sync timestamp

	// synchronize new tags
	{
		newLocalTags, err := getNewTags(tx)
		if err != nil {
			return fmt.Errorf("cannot get new local tags: %w", err)
		}

		newRemoteTags, err := getNewTags(syncTx)
		if err != nil {
			return fmt.Errorf("cannot get new remote tags: %w", err)
		}

		if err := storeNewTags(tx, newRemoteTags, now); err != nil {
			return fmt.Errorf("cannot synchronize new remote tags in local database: %w", err)
		}

		if err := storeNewTags(syncTx, newLocalTags, now); err != nil {
			return fmt.Errorf("cannot synchronize new local tags in remote database: %w", err)
		}
	}

	// syncrhonize new interval start
	{
		newLocalIntervalStart, err := getNewIntervalStart(tx)
		if err != nil {
			return fmt.Errorf("cannot get new local interval start: %w", err)
		}

		newRemoteIntervalStart, err := getNewIntervalStart(syncTx)
		if err != nil {
			return fmt.Errorf("cannot get new remote interval start: %w", err)
		}

		if err := storeNewIntervalStart(tx, newRemoteIntervalStart, now); err != nil {
			return fmt.Errorf(
				"cannot synchronize new remote interval start in local database: %w", err)
		}

		if err := storeNewIntervalStart(syncTx, newLocalIntervalStart, now); err != nil {
			return fmt.Errorf(
				"cannot synchronize new local interval start in remote database: %w", err)
		}
	}

	// synchronize new interval stop
	{
		newLocalIntervalStop, err := getNewIntervalStop(tx)
		if err != nil {
			return fmt.Errorf("cannot get new local interval stop: %w", err)
		}

		newRemoteIntervalStop, err := getNewIntervalStop(syncTx)
		if err != nil {
			return fmt.Errorf("cannot get new remote interval stop: %w", err)
		}

		if err := storeNewIntervalStop(tx, newRemoteIntervalStop, now); err != nil {
			return fmt.Errorf(
				"cannot synchronize new remote interval stop in local database: %w", err)
		}

		if err := storeNewIntervalStop(syncTx, newLocalIntervalStop, now); err != nil {
			return fmt.Errorf(
				"cannot synchronize new remote interval stop in local database: %w", err)
		}
	}

	// synchronize new interval tombstone
	{
		newLocalIntervalTombstone, err := getNewIntervalTombstone(tx)
		if err != nil {
			return fmt.Errorf("cannot get new local interval tombstone: %w", err)
		}

		newRemoteIntervalTombstone, err := getNewIntervalTombstone(syncTx)
		if err != nil {
			return fmt.Errorf("cannot get new remote interval tombstone: %w", err)
		}

		if err := storeNewIntervalTombstone(tx, newRemoteIntervalTombstone, now); err != nil {
			return fmt.Errorf("cannot sync new remote interval tombstone: %w", err)
		}

		if err := storeNewIntervalTombstone(syncTx, newLocalIntervalTombstone, now); err != nil {
			return fmt.Errorf("cannot sync new remote interval tombstone: %w", err)
		}
	}

	// synchronize interval tags
	{
		newLocalIntervalTags, err := getNewIntervalTags(tx)
		if err != nil {
			return fmt.Errorf("cannot get new local interval tags: %w", err)
		}

		newRemoteIntervalTags, err := getNewIntervalTags(syncTx)
		if err != nil {
			return fmt.Errorf("cannot get new remote interval tags: %w", err)
		}

		if err := storeNewIntervalTags(tx, newRemoteIntervalTags, now); err != nil {
			return fmt.Errorf("cannot sync new remote interval tags: %w", err)
		}

		if err := storeNewIntervalTags(syncTx, newLocalIntervalTags, now); err != nil {
			return fmt.Errorf("cannot syn new local interval tags: %w", err)
		}
	}

	// syncrhonize interval tags tombstone
	{
		newLocalIntervalTagsTombstone, err := getNewIntervalTagsTombstone(tx)
		if err != nil {
			return fmt.Errorf("cannot get new local interval tags tombstone: %w", err)
		}

		newRemoteIntervalTagsTombstone, err := getNewIntervalTagsTombstone(syncTx)
		if err != nil {
			return fmt.Errorf("cannot get new remote interval tags tombstone: %w", err)
		}

		if err := storeNewIntervalTagsTombstone(tx, newRemoteIntervalTagsTombstone, now); err != nil {
			return fmt.Errorf("cannot sync remote interval tags tombstone: %w", err)
		}

		if err := storeNewIntervalTagsTombstone(syncTx, newLocalIntervalTagsTombstone, now); err != nil {
			return fmt.Errorf("cannot sync local interval tags tombstone: %w", err)
		}
	}

	// Store the last sync timestamp

	return nil
}
