package db

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/GuiaBolso/darwin"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/mattn/go-sqlite3"
)

func init() {
	sql.Register("sqlite3_tt", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return conn.RegisterFunc("uuid", func() (string, error) {
				id, err := uuid.NewRandom()
				if err != nil {
					return "", fmt.Errorf("cannot generate random UUID: %w", err)
				}
				return id.String(), nil
			}, false)
		},
	})
}

func rollback(tx *sql.Tx) {
	// best effort here.
	_ = tx.Rollback() //nolint:errcheck
}

//go:embed migrations/01_base.sql
var baseMigration string

//go:embed migrations/02_add_timestamp.sql
var addTimestamp string

//go:embed migrations/03_add_uuid_key.sql
var addUUIDKey string

func runMigrations(db *sql.DB) error {
	return darwin.Migrate(
		darwin.NewGenericDriver(db, darwin.SqliteDialect{}),
		[]darwin.Migration{
			{
				Version:     1,
				Description: "base table definition to hold configuration variable",
				Script:      baseMigration,
			},
			{
				Version:     2,
				Description: "add timestamp on all tables",
				Script:      addTimestamp,
			},
			{
				Version:     3,
				Description: "add uuid unique key as conflict free identifier",
				Script:      addUUIDKey,
			},
		},
		nil)
}

func setupDB(databaseName string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3_tt", databaseName)
	if err != nil {
		return nil, fmt.Errorf("cannot open database %s: %w", databaseName, err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot validate database connection %s: %w", databaseName, err)
	}

	if err := runMigrations(db); err != nil {
		return nil, fmt.Errorf("cannot run schema migration on database %s: %w", databaseName, err)
	}

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		return nil, fmt.Errorf("cannot enforce foreign keys consistency mode: %w", err)
	}

	if _, err := db.Exec(`PRAGMA defer_foreign_keys = ON`); err != nil {
		return nil, fmt.Errorf(
			"cannot defer foreign keys consistency check at end of transaction time: %w", err)
	}

	return db, nil
}

type Interval struct {
	ID             string
	UUID           string
	StartTimestamp time.Time
	StopTimestamp  time.Time
}

type TaggedInterval struct {
	Interval
	Tags []string
}

type TimeTracker struct {
	db *sql.DB
}

func New(databaseName string) (*TimeTracker, error) {
	db, err := setupDB(databaseName)
	if err != nil {
		return nil, fmt.Errorf("cannot setup time tracker database: %w", err)
	}

	return &TimeTracker{db: db}, nil
}

// Close releases resources associated with the TimeTracker object.
func (tt *TimeTracker) Close() error {
	return tt.db.Close()
}

// SanityCheck performs a full database scan to validate data.
// It will call:
//   - checkNoOverlap
//   - intervalTagsUnicity
func (tt *TimeTracker) SanityCheck() error {
	err := multierror.Append(nil, tt.checkNoOverlap())
	err = multierror.Append(err, tt.intervalTagsUnicity())
	return err.ErrorOrNil()
}

// intervalTagsUnicity checks the database contains a single row
// for a interval_id, tag tuple with deleted_at being null.
func (tt *TimeTracker) intervalTagsUnicity() (ret error) {
	rows, err := tt.db.Query(`
		SELECT interval_uuid, tag
		FROM interval_tags
		WHERE deleted_at IS NULL
		GROUP BY interval_uuid, tag
		HAVING count(1) > 1`)
	if err != nil {
		return fmt.Errorf("cannot query the database: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			ret = multierror.Append(ret, err)
		}
	}()

	var merr *multierror.Error

	for rows.Next() {
		var (
			interval int
			tag      string
		)

		if err := rows.Scan(&interval, &tag); err != nil {
			return fmt.Errorf("cannot scan the database: %w", err)
		}

		merr = multierror.Append(merr, fmt.Errorf("%w (%d,%s)", IntervalTagsUnicityErr, interval, tag))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot browse interval_tags table: %w", err)
	}

	return merr.ErrorOrNil()
}

// checkNoOverlap browses the full interval table to check that no registered
// and closed interval overlaps with another one. Each interval validity is individually checked.
func (tt *TimeTracker) checkNoOverlap() (ret error) {
	rows, err := tt.db.Query(`
		SELECT id, start_timestamp, stop_timestamp
		FROM intervals
		WHERE stop_timestamp IS NOT NULL
			AND deleted_at IS NULL
		ORDER BY start_timestamp`)
	if err != nil {
		return fmt.Errorf("cannot query the database: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			ret = multierror.Append(ret, err)
		}
	}()

	var (
		current  *Interval
		previous *Interval
	)

	for rows.Next() {
		var unixStart, unixStop int64
		previous = current
		current = &Interval{}
		if err := rows.Scan(
			&current.ID,
			&unixStart,
			&unixStop,
		); err != nil {
			return fmt.Errorf("cannot scan table row: %w", err)
		}

		current.StartTimestamp = time.Unix(unixStart, 0)
		current.StopTimestamp = time.Unix(unixStop, 0)

		if current.StartTimestamp.Equal(current.StopTimestamp) ||
			current.StartTimestamp.After(current.StopTimestamp) {
			return fmt.Errorf("%w: %#v", InvalidIntervalErr, *current)
		}

		if previous == nil {
			continue
		}

		if current.StartTimestamp.Before(previous.StopTimestamp) {
			return fmt.Errorf(
				"%w: current(%#v), previous(%#v)", InvalidStartTimestampErr, *current, *previous)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot perform a full scan of intervals table: %w", err)
	}

	return nil
}

// Start registers a new opened interval with a set of tags. This method ensures
// that no other opened is currently registered in the database and that
// the wanted start time doesn't already belong to a closed interval.
func (tt *TimeTracker) Start(t time.Time, tags []string) (ret error) {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer rollback(tx)

	// Check we don't have an already running opened interval
	var count int
	row := tx.QueryRow(`
		SELECT count(1)
		FROM intervals
		WHERE stop_timestamp IS NULL
			AND deleted_at IS NULL`)
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count opened intervals: %w", err)
	}
	if count >= 1 {
		return ExistingOpenIntervalErr
	}

	// Check the requested start time doesn't fall in a known closed interval
	row = tx.QueryRow(`
		SELECT count(1)
		FROM intervals
		WHERE start_timestamp <= ?1 AND stop_timestamp > ?1
			AND deleted_at IS NULL`, t.Unix())
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count overlapping closed interval: %w", err)
	}
	if count >= 1 {
		return InvalidStartTimestampErr
	}

	// Preconditions ok. Start inserting the new opened interval.

	// Ensure all requested tags are already known
	for _, tag := range tags {
		if _, err := tx.Exec(
			`INSERT INTO tags (name, created_at) VALUES (?, unixepoch('now')) ON CONFLICT DO NOTHING`,
			tag,
		); err != nil {
			return fmt.Errorf("cannot insert missing tag %s: %w", tag, err)
		}
	}

	// Insert the new interval
	var newUUID string
	row = tx.QueryRow(`
		INSERT INTO intervals (uuid, start_timestamp, stop_timestamp, created_at)
		VALUES(uuid(), ?, NULL, unixepoch('now'))
		RETURNING (uuid)
	`, t.Unix())
	if err := row.Scan(&newUUID); err != nil {
		return fmt.Errorf("cannot insert new interval: %w", err)
	}

	// Link the new interval with its associated tags
	for _, tag := range tags {
		_, err := tx.Exec(`
			INSERT INTO interval_tags (interval_uuid, tag, created_at)
			VALUES (?1, ?2, unixepoch('now'))
		`, newUUID, tag)
		if err != nil {
			return fmt.Errorf("cannot link new interval with tag %s: %w", tag, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit new interval into the database: %w", err)
	}
	return nil
}

// Stop close the current opened interval at the requested timestamp.
func (tt *TimeTracker) Stop(t time.Time) (ret error) {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer rollback(tx)

	// Check we have a single running timestamp
	// and that the required stop timestamp is actually after the start timestamp
	var count, startTimestampUnix int64
	row := tx.QueryRow(`
		SELECT start_timestamp, count(1) over()
		FROM intervals
		WHERE stop_timestamp IS NULL AND deleted_at IS NULL
		LIMIT 1`)
	if err = row.Scan(&startTimestampUnix, &count); err != nil {
		return fmt.Errorf("cannot count opened interval: %w", err)
	}
	if count > 1 {
		return fmt.Errorf("%w: %d", MultipleOpenIntervalErr, count)
	}
	if startTimestampUnix >= t.Unix() {
		return InvalidStopTimestampErr
	}

	// Check the requested stop timestamp doesn't include other
	// closed interval.
	row = tx.QueryRow(`
		SELECT count(1)
		FROM intervals
		WHERE start_timestamp > ?
			AND start_timestamp < ?
			AND deleted_at IS NULL`, startTimestampUnix, t.Unix())
	if err = row.Scan(&count); err != nil {
		fmt.Errorf("cannot count enclosed interval: %w", err)
	}
	if count >= 1 {
		return InvalidStopTimestampErr
	}

	// preconditions ok. Close the currently opened interval.
	_, err = tx.Exec(`
		UPDATE intervals
		SET
			stop_timestamp = ?,
			updated_at = unixepoch('now')
		WHERE stop_timestamp IS NULL
			AND deleted_at IS NULL`, t.Unix())
	if err != nil {
		return fmt.Errorf("cannot update opened interval: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction: %w", err)
	}
	return nil
}

// List returns a list of interval whose start timestamp is equal
// or after the timestamp given as parameter.
func (tt *TimeTracker) List(since, until time.Time) (retTi []TaggedInterval, retErr error) {
	rows, err := tt.db.Query(`
		SELECT count(1) over(), id, uuid, start_timestamp, stop_timestamp
		FROM intervals
		WHERE start_timestamp >= ?
			AND start_timestamp < ?
			AND deleted_at IS NULL
		ORDER BY start_timestamp`,
		since.Unix(), until.Unix())
	if err != nil {
		return nil, fmt.Errorf("cannot query for interval: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			retTi = nil
			retErr = fmt.Errorf("closing intervals table rows object: %w", err)
		}
	}()

	var intervals []TaggedInterval
	for rows.Next() {
		var (
			count              int64
			unixStartTimestamp int64
			unixStopTimestamp  sql.NullInt64
			interval           TaggedInterval
		)

		if err := rows.Scan(
			&count,
			&interval.Interval.ID,
			&interval.Interval.UUID,
			&unixStartTimestamp,
			&unixStopTimestamp); err != nil {
			return nil, fmt.Errorf("cannot scan value for current row: %w", err)
		}

		interval.Interval.StartTimestamp = time.Unix(unixStartTimestamp, 0)
		if unixStopTimestamp.Valid {
			interval.Interval.StopTimestamp = time.Unix(unixStopTimestamp.Int64, 0)
		}

		if intervals == nil {
			intervals = make([]TaggedInterval, 0, count)
		}

		intervals = append(intervals, interval)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot iterate over query returned rows: %w", err)
	}

	for idx := range intervals {
		rows, err := tt.db.Query(`
			SELECT count(1) over(), tag
			FROM interval_tags
			WHERE interval_uuid = ?
				AND deleted_at IS NULL`, intervals[idx].Interval.UUID)
		if err != nil {
			return nil, fmt.Errorf("cannot retrieve associated tags: %w", err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				retTi = nil
				retErr = fmt.Errorf("closing interval_tags table rows object: %w", err)
			}
		}()

		var (
			count int64
			tag   string
		)
		for rows.Next() {
			if err := rows.Scan(&count, &tag); err != nil {
				return nil, fmt.Errorf("cannot scan value for current interval tags row: %w", err)
			}
			if intervals[idx].Tags == nil {
				intervals[idx].Tags = make([]string, 0, count)
			}
			intervals[idx].Tags = append(intervals[idx].Tags, tag)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("cannot iterate over associated tags rows: %w", err)
		}
	}

	return intervals, nil
}

func (tt *TimeTracker) Delete(id string) (ret error) {

	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer rollback(tx)

	_, err = tx.Exec(`UPDATE intervals SET deleted_at = ? WHERE id = ?`, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("cannot delete interval %s: %w", id, err)
	}
	_, err = tx.Exec(`
		UPDATE interval_tags
		SET deleted_at = ?
		WHERE interval_uuid = (SELECT uuid FROM intervals WHERE id = ? LIMIT 1)
			AND deleted_at IS NULL`, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("cannot delete interval_tags %s: %w", id, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit interval deletion: %w", err)
	}
	return nil
}

func (tt *TimeTracker) Tag(id string, tags []string) error {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start a transaction: %w", err)
	}
	defer rollback(tx)

	row := tx.QueryRow(`SELECT uuid FROM intervals WHERE id = ?`, id)
	var uuid string
	if err := row.Scan(&uuid); err != nil {
		return fmt.Errorf("cannot retrieve uuid from database scan: %w", err)
	}

	for _, tag := range tags {

		// We should try to implement that as a trigger
		row := tx.QueryRow(`
			SELECT count(1)
			FROM interval_tags
			WHERE interval_uuid = ? AND tag = ? AND deleted_at IS NULL`, uuid, tag)
		var count int
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("cannot scan database: %w", err)
		}
		if count >= 1 {
			return fmt.Errorf("%w: id:%s, tag:%s", DuplicatedIntervalTagErr, id, tag)
		}

		if _, err := tx.Exec(`
				INSERT INTO tags (name, created_at)
				VALUES (?, unixepoch('now'))
				ON CONFLICT DO NOTHING`,
			tag); err != nil {
			return fmt.Errorf("cannot insert new tags %s: %w", tag, err)
		}

		if _, err := tx.Exec(`
			INSERT INTO interval_tags (interval_uuid, tag)
			VALUES (?, ?)
			ON CONFLICT DO NOTHING`, uuid, tag); err != nil {
			return fmt.Errorf("cannot tag interval %s with %s: %w", id, tag, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit the transaction: %w", err)
	}
	return nil
}

func (tt *TimeTracker) Untag(id string, tags []string) error {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start a transaction: %w", err)
	}
	defer rollback(tx)

	for _, tag := range tags {
		if _, err := tx.Exec(`
			UPDATE interval_tags
			SET deleted_at = unixepoch('now')
			WHERE interval_uuid = (SELECT uuid FROM intervals WHERE id = ?) AND tag = ? AND deleted_at IS NULL
		`, id, tag); err != nil {
			return fmt.Errorf("cannot untag interval %s from %s: %w", id, tag, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction: %w", err)
	}
	return nil
}

// Current returned the currently single opened interval if any.
func (tt *TimeTracker) Current() (*TaggedInterval, error) {
	row := tt.db.QueryRow(`
		SELECT id, uuid, start_timestamp
		FROM intervals
		WHERE stop_timestamp IS NULL
			AND deleted_at IS NULL`)

	var (
		unixStartTimestamp int64
		interval           TaggedInterval
	)
	if err := row.Scan(&interval.Interval.ID, &interval.Interval.UUID, &unixStartTimestamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("cannot scan current opened interval: %w", err)
	}

	interval.Interval.StartTimestamp = time.Unix(unixStartTimestamp, 0)

	rows, err := tt.db.Query(`SELECT tag FROM interval_tags WHERE interval_uuid = ?`, interval.Interval.UUID)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch tags for interval %s: %w", interval.Interval.ID, err)
	}

	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("cannot scan a tag: %w", err)
		}
		interval.Tags = append(interval.Tags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot iterate over tags cursor: %w", err)
	}

	return &interval, nil
}

// Continue opens a new interval with the same tags as the last closed one.
// It will return an error if there is already an opened interval.
func (tt *TimeTracker) Continue(t time.Time, id string) error {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer rollback(tx)

	var count int64
	row := tx.QueryRow(`
		SELECT count(1)
		FROM intervals
		WHERE stop_timestamp IS NULL
			AND deleted_at IS NULL`)
	if err = row.Scan(&count); err != nil {
		return fmt.Errorf("cannot cound opened intervals: %w", err)
	}

	if count >= 1 {
		return MultipleOpenIntervalErr
	}

	row = tx.QueryRow(`
		SELECT count(1)
		FROM intervals
		WHERE deleted_at IS NULL
			AND start_timestamp <= ?1
			AND stop_timestamp > ?1`, t.Unix())
	if err = row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count overlapping intervals: %w", err)
	}

	if count >= 1 {
		return InvalidStartTimestampErr
	}

	var query string
	if id == "" {
		query = `WITH last_id AS (
			SELECT id, uuid
			FROM intervals
			WHERE deleted_at IS NULL
			ORDER BY start_timestamp DESC
			LIMIT 1
		)
		SELECT interval_tags.tag
		FROM interval_tags
			INNER JOIN last_id ON interval_tags.interval_uuid = last_id.uuid`
	} else {
		query = `
			SELECT interval_tags.tag
			FROM intervals JOIN interval_tags ON intervals.uuid = interval_tags.interval_uuid
			WHERE intervals.id = ?
		`
	}

	rows, err := tx.Query(query, id)
	if err != nil {
		return fmt.Errorf("cannot retrieve tags associated with last closed interval: %w", err)
	}

	var tags []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return fmt.Errorf("cannot scan tag: %w", err)
		}
		tags = append(tags, t)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot iterate over tags cursor: %w", err)
	}

	var newUUID string
	row = tx.QueryRow(`
		INSERT INTO intervals (uuid, start_timestamp, stop_timestamp, created_at)
		VALUES (uuid(), ?, NULL, unixepoch('now'))
		RETURNING (uuid)`, t.Unix())
	if err := row.Scan(&newUUID); err != nil {
		return fmt.Errorf("cannot insert new interval: %w", err)
	}

	for _, t := range tags {
		_, err := tx.Exec(`
			INSERT INTO interval_tags (interval_uuid, tag, created_at)
			VALUES (?, ?, unixepoch('now'))`, newUUID, t)
		if err != nil {
			return fmt.Errorf("cannot tag interval %s with value %s: %w", newUUID, t, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit the transaction: %w", err)
	}

	return nil
}

// Vacuum hard deletes all data which has been soft deleted before the timestamp.
// It will also remove unused tags. At the end of the clean process, it will
// perform a database vacuum.
func (tt *TimeTracker) Vacuum(before time.Time) error {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer rollback(tx)

	if _, err := tx.Exec(`
		DELETE FROM intervals
		WHERE deleted_at IS NOT NULL
			AND deleted_at < ?`, before.Unix()); err != nil {
		return fmt.Errorf("cannot delete lines from intervals table: %w", err)
	}

	if _, err := tx.Exec(`
		WITH deleted_ids AS (
			SELECT DISTINCT interval_uuid
			FROM interval_tags LEFT JOIN intervals ON interval_tags.interval_uuid = intervals.uuid
			WHERE intervals.uuid IS NULL
		)
		DELETE FROM interval_tags
		WHERE interval_uuid IN (SELECT interval_uuid FROM deleted_ids)`); err != nil {
		return fmt.Errorf("cannot delete lines from interval_tags table: %w", err)
	}

	if _, err := tx.Exec(`
		WITH unreferenced_tags AS (
			SELECT name
			FROM tags LEFT JOIN interval_tags ON tags.name = interval_tags.tag
			WHERE interval_tags.tag IS NULL
		)
		DELETE FROM tags
		WHERE name IN (SELECT name FROM unreferenced_tags)`); err != nil {
		return fmt.Errorf("cannot delete lines from tags table: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction: %w", err)
	}

	if _, err := tt.db.Exec(`VACUUM`); err != nil {
		return fmt.Errorf("cannot hard vacuum the database file: %w", err)
	}

	return nil
}
