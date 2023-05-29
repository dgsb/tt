package db

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
)

const customSqliteDriverName = "sqlite3_tt"

func init() {
	sql.Register(customSqliteDriverName, &sqlite3.SQLiteDriver{
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

type transactioner interface {
	Commit() error
	Rollback() error
}

func completeTransaction(
	tx transactioner,
	retErr *error, //nolint:gocritic
) {
	if *retErr != nil {
		*retErr = multierror.Append(*retErr, tx.Rollback())
	} else if err := tx.Commit(); err != nil {
		*retErr = multierror.Append(fmt.Errorf("cannot commit transaction: %w", err), tx.Rollback())
	}
}

func setupDB(databaseName string) (*sqlx.DB, error) {
	db, err := sql.Open(customSqliteDriverName, databaseName)
	if err != nil {
		return nil, fmt.Errorf("cannot open database %s: %w", databaseName, err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot validate database connection %s: %w", databaseName, err)
	}

	if err := runSqliteMigrations(db); err != nil {
		return nil, fmt.Errorf("cannot run schema migration on database %s: %w", databaseName, err)
	}

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		return nil, fmt.Errorf("cannot enforce foreign keys consistency mode: %w", err)
	}

	if _, err := db.Exec(`PRAGMA defer_foreign_keys = ON`); err != nil {
		return nil, fmt.Errorf(
			"cannot defer foreign keys consistency check at end of transaction time: %w", err)
	}

	return sqlx.NewDb(db, "sqlite3"), nil
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
	db  *sqlx.DB
	now func() time.Time
}

func New(databaseName string) (*TimeTracker, error) {
	db, err := setupDB(databaseName)
	if err != nil {
		return nil, fmt.Errorf("cannot setup time tracker database: %w", err)
	}

	return &TimeTracker{db: db, now: time.Now}, nil
}

// Close releases resources associated with the TimeTracker object.
func (tt *TimeTracker) Close() error {
	return tt.db.Close()
}

// countOpenedInterval counts the number of currently started
// and not stopped time interval.
// We should have at most one.
func (tt *TimeTracker) countOpenedInterval(tx *sqlx.Tx) (int, error) {
	var count int
	row := tx.QueryRow(`
		SELECT count(1)
		FROM interval_start
			LEFT JOIN interval_stop ON interval_start.uuid = interval_stop.start_uuid
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE interval_stop.uuid IS NULL
			AND interval_tombstone.uuid IS NULL`)
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// Start registers a new opened interval with a set of tags. This method ensures
// that no other opened is currently registered in the database and that
// the wanted start time doesn't already belong to a closed interval.
func (tt *TimeTracker) Start(t time.Time, tags []string) (ret error) {
	tx, err := tt.db.Beginx()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer completeTransaction(tx, &ret)

	// Check we don't have an already running opened interval
	if count, err := tt.countOpenedInterval(tx); err != nil {
		return fmt.Errorf("cannot count opened intervals: %w", err)
	} else if count >= 1 {
		return ErrExistingOpenInterval
	}

	// Check the requested start time doesn't fall in a known closed interval
	var count int
	row := tx.QueryRow(`
		SELECT count(1)
		FROM interval_start
			INNER JOIN interval_stop ON interval_start.uuid = interval_stop.start_uuid
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE start_timestamp <= ?1 AND stop_timestamp > ?1
			AND interval_tombstone.uuid IS NULL`, t.Unix())
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count overlapping closed interval: %w", err)
	}
	if count >= 1 {
		return ErrInvalidStartTimestamp
	}

	// Preconditions ok. Start inserting the new opened interval.

	// Ensure all requested tags are already known
	for _, tag := range tags {
		if _, err := tx.Exec(
			`INSERT INTO tags (name, created_at)
			VALUES (?, ?)
			ON CONFLICT DO NOTHING`,
			tag,
			tt.now().Unix(),
		); err != nil {
			return fmt.Errorf("cannot insert missing tag %s: %w", tag, err)
		}
	}

	// Insert the new interval
	var newUUID string
	row = tx.QueryRow(`
		INSERT INTO interval_start (uuid, start_timestamp, created_at)
		VALUES(uuid(), ?, ?)
		RETURNING (uuid)
	`, t.Unix(), tt.now().Unix())
	if err := row.Scan(&newUUID); err != nil {
		return fmt.Errorf("cannot insert new interval: %w", err)
	}

	// Link the new interval with its associated tags
	for _, tag := range tags {
		_, err := tx.Exec(`
			INSERT INTO interval_tags (uuid, interval_start_uuid, tag, created_at)
			VALUES (uuid(), ?1, ?2, ?3)
		`, newUUID, tag, tt.now().Unix())
		if err != nil {
			return fmt.Errorf("cannot link new interval with tag %s: %w", tag, err)
		}
	}

	return nil
}

// Stop close the current opened interval at the requested timestamp.
func (tt *TimeTracker) stop(t time.Time, d time.Duration) (ret error) {

	if (!t.IsZero() && d != 0) || (t.IsZero() && d == 0) {
		return fmt.Errorf("%w: one parameter must be set", ErrInvalidParam)
	}

	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer completeTransaction(tx, &ret)

	// Check we have a single running timestamp
	// and that the required stop timestamp is actually after the start timestamp
	var (
		intervalUUID              string
		count, startTimestampUnix int64
	)
	row := tx.QueryRow(`
		SELECT interval_start.uuid, start_timestamp, count(1) over()
		FROM interval_start
			LEFT JOIN interval_stop ON interval_start.uuid = interval_stop.start_uuid
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE stop_timestamp IS NULL AND interval_tombstone.created_at IS NULL
		LIMIT 1`)
	if err = row.Scan(&intervalUUID, &startTimestampUnix, &count); err != nil {
		return fmt.Errorf("cannot count opened interval: %w", err)
	}
	if count > 1 {
		return fmt.Errorf("%w: %d", ErrMultipleOpenInterval, count)
	}
	if d != 0 {
		t = time.Unix(startTimestampUnix, 0).Add(d)
	}
	if startTimestampUnix >= t.Unix() {
		return ErrInvalidStopTimestamp
	}

	// Check the requested stop timestamp doesn't include other
	// closed interval.
	row = tx.QueryRow(`
		SELECT count(1)
		FROM interval_start
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE start_timestamp > ?
			AND start_timestamp < ?
			AND interval_tombstone.uuid IS NULL`, startTimestampUnix, t.Unix())
	if err = row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count enclosed interval: %w", err)
	}
	if count >= 1 {
		return ErrInvalidStopTimestamp
	}

	// preconditions ok. Close the currently opened interval.
	_, err = tx.Exec(`
		INSERT INTO interval_stop (uuid, start_uuid, stop_timestamp, created_at)
		VALUES (uuid(), ?, ?, ?)`,
		intervalUUID, t.Unix(), tt.now().Unix())
	if err != nil {
		return fmt.Errorf("cannot insert interval tombstone: %w", err)
	}

	return nil
}

func (tt *TimeTracker) StopAt(t time.Time) error {
	return tt.stop(t, 0)
}

func (tt *TimeTracker) StopFor(d time.Duration) error {
	return tt.stop(time.Time{}, d)
}

// XXX add unit test
func (tt *TimeTracker) getIntervalTags(intervalUUID string) (tags []string, retErr error) {
	rows, err := tt.db.Query(`
		SELECT tag
		FROM interval_tags
			LEFT JOIN interval_tags_tombstone
				ON interval_tags.uuid = interval_tags_tombstone.interval_tag_uuid
		WHERE interval_start_uuid = ?
			AND interval_tags_tombstone.uuid IS NULL`, intervalUUID)
	if err != nil {
		return nil, fmt.Errorf("cannot retrieve associated tags: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			tags = nil
			retErr = fmt.Errorf("closing interval_tags table rows object: %w", err)
		}
	}()

	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("cannot scan value for current interval tags row: %w", err)
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot iterate over associated tags rows: %w", err)
	}

	return
}

// List returns a list of interval whose start timestamp is equal
// or after the timestamp given as parameter.
// XXX add unit test
func (tt *TimeTracker) List(since, until time.Time) (retTi []TaggedInterval, retErr error) {
	rows, err := tt.db.Query(`
		SELECT id, interval_start.uuid, start_timestamp, stop_timestamp
		FROM interval_start
			LEFT JOIN interval_stop ON interval_start.uuid = interval_stop.start_uuid
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE
			(
				(start_timestamp >= ?1  AND start_timestamp < ?2)
				OR (stop_timestamp >= ?1 AND stop_timestamp < ?2)
				OR stop_timestamp IS NULL
			) AND interval_tombstone.uuid IS NULL
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

	intervals := make([]TaggedInterval, 0, 126)
	for rows.Next() {
		var (
			unixStartTimestamp int64
			unixStopTimestamp  sql.NullInt64
			interval           TaggedInterval
		)

		if err := rows.Scan(
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

		intervals = append(intervals, interval)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot iterate over query returned rows: %w", err)
	}

	for idx := range intervals {
		tags, err := tt.getIntervalTags(intervals[idx].Interval.UUID)
		if err != nil {
			return nil, err
		}
		intervals[idx].Tags = tags
	}

	return intervals, nil
}

func (tt *TimeTracker) Delete(id string) (ret error) {

	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer completeTransaction(tx, &ret)

	_, err = tx.Exec(`
		INSERT OR IGNORE INTO interval_tombstone (uuid, start_uuid, created_at)
		SELECT uuid(), (SELECT uuid FROM interval_start WHERE id = ?), ?`, id, tt.now().Unix())
	if err != nil {
		return fmt.Errorf("cannot delete interval %s: %w", id, err)
	}

	return nil
}

// XXX add unit test
func (tt *TimeTracker) Tag(id string, tags []string) (ret error) {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start a transaction: %w", err)
	}
	defer completeTransaction(tx, &ret)

	row := tx.QueryRow(`
		SELECT interval_start.uuid
		FROM interval_start
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE interval_tombstone.uuid IS NULL
			AND interval_start.id = ?`, id)
	var intervalUUID string
	if err := row.Scan(&intervalUUID); err != nil {
		return fmt.Errorf("cannot retrieve uuid from database scan: %w", err)
	}

	for _, tag := range tags {

		// We should try to implement that as a trigger
		row := tx.QueryRow(`
			SELECT count(1)
			FROM interval_tags
				LEFT JOIN interval_tags_tombstone
					ON interval_tags.uuid = interval_tags_tombstone.interval_tag_uuid
			WHERE interval_start_uuid = ?
				AND tag = ?
				AND interval_tags_tombstone.uuid IS NULL`, intervalUUID, tag)
		var count int
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("cannot scan database: %w", err)
		}
		if count >= 1 {
			return fmt.Errorf("%w: id:%s, tag:%s", ErrDuplicatedIntervalTag, id, tag)
		}

		if _, err := tx.Exec(`
				INSERT INTO tags (name, created_at)
				VALUES (?, ?)
				ON CONFLICT DO NOTHING`,
			tag, tt.now().Unix()); err != nil {
			return fmt.Errorf("cannot insert new tags %s: %w", tag, err)
		}

		if _, err := tx.Exec(`
			INSERT INTO interval_tags (uuid, interval_start_uuid, tag, created_at)
			VALUES (uuid(), ?, ?, ?)
			ON CONFLICT DO NOTHING`, intervalUUID, tag, tt.now().Unix()); err != nil {
			return fmt.Errorf("cannot tag interval %s with %s: %w", id, tag, err)
		}
	}

	return nil
}

func (tt *TimeTracker) Untag(id string, tags []string) (ret error) {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start a transaction: %w", err)
	}
	defer completeTransaction(tx, &ret)

	row := tx.QueryRow(`
		SELECT interval_start.uuid
		FROM interval_start
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE interval_tombstone.uuid IS NULL
			AND interval_start.id = ?`, id)

	var intervalUUID string
	if err := row.Scan(&intervalUUID); err != nil {
		return multierror.Append(fmt.Errorf("%w: id %s", ErrNotFound, id), err)
	}

	for _, tag := range tags {
		if _, err := tx.Exec(`
			WITH to_delete AS (
				SELECT interval_tags.uuid
				FROM interval_tags
					JOIN interval_start
						ON interval_tags.interval_start_uuid = interval_start.uuid
					LEFT JOIN interval_tombstone
						ON interval_tombstone.start_uuid = interval_start.uuid
					LEFT JOIN interval_tags_tombstone
						ON interval_tags.uuid = interval_tags_tombstone.interval_tag_uuid
				WHERE interval_tags_tombstone.uuid IS NULL
					AND interval_tombstone.uuid IS NULL
					AND interval_start.id = ?
					AND interval_tags.tag = ?
			)
			INSERT INTO interval_tags_tombstone (uuid, interval_tag_uuid, created_at)
			SELECT uuid(), uuid, ? FROM to_delete
		`, id, tag, tt.now().Unix()); err != nil {
			return fmt.Errorf("cannot untag interval %s from %s: %w", id, tag, err)
		}
	}

	return nil
}

// Current returned the currently single opened interval if any.
func (tt *TimeTracker) Current() (*TaggedInterval, error) {
	row := tt.db.QueryRow(`
		SELECT id, interval_start.uuid, start_timestamp
		FROM interval_start
			LEFT JOIN interval_stop ON interval_start.uuid = interval_stop.start_uuid
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE interval_stop.uuid IS NULL
			AND interval_tombstone.uuid IS NULL`)

	var (
		unixStartTimestamp int64
		interval           TaggedInterval
	)
	if err := row.Scan(
		&interval.Interval.ID, &interval.Interval.UUID, &unixStartTimestamp,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("cannot scan current opened interval: %w", err)
	}

	interval.Interval.StartTimestamp = time.Unix(unixStartTimestamp, 0)

	rows, err := tt.db.Query(
		`SELECT tag FROM interval_tags WHERE interval_start_uuid = ?`,
		interval.Interval.UUID)
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
func (tt *TimeTracker) Continue(t time.Time, id string) (ret error) {
	tx, err := tt.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer completeTransaction(tx, &ret)

	var count int64
	row := tx.QueryRow(`
		SELECT count(1)
		FROM interval_start
			LEFT JOIN interval_stop ON interval_start.uuid = interval_stop.start_uuid
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE interval_stop.uuid IS NULL
			AND interval_tombstone.uuid IS NULL`)
	if err = row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count opened intervals: %w", err)
	}

	if count >= 1 {
		return ErrMultipleOpenInterval
	}

	row = tx.QueryRow(`
		SELECT count(1)
		FROM interval_start
			LEFT JOIN interval_stop ON interval_start.uuid = interval_stop.start_uuid
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE interval_tombstone.uuid IS NULL
			AND start_timestamp <= ?1
			AND stop_timestamp > ?1`, t.Unix())
	if err = row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count overlapping intervals: %w", err)
	}

	if count >= 1 {
		return ErrInvalidStartTimestamp
	}

	var query string
	if id == "" {
		query = `WITH last_id AS (
			SELECT id, interval_start.uuid
			FROM interval_start
				LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
			WHERE interval_tombstone.uuid IS NULL
			ORDER BY start_timestamp DESC
			LIMIT 1
		)
		SELECT last_id.uuid, interval_tags.tag
		FROM last_id
			LEFT JOIN interval_tags
				ON interval_tags.interval_start_uuid = last_id.uuid
			LEFT JOIN interval_tags_tombstone
				ON interval_tags_tombstone.interval_tag_uuid = interval_tags.uuid
		WHERE interval_tags_tombstone.uuid IS NULL`
	} else {
		query = `
			SELECT interval_start.uuid, interval_tags.tag
			FROM interval_start
				LEFT JOIN interval_tags
					ON interval_start.uuid = interval_tags.interval_start_uuid
				LEFT JOIN interval_tombstone
					ON interval_start.uuid = interval_tombstone.start_uuid
				LEFT JOIN interval_tags_tombstone
					ON interval_tags.uuid = interval_tags_tombstone.interval_tag_uuid
			WHERE interval_tombstone.uuid IS NULL
				AND interval_tags_tombstone.uuid IS NULL
				AND interval_start.id = ?
		`
	}

	rows, err := tx.Query(query, id)
	if err != nil {
		return fmt.Errorf("cannot retrieve tags associated with last closed interval: %w", err)
	}

	var UUID string
	var tags []string

	for rows.Next() {
		var t sql.NullString
		if err := rows.Scan(&UUID, &t); err != nil {
			return fmt.Errorf("cannot scan tag: %w", err)
		}
		if t.Valid {
			tags = append(tags, t.String)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot iterate over tags cursor: %w", err)
	}

	if UUID == "" {
		return fmt.Errorf("cannot find interval to continue: %w", ErrNotFound)
	}

	var newUUID string
	row = tx.QueryRow(`
		INSERT INTO interval_start (uuid, start_timestamp, created_at)
		VALUES (uuid(), ?, ?)
		RETURNING (uuid)`, t.Unix(), tt.now().Unix())
	if err := row.Scan(&newUUID); err != nil {
		return fmt.Errorf("cannot insert new interval: %w", err)
	}

	for _, t := range tags {
		_, err := tx.Exec(`
			INSERT INTO interval_tags (uuid, interval_start_uuid, tag, created_at)
			VALUES (uuid(), ?, ?, ?)`, newUUID, t, tt.now().Unix())
		if err != nil {
			return fmt.Errorf("cannot tag interval %s with value %s: %w", newUUID, t, err)
		}
	}

	return nil
}

// Vacuum hard deletes all data which has been soft deleted before the timestamp.
// It will also remove unused tags. At the end of the clean process, it will
// perform a database vacuum.
func (tt *TimeTracker) Vacuum(before time.Time) (ret error) {
	return ErrNotImplemented
}
