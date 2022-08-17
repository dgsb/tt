package main

import (
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	"github.com/GuiaBolso/darwin"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed internal/migrations/01_base.sql
var baseMigration string

func runMigrations(db *sql.DB) error {
	return darwin.Migrate(
		darwin.NewGenericDriver(db, darwin.SqliteDialect{}),
		[]darwin.Migration{
			{
				Version:     1,
				Description: "base table defintion to hold configuration variable",
				Script:      baseMigration,
			},
		},
		nil)
}

func setupDB(databaseName string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", databaseName)
	if err != nil {
		return nil, fmt.Errorf("cannot open database %s: %w", databaseName, err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot validate database connection %s: %w", databaseName, err)
	}

	if err := runMigrations(db); err != nil {
		return nil, fmt.Errorf("cannot run schema migration on database %s: %w", databaseName, err)
	}

	return db, nil
}

type Interval struct {
	ID             string
	StartTimestamp time.Time
	StopTimestamp  time.Time
}

type TimeTracker struct {
	db *sql.DB
}

// CheckNoOverlap browses the full interval table to check that no registered
// and closed interval overlaps with another one. Each interval validity is individually checked.
func (tt *TimeTracker) CheckNoOverlap() error {
	rows, err := tt.db.Query(`
		SELECT id, start_timestamp, stop_timestamp
		FROM intervals
		WHERE stop_timestamp IS NOT NULL
		ORDER BY start_timestamp`)
	if err != nil {
		return fmt.Errorf("cannot query the database: %w")
	}
	defer rows.Close()

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
			return fmt.Errorf("invalid interval: %#v", *current)
		}

		if previous == nil {
			continue
		}

		if current.StartTimestamp.Before(previous.StopTimestamp) {
			return fmt.Errorf(
				"bad starting timestamp: current(%#v), previous(%#v)", *current, *previous)
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
	defer func() {
		if ret != nil {
			tx.Rollback()
		}
	}()

	// Check we don't have an already running opened interval
	var count int
	row := tx.QueryRow(`SELECT count(1) FROM intervals WHERE stop_timestamp IS NULL`)
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count opened intervals: %w", err)
	}
	if count >= 1 {
		return fmt.Errorf("already existing opened interval")
	}

	// Check the requested start time doesn't fall in a known closed interval
	row = tx.QueryRow(`
		SELECT count(1)
		FROM intervals
		WHERE start_timestamp <= ?1 AND stop_timestamp > ?1`, t.Unix())
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("cannot count overlapping closed interval: %w", err)
	}
	if count >= 1 {
		return fmt.Errorf("required start time already belongs to a closed interval")
	}

	// Preconditions ok. Start inserting the new opened interval.

	// Ensure all requested tags are already known
	for _, tag := range tags {
		if _, err := tx.Exec(
			`INSERT INTO tags (name) VALUES (?) ON CONFLICT DO NOTHING`,
			tag,
		); err != nil {
			return fmt.Errorf("cannot insert missing tag %s: %w", tag, err)
		}
	}

	// Insert the new interval
	var newID uint64
	row = tx.QueryRow(`
		INSERT INTO intervals (start_timestamp, stop_timestamp)
		VALUES(?, NULL)
		RETURNING (id)
	`, t.Unix())
	if err := row.Scan(&newID); err != nil {
		return fmt.Errorf("cannot insert new interval: %w", err)
	}

	// Link the new interval with its associated tags
	for _, tag := range tags {
		_, err := tx.Exec(`
			INSERT INTO interval_tags (interval_id, tag)
			VALUES (?1, ?2)
		`, newID, tag)
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
		return fmt.Errorf("cannot start transaction: %w")
	}
	defer func() {
		if ret != nil {
			tx.Rollback()
		}
	}()

	// Check we have a single running timestamp
	var count, startTimestampUnix uint64
	row := tx.QueryRow(`
		SELECT start_timestamp, count(1) over()
		FROM intervals
		WHERE stop_timestamp IS NULL LIMIT 1`)
	if err := row.Scan(&startTimestampUnix, &count); err != nil {
		return fmt.Errorf("cannot count opened interval: %w", err)
	}
	if count > 1 {
		return fmt.Errorf("mulitple opened interval: %d", count)
	}

	// Check the requested stop timestamp doesn't include other
	// closed interval.
	row = tx.QueryRow(`
		SELECT count(1)
		FROM intervals
		WHERE start_timestamp > ?
			AND start_timestamp <= ?`, startTimestampUnix, t.Unix())
	if err := row.Scan(&count); err != nil {
		fmt.Errorf("cannot count enclosed interval: %w", err)
	}
	if count > 1 {
		return fmt.Errorf("invalid stop time")
	}

	// preconditions ok. Close the currently opened interval.
	_, err = tx.Exec(`UPDATE intervals SET stop_timestamp = ? WHERE stop_timestamp IS NULL`, t.Unix())
	if err != nil {
		return fmt.Errorf("cannot update opened interval: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cannot commit transaction: %w", err)
	}
	return nil
}
