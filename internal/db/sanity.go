package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
)

// Sanity object gathers all methods which implement the logic
// to check the data sanity in the database
type Sanity struct {
	db *sqlx.DB
}

func NewSanity(db *sql.DB) *Sanity {
	// We use default sqlite3 driver name instead of our custom one here
	// because this is supposed to be only used in sqlx layer to
	// identify query placeholder charaters depending on the database type.
	return &Sanity{db: sqlx.NewDb(db, "sqlite3")}
}

// Check performs a full database scan to validate data.
// It will call:
//   - checkNoOverlap
//   - intervalTagsUnicity
func (s *Sanity) Check() error {
	err := multierror.Append(nil, s.checkNoOverlap())
	err = multierror.Append(err, s.intervalTagsUnicity())
	err = multierror.Append(err, s.checkIntervalsUpdatedAt())
	return err.ErrorOrNil()
}

// intervalTagsUnicity checks the database contains a single row
// for a interval_id, tag tuple with deleted_at being null.
func (s *Sanity) intervalTagsUnicity() (ret error) {
	type sanityRow struct {
		Interval int    `db:"interval_uuid"`
		Tag      string `db:"tag"`
	}
	rows, err := getRows[sanityRow](s.db, `
		SELECT interval_start_uuid, tag
		FROM interval_tags
			LEFT JOIN interval_tags_tombstone
				ON interval_tags.uuid = interval_tags_tombstone.interval_tag_uuid
		WHERE interval_tags_tombstone.uuid IS NULL
		GROUP BY interval_start_uuid, tag
		HAVING count(1) > 1`)
	if err != nil {
		return fmt.Errorf("cannot query the database: %w", err)
	}

	var merr *multierror.Error

	for _, r := range rows {
		merr = multierror.Append(
			merr, fmt.Errorf("%w (%d,%s)", ErrIntervalTagsUnicity, r.Interval, r.Tag))
	}

	return merr.ErrorOrNil()
}

// checkNoOverlap browses the full interval table to check that no registered
// and closed interval overlaps with another one. Each interval validity is individually checked.
func (s *Sanity) checkNoOverlap() (ret error) {
	rows, err := s.db.Query(`
		SELECT id, start_timestamp, stop_timestamp
		FROM interval_start
			JOIN interval_stop ON interval_start.uuid = interval_stop.start_uuid
			LEFT JOIN interval_tombstone ON interval_start.uuid = interval_tombstone.start_uuid
		WHERE interval_tombstone.uuid IS NULL
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
			return fmt.Errorf("%w: %#v", ErrInvalidInterval, *current)
		}

		if previous == nil {
			continue
		}

		if current.StartTimestamp.Before(previous.StopTimestamp) {
			return fmt.Errorf(
				"%w: current(%#v), previous(%#v)", ErrInvalidStartTimestamp, *current, *previous)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("cannot perform a full scan of intervals table: %w", err)
	}

	return nil
}

func (s *Sanity) checkIntervalsUpdatedAt() (ret error) {
	type sanityRow struct {
		Id   int
		Type string
	}
	rows, err := getRows[sanityRow](s.db, `
		SELECT id, 'updated before created' as type
		FROM interval_start
			JOIN interval_stop ON interval_start.start_timestamp = interval_stop.uuid
		WHERE interval_start.created_at > interval_stop.created_at`)
	if err != nil {
		return fmt.Errorf("cannot query the database: %w", err)
	}

	var merr *multierror.Error
	for _, r := range rows {
		merr = multierror.Append(merr, fmt.Errorf("%w: %s %d", ErrInvalidInterval, r.Type, r.Id))
	}

	return merr.ErrorOrNil()
}
