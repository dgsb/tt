package db

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func setupTT(t *testing.T, file ...string) *TimeTracker {
	t.Helper()
	tt, err := New(
		func() string {
			if len(file) == 0 {
				return ":memory:"
			}
			return file[0]
		}(),
	)

	require.NoError(t, err)

	t.Cleanup(func() {
		err := tt.Close()
		require.NoError(t, err)
	})
	t.Cleanup(func() {
		err := NewSanity(tt.db).Check()
		require.NoError(t, err, "sanity check failed")
	})
	return tt
}

func TestDependenciesBehaviour(t *testing.T) {
	t.Run("sqlite3 uuid function", func(t *testing.T) {
		tt := setupTT(t)
		db := tt.db

		row := db.QueryRow(`SELECT uuid()`)
		require.NotNil(t, row)

		var uuidCol string
		err := row.Scan(&uuidCol)
		require.NoError(t, err)
	})
}

func TestTimeTracker(t *testing.T) {

	t.Run("simple start current stop list", func(t *testing.T) {
		tt := setupTT(t)

		ti, err := tt.Current()
		require.Nil(t, ti)
		require.NoError(t, err)

		now := time.Now()
		err = tt.Start(now, []string{})
		require.NoError(t, err)

		ti, err = tt.Current()
		require.NoError(t, err)
		_, err = uuid.Parse(ti.UUID)
		require.NoError(t, err)
		ti.UUID = ""
		require.Equal(t, &TaggedInterval{
			Interval: Interval{
				ID:             "1",
				StartTimestamp: now.Truncate(time.Second),
			},
		}, ti)

		err = tt.Stop(now.Add(time.Hour))
		require.NoError(t, err)

		ti, err = tt.Current()
		require.NoError(t, err)
		require.Nil(t, ti)
	})

	t.Run("stop timestamp before start - failed", func(t *testing.T) {
		tt := setupTT(t)
		now := time.Now()
		err := tt.Start(now, []string{})
		require.NoError(t, err)
		err = tt.Stop(now.Add(-time.Hour))
		require.Error(t, err)
	})

	t.Run("stop timestamp is zero - failed", func(t *testing.T) {
		tt := setupTT(t)
		now := time.Now()
		err := tt.Start(now, []string{})
		require.NoError(t, err)
		err = tt.Stop(time.Time{})
		require.Error(t, err)
	})

	t.Run("simple check stop timestamp after start timestamp", func(t *testing.T) {
		tt := setupTT(t)

		err := tt.Start(time.Date(2022, 2, 25, 13, 30, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC))
		require.Error(t, err)
	})

	t.Run("start in closed interval not allowed", func(t *testing.T) {
		tt := setupTT(t)

		err := tt.Start(time.Date(2022, 2, 25, 13, 30, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 14, 30, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC), nil)
		require.Error(t, err)
	})

	t.Run("stop in closed interval not allowed", func(t *testing.T) {
		tt := setupTT(t)

		err := tt.Start(time.Date(2022, 2, 25, 13, 30, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 14, 30, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC))
		require.Error(t, err)
	})

	t.Run("add new interval between 2 closed interval", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 15, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC))
		require.NoError(t, err)
	})

	t.Run("add new interval containing a closed one not allowed", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 11, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC))
		require.Error(t, err)
	})

	t.Run("tag, untag and list combo", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		itv, err := tt.List(
			time.Date(2022, 2, 24, 0, 0, 0, 0, time.UTC),
			time.Date(2022, 2, 26, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Len(t, itv, 1)
		require.Equal(t, []string{"tag1", "tag2"}, itv[0].Tags)

		err = tt.Tag(itv[0].ID, []string{"tag3", "tag4"})
		require.NoError(t, err)

		err = tt.Untag(itv[0].ID, []string{"tag2"})
		require.NoError(t, err)

		itv, err = tt.List(
			time.Date(2022, 2, 24, 0, 0, 0, 0, time.UTC),
			time.Date(2022, 2, 26, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Len(t, itv, 1)
		require.Equal(t, []string{"tag1", "tag3", "tag4"}, itv[0].Tags)
	})

	t.Run("delete", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		itv, err := tt.List(
			time.Date(2022, 2, 24, 0, 0, 0, 0, time.UTC),
			time.Date(2022, 2, 26, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Len(t, itv, 1)

		err = tt.Delete(itv[0].ID)
		require.NoError(t, err)

		itv, err = tt.List(
			time.Date(2022, 2, 24, 0, 0, 0, 0, time.UTC),
			time.Date(2022, 2, 26, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Len(t, itv, 0)
	})

	t.Run("continue", func(t *testing.T) {
		tt := setupTT(t)

		err := tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC), []string{"tag3", "tag4"})
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 15, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Continue(time.Date(2022, 2, 25, 15, 0, 0, 0, time.UTC), "")
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 16, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Continue(time.Date(2022, 2, 25, 16, 0, 0, 0, time.UTC), "1")
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 17, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Continue(time.Date(2022, 2, 25, 18, 0, 0, 0, time.UTC), "2")
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 19, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		itv, err := tt.List(
			time.Date(2022, 2, 25, 11, 0, 0, 0, time.UTC),
			time.Date(2022, 2, 25, 20, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Len(t, itv, 5)
		require.Equal(t, []string{"tag1", "tag2"}, itv[0].Tags)
		require.Equal(t, []string{"tag3", "tag4"}, itv[1].Tags)
		require.Equal(t, []string{"tag3", "tag4"}, itv[2].Tags)
		require.Equal(t, []string{"tag1", "tag2"}, itv[3].Tags)
		require.Equal(t, []string{"tag3", "tag4"}, itv[4].Tags)
	})
}
