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
		err = tt.Start(now, []string{"a", "b", "c"})
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
			Tags: []string{"a", "b", "c"},
		}, ti)

		err = tt.StopAt(now.Add(time.Hour))
		require.NoError(t, err)

		ti, err = tt.Current()
		require.NoError(t, err)
		require.Nil(t, ti)

		tia, err := tt.List(now.Add(-1*time.Hour), now.Add(2*time.Hour))
		require.NoError(t, err)
		tia[0].UUID = ""

		require.Equal(t, []TaggedInterval{
			{
				Interval: Interval{
					ID:             "1",
					StartTimestamp: now.Truncate(time.Second),
					StopTimestamp:  now.Add(time.Hour).Truncate(time.Second),
				},
				Tags: []string{"a", "b", "c"},
			},
		}, tia)
	})

	t.Run("stop timestamp before start - failed", func(t *testing.T) {
		tt := setupTT(t)
		now := time.Now()
		err := tt.Start(now, []string{})
		require.NoError(t, err)
		err = tt.StopAt(now.Add(-time.Hour))
		require.Error(t, err)
	})

	t.Run("stop timestamp is zero - failed", func(t *testing.T) {
		tt := setupTT(t)
		now := time.Now()
		err := tt.Start(now, []string{})
		require.NoError(t, err)
		err = tt.StopAt(time.Time{})
		require.Error(t, err)
	})

	t.Run("simple check stop timestamp after start timestamp", func(t *testing.T) {
		tt := setupTT(t)

		err := tt.Start(time.Date(2022, 2, 25, 13, 30, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC))
		require.Error(t, err)
	})

	t.Run("start in closed interval not allowed", func(t *testing.T) {
		tt := setupTT(t)

		err := tt.Start(time.Date(2022, 2, 25, 13, 30, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 14, 30, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC), nil)
		require.Error(t, err)
	})

	t.Run("stop in closed interval not allowed", func(t *testing.T) {
		tt := setupTT(t)

		err := tt.Start(time.Date(2022, 2, 25, 13, 30, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 14, 30, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC))
		require.Error(t, err)
	})

	t.Run("add new interval between 2 closed interval", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 15, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC))
		require.NoError(t, err)
	})

	t.Run("add new interval containing a closed one not allowed", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 11, 0, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC))
		require.Error(t, err)
	})

	t.Run("tag, untag and list combo", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
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

	t.Run("untag deleted interval", func(t *testing.T) {

		tt := setupTT(t)
		now := time.Now().Truncate(time.Second)

		err := tt.Start(now, []string{"tag1", "tag2"})
		require.NoError(t, err)

		itv, err := tt.List(now.Add(-time.Hour), now.Add(time.Hour))
		require.NoError(t, err)
		require.Len(t, itv, 1)
		itv[0].UUID = ""
		require.Equal(t, []TaggedInterval{
			{
				Interval: Interval{
					ID:             "1",
					StartTimestamp: now,
				},
				Tags: []string{"tag1", "tag2"},
			},
		}, itv)

		err = tt.Delete(itv[0].ID)
		require.NoError(t, err)

		err = tt.Untag(itv[0].ID, []string{"tag2"})
		require.Error(t, err)

		itv, err = tt.List(now.Add(-time.Hour), now.Add(time.Hour))
		require.NoError(t, err)
		require.Len(t, itv, 0)
	})

	t.Run("delete", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC), []string{"tag3", "tag4"})
		require.NoError(t, err)

		itv, err := tt.List(
			time.Date(2022, 2, 24, 0, 0, 0, 0, time.UTC),
			time.Date(2022, 2, 26, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Len(t, itv, 2)

		err = tt.Delete(itv[0].ID)
		require.NoError(t, err)

		err = tt.Delete(itv[1].ID)
		require.NoError(t, err)

		itv, err = tt.List(
			time.Date(2022, 2, 24, 0, 0, 0, 0, time.UTC),
			time.Date(2022, 2, 26, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Len(t, itv, 0)
	})

	t.Run("continue", func(t *testing.T) {
		tt := setupTT(t)

		// Implicit continue without previous interval should fail
		{
			err := tt.Continue(time.Now(), "")
			require.Error(t, err)
		}

		// Test count the query with an unclosed deleted interval
		{
			err := tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), []string{"tag1", "tag2"})
			require.NoError(t, err)

			err = tt.Delete("1")
			require.NoError(t, err)

			err = tt.Continue(time.Now(), "1")
			require.Error(t, err)
		}

		err := tt.Start(time.Date(2022, 2, 25, 12, 0, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 13, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 14, 0, 0, 0, time.UTC), []string{"tag3", "tag4"})
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 15, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Continue(time.Date(2022, 2, 25, 15, 0, 0, 0, time.UTC), "")
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 16, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Continue(time.Date(2022, 2, 25, 16, 0, 0, 0, time.UTC), "2")
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 17, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Continue(time.Date(2022, 2, 25, 18, 0, 0, 0, time.UTC), "3")
		require.NoError(t, err)

		err = tt.StopAt(time.Date(2022, 2, 25, 19, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		itv, err := tt.List(
			time.Date(2022, 2, 25, 11, 0, 0, 0, time.UTC),
			time.Date(2022, 2, 25, 20, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Len(t, itv, 5)
		require.Equal(t, "2", itv[0].ID)
		require.Equal(t, []string{"tag1", "tag2"}, itv[0].Tags)
		require.Equal(t, "3", itv[1].ID)
		require.Equal(t, []string{"tag3", "tag4"}, itv[1].Tags)
		require.Equal(t, "4", itv[2].ID)
		require.Equal(t, []string{"tag3", "tag4"}, itv[2].Tags)
		require.Equal(t, "5", itv[3].ID)
		require.Equal(t, []string{"tag1", "tag2"}, itv[3].Tags)
		require.Equal(t, "6", itv[4].ID)
		require.Equal(t, []string{"tag3", "tag4"}, itv[4].Tags)
	})

	t.Run("continue on id with deleted tags", func(t *testing.T) {
		now := time.Now().Truncate(time.Second)
		tt := setupTT(t)

		err := tt.Start(now.Add(-time.Hour), []string{"tag1", "tag2", "tag3"})
		require.NoError(t, err)

		err = tt.StopAt(now.Add(-59 * time.Minute))
		require.NoError(t, err)

		err = tt.Untag("1", []string{"tag2"})
		require.NoError(t, err)

		err = tt.Continue(now.Add(-58*time.Minute), "")
		require.NoError(t, err)

		err = tt.StopAt(now.Add(-57 * time.Minute))
		require.NoError(t, err)

		err = tt.Continue(now.Add(-56*time.Minute), "1")
		require.NoError(t, err)

		err = tt.StopAt(now.Add(-55 * time.Minute))
		require.NoError(t, err)

		itv, err := tt.List(now.Add(-2*time.Hour), now.Add(time.Hour))
		require.NoError(t, err)
		for idx := range itv {
			itv[idx].UUID = ""
		}
		require.Equal(t, []TaggedInterval{
			{
				Interval: Interval{
					ID:             "1",
					StartTimestamp: now.Add(-time.Hour),
					StopTimestamp:  now.Add(-59 * time.Minute),
				},
				Tags: []string{"tag1", "tag3"},
			},
			{
				Interval: Interval{
					ID:             "2",
					StartTimestamp: now.Add(-58 * time.Minute),
					StopTimestamp:  now.Add(-57 * time.Minute),
				},
				Tags: []string{"tag1", "tag3"},
			},
			{
				Interval: Interval{
					ID:             "3",
					StartTimestamp: now.Add(-56 * time.Minute),
					StopTimestamp:  now.Add(-55 * time.Minute),
				},
				Tags: []string{"tag1", "tag3"},
			},
		}, itv)
	})
}
