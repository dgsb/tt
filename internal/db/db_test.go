package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupTT(t *testing.T) *TimeTracker {
	tt, err := New(":memory:")
	require.NoError(t, err)

	/*	fixtures, err := testfixtures.New(
			testfixtures.Database(tt.db),
			testfixtures.Dialect("sqlite"),
			testfixtures.Directory(fixturePath),
		)
		require.NoError(t, err)

		err = fixtures.Load()
		require.NoError(t, err)*/

	t.Cleanup(func() { tt.Close() })
	return tt
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
		require.Equal(t, &TaggedInterval{
			Interval: Interval{
				ID:             "1",
				StartTimestamp: now.Truncate(time.Second),
			},
		}, ti)

		err = tt.Stop(time.Time{})
		require.NoError(t, err)

		ti, err = tt.Current()
		require.NoError(t, err)
		require.Nil(t, ti)
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

		err = tt.Start(time.Date(2022, 2, 25, 14, 00, 0, 0, time.UTC), nil)
		require.Error(t, err)
	})

	t.Run("stop in closed interval not allowed", func(t *testing.T) {
		tt := setupTT(t)

		err := tt.Start(time.Date(2022, 2, 25, 13, 30, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 14, 30, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 12, 00, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 14, 00, 0, 0, time.UTC))
		require.Error(t, err)
	})

	t.Run("add new interval between 2 closed interval", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 00, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 00, 0, 0, time.UTC))
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

		err = tt.Start(time.Date(2022, 2, 25, 12, 00, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 00, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 11, 00, 0, 0, time.UTC), nil)
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 14, 00, 0, 0, time.UTC))
		require.Error(t, err)
	})

	t.Run("tag, untag and list combo", func(t *testing.T) {
		tt := setupTT(t)

		var err error

		err = tt.Start(time.Date(2022, 2, 25, 12, 00, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 00, 0, 0, time.UTC))
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

		err = tt.Start(time.Date(2022, 2, 25, 12, 00, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 00, 0, 0, time.UTC))
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

		err := tt.Start(time.Date(2022, 2, 25, 12, 00, 0, 0, time.UTC), []string{"tag1", "tag2"})
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 13, 00, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Start(time.Date(2022, 2, 25, 14, 00, 0, 0, time.UTC), []string{"tag3", "tag4"})
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 15, 00, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Continue(time.Date(2022, 2, 25, 16, 00, 0, 0, time.UTC), "1")
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 17, 00, 0, 0, time.UTC))
		require.NoError(t, err)

		err = tt.Continue(time.Date(2022, 2, 25, 18, 00, 0, 0, time.UTC), "2")
		require.NoError(t, err)

		err = tt.Stop(time.Date(2022, 2, 25, 19, 00, 0, 0, time.UTC))
		require.NoError(t, err)

		itv, err := tt.List(
			time.Date(2022, 2, 25, 11, 00, 0, 0, time.UTC),
			time.Date(2022, 2, 25, 20, 00, 0, 0, time.UTC))
		require.Len(t, itv, 4)
		require.Equal(t, []string{"tag1", "tag2"}, itv[0].Tags)
		require.Equal(t, []string{"tag3", "tag4"}, itv[1].Tags)
		require.Equal(t, []string{"tag1", "tag2"}, itv[2].Tags)
		require.Equal(t, []string{"tag3", "tag4"}, itv[3].Tags)
	})
}