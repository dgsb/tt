package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startPostgres(t *testing.T) (hostname string, port int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pg, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "postgres:15",
				Env:          map[string]string{"POSTGRES_PASSWORD": "postgres"},
				ExposedPorts: []string{"5432/tcp"},
				WaitingFor:   wait.ForExposedPort(),
			},
			Started: true,
		})
	require.NoError(t, err, "cannot start postgres container")
	require.True(t, pg.IsRunning())

	t.Cleanup(func() {
		cleanupErr := pg.Terminate(context.Background())
		require.NoError(t, cleanupErr)
	})

	// this method returns the endpoint with the form <ip>:<port>
	endpoint, err := pg.Endpoint(ctx, "")
	require.NoError(t, err)
	splitted := strings.Split(endpoint, ":")
	require.Len(t, splitted, 2, "cannot split endpoint: %s", endpoint)
	port, err = strconv.Atoi(splitted[1])
	require.NoError(t, err)
	return splitted[0], port
}

func commit(t *testing.T, tx *sql.Tx) {
	t.Helper()
	err := tx.Commit()
	require.NoError(t, err)
}

func TestSetupSyncer(t *testing.T) {
	ip, port := startPostgres(t)

	db, err := setupSyncerDB(SyncerConfig{
		Login:        "postgres",
		Password:     "postgres",
		Hostname:     ip,
		Port:         port,
		DatabaseName: "postgres",
	})
	require.NoError(t, err)
	require.NotNil(t, db)
}

func TestSync(t *testing.T) {
	t.Run("get tags - null last sync", func(t *testing.T) {
		tt := setupTT(t)
		_, err := tt.db.Exec(`
			INSERT INTO TAGS (name, created_at)
			VALUES ('test_tag1', unixepoch('now')),
				('test_tag2', unixepoch('now'))`)
		require.NoError(t, err)

		tx, err := tt.db.Begin()
		require.NoError(t, err)

		t.Cleanup(func() { commit(t, tx) })

		tags, err := getNewLocalTags(tx)
		require.NoError(t, err)
		require.Equal(t, []string{"test_tag1", "test_tag2"}, tags)
	})

	t.Run("get tags - not null last sync", func(t *testing.T) {
		tt := setupTT(t)

		now := time.Now()

		_, err := tt.db.Exec(`
			INSERT INTO TAGS (name, created_at)
			VALUES ('test_tag1', ?),
				('test_tag2', ?)`, now.Add(-time.Hour).Unix(), now.Add(time.Hour).Unix())
		require.NoError(t, err)

		_, err = tt.db.Exec(`
			INSERT INTO sync_history
			VALUES (?), (?)`, now.Add(-2*time.Hour).Unix(), now.Unix())
		require.NoError(t, err)

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		tags, err := getNewLocalTags(tx)
		require.NoError(t, err)
		require.Equal(t, []string{"test_tag2"}, tags)
	})

	t.Run("get intervals - null last sync", func(t *testing.T) {
		tt := setupTT(t)

		now := time.Now()

		for idx, data := range []intervalRow{
			{
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
			{
				StartTimestamp: now.Add(-4 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-3 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-2 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-time.Hour).Unix(),
					Valid: true,
				},
			},
			{
				StartTimestamp: now.Add(-11 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-10 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-9 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-8 * time.Hour).Unix(),
					Valid: true,
				},
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-7 * time.Minute).Unix(),
					Valid: true,
				},
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO intervals (
					uuid, start_timestamp, stop_timestamp, created_at, updated_at, deleted_at)
				VALUES (?, ?, ?, ?, ?, ?)`,
				fmt.Sprintf("%d", idx+1),
				data.StartTimestamp,
				data.StopTimestamp,
				data.CreatedAt,
				data.UpdatedAt,
				data.DeletedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		ir, err := getNewLocalIntervals(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalRow{
			{
				ID:             1,
				UUID:           "1",
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
			{
				ID:             2,
				UUID:           "2",
				StartTimestamp: now.Add(-4 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-3 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-2 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-time.Hour).Unix(),
					Valid: true,
				},
			},
			{
				ID:             3,
				UUID:           "3",
				StartTimestamp: now.Add(-11 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-10 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-9 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-8 * time.Hour).Unix(),
					Valid: true,
				},
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-7 * time.Minute).Unix(),
					Valid: true,
				},
			},
		}, ir)
	})

	t.Run("get intervals - with last sync", func(t *testing.T) {

		tt := setupTT(t)

		now := time.Now()

		{
			_, err := tt.db.Exec(`
			INSERT INTO sync_history (sync_timestamp)
			VALUES (?), (?)
		`, now.Add(-5*24*time.Hour).Unix(), now.Add(-6*time.Hour).Unix())
			require.NoError(t, err)
		}

		for idx, data := range []intervalRow{
			{
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
			{
				StartTimestamp: now.Add(-7 * time.Hour).Unix(),
				CreatedAt:      now.Add(-7 * time.Hour).Unix(),
			},
			{
				StartTimestamp: now.Add(-8 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-7 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-8 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-1 * time.Hour).Unix(),
					Valid: true,
				},
			},
			{
				StartTimestamp: now.Add(-9 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-8 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-9 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-8 * time.Hour).Unix(),
					Valid: true,
				},
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-7 * time.Hour).Unix(),
					Valid: true,
				},
			},
			{
				StartTimestamp: now.Add(-10 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-9 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-10 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-9 * time.Hour).Unix(),
					Valid: true,
				},
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-1 * time.Hour).Unix(),
					Valid: true,
				},
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO intervals (
					uuid, start_timestamp, stop_timestamp, created_at, updated_at, deleted_at)
				VALUES (?, ?, ?, ?, ?, ?)`,
				fmt.Sprintf("%d", idx+1),
				data.StartTimestamp,
				data.StopTimestamp,
				data.CreatedAt,
				data.UpdatedAt,
				data.DeletedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		ir, err := getNewLocalIntervals(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalRow{
			{
				ID:             1,
				UUID:           "1",
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
			{
				ID:             3,
				UUID:           "3",
				StartTimestamp: now.Add(-8 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-7 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-8 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-1 * time.Hour).Unix(),
					Valid: true,
				},
			},
			{
				ID:             5,
				UUID:           "5",
				StartTimestamp: now.Add(-10 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-9 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-10 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-9 * time.Hour).Unix(),
					Valid: true,
				},
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-1 * time.Hour).Unix(),
					Valid: true,
				},
			},
		}, ir)
	})

	t.Run("get interval tags - null last sync", func(t *testing.T) {

		tt := setupTT(t)

		now := time.Now()

		for _, tag := range []string{"a", "b", "c"} {
			_, err := tt.db.Exec(`INSERT INTO tags (name, created_at)
				VALUES (?, unixepoch('now'))`, tag)
			require.NoError(t, err)
		}

		for _, ir := range []intervalRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-24 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-23 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-23 * time.Hour).Unix(),
					Valid: true,
				},
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO intervals (
					uuid, start_timestamp, stop_timestamp, deleted_at, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)`,
				ir.UUID,
				ir.StartTimestamp,
				ir.StopTimestamp,
				ir.DeletedAt,
				ir.CreatedAt,
				ir.UpdatedAt)
			require.NoError(t, err)
		}

		for _, data := range []intervalTagsRow{
			{
				UUID:      "1",
				Tag:       "a",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "1",
				Tag:       "b",
				CreatedAt: now.Add(-23 * time.Hour).Unix(),
			},
			{
				UUID:      "1",
				Tag:       "c",
				CreatedAt: now.Add(-22 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tags (interval_uuid, tag, created_at, deleted_at)
				VALUES (?, ?, ?, ?)`,
				data.UUID, data.Tag, data.CreatedAt, data.DeletedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		itr, err := getNewLocalIntervalTags(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalTagsRow{
			{
				UUID:      "1",
				Tag:       "a",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "1",
				Tag:       "b",
				CreatedAt: now.Add(-23 * time.Hour).Unix(),
			},
			{
				UUID:      "1",
				Tag:       "c",
				CreatedAt: now.Add(-22 * time.Hour).Unix(),
			},
		}, itr)
	})

	t.Run("get interval tags - with last sync", func(t *testing.T) {

		tt := setupTT(t)

		now := time.Now()

		{
			_, err := tt.db.Exec(`
					INSERT INTO sync_history (sync_timestamp)
					VALUES (?), (?)
				`, now.Add(-5*24*time.Hour).Unix(), now.Add(-6*time.Hour).Unix())
			require.NoError(t, err)
		}

		for _, tag := range []string{"a", "b", "c"} {
			_, err := tt.db.Exec(`INSERT INTO tags (name, created_at)
				VALUES (?, unixepoch('now'))`, tag)
			require.NoError(t, err)
		}

		for _, ir := range []intervalRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-24 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-23 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-23 * time.Hour).Unix(),
					Valid: true,
				},
			},
			{
				UUID:           "2",
				StartTimestamp: now.Add(-22 * time.Hour).Unix(),
				StopTimestamp: sql.NullInt64{
					Int64: now.Add(-21 * time.Hour).Unix(),
					Valid: true,
				},
				CreatedAt: now.Add(-22 * time.Hour).Unix(),
				UpdatedAt: sql.NullInt64{
					Int64: now.Add(-21 * time.Hour).Unix(),
					Valid: true,
				},
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO intervals (
					uuid, start_timestamp, stop_timestamp, deleted_at, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)`,
				ir.UUID,
				ir.StartTimestamp,
				ir.StopTimestamp,
				ir.DeletedAt,
				ir.CreatedAt,
				ir.UpdatedAt)
			require.NoError(t, err)
		}

		for _, data := range []intervalTagsRow{
			{
				UUID:      "1",
				Tag:       "a",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "1",
				Tag:       "b",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-4 * time.Hour).Unix(),
					Valid: true,
				},
			},
			{
				UUID:      "1",
				Tag:       "c",
				CreatedAt: now.Add(-4 * time.Hour).Unix(),
			},
			{
				UUID:      "2",
				Tag:       "a",
				CreatedAt: now.Add(-3 * time.Hour).Unix(),
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-2 * time.Hour).Unix(),
					Valid: true,
				},
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tags (interval_uuid, tag, created_at, deleted_at)
				VALUES (?, ?, ?, ?)`,
				data.UUID, data.Tag, data.CreatedAt, data.DeletedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		itr, err := getNewLocalIntervalTags(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalTagsRow{
			{
				UUID:      "1",
				Tag:       "b",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-4 * time.Hour).Unix(),
					Valid: true,
				},
			},
			{
				UUID:      "1",
				Tag:       "c",
				CreatedAt: now.Add(-4 * time.Hour).Unix(),
			},
			{
				UUID:      "2",
				Tag:       "a",
				CreatedAt: now.Add(-3 * time.Hour).Unix(),
				DeletedAt: sql.NullInt64{
					Int64: now.Add(-2 * time.Hour).Unix(),
					Valid: true,
				},
			},
		}, itr)
	})
}
