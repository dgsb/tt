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

	t.Run("get interval start - null last sync", func(t *testing.T) {
		tt := setupTT(t)

		now := time.Now()

		for idx, data := range []intervalStartRow{
			{
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
			{
				StartTimestamp: now.Add(-4 * time.Hour).Unix(),
				CreatedAt:      now.Add(-2 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_start (uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)`,
				fmt.Sprintf("%d", idx+1),
				data.StartTimestamp,
				data.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		ir, err := getNewLocalIntervalStart(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
			{
				UUID:           "2",
				StartTimestamp: now.Add(-4 * time.Hour).Unix(),
				CreatedAt:      now.Add(-2 * time.Hour).Unix(),
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

		for idx, data := range []intervalStartRow{
			{
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
			{
				StartTimestamp: now.Add(-7 * time.Hour).Unix(),
				CreatedAt:      now.Add(-7 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_start (uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)`,
				fmt.Sprintf("%d", idx+1),
				data.StartTimestamp,
				data.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		ir, err := getNewLocalIntervalStart(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
		}, ir)
	})

	t.Run("get interval stop - null last sync", func(t *testing.T) {
		tt := setupTT(t)
		now := time.Now()

		for idx, r := range []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-8 * time.Hour).Unix(),
				CreatedAt:      now.Add(-8 * time.Hour).Unix(),
			},
			{
				UUID:           "2",
				StartTimestamp: now.Add(-7 * time.Hour).Unix(),
				CreatedAt:      now.Add(-7 * time.Hour).Unix(),
			},
			{
				UUID:           "3",
				StartTimestamp: now.Add(-4 * time.Hour).Unix(),
				CreatedAt:      now.Add(-4 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_start(uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)`,
				fmt.Sprintf("%d", idx+1),
				r.StartTimestamp,
				r.CreatedAt)
			require.NoError(t, err)
		}

		for _, r := range []intervalStopRow{
			{
				UUID:          "4",
				StartUUID:     "1",
				StopTimestamp: now.Add(-7 * time.Hour).Unix(),
				CreatedAt:     now.Add(-7 * time.Hour).Unix(),
			},
			{
				UUID:          "5",
				StartUUID:     "2",
				StopTimestamp: now.Add(-5 * time.Hour).Unix(),
				CreatedAt:     now.Add(-5 * time.Hour).Unix(),
			},
			{
				UUID:          "6",
				StartUUID:     "3",
				StopTimestamp: now.Add(-3 * time.Hour).Unix(),
				CreatedAt:     now.Add(-3 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_stop(uuid, start_uuid, stop_timestamp, created_at)
				VALUES (?, ?, ?, ?)`,
				r.UUID,
				r.StartUUID,
				r.StopTimestamp,
				r.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		ir, err := getNewLocalIntervalStop(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalStopRow{
			{
				UUID:          "4",
				StartUUID:     "1",
				StopTimestamp: now.Add(-7 * time.Hour).Unix(),
				CreatedAt:     now.Add(-7 * time.Hour).Unix(),
			},
			{
				UUID:          "5",
				StartUUID:     "2",
				StopTimestamp: now.Add(-5 * time.Hour).Unix(),
				CreatedAt:     now.Add(-5 * time.Hour).Unix(),
			},
			{
				UUID:          "6",
				StartUUID:     "3",
				StopTimestamp: now.Add(-3 * time.Hour).Unix(),
				CreatedAt:     now.Add(-3 * time.Hour).Unix(),
			},
		}, ir)
	})

	t.Run("get interval stop - with last sync", func(t *testing.T) {
		tt := setupTT(t)
		now := time.Now()

		{
			_, err := tt.db.Exec(`
			INSERT INTO sync_history (sync_timestamp)
			VALUES (?), (?)
		`, now.Add(-5*24*time.Hour).Unix(), now.Add(-6*time.Hour).Unix())
			require.NoError(t, err)
		}

		for idx, r := range []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-8 * time.Hour).Unix(),
				CreatedAt:      now.Add(-8 * time.Hour).Unix(),
			},
			{
				UUID:           "2",
				StartTimestamp: now.Add(-7 * time.Hour).Unix(),
				CreatedAt:      now.Add(-7 * time.Hour).Unix(),
			},
			{
				UUID:           "3",
				StartTimestamp: now.Add(-4 * time.Hour).Unix(),
				CreatedAt:      now.Add(-4 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_start(uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)`,
				fmt.Sprintf("%d", idx+1),
				r.StartTimestamp,
				r.CreatedAt)
			require.NoError(t, err)
		}

		for _, r := range []intervalStopRow{
			{
				UUID:          "4",
				StartUUID:     "1",
				StopTimestamp: now.Add(-7 * time.Hour).Unix(),
				CreatedAt:     now.Add(-7 * time.Hour).Unix(),
			},
			{
				UUID:          "5",
				StartUUID:     "2",
				StopTimestamp: now.Add(-5 * time.Hour).Unix(),
				CreatedAt:     now.Add(-5 * time.Hour).Unix(),
			},
			{
				UUID:          "6",
				StartUUID:     "3",
				StopTimestamp: now.Add(-3 * time.Hour).Unix(),
				CreatedAt:     now.Add(-3 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_stop(uuid, start_uuid, stop_timestamp, created_at)
				VALUES (?, ?, ?, ?)`,
				r.UUID,
				r.StartUUID,
				r.StopTimestamp,
				r.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		ir, err := getNewLocalIntervalStop(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalStopRow{
			{
				UUID:          "5",
				StartUUID:     "2",
				StopTimestamp: now.Add(-5 * time.Hour).Unix(),
				CreatedAt:     now.Add(-5 * time.Hour).Unix(),
			},
			{
				UUID:          "6",
				StartUUID:     "3",
				StopTimestamp: now.Add(-3 * time.Hour).Unix(),
				CreatedAt:     now.Add(-3 * time.Hour).Unix(),
			},
		}, ir)
	})

	t.Run("get interval tombstone - null last sync", func(t *testing.T) {

		tt := setupTT(t)

		now := time.Now()

		for _, r := range []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-6 * time.Hour).Unix(),
				CreatedAt:      now.Add(-3 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_start (uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)`,
				r.UUID,
				r.StartTimestamp,
				r.CreatedAt)
			require.NoError(t, err)
		}

		for _, r := range []intervalTombstoneRow{
			{
				UUID:      "10",
				StartUUID: "1",
				CreatedAt: now.Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tombstone (uuid, start_uuid, created_at)
				VALUES (?, ?, ?)`,
				r.UUID,
				r.StartUUID,
				r.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		ir, err := getNewLocalIntervalTombstone(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalTombstoneRow{
			{
				UUID:      "10",
				StartUUID: "1",
				CreatedAt: now.Unix(),
			},
		}, ir)
	})

	t.Run("get interval tombstone - with last sync", func(t *testing.T) {

		tt := setupTT(t)

		now := time.Now()

		{
			_, err := tt.db.Exec(`
			INSERT INTO sync_history (sync_timestamp)
			VALUES (?), (?)
		`, now.Add(-5*24*time.Hour).Unix(), now.Add(-6*time.Hour).Unix())
			require.NoError(t, err)
		}

		for _, r := range []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-8 * time.Hour).Unix(),
				CreatedAt:      now.Add(-8 * time.Hour).Unix(),
			},
			{
				UUID:           "2",
				StartTimestamp: now.Add(-7 * time.Hour).Unix(),
				CreatedAt:      now.Add(-7 * time.Hour).Unix(),
			},
			{
				UUID:           "3",
				StartTimestamp: now.Add(-4 * time.Hour).Unix(),
				CreatedAt:      now.Add(-4 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_start(uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)`,
				r.UUID,
				r.StartTimestamp,
				r.CreatedAt)
			require.NoError(t, err)
		}

		for _, r := range []intervalTombstoneRow{
			{
				UUID:      "10",
				StartUUID: "1",
				CreatedAt: now.Add(-7 * time.Hour).Unix(),
			},
			{
				UUID:      "11",
				StartUUID: "2",
				CreatedAt: now.Add(-5 * time.Hour).Unix(),
			},
			{
				UUID:      "12",
				StartUUID: "3",
				CreatedAt: now.Add(-3 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tombstone (uuid, start_uuid, created_at)
				VALUES (?, ?, ?)`,
				r.UUID,
				r.StartUUID,
				r.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		ir, err := getNewLocalIntervalTombstone(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalTombstoneRow{
			{
				UUID:      "11",
				StartUUID: "2",
				CreatedAt: now.Add(-5 * time.Hour).Unix(),
			},
			{
				UUID:      "12",
				StartUUID: "3",
				CreatedAt: now.Add(-3 * time.Hour).Unix(),
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

		for _, ir := range []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-24 * time.Hour).Unix(),
				CreatedAt:      now.Add(-24 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_start (uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)`,
				ir.UUID,
				ir.StartTimestamp,
				ir.CreatedAt)
			require.NoError(t, err)
		}

		for _, data := range []intervalTagsRow{
			{
				UUID:      "10",
				StartUUID: "1",
				Tag:       "a",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "11",
				StartUUID: "1",
				Tag:       "b",
				CreatedAt: now.Add(-23 * time.Hour).Unix(),
			},
			{
				UUID:      "12",
				StartUUID: "1",
				Tag:       "c",
				CreatedAt: now.Add(-22 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tags (uuid, interval_start_uuid, tag, created_at)
				VALUES (?, ?, ?, ?)`,
				data.UUID, data.StartUUID, data.Tag, data.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		itr, err := getNewLocalIntervalTags(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalTagsRow{
			{
				UUID:      "10",
				StartUUID: "1",
				Tag:       "a",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "11",
				StartUUID: "1",
				Tag:       "b",
				CreatedAt: now.Add(-23 * time.Hour).Unix(),
			},
			{
				UUID:      "12",
				StartUUID: "1",
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
						VALUES (?), (?)`,
				now.Add(-5*24*time.Hour).Unix(), now.Add(-6*time.Hour).Unix())
			require.NoError(t, err)
		}

		for _, tag := range []string{"a", "b", "c"} {
			_, err := tt.db.Exec(`INSERT INTO tags (name, created_at)
					VALUES (?, unixepoch('now'))`, tag)
			require.NoError(t, err)
		}

		for _, ir := range []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-24 * time.Hour).Unix(),
				CreatedAt:      now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:           "2",
				StartTimestamp: now.Add(-22 * time.Hour).Unix(),
				CreatedAt:      now.Add(-22 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
					INSERT INTO interval_start (uuid, start_timestamp, created_at)
					VALUES (?, ?, ?)`,
				ir.UUID,
				ir.StartTimestamp,
				ir.CreatedAt)
			require.NoError(t, err)
		}

		for _, data := range []intervalTagsRow{
			{
				UUID:      "10",
				StartUUID: "1",
				Tag:       "a",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "11",
				StartUUID: "1",
				Tag:       "b",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "12",
				StartUUID: "1",
				Tag:       "c",
				CreatedAt: now.Add(-4 * time.Hour).Unix(),
			},
			{
				UUID:      "13",
				StartUUID: "2",
				Tag:       "a",
				CreatedAt: now.Add(-3 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
					INSERT INTO interval_tags (uuid, interval_start_uuid, tag, created_at)
					VALUES (?, ?, ?, ?)`,
				data.UUID, data.StartUUID, data.Tag, data.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		itr, err := getNewLocalIntervalTags(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalTagsRow{
			{
				UUID:      "12",
				StartUUID: "1",
				Tag:       "c",
				CreatedAt: now.Add(-4 * time.Hour).Unix(),
			},
			{
				UUID:      "13",
				StartUUID: "2",
				Tag:       "a",
				CreatedAt: now.Add(-3 * time.Hour).Unix(),
			},
		}, itr)
	})

	t.Run("get interval tags tombstone - null last sync", func(t *testing.T) {
		tt := setupTT(t)

		now := time.Now()

		for _, tag := range []string{"a", "b", "c"} {
			_, err := tt.db.Exec(`INSERT INTO tags (name, created_at)
				VALUES (?, unixepoch('now'))`, tag)
			require.NoError(t, err)
		}

		for _, ir := range []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-24 * time.Hour).Unix(),
				CreatedAt:      now.Add(-24 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_start (uuid, start_timestamp, created_at)
				VALUES (?, ?, ?)`,
				ir.UUID,
				ir.StartTimestamp,
				ir.CreatedAt)
			require.NoError(t, err)
		}

		for _, data := range []intervalTagsRow{
			{
				UUID:      "10",
				StartUUID: "1",
				Tag:       "a",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "11",
				StartUUID: "1",
				Tag:       "b",
				CreatedAt: now.Add(-23 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tags (uuid, interval_start_uuid, tag, created_at)
				VALUES (?, ?, ?, ?)`,
				data.UUID, data.StartUUID, data.Tag, data.CreatedAt)
			require.NoError(t, err)
		}

		for _, data := range []intervalTagsTombstoneRow{
			{
				UUID:            "100",
				IntervalTagUUID: "10",
				CreatedAt:       now.Add(-1 * time.Hour).Unix(),
			},
			{
				UUID:            "101",
				IntervalTagUUID: "11",
				CreatedAt:       now.Add(-1 * time.Minute).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tags_tombstone (uuid, interval_tag_uuid, created_at)
				VALUES (?, ?, ?)`,
				data.UUID, data.IntervalTagUUID, data.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		data, err := getNewLocalIntervalTagsTombstone(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalTagsTombstoneRow{
			{
				UUID:            "100",
				IntervalTagUUID: "10",
				CreatedAt:       now.Add(-1 * time.Hour).Unix(),
			},
			{
				UUID:            "101",
				IntervalTagUUID: "11",
				CreatedAt:       now.Add(-1 * time.Minute).Unix(),
			},
		}, data)
	})

	t.Run("get interval tags tombstone - with last sync", func(t *testing.T) {

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

		for _, ir := range []intervalStartRow{
			{
				UUID:           "1",
				StartTimestamp: now.Add(-24 * time.Hour).Unix(),
				CreatedAt:      now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:           "2",
				StartTimestamp: now.Add(-23 * time.Hour).Unix(),
				CreatedAt:      now.Add(-23 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
					INSERT INTO interval_start (uuid, start_timestamp, created_at)
					VALUES (?, ?, ?)`,
				ir.UUID,
				ir.StartTimestamp,
				ir.CreatedAt)
			require.NoError(t, err)
		}

		for _, ir := range []intervalStopRow{
			{
				UUID:          "11",
				StartUUID:     "1",
				StopTimestamp: now.Add(-23 * time.Hour).Unix(),
				CreatedAt:     now.Add(-23 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_stop (uuid, start_uuid, stop_timestamp, created_at)
				VALUES (?, ?, ?, ?)`,
				ir.UUID,
				ir.StartUUID,
				ir.StopTimestamp,
				ir.CreatedAt)
			require.NoError(t, err)
		}

		for _, ir := range []intervalTagsRow{
			{
				UUID:      "101",
				StartUUID: "1",
				Tag:       "a",
				CreatedAt: now.Add(-24 * time.Hour).Unix(),
			},
			{
				UUID:      "102",
				StartUUID: "2",
				Tag:       "b",
				CreatedAt: now.Add(-23 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tags (uuid, interval_start_uuid, tag, created_at)
				VALUES (?, ?, ?, ?)`,
				ir.UUID,
				ir.StartUUID,
				ir.Tag,
				ir.CreatedAt)
			require.NoError(t, err)
		}

		for _, ir := range []intervalTagsTombstoneRow{
			{
				UUID:            "1001",
				IntervalTagUUID: "101",
				CreatedAt:       now.Add(-4 * time.Hour).Unix(),
			},
			{
				UUID:            "1002",
				IntervalTagUUID: "102",
				CreatedAt:       now.Add(-24 * 2 * time.Hour).Unix(),
			},
		} {
			_, err := tt.db.Exec(`
				INSERT INTO interval_tags_tombstone (uuid, interval_tag_uuid, created_at)
				VALUES (?, ?, ?)`,
				ir.UUID,
				ir.IntervalTagUUID,
				ir.CreatedAt)
			require.NoError(t, err)
		}

		tx, err := tt.db.Begin()
		require.NoError(t, err)
		t.Cleanup(func() { commit(t, tx) })

		data, err := getNewLocalIntervalTagsTombstone(tx)
		require.NoError(t, err)
		require.Equal(t, []intervalTagsTombstoneRow{
			{
				UUID:            "1001",
				IntervalTagUUID: "101",
				CreatedAt:       now.Add(-4 * time.Hour).Unix(),
			},
		}, data)
	})
}
