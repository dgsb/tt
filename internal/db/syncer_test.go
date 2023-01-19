package db

import (
	"context"
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
