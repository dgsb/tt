package db

import (
	"strconv"
	"testing"

	"github.com/dgsb/configlite"
	"github.com/stretchr/testify/require"
)

const testAppName = "github.com/dgsb/tt.test"

func TestSetupSyncer(t *testing.T) {
	reg, err := configlite.New(configlite.DefaultConfigurationFile())
	require.NoError(t, err)
	require.NotNil(t, reg)

	err = reg.RegisterApplication(testAppName)
	require.NoError(t, err)

	login, err := reg.GetConfig(testAppName, "login")
	require.NoError(t, err)

	password, err := reg.GetConfig(testAppName, "key")
	require.NoError(t, err)

	host, err := reg.GetConfig(testAppName, "hostname")
	require.NoError(t, err)

	portStr, err := reg.GetConfig(testAppName, "port")
	require.NoError(t, err)

	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	dbname, err := reg.GetConfig(testAppName, "database")
	require.NoError(t, err)

	db, err := setupSyncerDB(SyncerConfig{
		Login:        login,
		Password:     password,
		Hostname:     host,
		Port:         port,
		DatabaseName: dbname,
	})
	require.NoError(t, err)
	require.NotNil(t, db)
}
