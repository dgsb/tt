package config_test

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/dgsb/tt/config"
)

func TestRepository(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		dbFile := path.Join(t.TempDir(), "config.db")
		repo, err := New(dbFile)
		require.NoError(t, err)
		require.NotNil(t, repo)
		t.Cleanup(repo.Close)

		otherRepo, err := New(dbFile)
		require.NoError(t, err)
		require.NotNil(t, repo)
		t.Cleanup(otherRepo.Close)
	})

	t.Run("simple config registration", func(t *testing.T) {
		repo, err := New(":memory:")
		require.NoError(t, err)
		require.NotNil(t, repo)
		t.Cleanup(repo.Close)

		err = repo.RegisterApplication("fakeApp")
		require.NoError(t, err)

		err = repo.UpsertConfig("fakeApp", "fakeConfig", "fakeValue")
		require.NoError(t, err)

		config, err := repo.GetConfig("fakeApp", "fakeConfig")
		require.NoError(t, err)
		require.Equal(t, "fakeValue", config)

		err = repo.UpsertConfig("fakeApp", "fakeConfig", "updatedFakeValue")
		require.NoError(t, err)

		config, err = repo.GetConfig("fakeApp", "fakeConfig")
		require.NoError(t, err)
		require.Equal(t, "updatedFakeValue", config)
	})

	t.Run("multiple apps get all config", func(t *testing.T) {
		repo, err := New(":memory:")
		require.NoError(t, err)
		require.NotNil(t, repo)
		t.Cleanup(repo.Close)

		err = repo.RegisterApplication("fakeApp")
		require.NoError(t, err)

		err = repo.RegisterApplication("otherFakeApp")
		require.NoError(t, err)

		err = repo.UpsertConfig("fakeApp", "fakeConfig", "fakeValue")
		require.NoError(t, err)

		err = repo.UpsertConfig("fakeApp", "secondFakeConfig", "secondFakeValue")
		require.NoError(t, err)

		err = repo.UpsertConfig("otherFakeApp", "otherFakeConfig", "otherFakeValue")
		require.NoError(t, err)

		err = repo.UpsertConfig("otherFakeApp", "secondOtherFakeConfig", "secondOtherFakeValue")
		require.NoError(t, err)

		configs, err := repo.GetConfigs("fakeApp")
		require.NoError(t, err)
		require.Equal(t, map[string]string{
			"fakeConfig":       "fakeValue",
			"secondFakeConfig": "secondFakeValue",
		}, configs)

		configs, err = repo.GetConfigs("otherFakeApp")
		require.NoError(t, err)
		require.Equal(t, map[string]string{
			"otherFakeConfig":       "otherFakeValue",
			"secondOtherFakeConfig": "secondOtherFakeValue",
		}, configs)
	})
}
