package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/dgsb/tt/config"
)

func TestRepository(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		repo, err := New("config.db")
		require.NoError(t, err)
		require.NotNil(t, repo)
	})
}
