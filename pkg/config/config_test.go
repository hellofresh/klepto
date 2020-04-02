package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSpecFromFile(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	// klepto/pkg/config/../../fixtures/.klepto.toml
	configPath := filepath.Join(cwd, "..", "..", "fixtures", ".klepto.toml")

	spec, err := LoadSpecFromFile(configPath)
	require.NoError(t, err)

	cfgTables := spec.Tables
	require.Len(t, cfgTables, 3)

	users := cfgTables.FindByName("users")
	require.NotNil(t, users)
	assert.Equal(t, "users.active = TRUE", users.Filter.Match)

	orders := cfgTables.FindByName("orders")
	require.NotNil(t, orders)
	assert.Equal(t, "ActiveUsers", orders.Filter.Match)
}
