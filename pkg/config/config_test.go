package config

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFromFile(t *testing.T) {
	_, err := LoadFromFile("")
	require.Error(t, err)

	cwd, err := os.Getwd()
	require.NoError(t, err)

	// klepto/pkg/config/../../fixtures/.klepto.toml
	configPath := filepath.Join(cwd, "..", "..", "fixtures", ".klepto.toml")

	cfgTables, err := LoadFromFile(configPath)
	require.NoError(t, err)
	require.Len(t, cfgTables, 3)

	users := cfgTables.FindByName("users")
	require.NotNil(t, users)
	assert.Equal(t, "users.active = TRUE", users.Filter.Match)

	orders := cfgTables.FindByName("orders")
	require.NotNil(t, orders)
	assert.Equal(t, "users.active = TRUE", orders.Filter.Match)
}

func TestWriteSample(t *testing.T) {
	w := new(bytes.Buffer)

	err := WriteSample(w)
	require.NoError(t, err)

	assert.Equal(t, sampleConfig, w.String())
}

const (
	sampleConfig = `[Matchers]
  ActiveUsers = "users.active = TRUE"

[[Tables]]
  Name = "users"
  IgnoreData = false
  [Tables.Filter]
    Match = "users.active = TRUE"
    Limit = 100
    [Tables.Filter.Sorts]
      "user.id" = "asc"
  [Tables.Anonymise]
    email = "EmailAddress"
    firstName = "FirstName"

[[Tables]]
  Name = "orders"
  IgnoreData = false
  [Tables.Filter]
    Match = "ActiveUsers"
    Limit = 10

  [[Tables.Relationships]]
    Table = ""
    ForeignKey = "user_id"
    ReferencedTable = "users"
    ReferencedKey = "id"

[[Tables]]
  Name = "logs"
  IgnoreData = true
  [Tables.Filter]
    Match = ""
    Limit = 0
`
)
