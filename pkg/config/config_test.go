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
	require.Len(t, cfgTables, 6)

	orders := cfgTables.FindByName("orders")
	require.NotNil(t, orders)
	assert.Equal(t, "_default", orders.Subsets[0].Name)
	assert.Equal(t, "users.active = TRUE", orders.Subsets[0].Filter.Match)

	// When no Subsets, Filters, or Anonymise blocks are defined we expect to
	// create a single Subset for the table root with no Filter or Anonymise.
	veggies := cfgTables.FindByName("vegetables")
	require.Equal(t, 1, len(veggies.Subsets))
	require.Equal(t, "_default", veggies.Subsets[0].Name)
	require.Equal(t, "", veggies.Subsets[0].Filter.Match)
	require.Equal(t, 0, len(orders.Subsets[0].Anonymise))

	// When no Subsets are defined, but Filters and Anonymise are present at the
	// table root, we expect to create a single Subset for the table root with
	// the Filter and Anonymise information copied over.
	fruits := cfgTables.FindByName("fruits")
	require.Equal(t, 1, len(fruits.Subsets))
	require.Equal(t, "_default", fruits.Subsets[0].Name)
	require.Equal(t, "fruits.color = 'red'", fruits.Subsets[0].Filter.Match)
	require.Equal(t, 1, len(fruits.Subsets[0].Anonymise))
	require.Equal(t, "FirstName", fruits.Subsets[0].Anonymise["name"])

	// When Subsets are explicitly defined and no Filters or Anonymise are defined
	// at the table root, we expect only the explicit Subsets to exist in the config.
	users := cfgTables.FindByName("users")
	require.NotNil(t, users)
	assert.Equal(t, "active", users.Subsets[0].Name)
	assert.Equal(t, "users.active = TRUE", users.Subsets[0].Filter.Match)
	require.Equal(t, 1, len(users.Subsets))

	// When Subsets are explicitly defined and Filters or Anonymise are also
	// defined at the table root, the root-level filters and anonymise are moved
	// to a new _default Subset.
	grains := cfgTables.FindByName("grains")
	require.Equal(t, 2, len(grains.Subsets))

	require.Equal(t, "starchy", grains.Subsets[0].Name)
	require.Equal(t, "grains.starchy = TRUE", grains.Subsets[0].Filter.Match)
	require.Equal(t, "FirstName", grains.Subsets[0].Anonymise["name"])

	require.Equal(t, "_default", grains.Subsets[1].Name)
	require.Equal(t, "grains.size = 'large'", grains.Subsets[1].Filter.Match)
	require.Equal(t, 1, len(grains.Subsets[1].Anonymise))
	require.Equal(t, "Digits", grains.Subsets[1].Anonymise["weight"])
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
