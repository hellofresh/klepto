package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hellofresh/klepto/pkg/config"
)

func TestNewReadTableOpt(t *testing.T) {
	tableCfg := &config.Table{
		Subsets: []*config.Subset{
			{
				Name: "_default",
				Filter: config.Filter{
					Match: "foo-match",
					Limit: 123,
					Sorts: map[string]string{"foo": "asc", "bar": "desc"},
				},
				Relationships: []*config.Relationship{
					{
						Table:           "r1-table",
						ForeignKey:      "r1-fk",
						ReferencedTable: "r1-reference-table",
						ReferencedKey:   "r1-reference-key",
					}, {
						Table:           "r2-table",
						ForeignKey:      "r2-fk",
						ReferencedTable: "r2-reference-table",
						ReferencedKey:   "r2-reference-key",
					},
				},
			},
		},
	}

	tableOpt := NewReadTableOpt(tableCfg)

	assert.Equal(t, tableCfg.Subsets[0].Filter.Match, tableOpt.Subsets[0].Match)
	assert.Equal(t, tableCfg.Subsets[0].Filter.Limit, tableOpt.Subsets[0].Limit)
	assert.Equal(t, tableCfg.Subsets[0].Filter.Sorts, tableOpt.Subsets[0].Sorts)

	require.Equal(t, len(tableCfg.Subsets[0].Relationships), len(tableOpt.Subsets[0].Relationships))
	for i := range tableCfg.Subsets[0].Relationships {
		assert.Equal(t, tableCfg.Subsets[0].Relationships[i].Table, tableOpt.Subsets[0].Relationships[i].Table)
		assert.Equal(t, tableCfg.Subsets[0].Relationships[i].ForeignKey, tableOpt.Subsets[0].Relationships[i].ForeignKey)
		assert.Equal(t, tableCfg.Subsets[0].Relationships[i].ReferencedTable, tableOpt.Subsets[0].Relationships[i].ReferencedTable)
		assert.Equal(t, tableCfg.Subsets[0].Relationships[i].ReferencedKey, tableOpt.Subsets[0].Relationships[i].ReferencedKey)
	}
}
