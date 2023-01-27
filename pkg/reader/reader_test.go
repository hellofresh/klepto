package reader

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hellofresh/klepto/pkg/config"
)

func TestNewReadTableOpt(t *testing.T) {
	tableCfg := &config.Table{
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
			{
				Table:           strconv.Quote("TableName"),
				ForeignKey:      "r1-fk",
				ReferencedTable: "r1-reference-table",
				ReferencedKey:   "r1-reference-key",
			},
		},
	}

	tableOpt := NewReadTableOpt(tableCfg)

	assert.Equal(t, tableCfg.Filter.Match, tableOpt.Match)
	assert.Equal(t, tableCfg.Filter.Limit, tableOpt.Limit)
	assert.Equal(t, tableCfg.Filter.Sorts, tableOpt.Sorts)

	require.Equal(t, len(tableCfg.Relationships), len(tableOpt.Relationships))
	for i := range tableCfg.Relationships {
		assert.Equal(t, tableCfg.Relationships[i].Table, tableOpt.Relationships[i].Table)
		assert.Equal(t, tableCfg.Relationships[i].ForeignKey, tableOpt.Relationships[i].ForeignKey)
		assert.Equal(t, tableCfg.Relationships[i].ReferencedTable, tableOpt.Relationships[i].ReferencedTable)
		assert.Equal(t, tableCfg.Relationships[i].ReferencedKey, tableOpt.Relationships[i].ReferencedKey)
	}
}
