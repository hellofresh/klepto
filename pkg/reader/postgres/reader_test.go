package postgres

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveQuotaFromTableName(t *testing.T) {
	//given : expected
	tables := map[string]string{
		strconv.Quote("TableName"): "TableName",
		strconv.Quote("tableName"): "tableName",
		"table-name":               "table-name",
	}

	for given, expected := range tables {
		assert.Equal(t, removeQuotesFromTableName(given), expected)
		fmt.Println(given, expected)
	}
}
