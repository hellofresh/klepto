package database

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

type (
	Table struct {
		Name string
		Row  Row
	}

	Row map[string]interface{}
)

func NewTable(name string) Table {
	return Table{
		Name: name,
		Row:  make(map[string]interface{}),
	}
}

// ToSQLStringValue accepts a value and attempts to determine its type
func ToSQLStringValue(src interface{}) (string, error) {
	switch src.(type) {
	case int64:
		if value, ok := src.(int64); ok {
			return strconv.FormatInt(value, 10), nil
		}
	case float64:
		if value, ok := src.(float64); ok {
			return fmt.Sprintf("%v", value), nil
		}
	case bool:
		if value, ok := src.(bool); ok {
			return strconv.FormatBool(value), nil
		}
	case string:
		if value, ok := src.(string); ok {
			return value, nil
		}
	case []byte:
		// TODO handle blobs?
		if value, ok := src.([]byte); ok {
			return string(value), nil
		}
	case time.Time:
		if value, ok := src.(time.Time); ok {
			return value.String(), nil
		}
	case nil:
		return "NULL", nil
	case *interface{}:
		if src == nil {
			return "NULL", nil
		}
		return ToSQLStringValue(*(src.(*interface{})))
	default:
		return "", errors.New("could not parse type")
	}

	return "", nil
}
