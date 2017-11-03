package utils

import (
	"time"
)

// TypeScanner tries to determine the type of a provided value
type TypeScanner struct {
	Valid    bool
	Value    interface{}
	Detected string
}

func (scanner *TypeScanner) getBytes(src interface{}) []byte {
	if a, ok := src.([]uint8); ok {
		return a
	}
	return nil
}

// Scan accepts a value and attempts to determine its type
func (scanner *TypeScanner) Scan(src interface{}) {
	switch src.(type) {
	case int64:
		if value, ok := src.(int64); ok {
			scanner.Value = value
			scanner.Valid = true
			scanner.Detected = "int"
		}
	case float64:
		if value, ok := src.(float64); ok {
			scanner.Value = value
			scanner.Valid = true
			scanner.Detected = "float"
		}
	case bool:
		if value, ok := src.(bool); ok {
			scanner.Value = value
			scanner.Valid = true
			scanner.Detected = "bool"
		}
	case string:
		if value, ok := src.(string); ok {
			scanner.Value = string(value)
			scanner.Valid = true
			scanner.Detected = "string"
		}
	case []byte:
		value := scanner.getBytes(src)
		scanner.Value = value
		scanner.Valid = true
		scanner.Detected = "string"
	case time.Time:
		if value, ok := src.(time.Time); ok {
			scanner.Value = value
			scanner.Valid = true
			scanner.Detected = "time"
		}
	case nil:
		scanner.Value = "NULL"
		scanner.Valid = true
		scanner.Detected = "null"
	}
}
