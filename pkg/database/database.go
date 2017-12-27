package database

type (
	Row map[string]*Cell

	// A Cell represents the value in a particular row and column
	Cell struct {
		Value interface{}
		Type  string
	}
)
