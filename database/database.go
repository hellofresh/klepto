package database

import "io"

// A Dumper writes a database's stucture to the provided stream.
type Dumper interface {
	DumpStructure(io.Writer) error
}

// An Anonymiser grabs a table's data and anonymises it, before sending it to a channel
type Anonymiser interface {
	DumpTable(string) error
}

// A Cell represents the value in a particular row and column
type Cell struct {
	column string
	value  interface{}
}
