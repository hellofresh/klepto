package database

// Error describes structure of error messages.
type Error struct {
	typ     string
	message string
	data    interface{}
}

// TODO: define all database-related errors here.

// var Errors (
// CouldNotAnonymiseCellWithColumn = "couldn't anonymise cell with column:"
// )

// func (e *Error) report() error {
// return errors.New(e.message)
// }
