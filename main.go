package main

import (
	"os"

	"github.com/alecthomas/colour"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("klepto", "Steals data from production to put on staging")

	steal = app.Command("steal", "Steal a live database")
)

func main() {
	command, err := app.Parse(os.Args[1:])

	if err != nil {
		colour.Stderr.Printf("^1%s^R, try --help\n", err)
		os.Exit(2)
	}

	switch command {
	case steal.FullCommand():
		break
	}
}
