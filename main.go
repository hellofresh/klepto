package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/alecthomas/colour"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hgfischer/mysqlsuperdump/dumper"
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

	dsn := "root:root@localhost/something" // TODO: take this from CLI
	db, err := sql.Open("mysql", dsn)
	logger := log.New(os.Stdout, "klepto: ", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)

	switch command {
	case steal.FullCommand():
		d := dumper.NewMySQLDumper(db, logger)
		// TODO: Define masks from config file
		d.Dump(os.Stdout) // TODO: Define out as another mysql db (config from CLI)
		break
	}
}
