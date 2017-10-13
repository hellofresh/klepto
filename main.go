package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/alecthomas/colour"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hellofresh/klepto/database"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("klepto", "Steals data from production to put on staging")

	steal   = app.Command("steal", "Steal a live database")
	fromDSN = steal.Flag("from", "DSN for the input database").Short('f').Required().String()
	toDSN   = steal.Flag("to", "DSN for the output database").Short('t').String()
)

func main() {
	command, err := app.Parse(os.Args[1:])
	if err != nil {
		colour.Stderr.Printf("^1%s^R, try --help\n", err)
		os.Exit(2)
	}

	switch command {
	case steal.FullCommand():
		err = stealAction()
		break
	}

	// Global error handler for simplicity
	if err != nil {
		colour.Stderr.Printf("^1%s^R\n", err)
		os.Exit(2)
	}
}

func stealAction() (err error) {
	inputConn, err := dbConnect(*fromDSN)
	if err != nil {
		return
	}
	defer inputConn.Close()

	dumper, err := database.NewMySQLDumper(inputConn)
	if err != nil {
		return
	}

	dumper.DumpStructure(os.Stdout)

	out := make(chan *database.Cell, 100)
	done := make(chan bool)
	go func() {
		for {
			select {
			case row := <-out:
				fmt.Println(row)
			case <-done:
				return
			}
		}
	}()

	err = database.DumpTable(inputConn, "users", out)
	done <- true

	// outputConn, err := dbConnect(*toDSN)
	// if err != nil {
	// 	return err
	// }

	return
}

func dbConnect(dsn string) (*sql.DB, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return conn, err
}
