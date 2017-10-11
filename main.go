package main

import (
	"database/sql"
	"log"
	"os"
	"bytes"

	"github.com/alecthomas/colour"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hgfischer/mysqlsuperdump/dumper"
	"gopkg.in/alecthomas/kingpin.v2"
	"bufio"
	"io"
)

var (
	app = kingpin.New("klepto", "Steals data from production to put on staging")

	steal = app.Command("steal", "Steal a live database")
	stealingDSN = steal.Flag("inputdsn", "DSN for the input database").Default("root:root@localhost/example").String()
	swagDSN = steal.Flag("outputdsn", "DSN for the output database (or just 'STDOUT' for a dump)").Default("root:root@localhost/example").String()
)

func ensureConnectionIsGood(db *sql.DB) error {
	// this is a copy of what the dumper does as it's first step with one exception, it actually returns an error not
	// just returns nothing in case of error!
	tables := make([]string, 0)
	var rows *sql.Rows
	var err = error(nil)
	if rows, err = db.Query("SHOW FULL TABLES"); err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tableName, tableType string
		if err := rows.Scan(&tableName, &tableType); err != nil {
			return err
		}
		if tableType == "BASE TABLE" {
			tables = append(tables, tableName)
		}
	}
	return nil
}

func main() {
	command, err := app.Parse(os.Args[1:])

	if err != nil {
		colour.Stderr.Printf("^1%s^R, try --help\n", err)
		os.Exit(2)
	}

	db, err := sql.Open("mysql", *stealingDSN)
	logger := log.New(os.Stdout, "klepto: ", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	if err != nil {
		log.Fatalf("Input MySQL connection failed: %s \n", err)
	}

	err = ensureConnectionIsGood(db)
	if err != nil {
		log.Fatalf("Error in MySQL input connection: %s \n", err)
	}

	outdb, err := sql.Open("mysql", *swagDSN)
	if err != nil {
		log.Fatalf("Output MySQL connection failed: %s \n", err)
	}

	switch command {
	case steal.FullCommand():
		d := dumper.NewMySQLDumper(db, logger)
		// TODO: Define masks from config file
		if *swagDSN == "STDOUT" {
			d.Dump(os.Stdout) // TODO: Define out as another mysql db (config from CLI)
		} else {
			var b bytes.Buffer
			writer := bufio.NewWriter(&b)
			d.Dump(writer)
			writer.Flush()
			for {
				s, err := b.ReadString(';')
				if (err == io.EOF) {
					break
				} else if (err != nil) {
					log.Fatalf("Error reading from dumped data : %s \n", err)
				}
				outdb.Exec(s)
			}
		}
		break
	}
}
