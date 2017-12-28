package postgres

import (
	"bytes"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

type (
	PgDump interface {
		GetStructure() (stmt string, err error)
	}

	pgDump struct {
		command string
		dsn     string
	}
)

func NewPgDump(dsn string) (PgDump, error) {
	pgDumpPath, err := exec.LookPath("pg_dump")
	if err != nil {
		return nil, err
	}

	return &pgDump{
		command: pgDumpPath,
		dsn:     dsn,
	}, nil
}

func (p *pgDump) GetStructure() (string, error) {
	logger := log.WithFields(log.Fields{
		"command": p.command,
	})

	cmd := exec.Command(
		p.command,
		"--dbname", p.dsn,
		"--schema-only",
	)

	logger.Debug("Loading schema for table")
	cmdErr := logger.WriterLevel(log.WarnLevel)
	defer cmdErr.Close()

	buf := new(bytes.Buffer)

	cmd.Stdin = nil
	cmd.Stderr = cmdErr
	cmd.Stdout = buf

	if err := cmd.Run(); err != nil {
		logger.Error("Failed to load schema for table")
	}

	return buf.String(), nil
}
