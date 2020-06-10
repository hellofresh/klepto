package postgres

import (
	"bytes"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

type (
	// PgDump is responsible for executing the pg dump command.
	PgDump struct {
		command string
		dsn     string
	}
)

// NewPgDump creates a new PgDump.
func NewPgDump(dsn string) (*PgDump, error) {
	path, err := exec.LookPath("pg_dump")
	if err != nil {
		return nil, err
	}

	return &PgDump{
		command: path,
		dsn:     dsn,
	}, nil
}

// GetStructure executes the pg dump command.
func (p *PgDump) GetStructure() (string, error) {
	logger := log.WithField("command", p.command)

	cmd := exec.Command(
		p.command,
		"--dbname", p.dsn,
		"--schema-only",
		"--no-privileges",
		"--no-owner",
	)

	logger.Debug("loading schema for table")
	cmdErr := logger.WriterLevel(log.WarnLevel)
	defer cmdErr.Close()

	buf := new(bytes.Buffer)

	cmd.Stdin = nil
	cmd.Stderr = cmdErr
	cmd.Stdout = buf

	err := cmd.Run()
	if err != nil {
		logger.WithError(err).Error("failed to load schema for table")
	}

	return buf.String(), err
}
