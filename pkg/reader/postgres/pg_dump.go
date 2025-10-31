package postgres

import (
	"bytes"
	"os/exec"
	"strings"

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
		"--no-comments",
	)

	logger.Debug("loading schema for table")
	cmdErr := logger.WriterLevel(log.WarnLevel)
	defer cmdErr.Close()

	buf := new(bytes.Buffer)

	cmd.Stdin = nil
	cmd.Stderr = cmdErr
	cmd.Stdout = buf

	if err := cmd.Run(); err != nil {
		logger.Error("failed to load schema for table")
	}

	// Process the output to drop lines starting with /restrict or /unrestrict
	output := buf.String()
	lines := strings.Split(output, "\n")
	filteredLines := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, `\restrict`) && !strings.HasPrefix(trimmed, `\unrestrict`) {
			filteredLines = append(filteredLines, line)
		}
	}

	return strings.Join(filteredLines, "\n"), nil
}
