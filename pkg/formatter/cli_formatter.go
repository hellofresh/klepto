package formatter

import (
	"bytes"
	"fmt"

	log "github.com/sirupsen/logrus"
)

// colors.
const (
	none   = 0
	red    = 31
	green  = 32
	yellow = 33
	blue   = 34
	gray   = 37
)

// Colors mapping.
var Colors = [...]int{
	log.DebugLevel: gray,
	log.InfoLevel:  blue,
	log.WarnLevel:  yellow,
	log.ErrorLevel: red,
	log.FatalLevel: red,
}

// Strings mapping.
var Strings = [...]string{
	log.DebugLevel: "•",
	log.InfoLevel:  "•",
	log.WarnLevel:  "•",
	log.ErrorLevel: "⨯",
	log.FatalLevel: "⨯",
}

// CliFormatter is a CLI formatter for logrus
type CliFormatter struct{}

// Format renders a single log entry
func (f *CliFormatter) Format(e *log.Entry) ([]byte, error) {
	var b *bytes.Buffer

	if e.Buffer != nil {
		b = e.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	color := Colors[e.Level]
	level := Strings[e.Level]
	keys := make([]string, 0, len(e.Data))
	for k := range e.Data {
		keys = append(keys, k)
	}

	fmt.Fprintf(b, "\033[%dm%*s\033[0m %-25s", color, 1, level, e.Message)

	for _, key := range keys {
		if key == "source" {
			continue
		}

		fmt.Fprintf(b, " \033[%dm%s\033[0m=%v", color, key, e.Data[key])
	}

	fmt.Fprintln(b)

	return b.Bytes(), nil
}
