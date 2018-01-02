package dsn

import (
	"errors"
	"log"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"strings"
)

var (
	// ErrEmptyDsn defines error returned when no dsn is provided
	ErrEmptyDsn = errors.New("Empty string provided for dsn")
	// ErrInvalidDsn defines error returned when the dsn is invalid
	ErrInvalidDsn = errors.New("Invalid dsn")
)

// DSN describes how a DSN looks like
type DSN struct {
	Type       string
	Username   string
	Password   string
	Protocol   string
	Address    string
	Host       string
	Port       string
	DataSource string
	Params     map[string]string
}

// Parse turns a dsn string into a parsed DSN struct.
// From https://play.golang.org/p/H3rbC4npryN
func Parse(s string) (*DSN, error) {
	if s == "" {
		return nil, ErrEmptyDsn
	}

	dsn := &DSN{}

	matches := regex.FindStringSubmatch(s)
	if len(matches) < 1 || len(matches) > 1 && matches[1] == "" {
		return nil, ErrInvalidDsn
	}
	names := regex.SubexpNames()
	log.Printf("matches: %v", matches)

	vof := reflect.ValueOf(dsn).Elem()

	if len(matches) > 0 {
		for n, match := range matches[1:] {
			name := names[n+1]
			if name == "Params" {
				values, err := url.ParseQuery(match)
				if err != nil {
					return nil, err
				}
				dsn.Params = make(map[string]string)
				for key, vals := range values {
					dsn.Params[key] = strings.Join(vals, ",")
				}
			} else {
				vof.FieldByName(name).SetString(match)
			}
		}
	}
	if dsn.Protocol != "" && dsn.Address == "" {
		dsn.Address = dsn.Protocol
		dsn.Protocol = ""
	}
	host, port, err := net.SplitHostPort(dsn.Address)
	if err == nil {
		dsn.Host = host
		dsn.Port = port
	}
	return dsn, nil
}

var (
	// From https://github.com/go-sql-driver/mysql/blob/f4bf8e8e0aa93d4ead0c6473503ca2f5d5eb65a8/utils.go#L34
	regex = regexp.MustCompile(
		`^(?:(?P<Type>.*?)?://)?` + // [type://]
			`(?:(?P<Username>.*?)(?::(?P<Password>.*))?@)?` + // [username[:password]@]
			`(?:(?P<Protocol>[^\(]*)(?:\((?P<Address>[^\)]*)\))?)?` + // [protocol[(address)]]
			`\/(?P<DataSource>.*?)` + // /datasource
			`(?:\?(?P<Params>[^\?]*))?$`) // [?param1=value1]
)

// Converts a DSN struct into its string representation.
func (d DSN) String() string {
	str := ""

	if d.Type != "" {
		str += d.Type + "://"
	}

	if d.Username != "" {
		str += d.Username
	}

	if d.Password != "" {
		str += ":" + d.Password
	}

	if d.Username != "" && d.Password != "" {
		str += "@"
	}

	if d.Protocol != "" {
		str += d.Protocol
	}

	if d.Address != "" {
		str += "(" + d.Address + ")"
	}

	str += "/"

	if d.DataSource != "" {
		str += d.DataSource
	}

	if d.Params != nil && len(d.Params) > 0 {
		str += "?"

		i := 0
		for key, value := range d.Params {
			str += key + "=" + value

			if i < len(d.Params)-1 {
				str += "&"
			}

			i++
		}
	}
	return str
}
