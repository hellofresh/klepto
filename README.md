<p align="center">  
  <img height="200px" src="./klepto_logo.png"  alt="Klepto" title="Klepto">
</p>

# Klepto

[![Build Status](https://travis-ci.org/hellofresh/klepto.svg?branch=master)](https://travis-ci.org/hellofresh/klepto)
[![Go Report Card](https://goreportcard.com/badge/github.com/hellofresh/klepto)](https://goreportcard.com/report/github.com/hellofresh/klepto)
[![Go Doc](https://godoc.org/github.com/hellofresh/klepto?status.svg)](https://godoc.org/github.com/hellofresh/klepto)

> Klepto is a tool for copying and anonymising data

Klepto is a tool that copies and anonymises data from other sources.

- [Readme Languages](#)
	- [English (Default)](#)
- [Intro](#intro)
	- [Features](#features)
	- [Supported Databases](#supported-databases)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Steal Options](#steal-options)
- [Configuration File Options](#configuration-file-options)
  - [IgnoreData](#ignoredata)
  - [Matchers](#matchers)
  - [Anonymise](#anonymise)
  - [Relationships](#relationships)
- [Contributing](#contributing)
- [License](#licence)

<a name="intro"></a>
## Intro

Klepto helps you to keep the data in your environment as consistent as possible by copying it from another environment's database.

You can use Klepto to get production data but without sensitive customer information for your testing or local debugging.

<a name="features"></a>
### Features
- Copy data to your local database or to stdout, stderr
- Filter the source data
- Anonymise the source data

<a name="supperted-databases"></a>
### Supported Databases
- PostgreSQL
- MySQL

>If you need to get data from a database type that you don't see here, build it yourself and add it to this list. Contributions are welcomed :)

<a name="requirements"></a>
## Requirements

- Active connection to the IT VPN
- Latest version of [pg_dump][pg_dump-docs] installed (_Only required when working with PostgreSQL databases_)

<a name="installation"></a>
## Installation

Klepto is written in Go with support for multiple platforms. Pre-built binaries are provided for the following:

- macOS (Darwin) for x64, i386, and ARM architectures
- Windows
- Linux

You can download the binary for your platform of choice from the [releases page](klepto-releases).

Once downloaded, the binary can be run from anywhere. We recommend that you move it into your `$PATH` for easy use, which is usually at `/usr/local/bin`.

<a name="usage"></a>
## Usage

Klepto uses a configuration file called `.klepto.toml` to define your table structure. If your table is normalized, the structure can be detected automatically.

For dumping the last 10 created active users, your file will look like this:

```toml
[[Tables]]
  Name = "users"
  [Tables.Anonymise]
    email = "EmailAddress"
    username = "FirstName"
    password = "SimplePassword"
  [Tables.Filter]
    Match = "users.status = 'active'"
    Limit = 10
    [Tables.Filter.Sorts]
      created_at = "desc"
```

After you have created the file, run:

Postgres:
```sh
klepto steal \
--from="postgres://user:pass@localhost/fromDB?sslmode=disable" \
--to="postgres://user:pass@localhost/toDB?sslmode=disable" \
```

MySQL:
```sh
klepto steal \
--from="user:pass@tcp(localhost:3306)/fromDB?sslmode=disable" \
--to="user:pass@tcp(localhost:3306)/toDB?sslmode=disable" \
```

Behind the scenes Klepto will establishes the connection with the source and target databases with the given parameters passed, and will dump the tables.


<a name="steal-options"></a>
## Steal Options
Available options can be seen by running `klepto steal --help`

```
❯ klepto steal --help
Steals and anonymises databases

Usage:
  klepto steal [flags]

Flags:
      --concurrency int                Sets the amount of dumps to be performed concurrently (default 12)
  -c, --config string                  Path to config file (default ".klepto.toml")
  -f, --from string                    Database dsn to steal from (default "mysql://root:root@tcp(localhost:3306)/klepto")
  -h, --help                           help for steal
      --read-conn-lifetime duration    Sets the maximum amount of time a connection may be reused on the read database
      --read-max-conns int             Sets the maximum number of open connections to the read database (default 5)
      --read-max-idle-conns int        Sets the maximum number of connections in the idle connection pool for the read database
      --read-timeout duration          Sets the timeout for read operations (default 5m0s)
  -t, --to string                      Database to output to (default writes to stdOut) (default "os://stdout/")
      --to-rds                         If the output server is an AWS RDS server
      --write-conn-lifetime duration   Sets the maximum amount of time a connection may be reused on the write database
      --write-max-conns int            Sets the maximum number of open connections to the write database (default 5)
      --write-max-idle-conns int       Sets the maximum number of connections in the idle connection pool for the write database
      --write-timeout duration         Sets the timeout for write operations (default 30s)

Global Flags:
  -v, --verbose   Make the operation more talkative
```

We recommend to always set the following parameters:
- `concurrency` to alleviate the pressure over both the source and target databases.
- `read-max-conns` to limit the number of open connections, so that the source database does not get overloaded.


<a name="configuration-file-options"></a>
## Configuration File Options
You can set a number of keys in the configuration file. Below is a list of all configuration options, followed by some examples of specific keys.

- `Matchers` - Variables to store filter data. You can declare a filter once and reuse it among tables.
- `Tables` - A Klepto table definition.
  - `Name` - The table name.
  - `IgnoreData` - A flag to indicate whether data should be imported or not. If set to true, it will dump the table structure without importing data.
  - `Filter` - A Klepto definition to filter results.
    - `Match` - A condition field to dump only certain amount data. The value may be either expression or correspond to an existing `Matchers` definition.
    - `Limit` - The number of results to be fetched.
    - `Sorts` - Defines how the table is sorted.
  - `Anonymise` - Indicates which columns to anonymise.
  - `Relationships` - Represents a relationship between the table and referenced table.
    - `Table` - The table name.
    - `ForeignKey` - The table's foreign key. 
    - `ReferencedTable` - The referenced table name.
    - `ReferencedKey` - The referenced table primary key.



<a name="ignoredata"></a>
### IgnoreData

You can dump the database structure without importing data by setting the `IgnoreData` value to `true`.
```toml
[[Tables]]
 Name = "logs"
 IgnoreData = true
```

<a name="matchers"></a>
### Matchers
Matchers are variables to store filter data. You can declare a filter once and reuse it among tables:
```toml
[[Matchers]]
  Latest100Users = "ORDER BY users.created_at DESC LIMIT 100"

[[Tables]]
  Name = "users"
  [Tables.Filter]
    Match = "Latest100Users"

[[Tables]]
  Name = "orders"
  [[Tables.Relationships]]
    ForeignKey = "user_id"
    ReferencedTable = "users"
    ReferencedKey = "id"
  [Tables.Filter]
    Match = "Latest100Users"
```

See [examples](./examples) for more.


<a name="anonymise"></a>
### Anonymise

You can anonymise specific columns in your table using the `Anonymise` key. Anonymisation is performed by running a Faker against the specified column.

```toml
[[Tables]]
  Name = "customers"
  [Tables.Anonymise]
    email = "EmailAddress"
    firstName = "FirstName"
    postalCode = "DigitsN:5"
    creditCard = "CreditCardNum"
    voucher = "Password:3:5:true"

[[Tables]]
  Name = "users"
  [Tables.Anonymise]
    email = "EmailAddress"
    password = "literal:1234"
```

This would replace all the specified columns from the `customer` and `users` tables with the spcified fake function. If a function requires arguments to be passed, we can specify them splitting with the `:` character, the default value of a argument type will be used in case the provided one is invalid or missing. There is also a special function `literal:[some-constant-value]` to specify a constant we want to write for a column. In this case, `password = "literal:1234"` would write `1234` for every row in the password column of the users table.

#### Available data types for anonymisation

Available data types can be found in [fake.go](pkg/anonymiser/fake.go). This file is generated from https://github.com/icrowley/fake (it must be generated because it is written in such a way that Go cannot reflect upon it).

We generate the file with the following:

```sh
$ go get github.com/ungerik/pkgreflect
$ fake master pkgreflect -notypes -novars -norecurs vendor/github.com/icrowley/fake/
```

<a name="relationships"></a>
### Relationships
The `Relationships` key represents a relationship between the table and referenced table.

To dump the latest 100 users with their orders:
```toml
[[Tables]]
  Name = "users"
  [Tables.Filter]
    Limit = 100
    [Tables.Filter.Sorts]
      created_at = "desc"

[[Tables]]
  Name = "orders"
  [[Tables.Relationships]]
    # behind the scenes klepto will create a inner join between orders and users
    ForeignKey = "user_id"
    ReferencedTable = "users"
    ReferencedKey = "id"
  [Tables.Filter]
    Limit = 100
    [Tables.Filter.Sorts]
      created_at = "desc"
```


<a name="contributing"></a>
## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

<a name="licence"></a>
## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details


[pg_dump-docs]: https://www.postgresql.org/docs/10/static/app-pgdump.html "pg_dump docs"
[klepto-releases]: https://github.com/hellofresh/klepto/releases "Klepto releases page"
