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
- [Examples](#examples)
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
The available options can be seen by running `klepto steal -h`

<p align="left">  
  <img src="./klepto-steal-help.png"  alt="Klepto Steal Help" title="Klepto Steal Help">
</p>

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
    - `Match` - A condition field to dump only certain amount data. The value should correspond to an existing `Matchers` definition.
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

[[Tables]]
  Name = "users"
  [Tables.Anonymise]
    email = "EmailAddress"
    password = "literal:1234"
```

This would replace these 4 columns from the `customer` and `users` tables and run `fake.EmailAddress` and `fake.FirstName` against them respectively. We can use `literal:[some-constant-value]` to specify a constant we want to write for a column. In this case, `password = "literal:1234"` would write `1234` for every row in the password column of the users table.

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



<a name="examples"></a>
## Examples

Example configuration files for intfood and the ordering tool can be found on [Klepto Examples][klepto-examples-confluence] on Confluence.

<a name="contributing"></a>
## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

<a name="licence"></a>
## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details



[pg_dump-docs]: https://www.postgresql.org/docs/10/static/app-pgdump.html "pg_dump docs"
[klepto-releases]: https://github.com/hellofresh/klepto/releases "Klepto releases page"
[klepto-examples-confluence]: https://hellofresh.atlassian.net/wiki/spaces/PLAT/pages/204505400/Klepto+Examples "Klepto examples confluence"
