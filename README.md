Klepto
=====

[![](https://travis-ci.org/hellofresh/klepto.svg?branch=master)](https://travis-ci.org/hellofresh/klepto)

> Klepto is a tool for copying and anonymising data

Klepto helps you keep the data in your environment as consistent as it can by copying it from another environment's database. The reason for this is that you might have production data that you'd like to use for testing but you don't want to use the real customer information for your testing or local debugging. That's when Klepto comes very handy and will deal with that for you!

## Prerequisites

Klepto tries to keep external dependencies to a minimum, but some functionality requires some dependencies. Here is a list:

- Postgres: If you are using Klepto to steal data from postgres databases you will need `pg_dump` installed

## Getting Started

All you need to have is a simple configuration file where you're going to define your table structure. Klepto can also try to figure that out for you (as long as your database is normalized properly).

Here is an example of how the config file should look:

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

In this configuration Klepto will dump the latest 10 created active users

After you have created the file just run:

Postgres:
```sh
klepto steal \
--from="postgres://user:pass@localhost/fromDB?sslmode=disable" \
--to="postgres://user:pass@localhost/toDB?sslmode=disable" \
--concurrency=6 \
--read-max-conns=10 \
--read-max-idle-conns=4
```

MySQL:
```sh
klepto steal \
--from 'user:pass@tcp(localhost:3306)/fromDB?sslmode=disable' \
--to 'user:pass@tcp(localhost:3306)/toDB?sslmode=disable' \
--concurrency=4 \
--read-max-conns=8
```

## Anonymisation

Each column can be set to anonymise. Anonymisation is performed by running a Faker against the specified column.

By specifying anonymisation config in your `.klepto.toml` file, you can define which tables' fields require anonymisation. This is done as follows:

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

### Available data types for anonymisation

Available data types can be found in [fake.go](pkg/anonymiser/fake.go). This file is generated from https://github.com/icrowley/fake (it must be generated because it is written in such a way that Go cannot reflect upon it).

We generate the file with the following:

```sh
$ go get github.com/ungerik/pkgreflect
$ fake master pkgreflect -notypes -novars -norecurs vendor/github.com/icrowley/fake/
```

## Relationships

Dump the latest 100 users with their orders:
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
    ForeignKey = "user_id"
    ReferencedTable = "users"
    ReferencedKey = "id"
  [Tables.Filter]
    Limit = 100
    [Tables.Filter.Sorts]
      created_at = "desc"
```

See [examples](./examples) for more.

## Ignore data

Additionally you can dump the database structure without importing data
```toml
[[Tables]]
 Name = "logs"
 IgnoreData = true
```

## Installing 

Klepto is written in Go with support for multiple platforms. Pre-built binaries are provided for the following:

- macOS (Darwin) for x64, i386, and ARM architectures
- Windows
- Linux

You can download the binary for your platform of choice from the [releases page](https://github.com/hellofresh/klepto/releases).

Once downloaded, the binary can be run from anywhere. Ideally, though, you should move it into your `$PATH` for easy use. `/usr/local/bin` is a popular location for this.

## Supported Databases

At the moment we only support 2 RDBMS which are `postgres` and `mysql`.

### Input
- Postgres
- MySQL


### Output
- Postgres
- MySQL
- Stdout
- Stderr

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
