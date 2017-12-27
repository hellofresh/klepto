Klepto
=====

[![](https://travis-ci.org/hellofresh/klepto.svg?branch=master)](https://travis-ci.org/hellofresh/klepto)

> Klepto is a tool for copying and anonymising data

Klepto helps you keep the data in your enviroment as consistent as it can by copying it form another enviroment's database. The reason for this is that you might have production data that you'd like to use for testing but you don't want to use the real customer information for your testing or local debuging. That's when Klepto comes very handy and will deal with that for you!

## Getting Started

All you need to have is a simple configuration file where you're going to define your table definition. Klepto can also try to figure that out for you (as long as your database is normalized properly).

Here is an example of how the config file should look like:

```toml
[[Tables]]
  Name = "orders"
  [Tables.Filter]
    Limit = 100
    [Tables.Filter.Sorts]
      orderNr = "asc"
  [Tables.Anonymise]
    email = "EmailAddress"
    firstName = "FirstName"

  [[Tables.Relationships]]
    ReferencedTable = "customers"
    ReferencedKey = "id"
    ForeignKey = "customer_id"
```

After you have this, just run:

```sh
klepto steal --from 'root:root@tcp(localhost:3306)/fromDB' --to 'root:root@tcp(localhost:3306)/toDB'
```

## Prerequisites

Klepto tries to keep external dependencies to a minimum, but some functionalities requires some dependencies. Here is a list:

- Postgres: If you are using klepto to steal data from postgres databases you will need `pg_dump` installed

## Installing 

Klepto is written in Go with support for multiple platforms. Pre-built binaries are provided for the following:

- macOS (Darwin) for x64, i386, and ARM architectures
- Windows
- Linux

You can download the binary for your platform of choice from the [releases page](https://github.com/hellofresh/klepto/releases).

Once downloaded, the binary can be run from anywhere. Ideally, though, you should move it into your $PATH for easy use. `/usr/local/bin` is a popular location for this.

## Anonymisation

Each column can be set to anonymise. Anonymisation is performed by running a Faker against the specified column.

By specifying anonymisation config in your `.klepto.toml` file, you can define which tables' fields require anonymisation. This is done with the format `"table.column" = "DataType"`, as follows:

```toml
[anonymise]
"customer.email" = "EmailAddress"
"customer.first_name" = "FirstName"
"customer.last_name" = "LastName"
"customer.password" = "literal:1234"
```

This would delete these 3 columns from the `customer` table and run `faker.Email`, `faker.FirstName`, and `faker.LastName` against them respectively. We can use `literal:[some-constant-value]` to specify a constant we want to write for a column. In this case, `password: literal:1234` would write `1234` for every row in the password column of the customer table.

### Available data types for anonymisation

Available data types can be found in `fake.go`. This file is generated from https://github.com/icrowley/fake (it must be generated because it is written in such a way that Go cannot reflect upon it).

We generate the file with the following:

```sh
$ go get github.com/ungerik/pkgreflect
$ fake master pkgreflect -notypes -novars -norecurs vendor/github.com/icrowley/fake/
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
