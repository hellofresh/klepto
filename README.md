Klepto
=====

[![](https://travis-ci.org/hellofresh/klepto.svg?branch=master)](https://travis-ci.org/hellofresh/klepto)

Steal data from a live (mysql) database, anonymise it where defined, and put it in a new database

Vision:

`klepto --from 'root:root@tcp(localhost:3306)/fromDB' --to 'root:root@tcp(localhost:3306)/toDB' --config example.toml`

By default it just downloads and dumps everything, but you can use the config to define fields to anonymise in yaml, toml, or any other [viper](https://github.com/spf13/viper)-supported format:

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

