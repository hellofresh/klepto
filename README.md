# Klepto

Steal data from a live (mysql) database, anonymise it where defined, and put it in a new database

Vision:

`klepto --from 'root:root@tcp(localhost:3306)/fromDB' --to 'root:root@tcp(localhost:3306)/toDB' --config example.toml`

By default it just downloads and dumps everything, but you can use the config to define fields to anonymise in yaml, toml, or any other [viper](https://github.com/spf13/viper)-supported format:

```yml
---
anonymise:
    customer:
        email: EmailAddress
        first_name: FirstName
        last_name: LastName
        password: literal:1234
```

```toml
[anonymise]
"customer.email" = "EmailAddress"
"customer.first_name" = "FirstName"
"customer.last_name" = "LastName"
"customer.password" = "literal:1234"
```

This would delete these 3 columns from the `customer` table and run `faker.Email`, `faker.FirstName`, and `faker.LastName` against them respectively. We can use `literal:[some-constant-value]` to specify a constant we want to write for a column. In this case, `password: literal:1234` would write `1234` for every row in the password column of the customer table. 
