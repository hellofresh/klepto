# Klepto

Steal data from a live (mysql) database, anonymise it where defined, and put it in a new database

Vision:

`klepto --from 'root:root@tcp(localhost:3306)/fromDB' --to 'root:root@tcp(localhost:3306)/toDB' --config example.toml`

By default it just downloads and dumps everything, but you can use the config to define fields to anonymise:

```yml
customer:
    seed: id_customer
    anonymise:
        - { email: Email }
        - { first_name: FirstName }
        - { middle_name: FirstName }
        - { last_name: LastName }
```

This would delete these 4 columns from the `customer` table and run `faker.Email`, `faker.FirstName`, and `faker.LastName` against them respectively.
