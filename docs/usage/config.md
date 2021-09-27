# Configuration

Klepto uses a configuration file called `.klepto.toml` to define your table structure and the Anonymise functions to be used.

If your table is normalized, the structure can be detected automatically.

## Keys

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

### **IgnoreData**

You can dump the database structure without importing data by setting the `IgnoreData` value to `true`.

```toml
[[Tables]]
 Name = "logs"
 IgnoreData = true
```

### **Matchers**

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

### **Anonymise**

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

This would replace all the specified columns from the `customer` and `users` tables with the spcified fake function.

If a function requires arguments to be passed, we can specify them splitting with the `:` character, the default value of a argument type will be used in case the provided one is invalid or missing.

There is also a special function `literal:[some-constant-value]` to specify a constant we want to write for a column. In this case, `password = "literal:1234"` would write `1234` for every row in the password column of the users table.

Available data types can be found in [fake.go](https://github.com/hellofresh/klepto/blob/master/pkg/anonymiser/fake.go). This file is generated from [https://github.com/icrowley/fake](https://github.com/icrowley/fake) (it had to be generated because it is written in such a way that Go cannot reflect upon it).

Bellow are the instructions used to generate the file:

```sh
go get github.com/ungerik/pkgreflect
fake master pkgreflect -notypes -novars -norecurs vendor/github.com/icrowley/fake/
```

### **Relationships**

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

!!! info "Tip"
    You can find some [configuration examples](https://github.com/hellofresh/klepto/tree/master/examples) in Klepto's repository.
