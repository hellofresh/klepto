# Command

Detailed list of Klepto's available commands

```sh
klepto --help
Klepto by HelloFresh.
                Takes the structure and data from one (mysql) database (--from),
                anonymises the data according to the provided configuration file,
                and inserts that data into another mysql database (--to).

                Perfect for bringing your live data to staging!

Usage:
  klepto [command]

Examples:
klepto steal -c .klepto.toml|yaml|json --from root:root@localhost:3306/fromDb --to root:root@localhost:3306/toDb

Available Commands:
  help        Help about any command
  init        Create a fresh config file
  steal       Steals and anonymises databases
  update      Check for new versions of klepto

Flags:
  -h, --help      help for klepto
  -v, --verbose   Make the operation more talkative
      --version   version for klepto

Use "klepto [command] --help" for more information about a command.
```

## Init

Klepto `init` command creates a example `.klepto.toml` file.

```sh
klepto init
• Initializing .klepto.toml
• Created .klepto.toml!    
```

## Update

Klepto can self update by running the `update` command

```sh
klepto update
• Checking for new versions of Klepto!
• Klepto! updated to version v0.3.1
```

## Steal

Klepto `steal` command starts the copy from the instructions defined in `.klepto.toml` file.

- **Postgres:**

  ```sh
  klepto steal \
  --from="postgres://user:pass@localhost/fromDB?sslmode=disable" \
  --to="postgres://user:pass@localhost/toDB?sslmode=disable" \
  ```

- **MySQL**

  ```sh
  klepto steal \
  --from="user:pass@tcp(localhost:3306)/fromDB?sslmode=disable" \
  --to="user:pass@tcp(localhost:3306)/toDB?sslmode=disable" \
  ```

Behind the scenes Klepto will establishes the connection with the source and target databases with the given parameters passed, and will dump the tables.

Available options can be seen by running `klepto steal --help`

```sh
klepto steal --help
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
