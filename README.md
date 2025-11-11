Pensieve is a library that helps you execute SQL queries over time using a DB snapshot (parquet) and binary logs of transactions.

Pensieve does the following:
- Parsing binary logs (generated using mysqlbinlog with the `--base64-output=DECODE-ROWS` option)
- Parsing parquet files into an in-memory DuckDB instance
- Applying transactions (as-is or inverted) to move the snapshot forwards and backwards in time

By letting Pensieve handle stepping through the binlog, you can write scripts that execute SQL queries over time with transaction-level accuracy.

Pensieve does not require the exact position of the snapshot within the binlog. (It is not always possible to find the GTID of the very next transaction after your snapshot).

You must give Pensieve a rough estimate of the snapshot's timestamp. Pensieve uses transactions within a several hour window around your estimate to generate a new, normalised snapshot, at a precisely known position within the binlog.

Pensieve is still in development and has only been tested on a small scale.
