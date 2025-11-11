Pensieve is a library that helps you execute SQL queries over time using a DB snapshot (parquet) and binary logs of transactions.

Pensieve does the following:
- Parsing binary logs (generated using mysqlbinlog with the `--base64-output=DECODE-ROWS` option)
- Parsing parquet files into an in-memory DuckDB instance
- Applying transactions (as-is or inverted) to move the snapshot forwards and backwards in time

By letting Pensieve handle stepping through the binlog, you can write scripts that execute SQL queries over time with transaction-level accuracy.

Pensieve does not require the exact position of the snapshot within the binlog. (It is not always possible to find the GTID of the very next transaction after your snapshot).

You must give Pensieve a rough estimate of the snapshot's timestamp. Pensieve uses transactions within a several hour window around your estimate to generate a new, normalised snapshot, at a precisely known position within the binlog.

Pensieve is still in development and has only been tested on a small scale.

## Building
Clone the repo and run:

```
cargo build
```

or, directly:

```
cargo run --release
```

## An example
Pensieve currently includes one sample table in `db_data/books`. Both its snapshot (parquet) and binlogs are included. The binlogs have transactions for other tables too, but Pensieve ignores these automatically.

Several values of `price` have been deleted from this table. 

Additionally, the snapshot only contains the first five books' worth of data. The remaining seven books were INSERTed after the snapshot was taken.

The binlog contains all transactions related to this table - inserting twelve books, followed by deleting the prices of several books.

The present state of the table looks like this:
```
id | price
1  | null
2  | 20
3  | 30
4  | 40
5  | 50
6  | null
7  | null
8  | null
9  | null
10 | null
11 | null
12 | null
```

We can use the included [LastNonNullScript](https://github.com/eigne/pensieve/blob/main/src/script/last_non_null.rs) to restore the lost prices.

This script gets the last non-null value of a specified column for all rows in a table.

Try running:

```
 cargo run --release --bin script last-non-null --table books --column price --output results.csv --timestamp '251111 01:33:00' --window 1
```

and check the output of results.csv:

```
id,last_non_null_value
1,10
2,20
3,30
4,40
5,50
6,60
7,70
8,80
9,90
10,100
11,110
12,120
```

