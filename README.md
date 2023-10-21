# Peeker into Parquets

I'm spending unhealthy amounts of time to get a parquet file and then do something trivial like see how many of rows are there, how many nulls are in a given column, checking what is the exact name name of a particular column, ...
Launching a python interpreter, typing the `import pandas as pd` and `df = pd.read_parquet("file.parquet")` and type the exact pandas query seems too much a chore and slow for something which is often quite standard.

I'm thus developing this Peeker into Parquets (`pepa`) tool to capture the most basic cases, and in a performant manner -- to get shape/schema, we need to just peek at the metadata file, not decrypt all the columns and everything.
The output is a json, to allow piping to eg `jq`.

And I also do this to gain some Rust practice -- the code itself will thus likely be pleasing to neither eye nor heart.

# Quickstart

Install the crate and `pepa <yo-parquet-file>`, which by default nets you something like
```
{
  "shape": {
    "num_cols_leaf": 2,
    "num_rows": 2
  },
  "schema": {
    "columns": {
      "a": "INT64",
      "b": "DOUBLE"
    }
  }
}
```
with the column types being physical, and number of columns going down to the leafs (thus a structure column is not counted as a 1).

For parquets with many columns, run with `-l0` instead to get just a stats of how many columns per physical type are there.

# Upcoming features
 - adding index stats to l0/l1 (`key_value_metadata.pandas -> parse json -> index_columns, partition_columns`)
 - adding disk size and memory usage as an option or l2,
 - supporting some simple filtering (though this is not supposed to replace any existing analytical engine),
 - per-column stats of null values, most frequent values as an option or l3
 - python interface for the library

# Possible bugs
 - non scalar types could crash things
 - tested on fastparquet and pyarrow, but not on others such as spark

# Internal improvements
 - start breaking up the lib.rs into metadata parser, pandas parser, etc
 - tests
 - build & publish pipeline
