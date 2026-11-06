# Lab - Explore Iceberg tables with Spark - Answers

The `lab.silver.orders` and `lab.silver.users` tables were created in the Spark silver lab. `orders` already had 2 snapshots at the start (the creation with 2829 orders, and the fake order appended in exercise 4 of that lab), so it started with 2830 rows instead of 5000.

Row counts per snapshot (time travel on every snapshot):

| Table | Operation | Rows |
|---|---|---|
| orders | creation (previous lab) | 2829 |
| orders | append of 1 fake order (previous lab) | 2830 |
| orders | append of 3 new orders | 2833 |
| orders | insert of 1 row with `source` | 2834 |
| users | creation (previous lab) | 50 |
| users | append of 2 users | 52 |
| users | append of 1 user with `country` | 53 |

## Inspect the files

**Which files contain the actual rows?**
The Parquet files in `data/`. At the end of the lab there were 4: the original one with 2829 orders (84 KB), the fake order of the previous lab, the 3 new orders, and the row inserted with the `source` column.

**Which files describe the table?**
The files in `metadata/`: the `vN.metadata.json` files (schema, partitioning, list of snapshots, current snapshot), the `snap-*.avro` manifest lists (one per snapshot), the `*-m0.avro` manifests (list of data files with their statistics), and `version-hint.text`, which gives the current version.

**Why does an Iceberg table need more than the Parquet files?**
A Parquet file only knows its own rows and its own schema. Nothing says which files belong to the table right now, what the table schema is, or what the previous versions were. The metadata turns a set of files into a table: atomic commits, snapshots and time travel, schema evolution, and file statistics to skip files without listing the bucket.

## Explore Iceberg metadata

**How many snapshots does the table currently have?**
2 at that point: the creation of the table and the append of the fake order in the previous lab.

**How many data files belong to the current table?**
2: one with 2829 rows and one with 1 row.

**What information does the `snapshots` table contain that the Parquet files do not?**
The history of the table: the id of each snapshot, its parent, when it was committed, the operation (overwrite, append...), the location of its manifest list and a summary (added and deleted records and files, total number of records). Parquet files have no notion of version or history.

## Create a second snapshot

**How many snapshots are there now?**
3, and 2833 rows.

**Did the original Parquet files disappear?**
No. The listing still shows the files of October 28 (the 84 KB file with the 2829 orders and the fake order file), and a new small file was added for the 3 new orders.

**Why does Iceberg create a new snapshot instead of simply replacing the table?**
Files are never modified: an append writes a new data file, then commits a new snapshot that points to the old files plus the new one. It's cheap (nothing is rewritten), it's atomic, readers that were using the previous snapshot are not disturbed, and the old versions stay available for time travel and rollback.

## Time travel

The first snapshot returns 2829 orders, the current version returns 2833.

**What does time travel read?**
The metadata of the requested snapshot: its manifest list, then its manifests, then only the data files that were part of the table at that moment. Here, only the original file with the 2829 orders.

**Why can Iceberg access an older version without making a complete copy of the table?**
Because data files are immutable and shared between snapshots. A snapshot is just a list of files: the new snapshot reuses the old files and only adds the new one, so keeping an old version only costs a few small metadata files.

**What would happen if the old snapshot's metadata and data files were physically deleted?**
Time travel to that snapshot would fail with file not found errors. And if the deleted data files are also used by the current snapshot, the current table would be broken too. That's why old snapshots must be removed with Iceberg's `expire_snapshots` procedure, which only deletes the files that no kept snapshot still uses.

## Schema evolution

After `ALTER TABLE ... ADD COLUMN source STRING` and the insert, the table has 2833 rows with `source` = NULL and 1 row with `source` = `spark`.

**Did the existing Parquet files have to be rewritten?**
No. The old files keep their dates in the listing and were not touched. Iceberg only wrote a new `v4.metadata.json` with the new schema, and the old rows are read with NULL in the new column. The `ALTER TABLE` created a new metadata version but no new snapshot (5 metadata files for 4 snapshots).

**Where is the new schema recorded?**
In the table metadata file (`v4.metadata.json`, then `v5`): it keeps the list of schemas with their id, and the `current-schema-id`. Each column has a unique field id.

**Why is schema evolution a table-level operation rather than simply changing a Parquet file?**
A table is made of many files written at different times with different schemas. Changing one file wouldn't change the others, and rewriting all of them would be very expensive. Iceberg defines the schema once for the table and tracks columns by id, so readers know how to read each old file with the new schema (a missing column becomes NULL, a renamed column is still found).

## Compare Iceberg with Parquet

**Which one is simply a collection of Parquet files?**
`silver/spark/orders.parquet/`: one `part-` file and a `_SUCCESS` marker, nothing else.

**Which one has table metadata and snapshots?**
`warehouse/silver/orders/`, with `data/` and `metadata/`.

**How would you safely replace a Parquet directory while another query is reading it?**
Never overwrite it in place. Write the new version in a new directory (for example with a date or version in the path), then switch the readers to it (change the path in the config or in a view), and delete the old directory only when no query uses it anymore.

**How does Iceberg make the table state visible atomically?**
It first writes the new data files, which are invisible because no metadata points to them. Then it writes a new `metadata.json` and commits by switching the pointer to the current metadata in one single step. A reader sees either the old version or the new one, never something in between.

**Why is a catalog needed once several engines need to share the same Iceberg table?**
All the engines (Spark, Trino, DuckDB...) need to agree on which metadata file is the current one, find the tables by their name, and commit without overwriting each other. The hadoop catalog only relies on files in the bucket, and object storage has no atomic rename. A catalog service (REST catalog, Polaris, Nessie, Glue, Hive Metastore) keeps the pointer of each table and updates it with an atomic compare-and-swap, and it also gives one central list of tables and access control.

## Exercises

**1. `lab.silver.users`**
The table already existed from the Spark silver lab (created with `writeTo(...).createOrReplace()`), with 50 users.

**2. Append a new batch of users**
2 users were copied with a new `user_id` (`uuid()`) and a `_new` suffix on the username, then appended with `writeTo(...).append()`. The snapshots table shows a new `append` snapshot, and a new small data file appears in `data/`.

**3. Time travel**
`VERSION AS OF` the snapshot before the append returns 50 users, the current version returns 52.

**4. `country` column**
`ALTER TABLE lab.silver.users ADD COLUMN country STRING`, then 1 user was appended with `country` = `US`. Result: 52 users with `country` NULL and 1 with `US`. The old files were not rewritten.

**5. S3 layout of users and orders**
Both tables have the same structure: a `data/` directory with the Parquet files, and a `metadata/` directory with the `vN.metadata.json` files, the manifest lists (`snap-*.avro`), the manifests (`*-m0.avro`) and `version-hint.text`.

| | orders | users |
|---|---|---|
| Data files | 4 | 3 |
| Snapshots / manifest lists | 4 | 3 |
| Metadata versions | 5 | 4 |

Each write adds 1 data file, 1 manifest, 1 manifest list and 1 `metadata.json`. A schema change only adds a `metadata.json`, without data or snapshot. Over time a table gets many small files and old snapshots, so it needs maintenance (compaction with `rewrite_data_files` and `expire_snapshots`).

**6. Parquet, Iceberg and an Iceberg catalog**
- **Parquet** is a columnar file format: one file stores rows, column by column, with a footer containing its schema and statistics. A directory of Parquet files is not a table.
- **Iceberg** is a table format on top of these files: metadata files describe the schema, the partitioning, the list of data files and the snapshots. It adds atomic commits, time travel and schema evolution.
- **An Iceberg catalog** is where each table name points to its current metadata file. It lets engines find the tables and commit safely. Here it is a hadoop catalog (a directory in the bucket); in production it's a service like a REST catalog, Polaris, Nessie or Glue.
