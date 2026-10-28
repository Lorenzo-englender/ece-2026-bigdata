# Lab - Bronze to silver with Spark DataFrames - Answers

## SparkSession with S3 access

**The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?**
In `local[*]` mode the driver and the executors run inside the service pod, so the job is killed with the service and everything in memory is lost (DataFrames, cache, variables). It happened to me: the first service had a 2000Mi memory limit and was OOMKilled 4 times. After the restart, new credentials are issued, and the old ones that were copied in the session config at startup expire. A session that lives longer than the token gets 403 / ExpiredToken errors, because the S3A config is not refreshed. We have to create a new session with the new credentials.

**Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?**
`SimpleAWSCredentialsProvider` only sends the access key and the secret key. Onyxia gives temporary STS credentials, which are only valid together with the session token. Without the token, S3 rejects the request. With permanent keys there is no token, so it worked. `TemporaryAWSCredentialsProvider` also sends the session token.

## Silver layer

**Explain the output of the code that was just run.**
The query counts the users per state and how many of them have no city. Only the military codes have missing cities: AE (3 users, 3 without city) and AP (2 users, 2 without city), because military addresses like `DPO AE 12345` have no comma and no city, so `city` is NULL. All the other states have 0 missing cities. It shows that the address parsing works. The 50 users are spread over many states, with at most 3 users per state.

**Which one would you choose for a one-off exploration, and which for a model reused by several colleagues? Why?**
Both compile to the same Catalyst plan, so performance is the same. For a one-off exploration I would use SQL: it's quick to write and everyone can read it. For a model reused by colleagues I would use the DataFrame API: the steps can be split into Python functions and variables, reused, tested and versioned in a module, and errors in column names are easier to find. If the colleagues only know SQL, SQL models (with dbt for example) are a better choice.

**`regexp_extract()` returned an empty string instead of `NULL` on no match. Where else could that same behavior silently produce a wrong result?**
The `state` and `zip_code` columns are also built with `regexp_extract()`. If an address doesn't match, they get an empty string instead of NULL: a `not null` check would not detect it, a count of missing values would be wrong, and a `groupBy("state")` would create a group `""`. The UDFs of exercise 2 also return `""` when there is no match.

## Write the silver layer as Parquet

**How many part-files are there, and why?**
There is 1 part-file in each directory (plus a `_SUCCESS` marker), because Spark writes one file per partition of the DataFrame. `users` comes from a single small CSV read with `multiLine=True`, which can't be split, so it has 1 partition. `orders` is shuffled by the window function, but Adaptive Query Execution coalesces the shuffle partitions into 1 because the data is tiny.

**`coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?**
The parallelism: all the data goes through one partition, so one single task on one core writes everything. It is slow, it puts all the memory and disk load on one executor (risk of OOM), and the previous steps can also end up running in a single task.

## Write the silver layer as Iceberg tables

**Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?**
In `silver/spark/orders.parquet/` there are only Parquet data files and a `_SUCCESS` marker, nothing describes the table. In `warehouse/silver/orders/`, the `data/` directory holds the data (one Parquet file of 84 KB) and the `metadata/` directory describes the table: `v1.metadata.json` (schema, partitioning, list of snapshots), the `snap-*.avro` manifest list, the `*-m0.avro` manifest (list of data files with their statistics) and `version-hint.text`, which gives the current version.

**A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?**
With plain Parquet, the overwrite deletes and writes files one by one, so the reader can see a mix of old and new files, miss data, or fail with a file not found error. With Iceberg, the new files are written first and a new metadata version is committed atomically at the end. A reader keeps reading the snapshot it started with, so it sees either the old version or the new one, never a mix.

**What can go wrong if two jobs commit at the same time, and what would a catalog service add?**
With the hadoop catalog, both jobs can read version 1 and both write version 2: one commit overwrites the other and changes are lost, because object storage has no atomic rename or compare-and-swap. A catalog service (REST catalog, Nessie, Polaris, Hive Metastore, Glue) updates the pointer with an atomic compare-and-swap: the second commit fails and is retried on top of the first one. It also gives one central list of tables and access control.

## Compare with the dbt lab

Both outputs have the same content: 2829 orders with a total quantity of 8615, and 50 users.

**What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers?**
With dbt, `order_id` and `user_id` are `UUID` and `ordered_at` is `timestamp with time zone`. With Spark they are `VARCHAR` and `timestamp`. We cast the UUIDs to strings. Consequences: they take 36 bytes instead of 16, joins and comparisons are done on strings, nothing checks that the value is a valid UUID, and consumers have to cast them back if they need a UUID. They also have to know that the timestamps are in UTC.

**One object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?**
One object per model is easier to read: one path, one file. The directory of part-files scales: each task writes its own file in parallel, and readers can read the files in parallel too, but consumers need a glob (`*.parquet`) and must ignore `_SUCCESS`.

**What did this lab use instead of the dbt tests, and what would you need to write to get the same guarantees?**
This lab only used Python `assert` on row counts and column names, and a `groupBy().count()` to look for duplicates by hand. To get the same guarantees we would need to write and run automatically: unique (groupBy on the key with count > 1 must return 0 rows), not_null (filter `isNull()` must return 0 rows), accepted_values (filter `~isin(...)` must return 0 rows) and relationships (a `left_anti` join on the referenced table must return 0 rows), and make the pipeline fail when a check fails. Or use a framework like Great Expectations, Soda or Deequ.

**`ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?**
The Python variables and the order of the cells: `stg_users` is built from the `users_bronze` variable, which is built from the CSV path, and the SQL queries use the names of the temporary views. The dependencies are only in the code, there is no graph, and we have to run the cells in the right order ourselves. After the service crashed, I had to run everything again by hand.

**Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?**
`dbt build` took about 8 seconds, but it only created views and ran about 20 tests on DuckDB. The Spark cells are run one by one by hand, they include the JVM startup, the download of the Iceberg package, the S3 reads, and they really write the data in Parquet and Iceberg. The work done, the engine and the way it is measured are different, and with such small data the time is mostly overhead.

**Would you choose dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?**
10 GB: dbt with DuckDB, one machine handles it easily and it is simpler and cheaper. 10 TB: Spark, it needs a cluster. Analysts who only know SQL: dbt, with models, tests and documentation in SQL (with DuckDB, or with the dbt-spark adapter if the data is big).

## Exercises

**1. `birthdate_is_valid`**
Both versions join `stg_users` with the first order date of each user (`min(ordered_at)` per `user_id`, left join), set `birthdate_is_valid` to false when the birthdate is after the first order, and replace the birthdate with NULL in that case. They give the same result: 45 valid and 5 invalid birthdates, and `exceptAll` between the two returns 0 rows. It is the same logic as the fix of the dbt lab, where it was a CTE with `ref('stg_orders')` in the `stg_users` model, but in dbt the column also got tests in the YAML file and the singular test was updated, while here nothing checks it automatically.

**2. UDFs**

| Version | Best time (3 runs) |
|---|---|
| `regexp_extract()` | 0.101 s |
| Python UDF | 0.112 s |
| pandas UDF | 0.114 s |

The 3 versions give the same zip codes (0 differences). With 50 rows the times are almost the same, it's mostly overhead. `regexp_extract()` runs in the JVM and is optimized by Catalyst. The Python UDF sends the rows one by one to a Python worker and back. The pandas UDF sends batches through Arrow, which is faster than a plain UDF. On a large dataset the order would be: `regexp_extract()` fastest, then pandas UDF, then Python UDF. Native functions should be used when they exist.

**3. Cache**
After `.cache()` and a first action, the Storage tab shows the two bronze DataFrames cached at 100%: 1 partition each, 9.2 KiB for users and 144.4 KiB for orders, storage level Disk Memory Deserialized. The next queries on `stg_users` and `stg_orders` use `InMemoryTableScan` instead of reading and parsing the CSV on S3 again.

**4. Time travel**
After `writeTo("lab.silver.orders").append()`, the table has 2 snapshots. `VERSION AS OF <first snapshot>` returns 2829 orders (before the append) and the current version returns 2830. With the Parquet directory, the append just adds a new file: the count becomes 2830 and there is no way to read the previous state, because nothing keeps track of the versions (except S3 versioning, which is not made for that). I then rewrote the Parquet directory with `stg_orders` to remove the fake order.
