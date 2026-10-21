# Lab - Bronze to Silver with dbt and DuckDB - Answers

## Bronze layer

**The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?**
We still have the original raw data. We just fix the SQL of the model and run dbt again: the silver layer is rebuilt correctly from the bronze files, and no data is lost.

**The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?**
Parquet files, partitioned by date (for example by year, month and day), or a table format like Iceberg on top of them. Queries would only read the partitions and the columns they need, instead of the whole history.

## Build

**The silver models are views. What happens when a view is queried, and when is it preferable to a table?**
A view only stores the SQL query. Each time it is queried, the query runs again: DuckDB reads the CSV from S3 and applies the transformations. A view is better when the data is small, changes often and we always want the latest version without storing a copy. A table is better when the data is queried often or the transformation is heavy.

**`UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?**
36 bytes, one per character (32 hex characters and 4 dashes), so more than twice the size of the UUID type.

## Data tests

**Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?**
For the age dashboard, the invalid birthdates would give wrong ages, so I would replace them with NULL and leave these users out of the age statistics. For the sales per region, the birthdate is not used, so I would keep all the orders and only monitor the issue with a warning, removing sales would make the numbers wrong.

**The addresses associate states with random zip codes. Which test would detect it, and which reference data would it require?**
A test that checks that the zip code matches the state. It needs reference data with the zip code ranges of each state, for example from the US Postal Service. This is what was done in exercise 4.

## Exercise 4 - Invalid ZIP codes

The test `assert_users_zip_matches_state` compares the first digit of the zip code with the state, using the `zip_regions` reference model. It finds 46 inconsistent users out of 50, as expected with random data. It is configured as a warning so it doesn't block the pipeline.

- **Where should this reference data come from?** From an official source like the US Postal Service, or a public zip code database.
- **Should it be stored as a dbt seed?** Yes, it is small, rarely changes and should be versioned in Git, which is exactly what seeds are for. Here it is a SQL model (`models/reference/`) because loading seeds failed on the platform, but it plays the same role.
- **Should the validation happen in the silver layer?** Yes, the silver layer is where the data is cleaned and validated before being used.
- **What happens when the reference data changes?** We update the file in Git and run `dbt build` again. The test results can change, and the Git history shows which version of the reference was used.

## Exercise 5 - View vs table

| | View | Table |
|---|---|---|
| Size of `lab_medallion.duckdb` | 268 KB | 780 KB |
| `SELECT count(*) FROM stg_orders` | 0.393 s | 0.002 s |
| `dbt build --select silver` | 7.07 s | 3.64 s |

- **Where is the data stored?** With a view, only the SQL is stored and the data stays in the CSV on S3. With a table, the transformed data is stored in the DuckDB file.
- **When is the transformation executed?** For a view, every time it is queried. For a table, once, during `dbt build`.
- **What happens when the underlying CSV changes?** A view shows the new data right away. A table keeps the old data until the next `dbt build`.
- **Which approach requires more storage?** The table (780 KB vs 268 KB).
- **Which approach avoids re-reading the source files for every query?** The table: the query is about 200 times faster because it doesn't read S3.

## Note

The `relationships` test on `state` references a `states` model which is not created in the lab. Loading it as a dbt seed failed on the platform (the CSV header was read as empty column names), so it is defined as a SQL model in `models/reference/states.sql`.
