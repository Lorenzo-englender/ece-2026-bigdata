# Labs - Polaris (deployment and governance) - Answers

## Lab 1 - Deployment

Polaris was deployed in my namespace with Helm (Polaris 1.8.0 + PostgreSQL). Before that, the VS Code service was patched down to 2 CPU / 2500Mi so that Polaris and PostgreSQL fit in the namespace quota (4 CPU / 4Gi). The bootstrap job created the catalog `user-l-englender-ece`, the principals `dev` and `analyst`, the principal roles `data_engineer` and `data_analyst`, and the catalog roles `catalog_contributor` and `catalog_reader`. The health check returned `"status": "UP"`. The credentials of `dev` and `analyst` are only shown once, in the logs of the bootstrap job, so they were saved outside the Git repository.

## Lab 2 - SparkSession with the Polaris catalog

Note: the `warehouse/` directory had been removed at the end of the Iceberg lab, so `lab.silver.orders` and `lab.silver.users` were first recreated from the Parquet files of `silver/spark/` (2829 orders, 50 users). The counts are 2829 orders instead of 5000.

**Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?**
`SHOW NAMESPACES IN lab` is answered by Spark itself: the hadoop catalog just lists the directories under `warehouse/` in the bucket with the S3 keys, and it returned `silver`. `SHOW NAMESPACES IN polaris` is answered by the Polaris server, through an HTTP request to the REST API (`http://polaris:8181/api/catalog`), and at first it returned nothing, even though the `silver` directory existed in the bucket. Polaris answers from its own database (PostgreSQL), not by listing the bucket.

**Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?**
In the requests to Polaris. Spark's REST client sends the `clientId:clientSecret` of the principal to Polaris to get an OAuth token, then sends this token with every catalog request, and Polaris checks the privileges of the principal. The bucket never sees it: the data files are read and written with the S3 keys configured in the Spark session (S3A).

## Register the Silver table in Polaris

**What did `register_table` write, in Polaris and in the bucket?**
In Polaris, it created an entry for the table `silver.orders` in the namespace `silver`, which points to the existing `v1.metadata.json`. This entry is stored in the PostgreSQL database of Polaris. In the bucket it wrote nothing: there were 5 objects under `warehouse/silver/orders/` before and 5 after. No data was copied, and the count through Polaris is the same (2829).

**Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?**
The table now has two pointers to its current version: `version-hint.text` for the hadoop catalog, and the entry in Polaris. A commit through `lab` only moves the hadoop pointer, so Polaris keeps pointing to the old version and doesn't see the new data. If someone then writes through Polaris, it starts from the old version, and we get two different histories of the same table, with lost data. Writing through `lab` also completely skips the access control of Polaris.

## Create tables as `dev`

**Where did Polaris store the files for `silver.users`, and why?**
In `s3://user-l-englender-ece/warehouse/silver/users/`, because the default location of a table is the `default-base-location` of the catalog (`s3://<bucket>/warehouse/`), plus the namespace and the table name. The metadata files written by Polaris are named `00000-<uuid>.metadata.json`, `00001-...`, and not `v1.metadata.json` like with the hadoop catalog. It's the same directory as the hadoop table `lab.silver.users`: the two tables only live together there because all the file names are unique.

**What is the difference between "registered" and "created through Polaris" for the data files?**
For `silver.orders` (registered), the files were written before by Spark with the hadoop catalog (`vN.metadata.json`, `version-hint.text`, `s3a://` paths), and Polaris only adopted the existing metadata file. For `silver.users` (created through Polaris), Polaris chose the location and writes a new metadata file at every commit, while Spark writes the data files. In both cases the data files are in my bucket and anyone with the S3 keys can read them directly.

## Schema change as `dev`

**Which metadata file records the change? Is it the same file that `register_table` pointed to?**
A new file, `00001-45a731d8-....metadata.json` in `warehouse/silver/users/metadata/`, written by Polaris, which then moved its pointer to it. It's not the file of `register_table` (the `v1.metadata.json` of `orders`, which didn't change): every change of a table creates a new metadata file, the old ones are never modified.

**Which privilege does Polaris check for this statement?**
Polaris checks the operation `ADD_TABLE_SCHEMA` (it's the operation named in the error of the analyst), which needs the `TABLE_WRITE_PROPERTIES` privilege on the table. `dev` has it through `CATALOG_MANAGE_CONTENT`, granted to `catalog_contributor` on the whole catalog.

## Read as the analyst

As `analyst`, `SHOW TABLES` and the `SELECT ... GROUP BY product` worked. The `INSERT` failed with `ForbiddenException: Principal 'analyst' ... is not authorized for op ADD_TABLE_SNAPSHOT`, and the `ALTER TABLE ... ADD COLUMN phone` with `is not authorized for op ADD_TABLE_SCHEMA`.

**Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?**
`TABLE_WRITE_DATA`, which is needed to add a snapshot to a table. `catalog_reader` only gives `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`: listing and loading the tables, reading their data and properties, and reading the properties of the catalog.

**Did the failed `INSERT` create a snapshot?**
No, `silver.orders` still has only 1 snapshot (the `overwrite` from the creation). But the logs show `IcebergBatchWrite ... is aborting`: Spark first wrote the new data file in the bucket with its own S3 keys, and only the final commit to Polaris was refused, then Spark aborted and tried to clean the file. Polaris controls the commits, not the access to the files.

**What Polaris enforces, and what it cannot enforce on this storage**
Polaris enforces everything that goes through the catalog: listing, loading, creating, committing and changing tables. But on this storage there is no credential vending (`stsUnavailable`, `HadoopFileIO`): Spark reads and writes the files with my own S3 keys. So the analyst could still read, write or delete the files directly in the bucket, without going through Polaris. To really protect the data, Polaris would need to give short-lived credentials limited to each table (credential vending with STS), and users should not have direct access to the bucket.

## Commit the Iceberg table metadata

After `aws s3 sync`, `git status` shows the new metadata files: the `v1.metadata.json` of the hadoop tables recreated in this lab, the `00000-...`, `00001-...` and `00002-...metadata.json` files written by Polaris for `users`, the new manifests (`.avro`), and the old metadata files of the previous labs as deleted, since they don't exist on S3 anymore. Only the `metadata/` directories are committed, not the data files.

## Exercises

**1. Remove the `country` column**
After `ALTER TABLE polaris.silver.users DROP COLUMN country`, the table still has the same single snapshot (`4471628206592718869`, the creation), but `metadata_log_entries` shows 3 metadata files: `00000` (creation), `00001` (add `country`), `00002` (drop `country`), and `DESCRIBE` doesn't show `country` anymore. It's a metadata operation: no data file is read or rewritten, Polaris only writes a new metadata file with a new schema. Iceberg identifies columns by id, so readers just ignore a column that is not in the current schema anymore.

**2. A `silver_reader` role limited to the `silver` namespace**
With the management API and the `root` token, I created:
- the catalog role `silver_reader`, with `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA` and `NAMESPACE_READ_PROPERTIES` granted **on the namespace `silver` only** (not on the catalog);
- the principal role `silver_analyst`, which gets `silver_reader`;
- a new principal `silver_user`, which gets `silver_analyst`.

As `dev` I created the namespace `gold` with a table `gold.orders_by_product` (orders per product). As `silver_user`, reading `polaris.silver.orders` works (2829 orders), but reading `polaris.gold.orders_by_product` fails with `ForbiddenException: Principal 'silver_user' ... is not authorized for op LOAD_TABLE`. Privileges granted on a namespace apply to the tables inside it, so `silver_user` can only see `silver`. It's the least privilege principle: `catalog_reader` gives read access to the whole catalog, `silver_reader` only to one layer.
