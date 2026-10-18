# Lab - SQL analytics with DuckDB - Answers

## S3 configuration

**The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?**
Anyone or any program that can read this file can use the S3 credentials, and the file could be copied or committed to Git by mistake. On Onyxia the risk is limited because the credentials are temporary and expire quickly, the home directory is only in my own service, and the file is outside the Git project.

**The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?**
Give the Job its own credentials with only the permissions it needs, for example only writing to the `bronze/` prefix of the bucket. The credentials go in a Secret that only this Job uses, or even better the Job gets its own service account linked to a role, with short-lived credentials.

## Query the bronze layer

**Why does `approx_unique` return 40 and 3167 instead of 50 and 2829?**
Because it is an approximation. DuckDB uses an algorithm (HyperLogLog) that estimates the number of distinct values very fast and with little memory, but the result is not exact.

**The types are inferred from a sample. Which issue may occur with a large file whose first rows are not representative?**
DuckDB can choose a wrong type. For example, if a column only has numbers in the first rows and text later, it is detected as a number and the reading fails or gives wrong values. To avoid this we can give the types ourselves or read a bigger sample.

## Parquet export

**Why is the `uuid` column barely compressed?**
Every uuid is different and random, so there are no repeated values. Dictionary encoding and compression need repetitions to work, so they can't reduce it much.

**Why is the compressed size of some columns larger than their uncompressed size?**
These columns are already very small or already encoded (dictionary, binary numbers). Snappy can't make them smaller and adds a bit of extra data, so the result is slightly bigger.

## Hive partitioning

**Why is partitioning by `uuid` a bad idea?**
It would create one directory and one file per order, so thousands of tiny files. On object storage each file is a separate request with its own latency, and listing many objects is slow. Also queries almost never filter on the uuid, so it wouldn't skip anything useful.

**Which partition column would you choose for a dataset of orders growing every day?**
The date, for example the day or the month of the order. New data just adds new partitions, and most queries filter on a period, so the other partitions are skipped.

## CSV vs. Parquet at scale

**How many row groups does the file contain, and how many are skipped by the filter `date >= '2100-01-01'`?**
The file contains 3 row groups. All 3 are skipped: the footer statistics show that the latest date of the file is in 2048, so no row group can contain a date after 2100 and DuckDB reads almost nothing.

**The orders are sorted by date. What would happen if they were shuffled?**
Every row group would contain dates from the whole period, so the min and max of each row group would cover almost everything. No row group could be skipped and the filter would read the whole column.

**Which part of the difference between CSV and Parquet is due to the network, which part to the parsing?**
The network part comes from the volume downloaded: the CSV query downloads the whole file (about 110 MiB) while the Parquet query only downloads the needed columns (less than 1 MiB). The parsing part comes from reading the CSV text, splitting it and converting every value, while Parquet is already binary and typed. On the platform the network is the biggest part because of the big difference in data size.
