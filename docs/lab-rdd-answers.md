# Lab - Introduction to Spark's RDD API - Answers

## RDD creation and partitions

Result: `users_lines` and `orders_lines` have 2 partitions each, and 8 after `repartition(8)`.

**What decides the number of partitions `textFile()` picks by default, for a file this size?**
`textFile()` splits the file in blocks (32 MB for the local file system, 128 MB on HDFS) and uses at least `minPartitions`, which is `min(defaultParallelism, 2)` = 2 by default. Our files are a few hundred KB, so they fit in one block and Spark uses the minimum: 2 partitions.

**`repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?**
`repartition()` always does a full shuffle and can increase or decrease the number of partitions. `coalesce()` only reduces the number of partitions by merging existing ones, without a shuffle (unless we set `shuffle=True`).

## Lazy evaluation and lineage

`toDebugString()` only shows the lineage, nothing is executed. The first `count()` returns 2829 and creates a job.

**Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?**
Yes. Each `count()` created a new job and a new stage, and both read the whole file again (Input 417.9 KiB for stages 38 and 39). An RDD doesn't keep its result: on every action Spark recomputes it from its lineage, starting from the file.

**At which point in this notebook would `.cache()` change that answer?**
If `orders_data.cache()` is called before the first `count()`. The first count still reads the file (417.9 KiB) and fills the cache, then the next count reads from memory: 162.2 KiB in 19 ms instead of 417.9 KiB. The Storage tab showed the RDD with 2 partitions, 100% cached, 162.2 KiB in memory.

## The cost of no schema: the multiline address

`users_lines.count()` returns 101 lines (header + 50 users x 2 lines), and 50 after keeping only the lines starting with a UUID.

**Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?**
`spark.read.csv` is a real CSV parser: it understands the quotes, so a line break inside a quoted field is part of the value and not the end of the record. With `multiLine=True` it also doesn't cut the file in the middle of a record. `textFile()` doesn't know anything about CSV, it just cuts on every line break.

**What has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?**
Each record must be on exactly one line, with no line break inside a value (or line breaks must be escaped). For example JSON lines, logs, or CSV without multiline fields.

## Manual parsing of orders

**What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?**
`line.split(",")` would return 6 values instead of 5, and the unpacking would fail with `ValueError: too many values to unpack`. Because Spark is lazy, the error only appears at the first action (`take(3)`) and the whole job fails.

**Where does the DataFrame API perform the equivalent check, and when?**
In the CSV reader, while reading the file: it applies the schema (given or inferred), handles the quotes, and checks the number of columns and the types. It happens when an action triggers the scan. With the `mode` option we choose what to do with bad rows: put them in null / `_corrupt_record` (PERMISSIVE, default), drop them (DROPMALFORMED) or fail (FAILFAST).

## Key-value RDDs: total quantity per user

Result in the Spark UI: `reduceByKey` wrote 2.5 KiB of shuffle, `groupByKey` wrote 6.1 KiB, about 2.5 times more.

**`reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is that more expensive?**
`groupByKey` sends every single `(user_id, quantity)` pair over the shuffle, so all 2829 records, while `reduceByKey` only sends one partial sum per user and per partition. More data means more disk, network and serialization, and all the values of a key must fit in the memory of one executor, which can crash with skewed keys.

**For which aggregations would `groupByKey` be unavoidable, `reduceByKey` not being able to express them?**
When we need all the values of a key at the same time and the result can't be computed by combining partial results: for example the exact median or percentiles, sorting all the values of a key, or building the full list of orders of each user.

## Manual deduplication

Result: 2829 orders before and after, the generator doesn't create duplicates.

**What plays the role of `partitionBy()` here?**
The shuffle of `reduceByKey`: it hash-partitions the pairs on the key `order_id`, so all the rows with the same `order_id` end up in the same partition before `most_recent` is applied.

**Which version states the intent more directly: the window function, or the `reduceByKey` with a custom combiner?**
The window function. `row_number() over (partition by order_id order by ordered_at desc) = 1` directly says "keep the most recent row of each order". With `reduceByKey` we have to read the custom function to understand what it does, and Spark can't optimize it.

## Joining orders with users

**What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?**
Both RDDs must be co-partitioned: partitioned with the same partitioner (same hash function and same number of partitions) on the join key, for example with `partitionBy(n)` on both and then `cache()`. Then the matching keys are already in the same partitions and the join becomes a narrow dependency.

**Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in `orders` has no matching user?**
With `join()` the order disappears. With `leftOuterJoin()` it is kept, with `None` instead of the username: `(user_id, (order, None))`. With our data all the users exist so the result is the same (see exercise 2).

**What is the risk of calling `.collect()` on `enriched` directly, on a much larger dataset?**
`collect()` brings all the data back to the driver. On a big dataset it doesn't fit in the driver memory and it crashes (OutOfMemoryError). It's better to filter or aggregate first, use `take()`, or write the result to storage.

## Exercises

**1. `aggregateByKey`**
`aggregateByKey((0, 0), seqOp, combOp)` computes `(order_count, total_quantity)` for each user in one pass, then `mapValues` divides the total by the count. The average quantity per order is around 3 for every user, which makes sense since the quantity is random between 1 and 5.

**2. Fake `user_id` with `leftOuterJoin()`**
One fake order with an unknown `user_id` was added with `union()`. After the `leftOuterJoin()`, filtering on `username is None` returns 1 order: the fake one.

**3. DataFrame API**
`spark.read.csv(...).groupBy("user_uuid").sum("quantity")` (the column is called `user_uuid` in the CSV) gives the same result in 2 lines, without writing a parsing function. `.explain()` shows an optimized physical plan: a partial `HashAggregate`, an `Exchange hashpartitioning` (the shuffle) and a final `HashAggregate`, so Spark does the same pre-aggregation as `reduceByKey` automatically. `.toDebugString()` only shows a chain of RDDs (`PythonRDD`, `ShuffledRDD`...) that Spark can't optimize.

**4. Cache**

| | With cache | Without cache |
|---|---|---|
| `orders_rdd.count()` | 0.478 s | 0.172 s |
| `user_quantity.count()` | 0.098 s | 0.144 s |

The times are too small to see a clear difference: with only 2829 lines, most of the time is the overhead of launching the tasks and the Python workers. The Spark UI shows the real difference: with cache, `orders_rdd` is read from memory (Input 168 KiB), without cache Spark reads and parses the CSV file again (Input 417.9 KiB). `user_quantity` only reads 2.5 KiB of shuffle in both cases, because Spark reuses the shuffle files already written and skips the map stage. The Storage tab shows the cached RDD with 100% of its partitions, and is empty after `unpersist()`.
