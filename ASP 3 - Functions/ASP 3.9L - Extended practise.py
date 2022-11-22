# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Abandoned Carts Lab
# MAGIC Get abandoned cart items for email without purchases.
# MAGIC 1. Get emails of converted users from transactions
# MAGIC 2. Join emails with user IDs
# MAGIC 3. Get cart item history for each user
# MAGIC 4. Join cart item history with emails
# MAGIC 5. Filter for emails with abandoned cart items
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>: **`collect_set`**, **`explode`**, **`lit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cells below to create DataFrames **`sales_df`**, **`users_df`**, and **`events_df`**.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# sale transactions at BedBricks
sales_df = spark.read.format("delta").load(DA.paths.sales)
display(sales_df)

# COMMAND ----------

# user IDs and emails at BedBricks
users_df = spark.read.format("delta").load(DA.paths.users)
display(users_df)

# COMMAND ----------

# events logged on the BedBricks website
events_df = spark.read.format("delta").load(DA.paths.events)
display(events_df)

# COMMAND ----------

print(sc.defaultMinPartitions)
print(sc.defaultParallelism)


# COMMAND ----------

# MAGIC %md ### 1: Get emails of converted users from transactions
# MAGIC - Select the **`email`** column in **`sales_df`** and remove duplicates
# MAGIC - Add a new column **`converted`** with the boolean **`True`** for all rows
# MAGIC 
# MAGIC Save the result as **`converted_users_df`**.

# COMMAND ----------

# TODO
from pyspark.sql.functions import *

converted_users_df = sales_df.select(
    "email",
    lit(True).alias("converted"),
    (col("transaction_timestamp") / 1e6).cast("timestamp").alias("transaction_ts"),
    "purchase_revenue_in_usd",
    # "unique_items",
    # round(col("purchase_revenue_in_usd") / col("unique_items"), 2).alias(
    #     "revenue_per_item"
    # ),
# ).drop_duplicates()
).groupBy(['email', 'converted']).agg(
    min("transaction_ts").alias("first_transaction_ts"),
    mean("purchase_revenue_in_usd").alias("avg_purchase_revenue_in_usd")
)

display(converted_users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Explain the plan, check the steps
# MAGIC 
# MAGIC Without drop, it looks like: 
# MAGIC 
# MAGIC ```
# MAGIC == Physical Plan ==
# MAGIC *(1) Project [email#42143, true AS converted#42515]
# MAGIC +- *(1) ColumnarToRow
# MAGIC    +- FileScan parquet [email#42143] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/mnt/dbacademy-datasets/apache-spark-programming-with-databr..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<email:string>
# MAGIC ```
# MAGIC 
# MAGIC With drop:
# MAGIC ```
# MAGIC == Physical Plan ==
# MAGIC AdaptiveSparkPlan isFinalPlan=false
# MAGIC +- HashAggregate(keys=[email#42143, true#42600], functions=[])
# MAGIC    +- Exchange hashpartitioning(email#42143, true#42600, 200), ENSURE_REQUIREMENTS, [plan_id=33383]
# MAGIC       +- HashAggregate(keys=[email#42143, true AS true#42600], functions=[])
# MAGIC          +- FileScan parquet [email#42143] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/mnt/dbacademy-datasets/apache-spark-programming-with-databr..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<email:string>
# MAGIC ```
# MAGIC 
# MAGIC Changing the drop to an aggregation: 
# MAGIC ```
# MAGIC == Physical Plan ==
# MAGIC AdaptiveSparkPlan isFinalPlan=false
# MAGIC +- HashAggregate(keys=[email#42143, true#43513], functions=[finalmerge_min(merge min#43456) AS min(transaction_ts#43433)#43442, finalmerge_avg(merge sum#43459, count#43460L) AS avg(purchase_revenue_in_usd#42146)#43444])
# MAGIC    +- Exchange hashpartitioning(email#42143, true#43513, 200), ENSURE_REQUIREMENTS, [plan_id=36735]
# MAGIC       +- HashAggregate(keys=[email#42143, true AS true#43513], functions=[partial_min(transaction_ts#43433) AS min#43456, partial_avg(purchase_revenue_in_usd#42146) AS (sum#43459, count#43460L)])
# MAGIC          +- Project [email#42143, cast((cast(transaction_timestamp#42144L as double) / 1000000.0) as timestamp) AS transaction_ts#43433, purchase_revenue_in_usd#42146]
# MAGIC             +- FileScan parquet [email#42143,transaction_timestamp#42144L,purchase_revenue_in_usd#42146] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/mnt/dbacademy-datasets/apache-spark-programming-with-databr..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<email:string,transaction_timestamp:bigint,purchase_revenue_in_usd:double>
# MAGIC ```

# COMMAND ----------

converted_users_df.explain()

# COMMAND ----------

# MAGIC %md #### 1.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["email", "converted", "first_transaction_ts", "avg_purchase_revenue_in_usd"]

expected_count = 210370

assert converted_users_df.columns == expected_columns, "converted_users_df does not have the correct columns"

assert converted_users_df.count() == expected_count, "converted_users_df does not have the correct number of rows"

assert converted_users_df.select(col("converted")).first()[0] == True, "converted column not correct"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2: Join emails with user IDs
# MAGIC - Perform an outer join on **`converted_users_df`** and **`users_df`** with the **`email`** field
# MAGIC - Filter for users where **`email`** is not null
# MAGIC - Fill null values in **`converted`** as **`False`**
# MAGIC 
# MAGIC Save the result as **`conversions_df`**.

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

# TODO
conversions_df = (users_df.join(converted_users_df, on="email", how="left")
                          .na.drop(subset=['email'])
                          .fillna(value=False, subset=["converted"])
                          .withColumn("user_first_touch_ts", (col("user_first_touch_timestamp") / 1e6).cast("timestamp"))
                          .drop("user_first_touch_timestamp")
                          # In seconds
                        #   .withColumn("first_transaction_delay", col("first_transaction_ts") - col("user_first_touch_ts"))
                          # In minutes
                          .withColumn("first_transaction_delay", round(
                                                                    (col("first_transaction_ts").cast(LongType()) - 
                                                                     col("user_first_touch_ts").cast(LongType())
                                                                    )/60, 2)
                                    )
                        # When there is a null value, the cast/round/... return NULL, like any other SQL
                )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Is there a connection between delay in the first buy and the average amount of buying? It seams that there is not.
# MAGIC 
# MAGIC Hint: Quick visualization is very useful in this case

# COMMAND ----------

display(conversions_df)

# COMMAND ----------

conversions_df.explain()

# COMMAND ----------

# MAGIC %md #### 2.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["email", 
                    "user_id", 
                    "converted", 
                    "first_transaction_ts", 
                    "avg_purchase_revenue_in_usd", 
                    "user_first_touch_ts", 
                    "first_transaction_delay", 
                    ]

expected_count = 782749

expected_false_count = 572379

assert conversions_df.columns == expected_columns, "Columns are not correct"

assert conversions_df.filter(col("email").isNull()).count() == 0, "Email column contains null"

assert conversions_df.count() == expected_count, "There is an incorrect number of rows"

assert conversions_df.filter(col("converted") == False).count() == expected_false_count, "There is an incorrect number of false entries in converted column"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3: Get cart item history for each user
# MAGIC - Explode the **`items`** field in **`events_df`** with the results replacing the existing **`items`** field
# MAGIC - Group by **`user_id`**
# MAGIC   - Collect a set of all **`items.item_id`** objects for each user and alias the column to "cart"
# MAGIC 
# MAGIC Save the result as **`carts_df`**.

# COMMAND ----------

# TODO
carts_df = (events_df.FILL_IN
)
display(carts_df)

# COMMAND ----------

# MAGIC %md #### 3.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "cart"]

expected_count = 488403

assert carts_df.columns == expected_columns, "Incorrect columns"

assert carts_df.count() == expected_count, "Incorrect number of rows"

assert carts_df.select(col("user_id")).drop_duplicates().count() == expected_count, "Duplicate user_ids present"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4: Join cart item history with emails
# MAGIC - Perform a left join on **`conversions_df`** and **`carts_df`** on the **`user_id`** field
# MAGIC 
# MAGIC Save result as **`email_carts_df`**.

# COMMAND ----------

# TODO
email_carts_df = conversions_df.FILL_IN
display(email_carts_df)

# COMMAND ----------

# MAGIC %md #### 4.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]

expected_count = 782749

expected_cart_null_count = 397799

assert email_carts_df.columns == expected_columns, "Columns do not match"

assert email_carts_df.count() == expected_count, "Counts do not match"

assert email_carts_df.filter(col("cart").isNull()).count() == expected_cart_null_count, "Cart null counts incorrect from join"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5: Filter for emails with abandoned cart items
# MAGIC - Filter **`email_carts_df`** for users where **`converted`** is False
# MAGIC - Filter for users with non-null carts
# MAGIC 
# MAGIC Save result as **`abandoned_carts_df`**.

# COMMAND ----------

# TODO
abandoned_carts_df = (email_carts_df.FILL_IN
)
display(abandoned_carts_df)

# COMMAND ----------

# MAGIC %md #### 5.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]

expected_count = 204272

assert abandoned_carts_df.columns == expected_columns, "Columns do not match"

assert abandoned_carts_df.count() == expected_count, "Counts do not match"
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6: Bonus Activity
# MAGIC Plot number of abandoned cart items by product

# COMMAND ----------

# TODO
abandoned_items_df = (abandoned_carts_df.FILL_IN
                     )
display(abandoned_items_df)

# COMMAND ----------

# MAGIC %md #### 6.1: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

abandoned_items_df.count()

# COMMAND ----------

expected_columns = ["items", "count"]

expected_count = 12

assert abandoned_items_df.count() == expected_count, "Counts do not match"

assert abandoned_items_df.columns == expected_columns, "Columns do not match"
print("All test pass")

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
