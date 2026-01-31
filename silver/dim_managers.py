# Import dependencies
from pyspark.sql.functions import regexp_replace, trim, lower, split, array_distinct, col, lit, current_timestamp, sha2, concat_ws, to_date

# Read data from bronze layer
retrieved_managers_data = spark.read \
    .format("delta") \
        .load("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/managers_info")

# Display data
display(retrieved_managers_data)

# Refine table
dim_managers = (
    retrieved_managers_data
        .withColumn("manager_name", lower(trim(col("manager_name"))))
        .withColumn("date_of_birth", to_date(col("date_of_birth")))
        .withColumnRenamed("nationality", "manager_nationality")
        .withColumn("manager_nationality", lower(trim(col("manager_nationality"))))
        .withColumn("contract_start_date", to_date(col("contract_start_date")))
        .withColumn("contract_end_date", to_date(col("contract_end_date")))
        .withColumn("valid_from", lit(current_timestamp()))
        .withColumn("valid_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source", lit("football-data.org"))

        # Track changes for SCD-Type 2
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("manager_id"),
                    col("manager_name"),
                    col("manager_nationality"),
                    col("date_of_birth"),
                    col("contract_start_date"),
                    col("contract_end_date"),
                    col("club_id")
                ),
                256
             )
        )
        # Drop irrelivant column
        .drop_duplicates(["manager_id"])
        # Select relevant columns
        .select("manager_id", "club_id", "manager_name", "date_of_birth", "manager_nationality", "contract_start_date", "contract_end_date", "valid_from", "valid_to", "is_current", "ingestion_ts", "source", "record_hash")
)

# Confirm changes
display(dim_managers)

# Write data to delta lake and save as table
dim_managers.write \
    .format("delta") \
        .mode("overwrite") \
            .option("path","abfss://silver@footballanalyticstorage.dfs.core.windows.net/dim_managers") \
                .saveAsTable("db_ws.silver.silver_dim_managers")
