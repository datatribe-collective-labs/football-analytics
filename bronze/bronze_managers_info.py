# Import dependencies
from pyspark.sql.functions import explode, col, collect_list, struct, array_distinct, current_timestamp

# Load the data
raw_managers = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/teams"
)

# Flatten teams array to retrieve mnagers' data
managers_exploded_data = (
    raw_managers
        .select(explode(col("teams")).alias("team"))
)

# Collect relevant columns
bronze_managers = (
    managers_exploded_data
        .select(
            col("team.coach.id").alias("manager_id"),
            col("team.id").alias("club_id"),
            col("team.coach.name").alias("manager_name"),
            col("team.coach.dateOfBirth").alias("date_of_birth"),
            col("team.coach.nationality"),
            col("team.coach.contract.start").alias("contract_start_date"),
            col("team.coach.contract.until").alias("contract_end_date")
        )
        .withColumn("ingestion_ts", current_timestamp())
)

# Show transformed tables
display(bronze_managers)

bronze_managers.write \
    .format("delta") \
        .mode("append") \
            .partitionBy("ingestion_ts") \
            .option("path","abfss://bronze@footballanalyticstorage.dfs.core.windows.net/managers_info") \
                .saveAsTable("db_ws.bronze.managers_info")
