# Import dependencies
from pyspark.sql.functions import explode, col, collect_list, struct, array_distinct, current_timestamp

# Load raw data
raw_season = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/teams"
)

# Select relevant columns
bronze_season = (
    raw_season
        .select(
            col("season.id").alias("season_id"),
            col("competition.name").alias("season_name"),
            col("filters.season").alias("season_year"),
            col("season.startDate").alias("season_start_date"),
            col("season.endDate").alias("season_end_date"),
            col("season.currentMatchday").alias("current_matchday"),
            col("season.winner").alias("winner")
        )
        .withColumn("ingestion_ts", current_timestamp())
)

bronze_season.write \
    .format("delta") \
        .mode("overwrite") \
            .partitionBy("ingestion_ts") \
            .option("path","abfss://bronze@footballanalyticstorage.dfs.core.windows.net/season_info") \
                .saveAsTable("db_ws.bronze.season_info")
