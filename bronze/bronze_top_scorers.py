# Import dependencies
from pyspark.sql.functions import explode, col, collect_list, struct, array_distinct, current_timestamp

# Load top scorers' data
raw_scorers_data = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/scorers"
)

# Show the data
display(raw_scorers_data)

# Flatten scorers data to obtain each team's scorers information
scorers_data = (
    raw_scorers_data
        .select(explode(col("scorers")).alias("scorer"))
)


# Deconstruct the scorers data
bronze_top_scorers = (
    scorers_data
        .select(
            col("scorer.team.id").alias("club_id"),
            col("scorer.team.name").alias("football_club"),
            col("scorer.playedMatches"),
            col("scorer.goals"),
            col("scorer.assists"),
            col("scorer.penalties"),
            col("scorer.player.id").alias("player_id"),
            col("scorer.player.name").alias("player_name"),
            col("scorer.player.section")
        )
        .withColumn("ingestion_ts", current_timestamp())
)

# Confirm data transformation
display(bronze_top_scorers)

# Write data to delta lake and save as table
bronze_top_scorers.write \
    .format("delta") \
        .mode("overwrite") \
            .partitionBy("ingestion_ts") \
            .option("path","abfss://bronze@footballanalyticstorage.dfs.core.windows.net/top_scorers") \
                .saveAsTable("db_ws.bronze.top_scorers")
