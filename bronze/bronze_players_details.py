# Import dependencies
from pyspark.sql.functions import explode, col, collect_list, struct, array_distinct, current_timestamp

# Prepare the league players' details data for the bronze layer

# Load the data
raw_players_details = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/player_details"
)

# Show the retrieved data
display(raw_players_details)

# Flatten the teams array
teams_exploded_data = (
    raw_players_details
        .select(explode(col("teams")).alias("team"))
)

# Collect relevant columns
bronze_players_details = (
    teams_exploded_data
        .select(
            col("team.id").alias("club_id"),
            col("team.name").alias("club_name"),
            explode(col("team.squad")).alias("player")
        )
        .select(
            col("player.id").alias("player_id"),
            col("player.name").alias("player_name"),
            col("player.position").alias("position"),
            col("player.dateOfBirth").alias("date_of_birth"),
            col("player.nationality").alias("nationality"),
            col("club_id"),
            col("club_name")
        )
        .withColumn("ingestion_ts", current_timestamp())
)

# Confirm changes
display(bronze_players_details)

# Write players details to the data lake and save as table
bronze_players_details.write \
    .format("delta") \
        .mode("append") \
            .partitionBy("ingestion_ts") \
            .option("path","abfss://bronze@footballanalyticstorage.dfs.core.windows.net/players_details") \
                .saveAsTable("db_ws.bronze.players_details")
