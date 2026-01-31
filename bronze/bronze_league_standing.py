# Import dependencies
from pyspark.sql.functions import explode, col, collect_list, struct, array_distinct, current_timestamp

# Load the league standing raw data from the data lake
raw_league_standing = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/standings"
)

# View data
display(raw_league_standing)

# Flatten the nested arrays to retrieve information on league standing and football teams
flattened_raw_standing = (
    raw_league_standing 
    .withColumn("standing", explode(col("standings")))
    .withColumn("team_info", explode(col("standing.table")))
)

# Extract relevant columns
bronze_league_standing = (
    flattened_raw_standing
        .select(
            col("season.id").alias("season_id"),
            col("filters.season").alias("season_year"),
            col("team_info.position").alias("position"),
            col("team_info.team.id").alias("team_id"),
            col("team_info.team.name").alias("team_name"),
            col("team_info.team.shortName").alias("team_short_name"),
            col("team_info.team.tla").alias("team_tla"),
            col("team_info.team.crest").alias("team_crest"),
            col("team_info.playedGames").alias("played_games"),
            col("team_info.won").alias("won"),
            col("team_info.draw").alias("draw"),
            col("team_info.lost").alias("lost"),
            col("team_info.points").alias("points"),
            col("team_info.goalsFor").alias("goals_for"),
            col("team_info.goalsAgainst").alias("goals_against"),
            col("team_info.goalDifference").alias("goal_difference")
        )
        .withColumn("ingestion_ts", current_timestamp())
)


# Show the bronze league standing
display(bronze_league_standing)

# Write the bronze league standing to the data lake and save as table
bronze_league_standing.write \
  .format("delta") \
    .mode("append") \
      .partitionBy("ingestion_ts") \
        .option("path","abfss://bronze@footballanalyticstorage.dfs.core.windows.net/league_standing") \
          .saveAsTable("db_ws.bronze.league_standing")
