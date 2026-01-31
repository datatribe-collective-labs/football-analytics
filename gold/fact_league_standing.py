# Import dependencies
from pyspark.sql.functions import col, lit, current_timestamp, when, lit, broadcast
from delta.tables import DeltaTable

# Read data from the data lake
silver_league_standing = spark.read \
  .format("delta") \
      .load("abfss://silver@footballanalyticstorage.dfs.core.windows.net/fact_league_standing")

display(silver_league_standing)

# Create delta table: fact_league_table
%sql
CREATE TABLE IF NOT EXISTS fact_league_table (
  club_id LONG,
  season_id LONG,
  total_games_played LONG,
  won LONG,
  draw LONG,
  lost LONG,
  total_goals_scored LONG,
  total_goals_conceded LONG,
  goal_difference LONG,
  points LONG,
  season_year INT,
  updated_at TIMESTAMP
)
USING DELTA
-- LOCATION '/mnt/datalake/silver/league_standing';

# Write data to the delta table
silver_league_standing.write \
    .format("delta") \
        .mode("overwrite") \
            .option("overwriteSchema", "true") \
                .saveAsTable("fact_league_table")

# Prepare the fact_league_standing table
fact_league_standing = (
    spark.table("fact_league_table").alias("l")
    .join(
        broadcast(spark.table("dim_managers")).alias("m"),
        col("l.club_id") == col("m.club_id"),
        "inner"
    )
    .join(
        broadcast(spark.table("dim_players")).alias("p"),
        col("l.club_id") == col("p.club_id"),
        "inner"
    )
    .select(
        col("m.manager_sk"),
        col("l.*")
    )
    .withColumn("updated_at", current_timestamp())
)

display(fact_league_standing)

# Write table to delta lake
fact_league_standing.coalesce(1).write \
    .format("delta") \
        .mode("overwrite") \
            .option("path","abfss://gold@footballanalyticstorage.dfs.core.windows.net/fact_league_standing") \
                 .saveAsTable("db_ws.gold.fact_league_standing")
