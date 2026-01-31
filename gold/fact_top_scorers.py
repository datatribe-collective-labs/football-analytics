# Import libraries
from pyspark.sql.functions import col, lit, current_timestamp, when, lit, broadcast
from delta.tables import DeltaTable

# Read top scorers data from the data lake
silver_top_scorers = spark.read \
  .format("delta") \
      .load("abfss://silver@footballanalyticstorage.dfs.core.windows.net/fact_top_scorers")

display(silver_top_scorers)

# Create the delta table to absorb incoming streams of data

%sql
CREATE TABLE IF NOT EXISTS fact_top_scorers_table (
  player_id INT,
  club_id INT,
  total_matches_played INT,
  goals INT,
  assists INT,
  penalties INT,
  updated_at TIMESTAMP
)
USING DELTA
-- LOCATION

# Write the data to the delta table
silver_top_scorers.write \
    .format("delta") \
        .mode("overwrite") \
            .option("overwriteSchema", "true") \
                .saveAsTable("fact_top_scorers_table")

# Create fact_top_scorers_table
fact_top_scorers = (
    spark.table("fact_top_scorers_table").alias("f")
    .join(
        broadcast(spark.table("dim_players")).alias("p"),
        col("f.player_id") == col("p.player_id"),
        "inner"
    )
    .select(
        col("p.player_sk"),
        col("f.*")
    )
    .withColumn("updated_at", current_timestamp())
    .drop("injestion_ts")
)

# Write the data to the delta lake
fact_top_scorers.coalesce(1).write \
    .format("delta") \
        .mode("overwrite") \
            .option("path","abfss://gold@footballanalyticstorage.dfs.core.windows.net/fact_top_scorers") \
                 .saveAsTable("db_ws.gold.fact_top_scorers")
