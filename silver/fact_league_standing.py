# Import dependencies
from pyspark.sql.functions import col, lower, trim, current_timestamp, lit

# Read data from bronze layer
retrieved_league_standing = spark.read \
    .format("delta") \
        .load("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/league_standing")


# Show data
display(retrieved_league_standing)

# Refine data

fact_league_standing = (
    retrieved_league_standing
        # ---- IDs & naming ----
        .withColumnRenamed("team_id", "club_id")

        # ---- Metrics ----
        .withColumnRenamed("played_games", "total_games_played")
        .withColumnRenamed("goals_for", "total_goals_scored")
        .withColumnRenamed("goals_against", "total_goals_conceded")

        # ---- Standardize club name (if needed before drop) ----
        .withColumn(
            "football_club",
            lower(trim(col("team_short_name")))
        )

        # ---- Metadata ----
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn(
            "source",
            lit("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/league_standing")
        )

        # ---- Drop unused descriptive columns ----
        .drop(
            "team_name",
            "team_short_name",
            "team_tla",
            "team_crest"
        )

        # ---- Final projection (FACT table) ----
        .select(
            col("club_id"),
            col("season_id"),
            col("total_games_played"),
            col("won"),
            col("draw"),
            col("lost"),
            col("total_goals_scored"),
            col("total_goals_conceded"),
            col("goal_difference"),
            col("points"),
            col("season_year"),
            col("ingestion_ts"),
            col("source")
        )
)

# Confirm changes
display(fact_league_standing)

# Write data to silver layer
fact_league_standing.write \
    .format("delta") \
        .mode("append") \
            .option("path","abfss://silver@footballanalyticstorage.dfs.core.windows.net/fact_league_standing") \
                .saveAsTable("db_ws.silver.silver_fact_league_standing")
