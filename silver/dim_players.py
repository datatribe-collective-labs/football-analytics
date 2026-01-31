# Import dependencies
from pyspark.sql.functions import regexp_replace, trim, lower, split, array_distinct, col, to_date, regexp_replace, concat_ws, sha2, lit, current_timestamp

# Read data from bronze layer
retrieved_players = spark.read \
    .format("delta") \
        .load("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/players_details")


# SHow retrieved data
display(retrieved_players)

# Refine data
dim_players = (
    retrieved_players
    # Refine attributes
    .withColumnRenamed("id", "player_id")
    .withColumnRenamed("club_id", "club_id")
    .withColumn("player_name", lower(trim(col("player_name"))))
    .withColumn("position", lower(trim(col("position"))))
    # Rid the position of needlass characters
    .withColumn(
        "position",
        regexp_replace(
            col("position"),
            r"[-]",
            " "
        )
    )
    .withColumnRenamed("nationality","player_nationality")
    .withColumn("player_nationality", lower(trim(col("player_nationality"))))
    .withColumn("date_of_birth", to_date(col("date_of_birth")))
   
    # Features to track SCD
    .withColumn("valid_from", lit(current_timestamp()))
    .withColumn("valid_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))

    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source", lit("bronze_league_standing"))

    .withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                col("player_id"),
                col("club_id"),
                col("player_name"),
                col("position"),
                col("date_of_birth"),
                col("player_nationality"),
            ),
            256
            )
    )

    # Remove unnecessary fields
    .drop("football_club", "club_name")

    # Deduplicate safely
    .dropDuplicates(["player_id"])
    .select("player_id", "club_id", "player_name", "position", "date_of_birth", "player_nationality", "valid_from", "valid_to", "is_current", "ingestion_ts", "source", "record_hash")
)


# Confirm changes
display(dim_players)

# Write data to silver layer
dim_players.write \
    .format("delta") \
        .mode("overwrite") \
            .option("path","abfss://silver@footballanalyticstorage.dfs.core.windows.net/dim_players") \
                .saveAsTable("db_ws.silver.silver_dim_players")
