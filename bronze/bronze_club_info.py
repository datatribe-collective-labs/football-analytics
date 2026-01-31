# Import dependencies
from pyspark.sql.functions import explode, col, collect_list, struct, array_distinct, current_timestamp

# Load the clubs' raw data from the data lake
raw_club_info = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/club_info"
)

# Show data
display(raw_club_info)

# Flatten teams array to get the running competitions
teams_info = raw_club_info.withColumn("team", explode(col("teams")))

# Flatten the running competitions
retrieved_team_info = teams_info.withColumn("competition", explode(col("team.runningCompetitions")))

# Extract relevant features
bronze_clubs_info = (
    retrieved_team_info
    .groupBy(
        col("team.id").alias("id"),
        col("team.name").alias("name"),
        col("team.shortName").alias("shortName"),
        col("team.tla").alias("TLA"),
        col("team.crest").alias("crest"),
        col("team.address").alias("address"),
        col("team.website").alias("website"),
        col("team.founded").alias("founded"),
        col("team.clubColors").alias("clubColors"),
        col("team.venue").alias("venue")
    )
    .agg(
        array_distinct(collect_list(col("competition.name"))).alias("runningCompetitions")
    )
    .withColumn("ingestion_ts", current_timestamp())
)


# Dicplay clus data
display(bronze_clubs_info)

# Write clubs data to the data lake's bronze layer
bronze_clubs_info.write \
  .format("delta") \
    .mode("append") \
      .partitionBy("ingestion_ts") \
        .option("path","abfss://bronze@footballanalyticstorage.dfs.core.windows.net/clubs_info") \
            .saveAsTable("db_ws.bronze.club_info")
