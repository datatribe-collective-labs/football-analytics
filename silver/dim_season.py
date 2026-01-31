# Import dependencies
from pyspark.sql.functions import regexp_replace, trim, lower, split, array_distinct, col, lit, current_timestamp, sha2, concat_ws, to_date

# Read in the silver season data
retrieve_season_data = spark.read \
    .format("delta") \
        .load("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/season_info")

# SHow the data
display(retrieve_season_data)

# Refine data
dim_season = (
    retrieve_season_data
        .withColumn("season_start_date", to_date(col("season_start_date")))
        .withColumn("season_end_date", to_date(col("season_end_date")))
        .withColumn("season_year", col("season_year").cast("int"))
        .withColumn("injested_ts", current_timestamp())
        .withColumn("source", lit("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/season_info"))
)

# Confirm changes
display(dim_season)

# Write data to silver layer
dim_season.write \
    .format("delta") \
        .mode("append") \
            .option("path","abfss://silver@footballanalyticstorage.dfs.core.windows.net/dim_season") \
                .saveAsTable("db_ws.silver.silver_dim_season_info")
