# Import dependencies
from pyspark.sql.functions import col, lit, current_timestamp, when, lit, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Load data from the delta lake
silver_season = spark.read \
  .format("delta") \
      .load("abfss://silver@footballanalyticstorage.dfs.core.windows.net/dim_season")

# Show schema
silver_season.printSchema()

# Create delta table for season
%sql
CREATE TABLE IF NOT EXISTS dim_season (
  season_id INT,
  season_name STRING,
  season_year INT,
  season_start_date DATE,
  season_end_date DATE,
  current_matchday LONG,
  winner STRING,
  source STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
-- LOCATION " "

# Remove duplicate rows
retrieved_row = Window.partitionBy("season_id").orderBy(col("injested_ts").desc())

silver_season_dedup = (
    silver_season
    .withColumn("rn", row_number().over(retrieved_row))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Implement SCD Type-1 for the season table
dim_season = DeltaTable.forName(spark, "dim_season")
(
    dim_season.alias("target")
    .merge(
        silver_season_dedup.alias("source"),
        "target.season_id = source.season_id"
    )
    .whenMatchedUpdate(
        set={
            "season_name": "source.season_name",
            "season_year": "source.season_year",
            "season_start_date": "source.season_start_date",
            "season_end_date": "source.season_end_date",
            "current_matchday": "source.current_matchday",
            "winner": "source.winner",
            "source": "source.source",
            "updated_at": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "season_id": "source.season_id",
            "season_name": "source.season_name",
            "season_year": "source.season_year",
            "season_start_date": "source.season_start_date",
            "season_end_date": "source.season_end_date",
            "current_matchday": "source.current_matchday",
            "winner": "source.winner",
            "source": "source.source",
            "created_at": "current_timestamp()",
        }
    )
    .execute()
)

Test for SCD-Type-1
silver_season = (
    silver_season
    .withColumn(
        "season_year",
        when(
            col("season_id") == "2403",
            lit("2026")
        ).otherwise(col("season_year")
        )
    )
)

# Confirm changes
display(silver_season)

             # Write the data to delta lake of the season's gold layer
dim_season.toDF().coalesce(1) \
    .write \
        .format("delta") \
           .mode("overwrite") \
               .option("path","abfss://gold@footballanalyticstorage.dfs.core.windows.net/dim_season") \
                    .saveAsTable("db_ws.gold.dim_season")
