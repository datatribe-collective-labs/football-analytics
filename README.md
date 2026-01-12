## Implementing Football Analytics Pipeline Using Azure, Databricks, and Delta Lake
<!-- <br> -->

This project presents an end-to-end data engineering solution for analyzing football events in the English Premier League during the 2025/2026 season. The goal is to design a scalable, automated, and analytics-ready data platform that ingests weekly football activities data and transforms it to support analysis over the course of the season.

#### Data Extraction & loading Pipeline.
<div align="center">
  <img src="https://github.com/datatribe-collective-labs/football-analytics/blob/main/Az%20data%20factory%20pipeline.png?raw=true" />
  <br>
   <sub><b>Data Factory Pipeline</b></sub>
</div>
<br>


In our quest to prepare a composite analytics pipeline for football events in the English Premier League for the 2025/2026 season, we started with the creation of a data lake designed to ingest batch data every week, since league matches are played every weekend.

To support this, we built a parameterized pipeline in Azure Data Factory to extract data from the Football-Data API (https://www.football-data.org/) and load it into Azure Data Lake Storage. The pipeline makes use of Lookup, ForEach, and Copy activities.

First, we uploaded a JSON configuration file to the data lake. This file contains an array of objects, each defining the relative API endpoint, the destination folder, and the destination file name for a specific dataset.


The Lookup activity reads this JSON file to determine the extraction parameters and passes this information to the ForEach activity. The ForEach activity then iterates over each object in the array and dynamically drives the Copy activity, ensuring that data is extracted and stored in a structured manner.

As a result, the pipeline separates the incoming data into dedicated folders for football clubs' information, players' information, league standings, top scorers, and team data.

<div align="center">
  <img src="https://github.com/datatribe-collective-labs/football-analytics/blob/main/Databricks%20Pipeline.png?raw=true" />
  <br>
   <sub><b>Medallion Architectural Pipeline</b></sub>
</div>


#### Meddallion Architecture using the Inman data Model
The project continues with the implementation of a medallion architecture using the Inman data model on Azure Databricks for analytics-wide data consistency and integrity. In the Bronze layer, raw data is ingested in its native JSON format, transformed into tabular structures, partitioned, and appended to Delta Lake tables.

The Silver layer extends the workflow with more extensive data processing. This includes renaming columns, refining column name characters, dropping unnecessary fields, removing duplicates, and writing the cleaned and standardized datasets back to the Delta Lake.

The Gold layer introduces Slowly Changing Dimensions to model historical and analytical requirements. Football club information and season data do not require historical tracking, so Slowly Changing Dimension Type 1 was applied to these datasets.
For players and managers, historical tracking is essential, as player performance evolves over time, and managers can be replaced mid-season. For this reason, Slowly Changing Dimension Type 2 was implemented. These tables introduce surrogate keys to handle record versioning and to prevent duplication issues that typically arise from expanding SCD Type 2 tables.

In addition to the dimension tables, the project includes two fact tables. These were optimized through broadcast joins with the relevant dimension tables to efficiently track overall club performance across the duration of the league, as well as individual player metrics.

Before being written to the Gold layer in Delta Lake, the final datasets were optimized using coalesce to reduce the number of output partitions. This was considered to help minimize small files, produce BI-friendly file sizes, and improve query performance for downstream analytics workloads.

Ultimately, the entire pipeline is scheduled to run every week to stay aligned with the latest English Premier League match results.
