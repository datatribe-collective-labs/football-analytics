## Implementing Football Analytics Pipeline Using Azure, Databricks, Delta Lake, Databricks AI/BI, and ML for SWIFT Insights.
<!-- <br> -->

## Problem Statement
Football leagues evolve every week, with each matchday determining league positions, manager performance views, and players' impact. However, stakeholders often rely on fragmented reports which often take time and fail to capture how performance changes week by week and what drives those changes in one piece.
This creates a challenge for decision-makers who need to **speedily** assess:
- how league standings shift after each weekend,
- whether managers are maintaining or improving their win effectiveness over time,
- which players are consistently contributing through goals and assists,
- and how team scoring intensity develops across the season.

This project presents an end-to-end data engineering & analytics solution for **SWIFT** insights into football events in the English Premier League during the 2025/2026 season. It provides weekly updates on league standings, manager win rates, player performance metrics, and team scoring efficiency through automated triggers.

#### Data Extraction & loading Pipeline.
<div align="center">
  <img src="https://github.com/datatribe-collective-labs/football-analytics/blob/main/images/Az%20data%20factory%20pipeline.png?raw=true" />
  <br>
   <sub><b>Data Factory Pipeline</b></sub>
</div>
<br>


In line with preparing a composite analytics pipeline for football events in the English Premier League for the 2025/2026 season, we started with the creation of a data lake designed to ingest batch data every week, since league matches are played every weekend.

To support this, we built a parameterized pipeline in Azure Data Factory to extract data from the Football-Data API (https://www.football-data.org/) and load it into Azure Data Lake Storage. The pipeline makes use of Lookup, ForEach, and Copy activities.

First, we uploaded a JSON configuration file to the data lake. This file contains an array of objects, each defining the relative API endpoint, the destination folder, and the destination file name for a specific dataset.


The Lookup activity reads this JSON file to determine the extraction parameters and passes this information to the ForEach activity. The ForEach activity then iterates over each object in the array and dynamically drives the Copy activity, ensuring that data is extracted and stored in a structured manner.

As a result, the pipeline separates the incoming data into dedicated folders for football clubs' information, players' information, league standings, top scorers, and teams' data.

<div align="center">
  <img src="https://github.com/datatribe-collective-labs/football-analytics/blob/main/images/pipeline_run.png?raw=true" />
  <br>
   <sub><b>Medallion Architectural Pipeline</b></sub>
</div>


#### Medallion Architecture using the Inmon Data Warehousing Model & Galaxy Schema
The Inmon data model is a top-down approach to data warehousing that builds on a centralized enterprise data warehouse, which forms the basis for subsequent data marts.

The project continues with the implementation of a medallion architecture using the Inmon data model on Azure Databricks for analytics-wide data consistency and integrity. In the Bronze layer, raw data is ingested in its native JSON format, transformed into tabular structures, partitioned, and appended to Delta Lake tables.

The Silver layer extends the workflow with more extensive data processing. This includes renaming columns, refining column name characters, dropping unnecessary fields, removing duplicates, and writing the cleaned and standardized datasets back to the Delta Lake.

The Gold layer introduces Slowly Changing Dimensions to model historical and analytical requirements. Football club information and season data do not require historical tracking, so Slowly Changing Dimension Type 1 was applied to these datasets.
For players and managers, historical tracking is essential, as player performance evolves over time, and managers can be replaced mid-season. For this reason, Slowly Changing Dimension Type 2 was implemented. These tables introduce surrogate keys to handle record versioning and to prevent duplication issues that typically arise from expanding SCD Type 2 tables.

In addition to the dimension tables, the project includes two fact tables in a galaxy schema. These were optimized through broadcast joins with the relevant dimension tables to efficiently track overall club performance across the duration of the league, as well as individual player metrics.

Before being written to the Gold layer in Delta Lake, the final datasets were optimized using "coalesce" to reduce the number of output partitions. This was considered to help minimize small files, produce BI-friendly file sizes, and improve query performance for downstream analytics workloads.

Ultimately, the entire pipeline is scheduled to run every week to stay aligned with the latest English Premier League match results.

#### Weekly Performance Dashboard After 24 Games (2nd Fedruary, 2026).

Upon refresh, the following dashboard provides a **swift** weekly updated view of league, manager, and player performance, enabling stakeholders to track momentum, identify performance drivers, and make informed decisions throughout the season.

<div align="center">
  <img src="https://github.com/datatribe-collective-labs/football-analytics/blob/main/images/report_2.png?raw=true" />
  <br>
   <sub><b>Weekly performance insights</b></sub>
</div>
<br>

### Explainable Performance segmentation Analysis After 24 Games (2nd Fedruary, 2026)

Though the dashboard shows weekly insights, but there’s also a need to generate insights into teams’ segmentation by performance. 
This project applies K-Means clustering to partition league performance data into distinct segments based on per-game performance metrics.

In this context, K-Means is used as a distance-based unsupervised learning algorithm to group teams by minimizing their Euclidean distances. Feature scaling is applied before modeling to ensure that metrics measured on different scales contribute equally to the clustering process. But, while K-Means performs well on structured numerical data, its results can become harder to interpret as dimensionality increases.
To address this, Principal Component Analysis is introduced as a dimensionality reduction and visualization mechanism to learn about teams’ characteristic similarities and cluster separations.

Cluster explainability is achieved by analyzing cluster centroids, which represents the average performance profile for each football club, culminating in insights into points per game, win rate, and goal difference.

Ultimately, each cluster is mapped to human-readable performance labels for insights into teams’ segmentation by performance.


<table align="center">
  <tr>
    <td align="center">
      <img src="https://github.com/datatribe-collective-labs/football-analytics/blob/main/images/elbow.png?raw=true" height="300"><br>
      <sub><b>Elbow method defining number of clusters</b>b</sub>
    </td>
    <td align="center">
      <img src="https://github.com/datatribe-collective-labs/football-analytics/blob/main/images/pca.png?raw=true" height="300"><br>
      <sub><b>Performance segmentation</b>b</sub>
    </td>
  </tr>
</table>

<div align="center">
  <img src="https://github.com/datatribe-collective-labs/football-analytics/blob/main/images/segmentation%20explainability.png?raw=true" />
  <br>
   <sub><b>Cluster Segmentation Explainability</b></sub>
</div>
<br>
