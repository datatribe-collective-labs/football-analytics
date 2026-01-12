<div align="center">
  <img src=" https://github.com/datatribe-collective-labs/football-analytics/blob/main/Az%20data%20factory%20pipeline.png?raw=true" />
  <br>
   <sub><b>Fig 1.</b> Workflow</sub>
</div>


The project focuses on a composite analytics pipeline for football events in the English Premier League for the 2025/2026 season. It started with the creation of a data lake designed to ingest batch data every week, since league matches are played every weekend.

To support this, we built a parameterized pipeline in Azure Data Factory to extract data from the Football-Data API (https://www.football-data.org/) and load it into Azure Data Lake Storage. The pipeline makes use of Lookup, ForEach, and Copy activities.

First, we uploaded a JSON configuration file to the data lake. This file contains an array of objects, each defining the relative API endpoint, the destination folder, and the destination file name for a specific dataset.

The Lookup activity reads this JSON file to determine the extraction parameters and passes this information to the ForEach activity. The ForEach activity then iterates over each object in the array and dynamically drives the Copy activity, ensuring that data is extracted and stored in a structured manner.

As a result, the pipeline separates the incoming data into dedicated folders for football clubs' information, players' information, league standings, top scorers, and team data.
