# Import dependencies
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

# Create table
%sql
CREATE OR REPLACE TABLE db_ws.gold_analytics.ml_performance_clustering AS
WITH ranked_clubs AS (
  SELECT
    l.club_id AS club_id,
    c.club_abbreviation AS club_abbreviation,
    CAST(ROUND(l.points * 1.0 / l.total_games_played, 2) AS DOUBLE) AS points_per_game,
    CAST(ROUND(l.goal_difference * 1.0 / l.total_games_played, 2) AS DOUBLE) AS goals_difference_per_game,
    CAST(ROUND(l.total_goals_scored * 1.0 / l.total_games_played, 2) AS DOUBLE) AS goals_per_game,
    CAST(ROUND(l.total_goals_conceded * 1.0 / l.total_games_played, 2) AS DOUBLE) AS goals_conceded_per_game,
    CAST(ROUND(l.won * 1.0 / l.total_games_played, 2) AS DOUBLE) AS win_rate,
    ROW_NUMBER() OVER (
      PARTITION BY l.club_id
      ORDER BY l.updated_at DESC
    ) AS rn
  FROM 
    db_ws.gold.fact_league_standing l
  JOIN
    db_ws.gold.dim_football_clubs c
  ON
    l.club_id = c.club_id
)

SELECT
  club_id,
  club_abbreviation,
  points_per_game,
  goals_difference_per_game,
  goals_per_game,
  goals_conceded_per_game,
  win_rate
FROM ranked_clubs
WHERE rn = 1;

# Read spark dataframe and convert into pandas dataframe
clusering_df = spark.table("db_ws.gold_analytics.ml_performance_clustering").toPandas()

# Display table
clusering_df.head()

# Define input features for clustering
features = [
    "points_per_game",
    "goals_difference_per_game",
    "goals_per_game",
    "goals_conceded_per_game",
    "win_rate"
]

# Create an instance of standard scaling utility
scaler = StandardScaler()
# standardize the data into normal distribution
scaled_features = scaler.fit_transform(clusering_df[features])
# Visualize the now normally distributed features
scaled_features_df = pd.DataFrame(scaled_features, columns=features)
# See the first 5 rows of those features
scaled_features_df.head()

# Define a list to absorb the within cluster sum of squares(inertia)
wcss = []
# Iterate through a range of k values and append the inertia to the list
for k in range(1, 11):
    # Create an instance of KMeans with the current k value as random state for reproducability
    kmeans = KMeans(n_clusters=k, random_state=42)
    # Fit the model to the normalized features
    kmeans.fit(scaled_features_df)
    # Append the inertia to the list
    wcss.append(kmeans.inertia_)

# Plot the inertia values against the number of clusters
plt.figure(figsize=(8, 5))

plt.plot(range(1, 11), wcss, marker='o')
# Label the x-axis
plt.xlabel('Number of Clusters (k)')
# Label the y-axis
plt.ylabel('Inertia')
# Create a title
plt.title('Elbow Method for Optimal k')
# Display the plot showing the elbow point
plt.show()

# SInce the plot shows an elbow at k=3, enter 3 for the number of clusters
kmeans = KMeans(n_clusters=3, random_state=42)
# Train the model on the normalized features
clusering_df["performance_segment"] = kmeans.fit_predict(scaled_features_df)
clusering_df.head(3)

# Count the number of teams in each segment
clusering_df.groupby("performance_segment")["club_abbreviation"].count()

# Kmeans is a distance based algorithm that depends on the euclidean distance between the normalized to group the population, but potentially struggles with high dimensional data.
# PCA is intriduced as a dimensionality reduction technique to overcome this limitation and also visualize the data in 2 dimensions

# Perform PCA on the normalized features
pca = PCA(n_components=2)
# Fit the PCA model to the normalized features
X_pca = pca.fit_transform(scaled_features_df)

# Add the PCA components to the DataFrame
clusering_df["pca_1"] = X_pca[:, 0]
clusering_df["pca_2"] = X_pca[:, 1]
# Confirm the addition of the PCA components
clusering_df.head(3)


# Plot the clusters
plt.figure(figsize=(10, 7))

# Iterate through each cluster
for each_performance in clusering_df["performance_segment"].unique():
    # Filter the DataFrame for the current cluster
    segment_subset = clusering_df[clusering_df["performance_segment"] == each_performance]
    # Plot the cluster
    plt.scatter(
        segment_subset["pca_1"],
        segment_subset["pca_2"],
        label=f"Cluster {each_performance}",
        alpha=0.7
    )

# Add labels to each cluster point
for index, row in clusering_df.iterrows():
    plt.text(
        row["pca_1"],
        row["pca_2"],
        str(row["club_abbreviation"]),
        fontsize=8,
        alpha=0.6
    )

# Add labels and title
plt.xlabel("PCA Component 1")
plt.ylabel("PCA Component 2")
plt.title("Club Performance Clusters (KMeans, k=3)")
# Add a legend
plt.legend()
plt.grid(True)
# Display the plot
plt.show()

# Model explainability
# Create a DataFrame to show the average feature importance for each cluster
centroids = pd.DataFrame(
    kmeans.cluster_centers_,
    columns=features
)

centroids["cluster"] = centroids.index
centroids

# Feature importance of each cluster
feature_separation = centroids[features].var().sort_values(ascending=False)
feature_separation

# Create a dictionary to map performance segments to labels
cluster_labels = {
    0: "Inconsistent",
    1: "Relegation Risk",
    2: "Contenders"
}
# Add a new column to the DataFrame with the mapped labels
clusering_df["cluster_label"] = clusering_df["performance_segment"].map(cluster_labels)

# Display the DataFrame for the top 5 rows with the cluster labels added
clusering_df.head()
