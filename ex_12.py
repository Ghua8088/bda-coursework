import os
import sys
if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = os.path.expanduser("~/.jdk17")
    os.environ["PATH"] = f"{os.path.expanduser('~/.jdk17')}/bin:" + os.environ.get("PATH", "")


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


spark = SparkSession.builder \
    .appName("Ex12_KMeans_Clustering") \
    .master("local[*]") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")
print(f"Spark Version: {spark.version}")
df = spark.read.csv("kmeans_dataset.csv", header=True, inferSchema=True)
print(f"Dataset Shape: ({df.count()}, {len(df.columns)})")
df.show(10)
feature_cols = ["Age", "Annual_Income", "Spending_Score", "Total_Purchases"]


# Vector Assembler
assembler = VectorAssembler(inputCols=feature_cols, outputCol="rawFeatures")
assembled_df = assembler.transform(df)


# Standard Scaler
scaler = StandardScaler(inputCol="rawFeatures", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(assembled_df)
scaled_df = scaler_model.transform(assembled_df)


scaled_df.select("CustomerID", "features").show(5, truncate=False)
k_values = list(range(2, 9))
silhouette_scores = []
cost_values = []


evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")


for k in k_values:
    kmeans = KMeans(featuresCol="features", k=k, seed=42)
    model = kmeans.fit(scaled_df)
    predictions = model.transform(scaled_df)
   
    score = evaluator.evaluate(predictions)
    cost = model.summary.trainingCost
   
    silhouette_scores.append(score)
    cost_values.append(cost)
    print(f"k = {k} | Silhouette Score = {score:.4f} | Training Cost (WSSSE) = {cost:.2f}")


# Plot Elbow and Silhouette Graphs
fig, ax1 = plt.subplots(figsize=(9, 4.5), dpi=150)
sns.set_theme(style="whitegrid")


color = 'tab:blue'
ax1.set_xlabel('Number of Clusters (k)', fontweight='bold')
ax1.set_ylabel('Training Cost (WSSSE)', color=color, fontweight='bold')
ax1.plot(k_values, cost_values, marker='o', color=color, linewidth=2)
ax1.tick_params(axis='y', labelcolor=color)


ax2 = ax1.twinx()
color = 'tab:green'
ax2.set_ylabel('Silhouette Score', color=color, fontweight='bold')
ax2.plot(k_values, silhouette_scores, marker='s', color=color, linewidth=2, linestyle='--')
ax2.tick_params(axis='y', labelcolor=color)


plt.title('Optimal k Evaluation: Elbow Method vs. Silhouette Score', fontweight='bold', pad=12)
plt.tight_layout()
plt.show()
optimal_k = 5
kmeans_final = KMeans(featuresCol="features", k=optimal_k, seed=42)
model_final = kmeans_final.fit(scaled_df)


# Predict clusters
clustered_df = model_final.transform(scaled_df)
clustered_df.select("CustomerID", "Age", "Annual_Income", "Spending_Score", "prediction").show(10)


# Cluster Counts
clustered_df.groupBy("prediction").count().orderBy("prediction").show()
# Convert to Pandas for visualization
pdf = clustered_df.select("CustomerID", "Age", "Annual_Income", "Spending_Score", "Total_Purchases", "prediction").toPandas()
pdf.rename(columns={"prediction": "Cluster"}, inplace=True)


# 2D Scatter Plot: Annual Income vs Spending Score
plt.figure(figsize=(10, 6), dpi=150)
sns.set_theme(style="whitegrid")


scatter = sns.scatterplot(
    data=pdf,
    x="Annual_Income",
    y="Spending_Score",
    hue="Cluster",
    palette="tab10",
    s=80,
    alpha=0.85,
    edgecolor="k"
)


plt.title(f"PySpark K-Means Clustering (k={optimal_k}): Customer Segmentation", fontsize=13, fontweight='bold', pad=12)
plt.xlabel("Annual Income ($k)", fontweight='bold')
plt.ylabel("Spending Score (1-100)", fontweight='bold')
plt.legend(title="Cluster", loc="upper right", frameon=True)
plt.tight_layout()
plt.show()


# Summary statistics per cluster
cluster_profile = pdf.groupby("Cluster")[["Age", "Annual_Income", "Spending_Score", "Total_Purchases"]].mean().round(2)
display(cluster_profile)
