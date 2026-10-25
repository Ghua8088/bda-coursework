import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
os.environ["TMPDIR"] = "C:/spark_temp"
os.environ["TEMP"] = "C:/spark_temp"
os.environ["TMP"] = "C:/spark_temp"
if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = os.path.expanduser("~/.jdk17")
    os.environ["PATH"] = f"{os.path.expanduser('~/.jdk17')}\\bin;" + os.environ.get("PATH", "")
os.makedirs("C:/spark_temp", exist_ok=True)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, size, array_intersect, array_union
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import urllib.request
import zipfile
import math

# Start Spark
spark = (
    SparkSession.builder.appName("Ex11_Classification")
    .config("spark.local.dir", "C:/spark_temp")
    .config(
        "spark.driver.extraJavaOptions",
        "-Djava.io.tmpdir=C:/spark_temp",
    )
    .getOrCreate()
)
print("Spark started successfully!")

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.0-bin-hadoop3"
"""## 2. Download and Load MovieLens Dataset
In the lab exam, dataset will be provided locally, so this cell can be ignored.
"""
'''
# Download MovieLens dataset
print("Downloading MovieLens dataset...")
url = "http://files.grouplens.org/datasets/movielens/ml-100k.zip"
urllib.request.urlretrieve(url, "ml-100k.zip")

# Extract the zip file
with zipfile.ZipFile("ml-100k.zip", 'r') as zip_ref:
    zip_ref.extractall()

print("Dataset downloaded and extracted!")
'''
"""We'll use the below method of loading the dataset for our exam"""

# Load ratings data
ratings = spark.read.csv("ml-100k/u.data", sep="\t",header=True, inferSchema=True).toDF("userId", "movieId", "rating", "timestamp")
ratings = ratings.drop("timestamp") # Remove timestamp as we don't need it

print(f"Loaded {ratings.count()} movie ratings")
ratings.show(5)

"""## 3. Prepare Data for Similarity Calculation"""

# Create user preference sets (movies with rating >= 3)
user_preferences = ratings.filter(col("rating") >= 3) \
    .groupBy("userId") \
    .agg(collect_set("movieId").alias("liked_movies"))

print("Created user preference sets for similarity calculation")
user_preferences.show(5)

"""## 4. Implement Jaccard Similarity with PySpark"""

00# Define Jaccard similarity function
def jaccard_similarity(set1, set2):
    if not set1 or not set2:
        return 0.0

    intersection = len(set(set1).intersection(set(set2)))
    union = len(set(set1).union(set(set2)))

    return float(intersection) / union if union > 0 else 0.0

# Register UDF for parallel execution
jaccard_udf = udf(jaccard_similarity, FloatType())

print("Jaccard similarity UDF created successfully!")

"""## 5. Calculate User Similarities using Parallel Processing"""

# Create user pairs and calculate Jaccard similarity in parallel
user_pairs = user_preferences.alias("u1").crossJoin(
    user_preferences.alias("u2")
).filter(col("u1.userId") < col("u2.userId"))  # Avoid duplicate pairs

# Calculate Jaccard similarities using PySpark for parallelization
similarities = user_pairs.select(
    col("u1.userId").alias("user1"),
    col("u2.userId").alias("user2"),
    jaccard_udf(col("u1.liked_movies"), col("u2.liked_movies")).alias("jaccard_similarity")
)

# Filter significant similarities
significant_similarities = similarities.filter(col("jaccard_similarity") > 0.2)

print("Calculated similarities using parallel processing:")
significant_similarities.orderBy(col("jaccard_similarity").desc()).show(5)

"""## 6. Implement Cosine Similarity with PySpark"""

# Define Cosine similarity function
def cosine_similarity(set1, set2):
    if not set1 or not set2:
        return 0.0

    intersection = len(set(set1).intersection(set(set2)))
    magnitude1 = math.sqrt(len(set1))
    magnitude2 = math.sqrt(len(set2))

    return float(intersection) / (magnitude1 * magnitude2) if magnitude1 * magnitude2 > 0 else 0.0

# Register Cosine UDF for parallel execution
cosine_udf = udf(cosine_similarity, FloatType())

# Calculate Cosine similarities in parallel
cosine_similarities = user_pairs.select(
    col("u1.userId").alias("user1"),
    col("u2.userId").alias("user2"),
    cosine_udf(col("u1.liked_movies"), col("u2.liked_movies")).alias("cosine_similarity")
)

# Filter significant cosine similarities
significant_cosine = cosine_similarities.filter(col("cosine_similarity") > 0.3)

print("Cosine similarity UDF created successfully!")
significant_cosine.orderBy(col("cosine_similarity").desc()).show(5)

"""## 7. Generate Recommendations"""

print("=== COLLABORATIVE FILTERING RESULTS ===")
print("✓ Implemented Jaccard and Cosine similarity measures")
print("✓ Used PySpark for parallel computation of similarities")
print("✓ Processed user preferences efficiently across multiple cores")

# Show sample similar users using Jaccard similarity
print("\nTop similar user pairs (Jaccard):")
significant_similarities.orderBy(col("jaccard_similarity").desc()).show(3)

print("\nTop similar user pairs (Cosine):")
significant_cosine.orderBy(col("cosine_similarity").desc()).show(3)

"""## 8. Clean Up"""

# Stop Spark
spark.stop()
print("Experiment completed successfully!")

