# Big Data Analytics Lab Exam Revision Guide (All 12 Exercises)

---

## Quick Navigation Index

|  Ex #  | Topic                                                                                  | Tool / Framework          | Key Concept                                                      |
| :----: | :------------------------------------------------------------------------------------- | :------------------------ | :--------------------------------------------------------------- |
| **1**  | [Word Count](#ex-1-word-count-mapreduce)                                               | Hadoop Streaming / Python | Basic MapReduce, key-value sorting                               |
| **2**  | [Weather Data Analysis](#ex-2-weather-data-analysis-mapreduce)                         | Hadoop Streaming / Python | Min/Max aggregation over CSV data                                |
| **3**  | [Bloom Filter](#ex-3-bloom-filter-for-duplicate-detection)                             | Python / Hashlib          | Probabilistic membership, zero false negatives                   |
| **4**  | [Flajolet-Martin (FM) Algorithm](#ex-4-flajolet-martin-fm-stream-mining)               | Python / Stream Mining    | Distinct elements count via trailing zeros: $\frac{2^R}{0.7735}$ |
| **5**  | [PageRank Algorithm](#ex-5-pagerank-algorithm-mapreduce)                               | Hadoop Streaming / Python | Graph link analysis with Damping factor $\beta$                  |
| **6**  | [Girvan-Newman Community Detection](#ex-6-girvan-newman-algorithm-community-detection) | Python / NetworkX         | Edge betweenness centrality & Modularity ($Q$)                   |
| **7**  | [Apache Hive (HQL)](#ex-7-apache-hive-hql-data-warehousing)                            | Hive CLI / HQL            | Schema-on-read, DDL/DML, joins, aggregations                     |
| **8**  | [Apache HBase (NoSQL)](#ex-8-apache-hbase-shell-crud)                                  | HBase Shell               | Column-family store, `put`, `get`, `scan`, `deleteall`           |
| **9**  | [Apache Pig (Pig Latin)](#ex-9-apache-pig-pig-latin)                                   | Pig CLI (`pig -x local`)  | Data flow ETL, `LOAD`, `FILTER`, `GROUP`, `DUMP`                 |
| **10** | [Collaborative Filtering](#ex-10-collaborative-filtering-pyspark)                      | PySpark SQL & UDFs        | Jaccard vs. Cosine similarity for recommendation                 |
| **11** | [Classification Models](#ex-11-machine-learning-classification-pyspark-mllib)          | PySpark MLlib             | `VectorAssembler`, `StandardScaler`, LR, SVM, DT                 |
| **12** | [K-Means Clustering](#ex-12-k-means-clustering-pyspark-mllib)                          | PySpark MLlib             | Unsupervised segmentation, Elbow curve, Silhouette               |

---

## Ex 1: Word Count (MapReduce)

### 1. Aim

Count the frequency of each unique word across an input text dataset using Hadoop MapReduce in Python.

### 2. Core Concepts

- **Mapper phase**: Tokenizes lines into individual words, emitting `(word, 1)`.
- **Shuffle & Sort phase** (handled by Hadoop): Groups identical keys together.
- **Reducer phase**: Streams through sorted `(word, count)` lines, accumulating counts while the word matches `prev`. Prints `prev curr` when a new word is encountered.

### 3. Code Skeleton

**`mapper.py`**

```python
import sys

for line in sys.stdin:
    for word in line.strip().split():
        print(f"{word} 1")
```

**`reducer.py`**

```python
import sys

prev, curr = None, 0
for line in sys.stdin:
    word, count = line.strip().lower().split()
    count = int(count)
    if word == prev:
        curr += count
    else:
        if prev is not None:
            print(f"{prev} {curr}")
        prev = word
        curr = count
if prev is not None:
    print(f"{prev} {curr}")
```

### 4. How to Test / Run

```bash
# Local terminal simulation (using pipe & sort):
cat input.txt | python mapper.py | sort | python reducer.py
```

### 5. Exam / Viva Tips

- **Why sort before reducer?** Reducer assumes sorted input so it can aggregate in a single $O(N)$ streaming pass without loading all keys into memory.

---

## Ex 2: Weather Data Analysis (MapReduce)

### 1. Aim

Analyze a weather CSV dataset (`month,min_temp,max_temp`) and find the overall minimum temperature (or min temperature per month).

### 2. Core Logic

- **Mapper**: Extracts month and target metric column (e.g., `min_temp`), strips headers/whitespace, and outputs `month min_temp`.
- **Reducer**: Finds the minimum value by comparing incoming temperatures against a running minimum initialized to `float('inf')`.

### 3. Code Skeleton

**`mapper.py`**

```python
import sys

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) == 3:
        month, min_t, max_t = parts
        print(f"{month} {min_t}")
```

**`reducer.py`**

```python
import sys

min_temp = float('inf')
min_month = None

for line in sys.stdin:
    month, temp = line.strip().split()
    temp = float(temp)
    if temp < min_temp:
        min_temp = temp
        min_month = month

print(f"{min_month} {min_temp}")
```

### 4. Exam / Viva Tips

- If asked to find **maximum**, initialize `max_temp = -float('inf')` and use `>` comparison.
- If asked **per month**, maintain the `prev_month` state pattern from Ex 1.

---

## Ex 3: Bloom Filter for Duplicate Detection

### 1. Aim

Detect duplicate entries (e.g., user IDs) in a streaming or batch dataset using a fixed-size probabilistic bit array.

### 2. Core Concepts & Properties

- **Bit Array**: An array of size $m$ initialized to `0`.
- **Hash Functions**: $k$ independent hash functions (e.g., MD5, SHA-256, SHA-1) mapped to $[0, m-1]$ via modulo $m$.
- **Membership Test**:
  - If **all** $k$ bits are `1` $\to$ Element is **PROBABLY DUPLICATE** (may be a false positive).
  - If **any** bit is `0` $\to$ Element is **DEFINITELY UNIQUE** (sets bits to `1`).
- **Guarantee**: **No False Negatives** (never misses an unseen element).

### 3. Code Skeleton

**`mapper.py`**

```python
import sys, hashlib

bloomsize = 10000
bloomfilter = [0] * bloomsize

def hash1(val): return int(hashlib.md5(val.encode()).hexdigest(), 16) % bloomsize
def hash2(val): return int(hashlib.sha256(val.encode()).hexdigest(), 16) % bloomsize
def hash3(val): return int(hashlib.sha1(val.encode()).hexdigest(), 16) % bloomsize

for line in sys.stdin:
    uid = line.strip().lower().split(",")[0]
    h1, h2, h3 = hash1(uid), hash2(uid), hash3(uid)
    if bloomfilter[h1] == bloomfilter[h2] == bloomfilter[h3] == 1:
        print(f"{uid} DUPLICATE")
    else:
        print(f"{uid} UNIQUE")
        bloomfilter[h1] = bloomfilter[h2] = bloomfilter[h3] = 1
```

**`reducer.py`**

```python
import sys

dupes, uniques = [], []
for line in sys.stdin:
    uid, status = line.strip().split()
    if status == "UNIQUE":
        uniques.append(uid)
    else:
        dupes.append(uid)

print(f"Unique: {','.join(uniques)}")
print(f"Duplicates: {','.join(dupes)}")
```

### 4. Exam / Viva Tips

- **Trade-off**: Uses tiny memory space $O(m)$ at the cost of a controllable false positive rate $p \approx (1 - e^{-kn/m})^k$.

---

## Ex 4: Flajolet-Martin (FM) Stream Mining

### 1. Aim

Estimate the number of distinct elements in a massive data stream using the Flajolet-Martin algorithm.

### 2. Formula & Mathematics

- Hash each element $x$ to an integer and express it as a binary string.
- Count the number of **trailing zeros** $r(x)$ in the binary representation.
- Track the maximum trailing zeros observed: $R = \max(r(x))$.
- **Distinct Elements Estimate**:
  $$\text{Distinct Count} = \frac{2^R}{\phi} \quad \text{where } \phi \approx 0.7735$$

### 3. Code Skeleton

**`mapper.py`**

```python
import sys, hashlib

for line in sys.stdin:
    uid = line.strip().split(",")[0]
    h = int(hashlib.sha256(uid.encode()).hexdigest(), 16)
    bin_h = bin(h)[2:]

    # Count trailing zeros
    trailing_zeros = 0
    for bit in bin_h[::-1]:
        if bit == '0':
            trailing_zeros += 1
        else:
            break
    print(f"{uid} {trailing_zeros}")
```

**`reducer.py`**

```python
import sys

max_r = -float('inf')
for line in sys.stdin:
    _, r = line.strip().split()
    max_r = max(max_r, float(r))

estimate = (2 ** max_r) / 0.7735
print(f"Max trailing zeros (R): {max_r}")
print(f"Unique values in stream: {round(estimate)}")
```

### 4. Exam / Viva Tips

- Why $2^R$? If there are $N$ unique elements, observing $R$ trailing zeros occurs with probability $2^{-(R+1)}$. The max $R$ scales logarithmically with $N$.
- What is 0.7735? It is the correction factor derived by Flajolet & Martin to remove estimation bias.

---

## Ex 5: PageRank Algorithm (MapReduce)

### 1. Aim

Compute the PageRank of web pages in a directed web graph using iterative MapReduce passes.

### 2. Formula

For a page $p$, with damping factor $\beta = 0.8$ and total nodes $N$:
$$PR(p) = \beta \sum_{q \in \text{In}(p)} \frac{PR(q)}{|\text{Out}(q)|} + \frac{1 - \beta}{N}$$

### 3. Input & Output Format

- Input row: `source_node current_rank target_node1 target_node2 ...`
  - Example: `A 1.0 B C` (Page A has rank 1.0 and links to B and C).

### 4. Code Skeleton

**`mapper.py`**

```python
import sys

for line in sys.stdin:
    parts = line.strip().split()
    source, rank, adj = parts[0], float(parts[1]), parts[2:]

    # Preserve graph topology
    print(f"{source} STRUCT {' '.join(adj)}")

    # Distribute rank share to neighbors
    if len(adj) > 0:
        contrib = rank / len(adj)
        for node in adj:
            print(f"{node} VALUE {contrib}")
```

**`reducer.py`**

```python
import sys

BETA, N = 0.8, 10
c_page, rank_sum, links = None, 0.0, []

for line in sys.stdin:
    page, record_type, *vals = line.strip().split()

    if c_page != page:
        if c_page is not None:
            new_rank = (BETA * rank_sum) + ((1 - BETA) / N)
            print(f"{c_page} {new_rank} {' '.join(links)}")
        c_page, rank_sum, links = page, 0.0, []

    if record_type == "STRUCT":
        links.extend(vals)
    elif record_type == "VALUE":
        rank_sum += float(vals[0])

if c_page is not None:
    new_rank = (BETA * rank_sum) + ((1 - BETA) / N)
    print(f"{c_page} {new_rank} {' '.join(links)}")
```

### 5. Exam / Viva Tips

- **Why the STRUCT record?** MapReduce has no shared memory; emitting `STRUCT` passes the adjacency list to the next iteration.
- **Why Damping Factor $\beta$?** Solves **Spider Traps** (cycles holding rank) and **Dead Ends** (pages with 0 out-links leaking rank).

---

## Ex 6: Girvan-Newman Algorithm (Community Detection)

### 1. Aim

Detect natural community clusters in a social network using edge betweenness centrality and modularity optimization.

### 2. Steps of the Algorithm

1. Compute **Edge Betweenness Centrality** (fraction of all shortest paths passing through each edge) for all edges in the network.
2. Remove the edge with the **highest** edge betweenness centrality.
3. Identify connected components (candidate communities) and compute **Modularity ($Q$)**.
4. Repeat until no edges remain.
5. Select the partition that achieved the **highest peak modularity score**.

### 3. Key Python Code (`networkx`)

```python
import networkx as nx
from networkx.algorithms.community import modularity

G = nx.relaxed_caveman_graph(l=3, k=5, p=0.15, seed=42)

def edge_to_remove(graph):
    edge_dict = nx.edge_betweenness_centrality(graph)
    return max(edge_dict.items(), key=lambda x: x[1])[0]

def automated_girvan_newman(graph):
    orig = graph.copy()
    working = graph.copy()
    best_modularity = -1
    best_communities = []

    while working.number_of_edges() > 0:
        edge = edge_to_remove(working)
        working.remove_edge(*edge)
        communities = tuple(nx.connected_components(working))

        if len(communities) > 1:
            q = modularity(orig, communities)
            if q > best_modularity:
                best_modularity = q
                best_communities = communities

    return best_communities
```

### 4. Exam / Viva Tips

- **What is Modularity ($Q$)?** Measures the density of links inside communities compared to links between communities. Range: $[-0.5, 1.0]$. Values above $0.3-0.7$ indicate strong community structure.

---

## Ex 7: Apache Hive (HQL) Data Warehousing

### 1. Aim

Perform SQL-like querying and schema-on-read operations on big data stored in HDFS using Hive Query Language (HQL).

### 2. Essential Commands & Syntax

```sql
-- 1. Database Operations
SHOW DATABASES;
CREATE DATABASE company_db;
USE company_db;

-- 2. Table Creation (with CSV delimiter)
CREATE TABLE employees (
    id STRING,
    name STRING,
    age INT,
    role STRING,
    salary FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- 3. Ingestion
INSERT INTO TABLE employees VALUES ('101', 'raghu', 21, 'sde', 88000);
LOAD DATA LOCAL INPATH 'data.csv' INTO TABLE employees;

-- 4. Analytical Queries
SELECT * FROM employees WHERE salary > 50000;

-- Aggregation with GROUP BY and HAVING
SELECT role, COUNT(*), AVG(salary)
FROM employees
GROUP BY role
HAVING AVG(salary) > 50000;

-- Joins
SELECT e.name, d.dept_name
FROM Employee e
JOIN Department d ON e.deptID = d.deptID
WHERE d.dept_name = 'Sales';
```

### 3. Exam / Viva Tips

- **Managed vs External Tables**:
  - `CREATE TABLE`: Managed table $\to$ Dropping the table deletes **both metadata and HDFS data**.
  - `CREATE EXTERNAL TABLE ... LOCATION 'hdfs://...'`: Dropping only deletes **metadata**; data stays safe in HDFS.

---

## Ex 8: Apache HBase Shell (CRUD)

### 1. Aim

Execute real-time random Read/Write/Delete (CRUD) operations on a distributed NoSQL column-family database via `hbase shell`.

### 2. Core Shell Commands Reference

```ruby
# 1. Start shell
hbase shell

# 2. DDL (Table Definition)
# Syntax: create 'table_name', 'family1', 'family2'
create 'employees', 'info', 'work'
list
describe 'employees'

# 3. Insert & Update (PUT)
# Syntax: put 'table', 'row_key', 'family:qualifier', 'value'
put 'employees', '101', 'info:name', 'raghu'
put 'employees', '101', 'info:age', '21'
put 'employees', '101', 'work:role', 'sde'
put 'employees', '101', 'work:salary', '88000'

# Update (overwrites / adds a new timestamp version)
put 'employees', '101', 'work:salary', '96800'

# 4. Read (GET & SCAN)
# Get specific row / column
get 'employees', '101'
get 'employees', '101', {COLUMN => 'info:name'}

# Scan all or key range
scan 'employees'
scan 'employees', {STARTROW => '101', STOPROW => '105'}
count 'employees'

# 5. Delete
delete 'employees', '101', 'work:salary'   # Delete single cell
deleteall 'employees', '101'                # Delete entire row

# 6. Drop Table (must disable first!)
disable 'employees'
drop 'employees'
```

### 3. Exam / Viva Tips

- HBase tables are sorted lexicographically by **Row Key**.
- Column qualifiers inside a column family do not need to be declared in schema (dynamic schema).

---

## Ex 9: Apache Pig (Pig Latin)

### 1. Aim

Build an ETL pipeline for data extraction, transformation, filtering, and aggregation using Pig Latin scripts.

### 2. Execution Modes

- Local mode: `pig -x local` (runs on local filesystem)
- MapReduce mode: `pig -x mapreduce` (runs on Hadoop cluster)

### 3. Script Structure & Key Operators

```pig
-- 1. Load Data
employees = LOAD 'data.csv' USING PigStorage(',')
            AS (id:chararray, name:chararray, age:int, role:chararray, salary:float);

-- 2. Filter (WHERE)
high_salary = FILTER employees BY salary > 50000;

-- 3. Projection (SELECT columns)
role_names = FOREACH employees GENERATE name, role;

-- 4. Grouping & Aggregation
group_roles = GROUP employees BY role;
role_count = FOREACH group_roles GENERATE group AS role, COUNT(employees) AS total_count;

-- 5. Output
DUMP role_count;                                              -- Prints to terminal
STORE role_count INTO 'outputs/role_counts' USING PigStorage(','); -- Saves to folder
```

### 4. Exam / Viva Tips

- Pig Latin uses **lazy evaluation**: expressions are not executed until a terminal operator like `DUMP` or `STORE` is called (compiles into a DAG of MapReduce jobs).

---

## Ex 10: Collaborative Filtering (PySpark)

### 1. Aim

Implement user-based collaborative filtering recommender system using Jaccard and Cosine similarity on user interaction sets with PySpark.

### 2. Similarity Formulas

For two users' preference sets $A$ and $B$:

- **Jaccard Similarity**:
  $$J(A, B) = \frac{|A \cap B|}{|A \cup B|}$$
- **Cosine Similarity** (binary sets):
  $$\cos(A, B) = \frac{|A \cap B|}{\sqrt{|A|} \cdot \sqrt{|B|}}$$

### 3. PySpark Implementation Skeleton

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, udf
from pyspark.sql.types import FloatType
import math

spark = SparkSession.builder.appName("CF").getOrCreate()

# 1. Filter positive preferences (rating >= 3)
user_prefs = ratings.filter(col("rating") >= 3) \
    .groupBy("userId") \
    .agg(collect_set("movieId").alias("liked_movies"))

# 2. Define similarity functions & register UDFs
def jaccard_similarity(s1, s2):
    if not s1 or not s2: return 0.0
    u1, u2 = set(s1), set(s2)
    return float(len(u1 & u2)) / len(u1 | u2) if len(u1 | u2) > 0 else 0.0

def cosine_similarity(s1, s2):
    if not s1 or not s2: return 0.0
    u1, u2 = set(s1), set(s2)
    denom = math.sqrt(len(u1)) * math.sqrt(len(u2))
    return float(len(u1 & u2)) / denom if denom > 0 else 0.0

jaccard_udf = udf(jaccard_similarity, FloatType())
cosine_udf = udf(cosine_similarity, FloatType())

# 3. Cross join to form pairs (u1 < u2 to prevent duplicates & self-pairs)
user_pairs = user_prefs.alias("u1").crossJoin(user_prefs.alias("u2")) \
    .filter(col("u1.userId") < col("u2.userId"))

similarities = user_pairs.select(
    col("u1.userId").alias("user1"),
    col("u2.userId").alias("user2"),
    jaccard_udf("u1.liked_movies", "u2.liked_movies").alias("jaccard"),
    cosine_udf("u1.liked_movies", "u2.liked_movies").alias("cosine")
)

similarities.filter(col("jaccard") > 0.2).orderBy(col("jaccard").desc()).show(5)
```

---

## Ex 11: Machine Learning Classification (PySpark MLlib)

### 1. Aim

Train, evaluate, and compare three classification models (**Logistic Regression**, **Linear SVC / SVM**, **Decision Tree**) on structured tabular data using PySpark MLlib.

### 2. MLlib Feature Pipeline Steps

1. **`VectorAssembler`**: Combines multiple numeric feature columns into a single vector column `rawFeatures`.
2. **`StandardScaler`**: Normalizes features with mean and unit standard deviation into `features`.
3. **`randomSplit([0.8, 0.2], seed=42)`**: Splits dataset into training and testing sets.

### 3. Model Training & Evaluation Skeleton

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, LinearSVC, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Feature Engineering
assembler = VectorAssembler(inputCols=feature_cols, outputCol="rawFeatures")
assembled = assembler.transform(df)

scaler = StandardScaler(inputCol="rawFeatures", outputCol="features", withStd=True, withMean=True)
scaled_df = scaler.fit(assembled).transform(assembled)

train_data, test_data = scaled_df.randomSplit([0.8, 0.2], seed=42)

# Models
models = {
    "Logistic Regression": LogisticRegression(featuresCol="features", labelCol="label", maxIter=50),
    "Linear SVM": LinearSVC(featuresCol="features", labelCol="label", maxIter=50),
    "Decision Tree": DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=5)
}

# Evaluation
eval_acc = MulticlassClassificationEvaluator(metricName="accuracy")
eval_f1 = MulticlassClassificationEvaluator(metricName="f1")

for name, model in models.items():
    trained_model = model.fit(train_data)
    predictions = trained_model.transform(test_data)
    acc = eval_acc.evaluate(predictions)
    f1 = eval_f1.evaluate(predictions)
    print(f"{name} -> Accuracy: {acc*100:.2f}%, F1: {f1*100:.2f}%")
```

---

## Ex 12: K-Means Clustering (PySpark MLlib)

### 1. Aim

Perform customer segmentation using unsupervised K-Means clustering and find the optimal number of clusters ($k$) via the Elbow method and Silhouette score.

### 2. Core Concepts

- **Elbow Method**: Plots the training cost / WSSSE (Within Set Sum of Squared Errors) against $k$. Look for the bend ("elbow") where rate of decrease sharply slows down.
- **Silhouette Score**: Measures how similar a point is to its own cluster compared to other clusters (range: $-1$ to $+1$; higher is better).

### 3. Code Skeleton

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# 1. Feature preparation
assembler = VectorAssembler(inputCols=["Age", "Annual_Income", "Spending_Score"], outputCol="rawFeatures")
assembled = assembler.transform(df)
scaler = StandardScaler(inputCol="rawFeatures", outputCol="features", withStd=True, withMean=True)
scaled_df = scaler.fit(assembled).transform(assembled)

# 2. Evaluate optimal k (e.g. k = 2 to 8)
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")

for k in range(2, 9):
    kmeans = KMeans(featuresCol="features", k=k, seed=42)
    model = kmeans.fit(scaled_df)
    predictions = model.transform(scaled_df)

    sil = evaluator.evaluate(predictions)
    cost = model.summary.trainingCost
    print(f"k={k} | Silhouette: {sil:.4f} | WSSSE Cost: {cost:.2f}")

# 3. Fit final optimal model
final_kmeans = KMeans(featuresCol="features", k=5, seed=42)
final_model = final_kmeans.fit(scaled_df)
clustered_df = final_model.transform(scaled_df)

# Show distribution across clusters
clustered_df.groupBy("prediction").count().show()
```

---

## 30-Minute Pre-Exam Cheat Sheet

- **MapReduce (Python)**:
  - Mapper reads `sys.stdin`, splits string, prints `key value`.
  - Reducer reads sorted `sys.stdin`, checks `if key == prev_key`, accumulates, prints on change.
- **Streaming & Graph Algorithms**:
  - **Bloom Filter**: $k$ hashes $\to$ Bit array $\to$ All 1s = duplicate (no false negatives).
  - **Flajolet-Martin**: Binary hash $\to$ max trailing zeros $R \to \frac{2^R}{0.7735}$.
  - **PageRank**: Damping formula: $PR = \beta \cdot \text{sum} + \frac{1-\beta}{N}$.
  - **Girvan-Newman**: Remove edge with highest betweenness $\to$ check modularity peak.
- **Hive & HBase & Pig**:
  - **Hive**: `CREATE TABLE (...) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';`
  - **HBase**: `create 't', 'cf'`, `put 't', 'rk', 'cf:col', 'val'`, `get 't', 'rk'`, `scan 't'`, `deleteall 't', 'rk'`.
  - **Pig**: `LOAD ... USING PigStorage(',')`, `FILTER ... BY`, `GROUP ... BY`, `FOREACH ... GENERATE`, `DUMP`.
- **PySpark ML (Ex 10-12)**:
  - Always assemble features with `VectorAssembler(inputCols=..., outputCol="rawFeatures")`.
  - Always scale with `StandardScaler(inputCol="rawFeatures", outputCol="features")`.
  - Classification: `model.fit(train)` $\to$ `model.transform(test)` $\to$ `MulticlassClassificationEvaluator`.
  - Clustering: `KMeans(k=k)` $\to$ `model.summary.trainingCost` & `ClusteringEvaluator(metricName="silhouette")`.
