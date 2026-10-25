import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
os.environ["TMPDIR"] = "C:/spark_temp"
os.environ["TEMP"] = "C:/spark_temp"
os.environ["TMP"] = "C:/spark_temp"

# 2. Setup Java paths (Updated to handle potential path issues)
if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = os.path.expanduser("~/.jdk17")
    # Windows uses semicolons (;) to split PATH variables, not colons (:)
    os.environ["PATH"] = f"{os.path.expanduser('~/.jdk17')}\\bin;" + os.environ.get("PATH", "")

os.makedirs("C:/spark_temp", exist_ok=True)
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, LinearSVC, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sklearn.metrics import confusion_matrix, classification_report


spark = (
    SparkSession.builder.appName("Ex11_Classification")
    .config("spark.local.dir", "C:/spark_temp")
    .config(
        "spark.driver.extraJavaOptions",
        "-Djava.io.tmpdir=C:/spark_temp",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark Version: {spark.version}")
df = spark.read.csv("dataset.csv", header=True, inferSchema=True)
print(f"Dataset Shape: ({df.count()}, {len(df.columns)})")
df.show(5)
feature_cols = [c for c in df.columns if c != 'target']


for c in feature_cols:
    df = df.withColumn(c, df[c].cast("double"))
df = df.withColumn("label", df["target"].cast("double"))


assembler = VectorAssembler(inputCols=feature_cols, outputCol="rawFeatures")
assembled_df = assembler.transform(df)
scaler = StandardScaler(inputCol="rawFeatures", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(assembled_df)
scaled_df = scaler_model.transform(assembled_df)
train_data, test_data = scaled_df.randomSplit([0.8, 0.2], seed=42)
print(f"Training set count: {train_data.count()}, Test set count: {test_data.count()}")


lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=50, regParam=0.01)
lr_model = lr.fit(train_data)
lr_preds = lr_model.transform(test_data)


svm = LinearSVC(featuresCol="features", labelCol="label", maxIter=50, regParam=0.01)
svm_model = svm.fit(train_data)
svm_preds = svm_model.transform(test_data)


dt = DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=5, seed=42)
dt_model = dt.fit(train_data)
dt_preds = dt_model.transform(test_data)


print("All 3 models trained and tested successfully!")
def evaluate(predictions, model_name):
    acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(predictions)
    prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)
    rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(predictions)
   
    pdf = predictions.select("label", "prediction").toPandas()
    cm = confusion_matrix(pdf["label"].astype(int), pdf["prediction"].astype(int))
   
    return {
        "Model": model_name,
        "Accuracy": acc,
        "Precision": prec,
        "Recall": rec,
        "F1_Measure": f1,
        "Confusion_Matrix": cm,
        "y_true": pdf["label"].astype(int),
        "y_pred": pdf["prediction"].astype(int)
    }


results = {
    "Logistic Regression": evaluate(lr_preds, "Logistic Regression"),
    "Support Vector Machine (SVM)": evaluate(svm_preds, "Support Vector Machine (SVM)"),
    "Decision Tree": evaluate(dt_preds, "Decision Tree")
}


comparison_list = []
for name, m in results.items():
    comparison_list.append({
        "Algorithm": name,
        "Accuracy (%)": f"{m['Accuracy']*100:.2f}%",
        "Precision (%)": f"{m['Precision']*100:.2f}%",
        "Recall (%)": f"{m['Recall']*100:.2f}%",
        "F1-Measure (%)": f"{m['F1_Measure']*100:.2f}%"
    })


df_comparison = pd.DataFrame(comparison_list)
print(df_comparison)
plot_rows = []
for model, m in results.items():
    for metric_name, val in [('Accuracy', m['Accuracy']), ('Precision', m['Precision']), ('Recall', m['Recall']), ('F1-Measure', m['F1_Measure'])]:
        plot_rows.append({'Algorithm': model, 'Metric': metric_name, 'Score (%)': val * 100})


plot_df = pd.DataFrame(plot_rows)
plt.figure(figsize=(10, 5), dpi=150)
sns.set_theme(style="whitegrid")
ax = sns.barplot(data=plot_df, x="Algorithm", y="Score (%)", hue="Metric", palette=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"])
plt.title("PySpark Classification Algorithm Comparison", fontsize=14, fontweight='bold')
plt.ylim(0, 115)
for p in ax.patches:
    h = p.get_height()
    if h > 0:
        ax.annotate(f"{h:.1f}%", (p.get_x() + p.get_width() / 2., h + 1.5), ha='center', va='bottom', fontsize=9, fontweight='bold')
plt.show()


fig, axes = plt.subplots(1, 3, figsize=(15, 4.5), dpi=150)
cm_cmaps = ["Blues", "Greens", "Oranges"]
for i, (name, m) in enumerate(results.items()):
    sns.heatmap(m["Confusion_Matrix"], annot=True, fmt="d", cmap=cm_cmaps[i], cbar=False, ax=axes[i],
                annot_kws={"size": 13, "weight": "bold"},
                xticklabels=["Pred 0", "Pred 1"], yticklabels=["Actual 0", "Actual 1"])
    axes[i].set_title(f"{name}\nAcc: {m['Accuracy']*100:.1f}% | F1: {m['F1_Measure']*100:.1f}%", fontweight='bold')
    axes[i].set_xlabel("Predicted")
    axes[i].set_ylabel("Actual")
plt.tight_layout()
plt.show()
