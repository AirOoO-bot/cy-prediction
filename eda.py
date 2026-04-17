import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import os

# 1. Initialize Spark
spark = SparkSession.builder \
    .appName("Crop_Weather_Before_After_EDA") \
    .master("local[*]") \
    .getOrCreate()

# ==========================================
# 1. EDA BEFORE: THE RAW MESS (Null Heatmap)
# ==========================================
print("Generating 'Before' Heatmap from Raw Data...")
# We load the raw CSV to show the 'mess' (nulls/duplicates)
raw_crops = spark.read.csv("raw-data/crops.csv", header=True, inferSchema=True)
# Sample 0.1% to visualize the gaps
raw_sample_pd = raw_crops.sample(False, 0.001).toPandas()

plt.figure(figsize=(10, 4))
sns.heatmap(raw_sample_pd.isnull(), cbar=False, cmap='viridis', yticklabels=False)
plt.title("EDA BEFORE: Missing Values & Raw Data Gaps")
plt.tight_layout()
plt.savefig("eda_1_BEFORE_preprocessed.png")
plt.close()

# ==========================================
# 2. EDA AFTER: MULTIVARIATE HEATMAP (Clean Data)
# ==========================================
print("Generating 'After' Multivariate Heatmap...")
base_path = os.getcwd()
data_path = os.path.join(base_path, "processed-data", "crop_weather_final")

try:
    # Load the cleaned Parquet data
    df = spark.read.parquet(data_path)
    
    # Select our target features for correlation
    features = ["yield", "avg_tmax", "avg_tmin", "total_prcp"]
    # Sample 20% for the heatmap
    corr_pd = df.select(features).sample(False, 0.2).toPandas()

    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_pd.corr(), annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
    plt.title("EDA AFTER: Multivariate Correlation (Weather vs. Yield)")
    plt.tight_layout()
    plt.savefig("eda_2_AFTER_preprocessed.png")
    plt.close()
    
    print("\nSUCCESS! Both images saved:")
    print("1. eda_1_BEFORE_preprocessed.png")
    print("2. eda_2_AFTER_preprocessed.png")

except Exception as e:
    print(f"ERROR: Could not find processed data. Run 'data_cleaning.py' first!")