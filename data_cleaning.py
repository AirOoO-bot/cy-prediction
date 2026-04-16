import seaborn as sns # type: ignore
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, regexp_replace, expr, split, avg, sum, round

# ==========================================
# 1. INITIALIZE SPARK WITH MORE MEMORY
# ==========================================
# We add memory configs to prevent the "Java heap space" OutOfMemoryError
spark = SparkSession.builder \
    .appName("Crop_Weather_Merge") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# ==========================================
# 2. LOAD DATA
# ==========================================
crops_df = spark.read.csv("raw-data/crops.csv", header=True, inferSchema=True)
weather_df = spark.read.csv("raw-data/weather.csv", header=True, inferSchema=True)

print(f"Original crops data count: {crops_df.count()}")
print(f"Original weather data count: {weather_df.count()}")

# ==========================================
# 3. PRE-CLEANING EDA
# ==========================================
# We sample a tiny bit just to document missing values for your report
sample_pre_pd = crops_df.sample(False, 0.001).toPandas()
plt.figure(figsize=(10, 4))
sns.heatmap(sample_pre_pd.isnull(), cbar=False, cmap='viridis')
plt.title("EDA: Missing Values Before Cleaning")
plt.savefig("eda_pre_cleaning.png")
plt.close()

# ==========================================
# 4. CROP DATA CLEANING PIPELINE
# ==========================================
crops_clean = crops_df.filter(col("STATISTICCAT_DESC") == "YIELD") \
    .select(
        col("YEAR").alias("year"),
        col("STATE_NAME").alias("state"),
        col("COUNTY_NAME").alias("county"),
        col("COMMODITY_DESC").alias("crop"),
        col("VALUE").alias("yield"),
        col("UNIT_DESC").alias("unit")
    )

# Clean yield: Remove commas and cast to double
crops_clean = crops_clean.withColumn(
    "yield", 
    expr("try_cast(regexp_replace(yield, ',', '') as double)")
)

# Drop rows where critical data is missing
crops_clean = crops_clean.na.drop(subset=["county", "yield"])

# Standardize text to Title Case
crops_clean = crops_clean.withColumn("state", initcap(col("state"))) \
                         .withColumn("county", initcap(col("county"))) \
                         .withColumn("crop", initcap(col("crop")))

print(f"Cleaned crops count: {crops_clean.count()}")

# ==========================================
# 5. WEATHER DATA CLEANING & AGGREGATION
# ==========================================
# Step 1: Extract year
weather_clean = weather_df.withColumn("year", split(col("DATE"), "/").getItem(2).cast("integer"))

# Step 2: Rename and drop unused
weather_clean = weather_clean.select(
    col("year"),
    col("TMAX").alias("tmax"),
    col("TMIN").alias("tmin"),
    col("PRCP").alias("prcp")
).drop("EVAP")

# Step 3: AGGREGATE TO NATIONAL YEARLY AVERAGE
# This prevents the "Cartesian Product" that crashed your previous run.
# We get one weather summary per year.
weather_national_yearly = weather_clean.groupBy("year").agg(
    round(avg("tmax"), 2).alias("avg_tmax"),
    round(avg("tmin"), 2).alias("avg_tmin"),
    round(sum("prcp"), 2).alias("total_prcp")
).na.drop()

print(f"Aggregated weather years: {weather_national_yearly.count()}")

# ==========================================
# 6. JOIN & MULTIVARIATE ANALYSIS
# ==========================================
# Inner join ensures we only keep years where both crop and weather data exist
final_df = crops_clean.join(weather_national_yearly, on="year", how="inner")

print(f"Final Merged count: {final_df.count()}")

# Select numeric features for the Correlation Heatmap
features = ["yield", "avg_tmax", "avg_tmin", "total_prcp"]
corr_pd = final_df.select(features).sample(False, 0.5).toPandas()

# Generate Heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(corr_pd.corr(), annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
plt.title("Multivariate Analysis: Weather Features vs. Crop Yield")
plt.savefig("eda_multivariate_heatmap.png")
print("Multivariate Heatmap saved as eda_multivariate_heatmap.png")

# Final Inspection
print("\n--- Final Preprocessed Sample Data ---")
final_df.show(5)

# Save the preprocessed dataset for the next project phase
final_df.write.mode("overwrite").parquet("processed-data/crop_weather_final.parquet")