import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    initcap,
    regexp_replace,
    expr,
    split,
    avg,
    sum,
    round,
)

# Airoooooooooo
spark = (
    SparkSession.builder.appName("Crop_Weather_Merge").master("local[*]").getOrCreate()
)

crops_df = spark.read.csv("raw-data/crops.csv", header=True, inferSchema=True)
# print original data counts
print(f"Original crops data count: {crops_df.count()}")
print("\n--- First 5 rows of RAW Crops Data ---")
crops_df.show(5, truncate=False)

#! CROP DATA CLEANING PIPELINE (PYSPARK)
# We sample 5% of the 500k rows to visualize the "messy" state of the data
sample_pre_pd = crops_df.sample(False, 0.001).toPandas()

plt.figure(figsize=(10, 6))
sns.heatmap(sample_pre_pd.isnull(), cbar=False, cmap="viridis")
plt.title("EDA: Missing Values Before Cleaning")
plt.savefig("eda_pre_cleaning.png")
print("Pre-cleaning EDA saved as eda_pre_cleaning.png")

# Step 1: Filter for YIELD and Select/Rename essential columns
crops_clean = crops_df.filter(col("STATISTICCAT_DESC") == "YIELD").select(
    col("YEAR").alias("year"),
    col("STATE_NAME").alias("state"),
    col("COUNTY_NAME").alias("county"),
    col("COMMODITY_DESC").alias("crop"),
    col("VALUE").alias("yield"),
    col("UNIT_DESC").alias("unit"),
)

# Step 2: Clean the 'yield' values
# We use expr("try_cast(...)") so that weird strings like "(D)" safely become nulls instead of crashing :D.
crops_clean = crops_clean.withColumn(
    "yield", expr("try_cast(regexp_replace(yield, ',', '') as double)")
)

# Step 3: Drop rows where county or yield is null
crops_clean = crops_clean.na.drop(subset=["county", "yield"])

# Step 4: Make text columns uniform (Title Case) to ensure a perfect merge later
crops_clean = (
    crops_clean.withColumn("state", initcap(col("state")))
    .withColumn("county", initcap(col("county")))
    .withColumn("crop", initcap(col("crop")))
)

print(f"Cleaned crops data count: {crops_clean.count()}")
print("\n--- First 5 rows of Cleaned Crops Data ---")
crops_clean.show(5, truncate=False)

# Jeromeeeee & Airoooooooooo
# WEATHER CLEANING
weather_df = spark.read.csv("raw-data/weather.csv", header=True, inferSchema=True)

# WEATHER DATA CLEANING PIPELINE

# PRE-CLEANING INSPECTION

print(f"Original weather data count: {weather_df.count()}")
print("\n--- First 5 rows of RAW Weather Data ---")
weather_df.show(5, truncate=False)

# Step 1: Extract 'year' from the DATE column (Format: MM/DD/YYYY)
# We split the string by '/' and grab the last part (the year)
weather_clean = weather_df.withColumn(
    "year", split(col("DATE"), "/").getItem(2).cast("integer")
)

# Step 2: Drop EVAP (Evaporation) since it's almost entirely null
weather_clean = weather_clean.drop("EVAP")

# Step 3: Rename columns to lowercase for a cleaner PySpark experience
weather_clean = (
    weather_clean.withColumnRenamed("TMAX", "tmax")
    .withColumnRenamed("TMIN", "tmin")
    .withColumnRenamed("PRCP", "prcp")
    .withColumnRenamed("Latitude", "lat")
    .withColumnRenamed("Longitude", "lon")
    .withColumnRenamed("ID", "station_id")
)

# Step 4: Aggregate!
# We group by Station and Year, then calculate the Average Temps and Total Rainfall.
# We also keep the Lat/Lon so we know where the station is!
weather_yearly = weather_clean.groupBy("station_id", "year", "lat", "lon").agg(
    round(avg("tmax"), 2).alias("avg_tmax"),
    round(avg("tmin"), 2).alias("avg_tmin"),
    round(sum("prcp"), 2).alias("total_prcp"),
)

# Step 5: Drop rows that still have nulls after averaging
weather_yearly = weather_yearly.na.drop()

# ==========================================
# POST-CLEANING INSPECTION
# ==========================================
print(f"\nCleaned & Aggregated weather data count: {weather_yearly.count()}")
print("\n--- First 5 rows of CLEANED Weather Data ---")
weather_yearly.show(5, truncate=False)
