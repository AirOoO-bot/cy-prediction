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
    year,
    try_to_date
)

import pandas as pd
import geopandas as gpd

from schemas.raw_crops_schema import raw_crops_schema
from schemas.raw_weather_schema import raw_weather_schema

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# =========================================
# CROP DATA CLEANING PIPELINE (PYSPARK)
# =========================================
crops_raw_df = spark.read.csv(
    "raw-data/crops.csv", header=True, schema=raw_crops_schema
)

# Cache the for better performance
crops_raw_df.cache()

# Show original data counts and a sample of the raw crops data
print(f"Original crops data count: {crops_raw_df.count()}")
print("\n--- First 10 rows of RAW Crops Data ---")
crops_raw_df.show(10)

# Step 1: Filter for YIELD and Select/Rename essential columns
crops_clean_df = crops_raw_df.filter(col("STATISTICCAT_DESC") == "YIELD").select(
    col("YEAR").alias("year"),
    col("STATE_NAME").alias("state"),
    col("COUNTY_NAME").alias("county"),
    col("COMMODITY_DESC").alias("crop"),
    col("VALUE").alias("yield"),
    col("UNIT_DESC").alias("unit"),
)

# Step 2: Clean the 'yield' values and cast it to double
crops_clean_df = crops_clean_df.withColumn(
    "yield", expr("try_cast(regexp_replace(yield, ',', '') as double)")
)

# Step 3: Drop rows where county or yield is null
crops_clean_df = crops_clean_df.na.drop(subset=["county", "yield"])

# Step 4: Make text columns uniform (Title Case) to ensure a perfect merge later
crops_clean_df = (
    crops_clean_df.withColumn("state", initcap(col("state")))
    .withColumn("county", initcap(col("county")))
    .withColumn("crop", initcap(col("crop")))
)

crops_clean_df = crops_clean_df.orderBy("year", "state", "county")

print(f"Cleaned crops data count: {crops_clean_df.count()}")
print("\n--- First 5 rows of Cleaned Crops Data ---")
crops_clean_df.show(5, truncate=False)
crops_clean_df.write.mode("overwrite").parquet("cleaned-data/crops_parquet")


# =========================================
# WEATHER DATA CLEANING PIPELINE (PYSPARK)
# ==========================================

weather_raw_df = spark.read.csv("raw-data/weather.csv", header=True, schema=raw_weather_schema).cache()

print(f"Original weather data count: {weather_raw_df.count()}")
print("\n--- First 5 rows of RAW Weather Data ---")
weather_raw_df.show(5, truncate=False)

weather_clean_df = weather_raw_df.withColumn(
    "date_parsed", try_to_date(col("DATE"), "MM/dd/yyyy")
).withColumn("year", year(col("date_parsed")))

weather_clean_df = weather_clean_df.drop("EVAP")

weather_clean_df = (
    weather_clean_df.withColumnRenamed("TMAX", "tmax")
    .withColumnRenamed("TMIN", "tmin")
    .withColumnRenamed("PRCP", "prcp")
    .withColumnRenamed("Latitude", "lat")
    .withColumnRenamed("Longitude", "lon")
    .withColumnRenamed("ID", "station_id")
)

weather_clean_df = weather_clean_df.dropna(subset=["date_parsed", "tmax", "tmin", "prcp", "lat", "lon"])

weather_clean_df.show(1000)

weather_yearly = (
    weather_clean_df.groupBy("station_id", "year", "lat", "lon")
    .agg(
        round(avg("tmax"), 2).alias("avg_tmax"),
        round(avg("tmin"), 2).alias("avg_tmin"),
        round(sum("prcp"), 2).alias("total_prcp"),
    )
    .orderBy("year", "station_id")
)

weather_yearly.show(1000)


print(f"\nCleaned & Aggregated weather data count: {weather_yearly.count()}")
print("\n--- First 5 rows of CLEANED Weather Data ---")
weather_yearly.show(5, truncate=False)
weather_clean_df.write.mode("overwrite").parquet("cleaned-data/weather_parquet")