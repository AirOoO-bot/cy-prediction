from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, regexp_replace, expr, split, avg, sum, round

# 1. Initialize Spark
spark = SparkSession.builder \
    .appName("Crop_Weather_Cleaning") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. Load Data
crops_df = spark.read.csv("raw-data/crops.csv", header=True, inferSchema=True)
weather_df = spark.read.csv("raw-data/weather.csv", header=True, inferSchema=True)

# 3. Crop Cleaning
crops_clean = crops_df.filter(col("STATISTICCAT_DESC") == "YIELD") \
    .select(
        col("YEAR").alias("year"),
        col("STATE_NAME").alias("state"),
        col("COUNTY_NAME").alias("county"),
        col("COMMODITY_DESC").alias("crop"),
        col("VALUE").alias("yield"),
        col("UNIT_DESC").alias("unit")
    ) \
    .withColumn("yield", expr("try_cast(regexp_replace(yield, ',', '') as double)")) \
    .na.drop(subset=["county", "yield"]) \
    .withColumn("state", initcap(col("state"))) \
    .withColumn("county", initcap(col("county"))) \
    .withColumn("crop", initcap(col("crop")))

# 4. Weather Cleaning & National Aggregation
weather_national_yearly = weather_df.withColumn("year", split(col("DATE"), "/").getItem(2).cast("integer")) \
    .groupBy("year").agg(
        round(avg("TMAX"), 2).alias("avg_tmax"),
        round(avg("TMIN"), 2).alias("avg_tmin"),
        round(sum("PRCP"), 2).alias("total_prcp")
    ).na.drop()

# 5. Join and Save
final_df = crops_clean.join(weather_national_yearly, on="year", how="inner")

# Write to a folder named 'processed-data'
final_df.write.mode("overwrite").parquet("processed-data/crop_weather_final")
print("Data Cleaning Complete. Saved to processed-data/crop_weather_final")