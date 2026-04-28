from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, sum, round, try_to_date, year
import os

#  Schema import
from src.schemas.raw_weather_schema import raw_weather_schema

spark = (
    SparkSession.builder.appName("DataCleaning")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .master("local[3]")
    .getOrCreate()
)


weather_raw_path = "raw_data/weather_large.csv"
weather_parquet_path = "raw_data/parquet/weather_raw_parquet"
weather_clean_parquet_path = "cleaned_data/weather_clean_parquet"

parquet_file_exists = os.path.exists(weather_parquet_path)

if not parquet_file_exists:
    print(">> Parquet file not found. Reading from CSV and writing to Parquet...")
    weather_csv_df = spark.read.csv(
        weather_raw_path, header=True, schema=raw_weather_schema
    )
    weather_csv_df.write.mode("overwrite").parquet(weather_parquet_path)
print(">> Parquet file exists. Reading from Parquet...")
weather_raw_df = spark.read.parquet(weather_parquet_path).select(
    col("ID").alias("station_id"),
    col("DATE").alias("date"),
    col("TMAX").alias("tmax"),
    col("TMIN").alias("tmin"),
    col("PRCP").alias("prcp"),
    col("Latitude").alias("lat"),
    col("Longitude").alias("lon"),
)

print(f"Original weather data count: {weather_raw_df.count()}")
print("\n--- First 10 rows of RAW Weather Data ---")
weather_raw_df.show(10)

weather_clean_df = (
    weather_raw_df.dropna(subset=["date", "tmax", "tmin", "prcp", "lat", "lon"])
    .withColumn("date_parsed", try_to_date(col("date"), "M/d/yyyy"))
    .withColumn("year", year(col("date_parsed")))
)

weather_yearly = weather_clean_df.groupBy("station_id", "year", "lat", "lon").agg(
    round(avg("tmax"), 2).alias("avg_tmax"),
    round(avg("tmin"), 2).alias("avg_tmin"),
    round(sum("prcp"), 2).alias("total_prcp"),
)

print(f"Cleaned weather data count: {weather_yearly.count()}")
print("\n--- First 10 rows of Cleaned Weather Data ---")
weather_yearly.show(10, truncate=False)
weather_yearly.write.mode("overwrite").parquet(weather_clean_parquet_path)
