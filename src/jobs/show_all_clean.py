from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    desc,
    expr,
    when,
    row_number,
    lower,
    substring,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

spark = (
    SparkSession.builder.appName("DataCleaning")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .master("local[3]")
    .getOrCreate()
)

crops_clean_parquet_path = "cleaned_data/crops_clean_parquet"
soil_clean_path = "cleaned_data/soil_clean_parquet"
weather_clean_parquet_path = "cleaned_data/weather_clean_parquet"
crops_clean_df = spark.read.parquet(crops_clean_parquet_path)
soil_clean_df = spark.read.parquet(soil_clean_path)
weather_clean_df = spark.read.parquet(weather_clean_parquet_path)

print(">> Crops Cleaned Data Sample")
crops_clean_df.show(200, truncate=False)
print(">> Soil Cleaned Data Sample")
soil_clean_df.show(200, truncate=False)
print(">> Weather Cleaned Data Sample")
weather_clean_df.show(200, truncate=False)