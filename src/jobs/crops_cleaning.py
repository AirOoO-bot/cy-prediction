from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import os

#  Schema import
from src.schemas.raw_crops_schema import raw_crops_schema

spark = (
    SparkSession.builder.appName("DataCleaning")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .master("local[3]")
    .getOrCreate()
)


crops_raw_path = "raw_data/crops_large.txt"
crops_parquet_path = "raw_data/parquet/crops_raw_parquet"
crops_clean_parquet_path = "cleaned_data/crops_clean_parquet"

# Reading the raw crops data with the defined schema
if not os.path.exists(crops_parquet_path + "/_SUCCESS"):
    crops_csv_df = spark.read.csv(
        crops_raw_path, header=True, schema=raw_crops_schema, sep="\t"
    )
    crops_csv_df.write.mode("overwrite").parquet(crops_parquet_path)

crops_raw_df = spark.read.parquet(crops_parquet_path).select(
    col("YEAR").alias("year"),
    col("STATE_NAME").alias("state"),
    col("COUNTY_NAME").alias("county"),
    col("COMMODITY_DESC").alias("crop"),
    col("VALUE").alias("yield"),
    col("UNIT_DESC").alias("unit"),
)

# Show original data counts and a sample of the raw crops data
print(f"Original crops data count: {crops_raw_df.count()}")
print("\n--- First 10 rows of RAW Crops Data ---")
crops_raw_df.show(10)

# Step 2: Clean the 'yield' values and cast it to double
crops_clean_df = crops_raw_df.withColumn(
    "yield", regexp_replace(col("yield"), ",", "").try_cast("double")
)

# Step 3: Drop rows where county or yield is null
crops_clean_df = crops_clean_df.dropna(subset=["county", "yield"])

crops_clean_df = crops_clean_df.sortWithinPartitions("year", "state")

print(f"Cleaned crops data count: {crops_clean_df.count()}")
print("\n--- First 10 rows of Cleaned Crops Data ---")
crops_clean_df.show(10, truncate=False)
crops_clean_df.write.mode("overwrite").parquet(crops_clean_parquet_path)
