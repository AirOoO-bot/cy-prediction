from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, desc, expr, split, when, row_number,
    lower, trim, regexp_replace, explode, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

soil_schema = StructType(
    [
        StructField("county_id", StringType(), True),
        StructField("county_name", StringType(), True),
        StructField("soil_name", StringType(), True),
        StructField("soil_order", StringType(), True),
        StructField("soil_subgroup", StringType(), True),
        StructField("drainage", StringType(), True),
        StructField("texture", StringType(), True),
        StructField("soil_score", DoubleType(), True),
    ]
)

spark = (
    SparkSession.builder.appName("SoilCleaning")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .master("local[5]")
    .getOrCreate()
)

soil_raw_path = "raw_data/soil.csv"
soil_parquet_path = "raw_data/parquet/soil_raw_parquet"
soil_clean_path = "cleaned_data/soil_clean_parquet"

# -----------------------------
# Load CSV → Parquet (cache layer)
# -----------------------------
if not os.path.exists(soil_parquet_path + "/_SUCCESS"):
    soil_raw_df = spark.read.csv(
        soil_raw_path, header=False, schema=soil_schema
    )
    soil_raw_df.write.mode("overwrite").parquet(soil_parquet_path)

df = spark.read.parquet(soil_parquet_path)

# -----------------------------
# Basic cleaning
# -----------------------------
df = df.replace("", None)
df = df.withColumn("soil_score", col("soil_score").cast("double"))
df = df.dropna(subset=["county_id", "county_name", "soil_score"])

df = df.withColumn(
    "drainage_score",
    when(col("drainage") == "Well drained", 1)
    .when(col("drainage") == "Moderately drained", 0)
    .when(col("drainage") == "Poorly drained", -1)
)

# -----------------------------
# Aggregate per soil region
# -----------------------------
soil_quality = df.groupBy("county_id", "county_name").agg(
    avg("soil_score").alias("avg_soil_quality"),
    avg("drainage_score").alias("avg_drainage_score"),
    count("soil_name").alias("soil_samples"),
)

# -----------------------------
# Extract state + county text
# -----------------------------
soil_quality = soil_quality.withColumn(
    "parts", split(col("county_name"), ",")
)

soil_quality = soil_quality.withColumn(
    "state",
    lower(trim(expr("parts[size(parts) - 1]")))
)

soil_quality = soil_quality.withColumn(
    "county_text",
    lower(trim(col("parts")[0]))
)

# -----------------------------
# Remove noise words
# -----------------------------
soil_quality = soil_quality.withColumn(
    "county_text",
    regexp_replace(
        col("county_text"),
        r"(county|counties|area|parts?|part of|valley|region)",
        ""
    )
)

# -----------------------------
# Split multi-counties → explode
# -----------------------------
soil_quality = soil_quality.withColumn(
    "county_list",
    split(col("county_text"), r",|and")
)

soil_quality = soil_quality.withColumn(
    "county_clean",
    explode(col("county_list"))
)

soil_quality = soil_quality.withColumn(
    "county_clean",
    trim(col("county_clean"))
)

soil_quality = soil_quality.filter(col("county_clean") != "")

# -----------------------------
# Build join key
# -----------------------------
soil_quality = soil_quality.withColumn(
    "county_key",
    concat_ws("_", col("state"), col("county_clean"))
)

# -----------------------------
# Dominant soil per region
# -----------------------------
soil_counts = df.groupBy("county_id", "soil_name").count()

window = Window.partitionBy("county_id").orderBy(desc("count"))

dominant_soil = (
    soil_counts.withColumn("rank", row_number().over(window))
    .filter(col("rank") == 1)
    .select("county_id", col("soil_name").alias("dominant_soil"))
)

# -----------------------------
# Final dataset
# -----------------------------
soil_final = soil_quality.join(dominant_soil, "county_id")

# -----------------------------
# Save
# -----------------------------
soil_final.write.mode("overwrite").parquet(soil_clean_path)

print("Count: ", soil_final.count())
soil_final.show(500, truncate=False)