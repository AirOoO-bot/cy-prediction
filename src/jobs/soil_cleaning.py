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
    soil_raw_df = spark.read.csv(soil_raw_path, header=False, schema=soil_schema)
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
    .when(col("drainage") == "Poorly drained", -1),
)

# -----------------------------
# Extract state from county_id (e.g., AK990 → AK → alaska)
# -----------------------------
state_map = {
    "ak": "alaska",
    "al": "alabama",
    "ar": "arkansas",
    "az": "arizona",
    "ca": "california",
    "co": "colorado",
    "ct": "connecticut",
    "de": "delaware",
    "fl": "florida",
    "ga": "georgia",
    "hi": "hawaii",
    "ia": "iowa",
    "id": "idaho",
    "il": "illinois",
    "in": "indiana",
    "ks": "kansas",
    "ky": "kentucky",
    "la": "louisiana",
    "ma": "massachusetts",
    "md": "maryland",
    "me": "maine",
    "mi": "michigan",
    "mn": "minnesota",
    "mo": "missouri",
    "ms": "mississippi",
    "mt": "montana",
    "nc": "north carolina",
    "nd": "north dakota",
    "ne": "nebraska",
    "nh": "new hampshire",
    "nj": "new jersey",
    "nm": "new mexico",
    "nv": "nevada",
    "ny": "new york",
    "oh": "ohio",
    "ok": "oklahoma",
    "or": "oregon",
    "pa": "pennsylvania",
    "ri": "rhode island",
    "sc": "south carolina",
    "sd": "south dakota",
    "tn": "tennessee",
    "tx": "texas",
    "ut": "utah",
    "va": "virginia",
    "vt": "vermont",
    "wa": "washington",
    "wi": "wisconsin",
    "wv": "west virginia",
    "wy": "wyoming",
}

mapping_expr = expr(
    "map(" + ",".join([f"'{k}','{v}'" for k, v in state_map.items()]) + ")"
)

df = df.withColumn("state_abbr", lower(substring(col("county_id"), 1, 2)))

df = df.withColumn("state", mapping_expr.getItem(col("state_abbr")))

# -----------------------------
# Aggregate per soil region
# -----------------------------
soil_quality = df.groupBy("county_id", "state").agg(
    avg("soil_score").alias("avg_soil_quality"),
    avg("drainage_score").alias("avg_drainage_score"),
    count("soil_name").alias("soil_samples"),
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
# Final dataset (ML-ready columns only)
# -----------------------------
soil_final = soil_quality.join(dominant_soil, "county_id").select(
    "county_id",
    "state",
    "avg_soil_quality",
    "avg_drainage_score",
    "soil_samples",
    "dominant_soil",
)

# -----------------------------
# Save
# -----------------------------
soil_final.write.mode("overwrite").parquet(soil_clean_path)

print("Count: ", soil_final.count())
soil_final.show(1000, truncate=False)
