from sedona.spark import *
from pyspark.sql import functions as F

# 1. Initialize Sedona-enabled Spark Session

config = SedonaContext.builder() .\
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.8.1,'
           'org.datasyslab:geotools-wrapper:1.8.1-33.1'). \
    getOrCreate()
sedona = SedonaContext.create(config)

# 2. Load Weather Data
# We load as a regular Spark DF first, then convert to a Spatial DF
weather_df = sedona.read.csv("raw-data/weather.csv", header=True, inferSchema=True)
weather_df.createOrReplaceTempView("weather_raw")

# Convert coordinates to Geometries using Sedona SQL
weather_gdf = sedona.sql("""
    SELECT *, ST_Point(CAST(Longitude AS Decimal(24,20)), CAST(Latitude AS Decimal(24,20))) as geometry
    FROM weather_raw
""")

# 3. Load States Data
# Sedona can read GeoJSON directly into a Spatial DataFrame
states_gdf = sedona.read.format("geojson").load("raw-data/us-states.json")
states_gdf.createOrReplaceTempView("states")
weather_gdf.createOrReplaceTempView("weather")

# 4. Perform Spatial Join
# ST_Contains(A, B) checks if geometry B is inside geometry A
result = sedona.sql("""
    SELECT w.*, s.NAME as state_name
    FROM weather w
    LEFT JOIN states s
    ON ST_Contains(s.geometry, w.geometry)
""")

# 5. Export
# Drop the geometry object before saving to CSV (Spark cannot save raw objects to CSV)
result.drop("geometry").write.csv("raw-data/weather_with_states_pyspark.csv", header=True)