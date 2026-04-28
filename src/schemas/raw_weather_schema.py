from pyspark.sql.types import *

raw_weather_schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("TMAX", DoubleType(), True),
        StructField("TMIN", DoubleType(), True),
        StructField("EVAP", DoubleType(), True),
        StructField("PRCP", DoubleType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Elevation", DoubleType(), True),
        # StructField("ELEMENT", StringType(), True),
        # StructField("DATA_VALUE", StringType(), True),
        # StructField("M_FLAG", StringType(), True),
        # StructField("Q_FLAG", StringType(), True),
        # StructField("S_FLAG", StringType(), True),
        # StructField("OBS_TIME", StringType(), True),
    ]
)
