import pandas as pd
import geopandas as gpd

# read weather data and create a dataframe
weather_df = pd.read_csv("raw-data/weather.csv")
crops_df = pd.read_csv("raw-data/crops.csv")
# soil_df = pd.read_csv("raw-data/soil.csv")

# create a GeoDataFrame from the weather data, using the Latitude and Longitude columns to create Point geometries
gdf = gpd.GeoDataFrame(
    weather_df, geometry=gpd.points_from_xy(weather_df["Longitude"], weather_df["Latitude"]), crs="EPSG:4326"
)
# read the US states GeoJSON file into a GeoDataFrame
states = gpd.read_file("us-states.json")

# perform a spatial join to find out which state each weather observation falls into
result = gpd.sjoin(gdf, states, how="left", predicate="within")

# print the resulting dataframe with the state names included
result.to_csv("raw-data/weather_with_states.csv", index=False)
