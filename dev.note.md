# 🌾 Crop Yield Prediction

A data science project that predicts crop yields by combining weather data, geographic location information, and agricultural data using PySpark and geospatial analysis.

## 📋 Project Overview

This project processes and cleans large-scale agricultural and weather datasets to build a foundation for crop yield prediction models. It includes:
- **Weather data processing**: Geographic mapping of weather stations to US states
- **Crop yield data cleaning**: Standardization and validation of USDA crop yield statistics
- **Data pipeline**: Transforms raw data into cleaned datasets ready for machine learning

## 🛠️ Prerequisites

- **Python 3.8+**
- **Java Runtime Environment (JRE)** - Required for PySpark
- **pip** (Python package manager)

### Check Prerequisites

```bash
# Check Python version
python --version

# Check Java installation
java -version
```

## 📦 Installation

### 1. Clone/Setup the Project

```bash
cd crop-yield-prediction
```

### 2. Create a Virtual Environment (Recommended)

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies included:**
- `pyspark>=3.3.0` - Distributed data processing
- `pandas>=1.5.0` - Data manipulation
- `geopandas>=0.12.0` - Geospatial data handling
- `matplotlib>=3.5.0` - Visualization
- `seaborn>=0.12.0` - Statistical visualization

## 📁 Project Structure

```
crop-yield-prediction/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── us-states.json           # US states GeoJSON boundaries
├── data_cleaning.py         # Crop yield data cleaning pipeline
├── geojoin.py              # Geographic join of weather to states
├── test.py                 # Automated tests
├── Notes.md                # Project research notes
├── raw-data/               # Input data
│   ├── crops.csv           # USDA crop yield statistics
│   ├── weather.csv         # Weather station observations
│   └── weather_with_states.csv  # Weather data with state mapping
└── cleaned-data/           # Output data (Parquet format)
    ├── crops_cleaned.csv/
    ├── weather_cleaned_raw.csv/
    └── weather_cleaned.csv/
```

## 🚀 How to Run

### 1. Geographic Join (Weather to States)

Maps weather station observations to US states using geographic boundaries:

```bash
python geojoin.py
```

**Output:** Creates `raw-data/weather_with_states.csv` with state information added to each weather observation.

### 2. Data Cleaning (PySpark Pipeline)

Cleans and standardizes the crop yield data with the following steps:
- Filters for yield statistics
- Removes invalid/missing values
- Normalizes text (title case)
- Generates EDA visualizations
- Exports cleaned data in Parquet format

```bash
python data_cleaning.py
```

**Outputs:**
- `cleaned-data/crops_cleaned.csv/` - Cleaned crop yield data
- `cleaned-data/weather_cleaned.csv/` - Cleaned weather data
- `eda_pre_cleaning.png` - Visualization of missing values before cleaning

### 3. Run Tests

```bash
python test.py
```

## 📊 Data Pipeline

```
Raw Data
├── raw-data/crops.csv
│   └─→ data_cleaning.py
│       └─→ cleaned-data/crops_cleaned.csv/
│
├── raw-data/weather.csv
│   └─→ geojoin.py
│       └─→ raw-data/weather_with_states.csv
│           └─→ data_cleaning.py
│               └─→ cleaned-data/weather_cleaned.csv/
│
└── us-states.json (GeoJSON boundaries)
    └─→ Used by geojoin.py for spatial join
```

## 📝 Data Dictionary

### Crops Data
| Column | Type | Description |
|--------|------|-------------|
| `year` | int | Year of observation |
| `state` | str | US state name (title case) |
| `county` | str | County name (title case) |
| `crop` | str | Type of crop (e.g., CORN, WHEAT, SOYBEANS) |
| `yield` | float | Crop yield value |
| `unit` | str | Unit of measurement (e.g., BU/ACRE) |

### Weather Data
| Column | Type | Description |
|--------|------|-------------|
| `Latitude` | float | Station latitude |
| `Longitude` | float | Station longitude |
| `State` | str | US state (from geographic join) |
| `TMAX` | float | Maximum temperature |
| `TMIN` | float | Minimum temperature |
| `PRCP` | float | Precipitation |
| `EVAP` | float | Evapotranspiration |

## ⚠️ Important Notes

### PySpark Configuration
- Scripts use local mode: `master("local[*]")` - Utilizes all available CPU cores
- For large datasets, consider increasing memory: Modify the `SparkSession` configuration in `data_cleaning.py`

### Performance Considerations
- First run may be slower due to Java startup and library loading
- Data cleaning uses 0.1% sampling for EDA visualization to keep processing fast
- Cleaned data is stored in Parquet format for efficient storage and retrieval

### Data Quality
- Crop yield values with special codes like "(D)" (withheld) are converted to null
- Missing values in county or yield columns are removed
- Text normalization ensures consistent merging across datasets

## 🔧 Troubleshooting

### Java not found
```
Error: Java not found
Solution: Install JRE/JDK. Point JAVA_HOME to installation directory
```

### Memory issues with PySpark
Modify in `data_cleaning.py`:
```python
spark = SparkSession.builder \
    .appName("Crop_Weather_Merge") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

### Parquet files won't read
Ensure `pyarrow` is installed:
```bash
pip install pyarrow
```

## 📚 Resources

- [USDA NASS Dataset](https://quickstats.nass.usda.gov/) - Crop yield data source
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [GeoPandas Documentation](https://geopandas.org/)
- [Notes.md](Notes.md) - Detailed research notes on required data sources for improved predictions

## 👥 Contributing

When contributing:
1. Ensure all scripts run without errors: `python script_name.py`
2. Run tests: `python test.py`
3. Document any new dependencies in `requirements.txt`
4. Add comments for complex data transformations
5. Update this README if adding new functionality

## 📄 License

[Add your license here]

---

**Last Updated:** 2026-04-17  
**Data Source:** USDA NASS, NOAA Weather Data
