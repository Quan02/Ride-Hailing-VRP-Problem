"""
This script processes ride-hailing data from a Parquet file and maps Taxi Zone IDs to their respective latitude
and longitude coordinates using a geospatial shapefile. It performs the following preprocessing tasks:

1. Loads raw ride data from a Parquet file.
2. Loads geospatial info (Taxi Zone shapefile) for mapping LocationID.
3. Maps pickup and drop-off zones to latitude/longitude using the centroid of each Taxi Zone polygon.
4. Validates data by identifying and optionally removing rows with missing latitude/longitude values.
5. Generates new features such as trip duration, average speed, pickup hour, and day of the week.
6. Saves the preprocessed data to CSV and Parquet formats for downstream analysis or machine learning workflows.
"""

from pathlib import Path
import logging

import pandas as pd
import geopandas as gpd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_parquet(parquet_file: Path) -> pd.DataFrame:
    """
    Loads Parquet file into a Pandas DataFrame.
    
    Args:
        parquet_file (Path): Path to the Parquet file.
    
    Returns:
        pd.DataFrame: Loaded data.
    """
    try:
        logging.info("Loading Ride Data Parquet file...")
        raw_data: pd.DataFrame = pd.read_parquet(parquet_file)
        logging.info("Parquet file loaded successfully.")
        return raw_data
    except FileNotFoundError as exc:
        logging.error("File not found at %s.", parquet_file)
        raise FileNotFoundError(f"File not found: {parquet_file}") from exc

def load_shapefile(shape_file: Path) -> gpd.GeoDataFrame:
    """
    Loads Taxi Zone shapefile into a GeoDataFrame
    
    Args:
        shape_file (Path): Path to the shapefile.
    
    Returns:
        gpd.GeoDataFrame: Loaded gepspatial data.
    """
    try:
        logging.info("Loading Taxi Zone shapefile...")
        taxi_zones: gpd.GeoDataFrame = gpd.read_file(shape_file)
        # taxi_zones got LocationID, geometry col.
        logging.info("Shapefile loaded successfully!")
        return taxi_zones
    except FileNotFoundError as exc:
        logging.error("Error: Shapefile not found at %s.", shape_file)
        raise FileNotFoundError(f"File not found: {shape_file}") from exc

def validate_input_columns(ride_data: pd.DataFrame, required_columns: list) -> None:
    """
    Validate input columns in the ride_data

    Args:
        ride_data (pd.DataFrame): Ride data.
        required_columns (list): List of required columns.
    
    Raises:
        ValueError: If required columns are missing.
    """
    missing_columns = [col for col in required_columns if col not in ride_data.columns]
    if missing_columns:
        logging.error("Missing required columns: %s", missing_columns)
        raise ValueError(f"Required columns are missing: {missing_columns}")

def map_zone_ids_to_coords(ride_data: pd.DataFrame, taxi_zones: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Maps Taxi Zone IDs to latitude and longitude for both pickup and drop-off locations.

    Args:
        ride_data (pd.DataFrame): Ride data with PULocationID and DOLocationID.
        taxi_zones (gpd.GeoDataFrame): Taxi Zone geospatial data
    Returns:
        pd.DataFrame: Data with latitude and longitude for pickup and drop-off.
    """
    logging.info("Mapping Taxi Zone IDs to latitude and longtitude")

    taxi_zones_combined = taxi_zones[["LocationID", "geometry"]].copy()
    taxi_zones_combined["pickup_lat"] = taxi_zones_combined.geometry.centroid.y
    taxi_zones_combined["pickup_lon"] = taxi_zones_combined.geometry.centroid.x
    taxi_zones_combined["dropoff_lat"] = taxi_zones_combined.geometry.centroid.y
    taxi_zones_combined["dropoff_lon"] = taxi_zones_combined.geometry.centroid.x

    # Filter ride_data to only rows with valid LocationID
    valid_ids = taxi_zones_combined["LocationID"].unique()
    ride_data = ride_data[
        ride_data["PULocationID"].isin(valid_ids) & ride_data["DOLocationID"].isin(valid_ids)
    ]

    # Chunk processing to manage large datasets.
    chunk_size = 100000
    chunks = []
    for i in range(0, len(ride_data), chunk_size):
        chunk = ride_data.iloc[i:i + chunk_size]
        chunk_final_data = chunk.merge(
            taxi_zones_combined[["LocationID", "pickup_lat", "pickup_lon"]],
            left_on="PULocationID", right_on="LocationID", how="left"
        ).merge(
            taxi_zones_combined[["LocationID", "dropoff_lat", "dropoff_lon"]],
            left_on="DOLocationID", right_on="LocationID", how="left"
        )
        chunks.append(chunk_final_data)

    final_data = pd.concat(chunks, ignore_index=True)

    logging.info("Latitude and longitude mapping completed.")
    return final_data

def validate_data(final_data: pd.DataFrame) -> pd.DataFrame:
    """
    Validates for missing lat/lon data and drop invalid rows.

    Args:
        final_data (pd.DataFrame): Data with latitude and longitude columns.
    
    Returns:
        pd.DataFrame: Cleaned data without missing values.
    """
    logging.info("Validating data for missing or invalid entries...")
    missing_pickup: int = final_data["pickup_lat"].isna().sum()
    missing_dropoff: int = final_data["dropoff_lat"].isna().sum()
    if missing_pickup > 0 or missing_dropoff > 0:
        logging.warning("%s rows missing pickup coord, %s missing dropoff coord", missing_pickup, missing_dropoff)
        # Drop rows with missing data
        final_data = final_data.dropna(subset=["pickup_lat", "dropoff_lat"])
    return final_data

def feature_engineering(final_data: pd.DataFrame) -> pd.DataFrame:
    """
    Add new columns for trip duration, average speed, and time-based features.
    Remove columns that not used for future analysis.

    Args:
        final_data (pd.DataFrame): DataFrame containing ride data.
    
    Returns:
        pd.DataFrame: Data with new and needed features.
    """
    logging.info("Adding new features for future analysis...")
    final_data["trip_duration"] = (final_data["dropoff_datetime"] - final_data["pickup_datetime"]).dt.total_seconds()
    final_data["wait_time"] = (final_data["pickup_datetime"] - final_data["request_datetime"]).dt.total_seconds()
    final_data["avg_speed"] = final_data["trip_miles"] / (final_data["trip_duration"]/3600) #miles/hour
    final_data['pickup_hour'] = final_data['pickup_datetime'].dt.hour
    final_data['pickup_day_of_week'] = final_data['pickup_datetime'].dt.dayofweek  # 0 = Monday, 6 = Sunday

    logging.info("Removing redundant features...")
    columns_to_drop = [
        'hvfhs_license_num',  # License number is optional
        'dispatching_base_num',  # Dispatching base might not be critical
        'originating_base_num',  # Originating base might not be critical
        'trip_time',  # Redundant because trip_duration exists
        'shared_request_flag',  # Shared ride flag might be optional
        'shared_match_flag',  # Shared match flag might be optional
        'access_a_ride_flag',  # Access-a-ride flag might be optional
        'wav_request_flag',  # WAV request flag might be optional
        'wav_match_flag',  # WAV match flag might be optional
        'LocationID_x',  # Redundant; serves no purpose after processing
        'LocationID_y',  # Redundant; serves no purpose after processing
        'on_scene_datetime',  # Likely redundant, as pickup_datetime is more important
    ]
    final_data = final_data.drop(columns=columns_to_drop)

    logging.info("Feature Engineering Done! Final data column %s", final_data.columns.str)
    return final_data

def save_data(final_data: pd.DataFrame, output_csv: Path, output_parquet: Path) -> None:
    """
    Save preprocessed data to CSV and Parquet formats.

    Args:
        final_data (pd.DataFrame): DataFrame containing preprocessing data.
        output_csv (Path): Path to save the CSV file.
        output_parquet (Path): Path to save the Parquet file.
    """
    logging.info("Saving preprocessed data to CSV and Parquet...")
    print(final_data.columns)
    final_data.to_csv(output_csv, index=False)
    logging.info("CSV saved at: %s", output_csv)
    final_data.to_parquet(output_parquet, index=False, compression="snappy")
    logging.info("Parquet saved at %s", output_parquet)
    logging.info("Preprocessing completed successfully!")

def main():
    """Main pipeline for preprocessing ride data."""
    # Hardcode paths for input and output files
    parquet_file = Path("data/raw/nyc_tripdata_2024-01.parquet")
    shapefile_path = Path("data/raw/taxi_zones/taxi_zones.shp")
    output_csv = Path("data/processed/preprocessed_ride_data.csv")
    output_parquet = Path("data/processed/preprocessed_ride_data.parquet")

    required_columns = ["PULocationID", "DOLocationID", "pickup_datetime", "dropoff_datetime", "trip_miles"]

    raw_data = load_parquet(parquet_file)
    validate_input_columns(raw_data, required_columns)
    taxi_zones = load_shapefile(shapefile_path)
    final_data = map_zone_ids_to_coords(raw_data, taxi_zones)
    final_data = validate_data(final_data)
    final_data = feature_engineering(final_data)
    save_data(final_data, output_csv, output_parquet)

if __name__ == "__main__":
    main()
