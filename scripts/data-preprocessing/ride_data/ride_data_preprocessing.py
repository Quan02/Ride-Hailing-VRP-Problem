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
    logging.info("Mapping Taxi Zone IDs to latitude abd longtitude")

    taxi_zones_combined = taxi_zones.copy()
    taxi_zones_combined["pickup_lat"] = taxi_zones_combined.geometry.centroid.y
    taxi_zones_combined["pickup_lon"] = taxi_zones_combined.geometry.centroid.x
    taxi_zones_combined["dropoff_lat"] = taxi_zones_combined.geometry.centroid.y
    taxi_zones_combined["dropoff_lon"] = taxi_zones_combined.geometry.centroid.x

    final_data: pd.DataFrame = ride_data.merge(
        taxi_zones_combined[["LocationID", "pickup_lat", "pickup_lon"]],
        left_on="PULocationID", right_on="LocationID", how="left"
    ).merge(
        taxi_zones_combined[["LocationID", "dropoff_lat", "dropoff_lon"]],
        left_on="DOLocationID", right_on="LocationID", how="left"
    )

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

    Args:
        final_data (pd.DataFrame): DataFrame containing ride data.
    
    Returns:
        pd.DataFrame: Data with new features.
    """
    logging.info("Adding new features for future analysis...")
    final_data["trip_duration"] = (final_data["dropoff_datetime"] - final_data["pickup_datetime"]).dt.total_seconds()
    final_data["avg_speed"] = final_data["trip_miles"] / (final_data["trip_duration"]/3600) #miles/hour
    final_data['pickup_hour'] = final_data['pickup_datetime'].dt.hour
    final_data['pickup_day_of_week'] = final_data['pickup_datetime'].dt.dayofweek  # 0 = Monday, 6 = Sunday
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
    final_data.to_csv(output_csv, index=False)
    final_data.to_parquet(output_parquet, index=False, compression="snappy")
    logging.info("Preprocessing completed successfully!")
    logging.info("CSV saved at: %s", output_csv)
    logging.info("Parquet saved at %s", output_parquet)

def main(parquet_file: Path, shapefile_path: Path, output_csv: Path, output_parquet: Path) -> None:
    """Main pipeline for preprocessing ride data."""
    required_columns = ["PULocationID", "DOLocationID", "pickup_datetime", "dropoff_datetime", "trip_miles"]
    raw_data = load_parquet(parquet_file)
    validate_input_columns(raw_data, required_columns)
    taxi_zones = load_shapefile(shapefile_path)
    final_data = map_zone_ids_to_coords(raw_data, taxi_zones)
    final_data = validate_data(final_data)
    final_data = feature_engineering(final_data)
    save_data(final_data, output_csv, output_parquet)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess ride-hailing data.")
    parser.add_argument("--parquet", required=True, help="Path to the Parquet file.")
    parser.add_argument("--shapefile", required=True, help="Path to the shapefile.")
    parser.add_argument("--output_csv", required=True, help="Path for the output CSV file.")
    parser.add_argument("--output_parquet", required=True, help="Path for the output Parquet file.")

    args = parser.parse_args()

    main(
        parquet_file=Path(args.parquet),
        shapefile_path=Path(args.shapefile),
        output_csv=Path(args.output_csv),
        output_parquet=Path(args.output_parquet)
    )
