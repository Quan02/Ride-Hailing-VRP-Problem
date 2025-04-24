"""
This script provide utilities function for ride data preprocessing.
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
