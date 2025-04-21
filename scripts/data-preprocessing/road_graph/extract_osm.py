"""This module extract OpenStreetMap Data and download specific region as graph."""

import logging
from pathlib import Path
from typing import Literal

import osmnx as ox

from graph_utils import save_graph

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOCATION: str = "New York City, USA"
NETWORK_TYPE: Literal['drive', 'walk', 'bike', 'all'] = "drive"
OUTPUT_FILE: Path = Path("data/raw/new_york_drive_network.graphml")

def extract_road_network(location_name: str,
                         network_type: Literal['drive','walk','bike','all'],
                         output_file: Path) -> None:
    """
    Extract the road network for given location and save it as a GraphML file.

    Args:
        location_name (str): Name of the location (e.g., "New York City, USA").
        network_type (str): Type of network to extract ("drive", "walk", "bike", or "all").
        output_file (Path): Path to save the GraphML File
    """
    try:
        logging.info("Extract %s for location %s", network_type, location_name)

        graph = ox.graph_from_place(location_name, network_type= network_type)

        save_graph(graph=graph, output_path=OUTPUT_FILE)
        logging.info("Road network saved successfully to %s", output_file)
    except ValueError as err:  # For invalid location_name or network_type
        logging.error("ValueError occurred: %s", err, exc_info=True)
    except FileNotFoundError as err:  # If the OUTPUT_FILE path is invalid
        logging.error("FileNotFoundError occurred: %s", err, exc_info=True)
    except Exception as err:  # Catch any other unexpected exceptions
        logging.error("Unexpected error occurred: %s", err, exc_info=True)

if __name__ == "__main__":
    extract_road_network(LOCATION, NETWORK_TYPE, OUTPUT_FILE)
