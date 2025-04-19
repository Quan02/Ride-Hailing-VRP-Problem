"""
This script validates the processed road network graph.

It performs the following tasks:
- Ensure no isolated subgraph inside road network graph.
- Ensure each edges got geometry attributes in their data.

More preprocessing steps may be added as needed.
"""

import logging
from pathlib import Path

import networkx as nx

from graph_utils import load_graph_from_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
GRAPH_FILE = Path("data/processed/new_york_processed_network.graphml")

def validate_connectivity(graph: nx.MultiDiGraph) -> None:
    """
    Ensure the graph does not contain isolated subgraphs.

    Args:
        graph (nx.MultiDiGraph): Road Network Graph to be validated.
    """
    if not nx.is_weakly_connected(graph):
        logging.warning("Graph is not fully connected.")
        return
    logging.info("Graph is connected.")

def validate_geometry(graph: nx.MultiDiGraph) -> None:
    """
    Ensure every edge in the graph has geometry attribute in data.

    Args:
        graph (nx.MultiDiGraph): Road Network Graph to be validated.
    """
    missing: int = 0
    for _, _, data in graph.edges(data=True):
        if "geometry" not in data or data["geometry"] is None:
            missing += 1
    if missing:
        logging.warning("Found %s edges with missing geometry.", missing)
        return
    logging.info("All edges have geometry.")

if __name__=="__main__":
    road_graph = load_graph_from_file(GRAPH_FILE)

    logging.info("Start validating graph from %s.", GRAPH_FILE)
    validate_connectivity(road_graph)
    validate_geometry(road_graph)
    logging.info("Finished validate road network graph.")
