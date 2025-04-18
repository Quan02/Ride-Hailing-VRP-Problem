"""
This script preprocesses the road network graph.

It performs the following tasks:
- Adds additional attributes to each edge (e.g., custom weights or metadata).
- Fills in missing geometry data for edges that do not have any.
- Computes the graph centroid for visualization or spatial reference.

More preprocessing steps may be added as needed.
"""

import networkx as nx
from shapely.geometry import LineString
import logging
from pathlib import Path

from graph_utils import load_graph_from_file, save_graph

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
GRAPH_FILE: Path = Path("data/raw/new_york_drive_network.graphml")
PROCESSED_GRAPH_FILE: Path = Path("data/processed/new_york_processed_network.graphml")

def add_missing_geometry(graph: nx.MultiDiGraph)->nx.MultiDiGraph:
    """
    Add missing geometry to edges in a road network graph.

    Args:
        graph (nx.MultiDiGraph): The input road network graph with node coordinates.

    Returns:
        nx.MultiDiGraph: Graph with edges geometry data.
    """
    for u, v, data in graph.edges(data=True):
        if "geometry" not in data or data["geometry"] is None:
            point_start = (graph.nodes[u]["x"], graph.nodes[u]["y"])
            point_end = (graph.nodes[v]["x"], graph.nodes[v]["y"])
            data["geometry"] = LineString([point_start, point_end])
    return graph

if __name__=="__main__":
    road_graph: nx.MultiDiGraph = load_graph_from_file(GRAPH_FILE)
    road_graph_processed = add_missing_geometry(road_graph)
    save_graph(road_graph_processed, PROCESSED_GRAPH_FILE)