"""This module provides utilities for graph preprocessing scripts."""

import osmnx as ox
import networkx as nx
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_graph_from_file(file_path: Path) -> nx.MultiDiGraph:
    """
    Load a raod network graph from a GraphML file.

    Args:
        file_path (Path): Path of the .graphml file.

    Returns:
        nx.MultiDiGraph: The loaded road network graph.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"GraphML File not found: {file_path}")
    logging.info(f"Loading graph from: {file_path}")
    return ox.load_graphml(filepath=str(file_path))

def save_graph(graph: nx.MultiDiGraph, output_path: Path)->None:
    """
    Save a road network graph to a GraphML file.

    Args:
        graph (nx.MultiDiGraph): Road network graph to save.
        output_path (Path): File path where the graph will be saved.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ox.save_graphml(graph, filepath=str(output_path))
    logging.info(f"Graph saved successfully to: {output_path}")
