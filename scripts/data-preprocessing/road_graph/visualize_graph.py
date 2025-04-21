"""
This script visualizes the road network graph interactively with Folium.
"""

import logging
from pathlib import Path

import osmnx as ox
import networkx as nx
import folium

from graph_utils import load_graph_from_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
GRAPH_FILE: Path = Path("data/processed/new_york_processed_network.graphml")
OUTPUT_HTML_MAP: Path = Path("data/processed/interactive_new_york_map.html")

def visualize_interactive_graph(graph: nx.MultiDiGraph, output_file: Path) -> None:
    """
    Create an interactive map using road network graph with Folium.

    Args:
        graph (nx.MultiDiGraph): Road network graph to be visualized.
        output_file (Path): Path to save the interactive map.
    """
    try:
        # Calculate center of Folium map and apply it.
        graph_center = ox.graph_to_gdfs(graph, nodes=True, edges=False).geometry.unary_union.centroid
        folium_map = folium.Map(location=[graph_center.y, graph_center.x], zoom_start=12)

        logging.info("Adding edges to the interactive map...")
        for u, v, data in graph.edges(data=True):
            # Check if the edge is bidirectional
            if graph.has_edge(v, u):  # If reverse edge exists, it's bidirectional
                edge_color = "blue"
            else:  # If no reverse edge, it's unidirectional
                edge_color = "red"

            # Add the edge with the appropriate color
            if "geometry" in data:
                coords = [(y, x) for x, y in data["geometry"].coords]  # (x, y) to (lat, lon)
                folium.PolyLine(coords, color=edge_color, weight=1).add_to(folium_map)

        logging.info("Adding nodes to the interactive map...")
        for _, data in graph.nodes(data=True):
            folium.CircleMarker(
                location=(data["y"], data["x"]),
                radius=0.2,  # Adjusted size for better visibility
                color="green",
                fill=True,
                fill_color="green"
            ).add_to(folium_map)

        output_file.parent.mkdir(parents=True, exist_ok=True)
        folium_map.save(str(output_file))
        logging.info("Interactive map saved to %s", output_file)
    except Exception as err:
        logging.error("An error occurred while creating the interactive map: %s",err, exc_info=True)

if __name__ == "__main__":
    road_graph = load_graph_from_file(GRAPH_FILE)
    visualize_interactive_graph(road_graph, OUTPUT_HTML_MAP)
