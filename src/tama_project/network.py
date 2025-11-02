import osmnx as ox
import networkx as nx
import geopandas as gpd
import pandas as pd
import os
from rich.console import Console

console = Console()


def load_network(od_zones_area: gpd.GeoDataFrame, graph_path: str) -> nx.MultiDiGraph:
    """
    Load or create a street network graph for the given area.

    Args:
        od_zones_area: GeoDataFrame with the area to extract the network from
        graph_path: Path to save/load the graph file

    Returns:
        NetworkX MultiDiGraph of the street network
    """
    if os.path.exists(graph_path):
        console.print(f"Graph already exists, loading from '{graph_path}'")
        G = ox.load_graphml(graph_path)
    else:
        console.print("Creating network graph from polygon")
        graph_area = od_zones_area.to_crs(4326).make_valid().union_all().buffer(0)
        G = ox.graph_from_polygon(graph_area, network_type="drive")
        console.print(f"Saving graph to '{graph_path}'")
        ox.save_graphml(G, graph_path)
    return G


def calc_params(G: nx.MultiDiGraph):
    ki = dict(G.degree())
    if G.is_multigraph():
        G = nx.Graph(G)
    ci = dict(nx.clustering(G))
    return pd.DataFrame(
        {
            "node": list(G.nodes()),
            "k": [ki[n] for n in G.nodes()],
            "c": [ci[n] for n in G.nodes()],
        }
    )


def calc_ebc(G, dest_path: str) -> gpd.GeoDataFrame:
    if os.path.exists(dest_path):
        console.print(f"Ebc already calculated, loading from '{dest_path}'")
        G_gdf = gpd.read_file(dest_path)
    else:
        console.print("Calculating Betweenness")
        G_ebc = nx.edge_betweenness_centrality(G, backend="parallel")
        nx.set_edge_attributes(G, G_ebc, "ebc")
        G_gdf = ox.graph_to_gdfs(G, nodes=False, edges=True).reset_index()
        console.print(f"Saving edge gdf to {dest_path}")
    return G_gdf
