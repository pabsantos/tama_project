import math

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


def calc_network_comparison(G: nx.MultiDiGraph) -> pd.DataFrame:
    G_undirected = nx.Graph(G)
    largest_cc = max(nx.connected_components(G_undirected), key=len)
    G_cc = G_undirected.subgraph(largest_cc).copy()

    N = G_cc.number_of_nodes()
    L = G_cc.number_of_edges()

    k_mean = 2 * L / N
    c_mean = nx.average_clustering(G_cc)
    l_mean = nx.average_shortest_path_length(G_cc)

    p = 2 * L / (N * (N - 1))
    k_rand = p * (N - 1)
    c_rand = p
    l_rand = math.log(N) / math.log(k_mean)

    return pd.DataFrame(
        {
            "parameter": ["N", "L", "<k>", "<c>", "<l>"],
            "real": [N, L, k_mean, c_mean, l_mean],
            "random": [N, L, k_rand, c_rand, l_rand],
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
