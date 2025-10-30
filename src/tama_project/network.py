import osmnx as ox
import networkx as nx
import geopandas as gpd
import pandas as pd


def load_network(od_zones_area: gpd.GeoDataFrame):
    graph_area = od_zones_area.to_crs(4326).make_valid().union_all().buffer(0)
    return ox.graph_from_polygon(graph_area, network_type="drive")


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


def calc_bc():
    pass
