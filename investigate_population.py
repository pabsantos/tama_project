"""
Script to investigate the difference between census tract population
and edge population totals.
"""

import geopandas as gpd
import networkx as nx
import osmnx as ox
from rich.console import Console
from src.tama_project.population import (
    load_census_tracts_intersected,
    assign_population_to_nodes,
    assign_population_to_edges,
)
from src.tama_project.filter_sample import filter_od_zones, select_tti_basin
from src.tama_project.network import load_network, calc_ebc

console = Console()

console.rule("Loading Data")

console.print("Loading geo data")
od_zones_path = "data/od_zones/Zonas_2023.shp"
tti_path = "data/tti_shape/Microbacias_Tamanduatei.shp"

console.print(f"Loading OD zones from '{od_zones_path}'")
od_zones = gpd.read_file(od_zones_path)

console.print(f"Loading TTI shapes from '{tti_path}'")
tti_shapes = gpd.read_file(tti_path)

console.print("Selecting sample microbasins")
tti_sample = select_tti_basin(tti_shapes)

console.print("Selecting sample OD zones")
od_zones_sample = filter_od_zones(od_zones, tti_sample)

console.print("Loading network")
graph_path = "data/network.graphml"
G = load_network(od_zones_sample, graph_path)

console.print("Calculating EBC")
G_path = "data/G_gdf.gpkg"
G_gdf = calc_ebc(G, G_path)

console.rule("Population Analysis")

console.print("Loading census tracts")
census_tracts = load_census_tracts_intersected(od_zones_sample)
total_census_pop = census_tracts["v0001"].sum()
console.print(f"Total population in census tracts: {total_census_pop:,.0f}")

console.print("\nAnalyzing spatial join...")
G_nodes = ox.graph_to_gdfs(G, nodes=True, edges=False)
if G_nodes.crs != census_tracts.crs:
    G_nodes = G_nodes.to_crs(census_tracts.crs)

census_tracts_with_id = census_tracts.copy()
census_tracts_with_id["tract_id"] = range(len(census_tracts_with_id))

nodes_with_tracts = gpd.sjoin(
    G_nodes,
    census_tracts_with_id,
    predicate="within",
    how="left",
)

nodes_without_tract = nodes_with_tracts["tract_id"].isna().sum()
console.print(f"Nodes without assigned tract: {nodes_without_tract} ({nodes_without_tract/len(G_nodes)*100:.2f}%)")

tracts_without_nodes = census_tracts_with_id[
    ~census_tracts_with_id["tract_id"].isin(nodes_with_tracts["tract_id"].dropna())
]
if len(tracts_without_nodes) > 0:
    pop_in_tracts_without_nodes = tracts_without_nodes["v0001"].sum()
    console.print(f"\nTracts without any nodes: {len(tracts_without_nodes)}")
    console.print(f"Population in tracts without nodes: {pop_in_tracts_without_nodes:,.0f}")

console.print("\nAssigning population to nodes")
nodes_with_population = assign_population_to_nodes(G, census_tracts)
total_node_pop = nodes_with_population["population"].sum()
console.print(f"Total population assigned to nodes: {total_node_pop:,.0f}")
console.print(f"Difference from census tracts: {total_census_pop - total_node_pop:,.0f}")

console.print("\nAnalyzing node population distribution...")
nodes_zero_pop = (nodes_with_population["population"] == 0).sum()
console.print(f"Nodes with zero population: {nodes_zero_pop} ({nodes_zero_pop/len(nodes_with_population)*100:.2f}%)")

console.print("\nAssigning population to edges")
G_gdf = assign_population_to_edges(G, nodes_with_population, G_gdf)
total_edge_pop = G_gdf["population"].sum()
console.print(f"Total population assigned to edges: {total_edge_pop:,.0f}")
console.print(f"Difference from nodes: {total_edge_pop - total_node_pop:,.0f}")

console.print("\n" + "="*60)
console.print("SUMMARY")
console.print("="*60)
console.print(f"Census tracts total:     {total_census_pop:>15,.0f}")
console.print(f"Nodes total:             {total_node_pop:>15,.0f}")
console.print(f"Edges total:             {total_edge_pop:>15,.0f}")
console.print(f"\nCensus → Nodes diff:     {total_census_pop - total_node_pop:>15,.0f}")
console.print(f"Nodes → Edges diff:      {total_node_pop - total_edge_pop:>15,.0f}")
console.print(f"Census → Edges diff:      {total_census_pop - total_edge_pop:>15,.0f}")

console.print("\nTheoretical check:")
console.print("If each node with population P and degree k contributes P/k to each edge,")
console.print("the sum of edge populations should equal the sum of node populations.")
console.print("However, if nodes have degree 0 or are isolated, they won't contribute to edges.")

