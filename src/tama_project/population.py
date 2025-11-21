import geopandas as gpd
import os
import networkx as nx
import osmnx as ox
from rich.console import Console

console = Console()


def load_census_tracts_intersected(
    od_zones_sample: gpd.GeoDataFrame,
    population_url: str = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/setores/gpkg/UF/SP/SP_setores_CD2022.gpkg",
    output_path: str = "data/census_tracts_intersected.gpkg",
) -> gpd.GeoDataFrame:
    """
    Load census tracts intersected with OD zones sample. If the output file already exists,
    loads it. Otherwise, downloads census tracts from URL, performs spatial intersection
    with OD zones sample, and saves the result.

    Args:
        od_zones_sample: GeoDataFrame with sample OD zones already loaded
        population_url: URL of the gpkg file with census tracts
        output_path: Path where the intersected census tracts will be saved/loaded

    Returns:
        GeoDataFrame with intersected census tracts containing only geometry and v0001 variable
    """
    if os.path.exists(output_path):
        console.print(
            f"Census tracts file already exists, loading from '{output_path}'"
        )
        result = gpd.read_file(output_path)
        return result

    console.print("Downloading census tracts from IBGE FTP server...")
    census_tracts = gpd.read_file(population_url)
    console.print(f"-> {census_tracts.shape[0]} census tracts loaded")

    if census_tracts.crs != od_zones_sample.crs:
        od_zones_sample = od_zones_sample.to_crs(census_tracts.crs)

    console.print("Performing spatial intersection with OD zones sample")
    intersected = gpd.sjoin(
        census_tracts,
        od_zones_sample,
        predicate="intersects",
    )

    census_tracts_columns = census_tracts.columns.tolist()
    intersected = intersected[census_tracts_columns].copy()

    if "v0001" in intersected.columns:
        result = intersected[["geometry", "v0001"]].copy()
    else:
        result = intersected[["geometry"]].copy()
        console.print("Warning: Variable 'v0001' was not found in the GeoDataFrame")

    console.print(f"Saving intersected census tracts to '{output_path}'")
    result.to_file(output_path, driver="GPKG")
    console.print(f"-> {result.shape[0]} census tracts saved")

    return result


def assign_population_to_nodes(
    G: nx.MultiDiGraph,
    census_tracts: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Assigns population values from census tracts to graph nodes.

    The population of each census tract is divided equally among all nodes
    that fall within that tract.

    Args:
        G: NetworkX MultiDiGraph of the street network
        census_tracts: GeoDataFrame with census tracts and 'v0001' column (population)

    Returns:
        GeoDataFrame with graph nodes and 'population' column
    """
    console.print("Converting graph nodes to GeoDataFrame")
    G_nodes = ox.graph_to_gdfs(G, nodes=True, edges=False)

    if G_nodes.crs != census_tracts.crs:
        console.print(f"Converting nodes CRS from {G_nodes.crs} to {census_tracts.crs}")
        G_nodes = G_nodes.to_crs(census_tracts.crs)

    console.print(f"-> {G_nodes.shape[0]} nodes extracted")

    census_tracts_with_id = census_tracts.copy()
    census_tracts_with_id["tract_id"] = range(len(census_tracts_with_id))

    console.print("Performing spatial join between nodes and census tracts")
    nodes_with_tracts = gpd.sjoin(
        G_nodes,
        census_tracts_with_id,
        predicate="within",
        how="left",
    )

    if "v0001" not in nodes_with_tracts.columns:
        raise ValueError("census_tracts must contain a 'v0001' column")

    console.print(
        "Calculating population per node (dividing equally within each tract)"
    )

    tract_node_counts = nodes_with_tracts.groupby("tract_id").size()

    nodes_with_tracts["tract_node_count"] = nodes_with_tracts["tract_id"].map(
        tract_node_counts
    )
    nodes_with_tracts["population"] = (
        nodes_with_tracts["v0001"] / nodes_with_tracts["tract_node_count"]
    )

    nodes_with_tracts["population"] = nodes_with_tracts["population"].fillna(0)

    node_columns = G_nodes.columns.tolist()
    result = nodes_with_tracts[node_columns + ["population"]].copy()

    if result.index.duplicated().any():
        console.print(
            "Warning: Some nodes appear in multiple tracts, keeping first occurrence"
        )
        result = result[~result.index.duplicated(keep="first")]

    total_population = result["population"].sum()
    console.print(f"-> Total population assigned to nodes: {total_population:.0f}")

    return result


def assign_population_to_edges(
    G: nx.MultiDiGraph,
    nodes_with_population: gpd.GeoDataFrame,
    G_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Assigns population values from nodes to graph edges.

    The population of each node is divided by its degree (number of connections),
    and each edge receives this weighted population from both its endpoints.
    The final edge population is the sum of weighted populations from nodes u and v.

    Args:
        G: NetworkX MultiDiGraph of the street network
        nodes_with_population: GeoDataFrame with graph nodes and 'population' column
        G_gdf: GeoDataFrame with graph edges (must have 'u' and 'v' columns)

    Returns:
        GeoDataFrame with graph edges and added 'population' column
    """
    console.print("Calculating node degrees")
    node_degrees = dict(G.degree())

    console.print("Creating population dictionary from nodes")
    node_population_dict = dict(
        zip(nodes_with_population.index, nodes_with_population["population"])
    )

    console.print("Calculating weighted population per node (population / degree)")
    node_weighted_population = {}
    for node_id, population in node_population_dict.items():
        degree = node_degrees.get(node_id, 1)
        if degree == 0:
            degree = 1
        node_weighted_population[node_id] = population / degree

    console.print("Assigning population to edges")
    if "u" not in G_gdf.columns or "v" not in G_gdf.columns:
        raise ValueError(
            "G_gdf must have 'u' and 'v' columns (use reset_index() if needed)"
        )

    G_gdf_result = G_gdf.copy()

    G_gdf_result["pop_u"] = G_gdf_result["u"].map(
        lambda x: node_weighted_population.get(x, 0)
    )
    G_gdf_result["pop_v"] = G_gdf_result["v"].map(
        lambda x: node_weighted_population.get(x, 0)
    )

    G_gdf_result["population"] = G_gdf_result["pop_u"] + G_gdf_result["pop_v"]

    G_gdf_result = G_gdf_result.drop(columns=["pop_u", "pop_v"])

    total_edge_population = G_gdf_result["population"].sum()
    console.print(f"-> Total population assigned to edges: {total_edge_population:.0f}")

    return G_gdf_result
