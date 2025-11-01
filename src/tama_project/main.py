from rain_data import load_rain_data, create_pcd_data, calc_daily_rain
from filter_sample import (
    select_tti_basin,
    filter_points,
    filter_od_zones,
    filter_rain_data,
)
import geopandas as gpd
import networkx as nx
from rich.console import Console
from network import load_network, calc_params, calc_ebc
from level_data import load_level_data, calc_daily_level

# setup ---

console = Console()
nxp_config = nx.config.backends.parallel
nxp_config.n_jobs = 10
nxp_config.verbose = 50

# data ---

## rain data ---

console.rule("Loading rain data")

rain_data_path = "data/rain.parquet"
daily_rain_data_path = "data/daily_rain.parquet"
rain_files_path = "data/ped_rain/"

console.print("Loading rain data")
df_rain = load_rain_data(rain_files_path, rain_data_path)

console.print("Calculating daily rain data")
df_daily_rain = calc_daily_rain(df_rain, daily_rain_data_path)

console.print("Extracting 'pcd_data'")
pcd_data = create_pcd_data(df_rain)

## Water level data ---

console.rule("Loading water level data")

level_csv_path = "data/water_level.csv"
level_parquet_path = "data/water_level.parquet"
daily_level_data_path = "data/daily_level.parquet"

console.print("Loading water level input data")
df_level = load_level_data(level_csv_path, level_parquet_path)

console.print("Calculating daily water level data")
df_daily_level = calc_daily_level(df_level, daily_level_data_path)

## geo data ---

console.rule("Loading geo data")

flood_points_path = "data/flood_cge/2018_a_04-2025_TTI_EDITADO.shp"
od_zones_path = "data/od_zones/Zonas_2023.shp"
tti_path = "data/tti_shape/Microbacias_Tamanduatei.shp"
sp_limits_path = "data/sp_limits/Municipios_2023.shp"

console.print(f"Loading flood points from '{flood_points_path}'")
flood_points = gpd.read_file(flood_points_path)

console.print(f"Loading OD zones from '{od_zones_path}'")
od_zones = gpd.read_file(od_zones_path)

console.print(f"Loading TTI shapes from '{tti_path}'")
tti_shapes = gpd.read_file(tti_path)

console.print(f"Loading city limits from '{sp_limits_path}'")
sp_limits = gpd.read_file(sp_limits_path)

# study sample ---

console.rule("Filtering data")

console.print("Selecting sample microbasins")
tti_sample = select_tti_basin(tti_shapes)

console.print("Selecting sample PCDs")
pcd_sample = filter_points(pcd_data, tti_sample)

console.print("Selecting sample flood points")
sample_flood_points = filter_points(flood_points, tti_sample)

console.print("Selecting sample OD zones")
od_zones_sample = filter_od_zones(od_zones, tti_sample)

console.print("Selecting sample rain data")
sample_rain_df = filter_rain_data(df_daily_rain, pcd_data)

# Street network ---

console.rule("Street network")

console.print("Loading network")
G = load_network(od_zones_sample)

console.print("Calculating network stats")
# console.print(ox.basic_stats(G))

console.print("Calculating network nodes degree and clustering")
G_params = calc_params(G)
# console.print(G_params)

console.print("Calculating edge betweenness centrality and converting to gdf")

G_path = "data/G_gdf.gpkg"
G_gdf = calc_ebc(G, G_path)
