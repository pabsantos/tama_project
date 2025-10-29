from rain_data import load_rain_data, create_pcd_data, calc_daily_rain
from filter_sample import (
    select_tti_basin,
    filter_points,
    filter_od_zones,
    filter_rain_data,
)
import os
import pandas as pd
import geopandas as gpd
from rich.console import Console

console = Console()

# data ---

## rain data ---

console.rule("Loading input data", align="left")

rain_data_path = "data/rain.parquet"
rain_files_path = "data/ped_rain/"

console.print(f"Loading rain csv files from '{rain_files_path}'")
full_df = load_rain_data(rain_files_path)

if os.path.exists(rain_data_path):
    console.print(f"'{rain_data_path}' already exists, loading parquet")
    rain_df = pd.read_parquet(rain_data_path)
else:
    console.print(f"Calculating from '{rain_files_path}'")
    rain_df = calc_daily_rain(full_df)
    rain_df.to_parquet(path=rain_data_path)

console.print("Extracting 'pcd_data'")
pcd_data = create_pcd_data(full_df)

## geo data ---

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

console.rule("Filtering data", align="left")

console.print("Selecting sample microbasins")
tti_sample = select_tti_basin(tti_shapes)

console.print("Selecting sample PCDs")
pcd_sample = filter_points(pcd_data, tti_sample)

console.print("Selecting sample flood points")
sample_flood_points = filter_points(flood_points, tti_sample)

console.print("Selecting sample OD zones")
od_zones_sample = filter_od_zones(od_zones, tti_sample)

console.print("Selecting sample rain data")
sample_rain_df = filter_rain_data(rain_df, pcd_data)
