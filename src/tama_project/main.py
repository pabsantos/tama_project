from tama_project.rain_data import load_rain_data, calc_daily_rain
from tama_project.filter_sample import (
    select_tti_basin,
    filter_points,
    filter_flood_points,
    filter_od_zones,
    filter_rain_data,
    filter_level_data,
)
import geopandas as gpd
import networkx as nx
from rich.console import Console
from tama_project.network import load_network, calc_params, calc_ebc
from tama_project.level_data import load_level_data, calc_daily_level
from tama_project.analysis import (
    fill_missing_dates,
    plot_rain_time_series,
    plot_spearman_correlogram,
    plot_mean_rain_time_series,
    plot_rain_flood_violin,
    plot_level_time_series,
    plot_spearman_correlogram_level,
    plot_mean_level_time_series,
    plot_network_scatter,
    calc_level_zscore_mean_series,
    plot_zscore_mean_time_series,
    plot_zscore_rain_correlation,
    plot_zscore_flood_violin,
    count_flood_occurrences_by_link,
    plot_ebc_flood_scatter,
)
from tama_project.maps import (
    plot_study_area_map,
    plot_network_flood_map,
    plot_network_ebc_map,
    plot_network_ebc_flood_highlight_map,
)


def main() -> None:
    # setup ---

    console = Console()
    nxp_config = nx.config.backends.parallel
    nxp_config.n_jobs = 10
    nxp_config.verbose = 50

    # data ---

    ## rain data ---

    console.rule("Loading rain data")

    rain_csv_path = "data/rain.csv"
    rain_data_path = "data/rain.parquet"
    daily_rain_data_path = "data/daily_rain.parquet"

    console.print("Loading rain data")
    df_rain = load_rain_data(rain_csv_path, rain_data_path)

    console.print("Calculating daily rain data")
    df_daily_rain = calc_daily_rain(df_rain, daily_rain_data_path)

    ## Water level data ---

    console.rule("Loading water level data")

    level_csv_path = "data/level.csv"
    level_parquet_path = "data/level.parquet"
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
    pcd_level_path = "data/pcd_level/CoordenadasSerieHistorica_2015-2025.shp"

    console.print(f"Loading flood points from '{flood_points_path}'")
    flood_points = gpd.read_file(flood_points_path)

    console.print(f"Loading OD zones from '{od_zones_path}'")
    od_zones = gpd.read_file(od_zones_path)

    console.print(f"Loading TTI shapes from '{tti_path}'")
    tti_shapes = gpd.read_file(tti_path)

    console.print(f"Loading city limits from '{sp_limits_path}'")
    sp_limits = gpd.read_file(sp_limits_path)

    console.print(f"Loading PCD level data from '{pcd_level_path}'")
    pcd_level = gpd.read_file(pcd_level_path)

    # study sample ---

    console.rule("Filtering data")

    console.print("Selecting sample microbasins")
    tti_sample = select_tti_basin(tti_shapes)

    console.print("Selecting sample PCDs")
    pcd_sample = filter_points(pcd_level, tti_sample)

    console.print("Selecting sample flood points")
    sample_flood_points = filter_flood_points(flood_points, tti_sample)

    console.print("Selecting sample OD zones")
    od_zones_sample = filter_od_zones(od_zones, tti_sample)

    console.print("Selecting sample rain data")
    sample_rain_df = filter_rain_data(df_daily_rain, pcd_sample)

    console.print("Filling missing dates in rain data")
    sample_rain_df = fill_missing_dates(sample_rain_df)

    console.print("Selecting sample level data")
    sample_level_df = filter_level_data(df_daily_level, pcd_sample)

    # Street network ---

    console.rule("Street network")

    console.print("Loading network")
    graph_path = "data/network.graphml"
    G = load_network(od_zones_sample, graph_path)

    console.print("Calculating network nodes degree and clustering")
    G_params = calc_params(G)

    console.print("Calculating edge betweenness centrality and converting to gdf")
    G_path = "data/G_gdf.gpkg"
    G_gdf = calc_ebc(G, G_path)

    console.print("Counting flood occurrences by link")
    G_gdf = count_flood_occurrences_by_link(G_gdf, sample_flood_points)
    # console.print("Saving G_gdf with flood count to 'data/G_gdf_with_flood_count.gpkg'")
    # G_gdf.to_file("data/G_gdf_with_flood_count.gpkg", driver="GPKG")

    # Analysis ---

    console.rule("Data Analysis")

    console.print("Plotting rain time series")
    plot_rain_time_series(sample_rain_df, figsize=(8, 10))

    console.print("Plotting Spearman correlogram of rain data between pcds")
    plot_spearman_correlogram(sample_rain_df, figsize=(8, 8))

    console.print("Plotting mean rain time series")
    plot_mean_rain_time_series(sample_rain_df, figsize=(8, 5))

    console.print("Plotting rain flood violin plot")
    plot_rain_flood_violin(sample_rain_df, sample_flood_points, figsize=(8, 5))

    console.print("Plotting level time series")
    plot_level_time_series(sample_level_df, figsize=(8, 10))

    console.print("Plotting Spearman correlogram of level data between pcds")
    plot_spearman_correlogram_level(sample_level_df, figsize=(8, 8))

    console.print("Plotting mean level time series")
    plot_mean_level_time_series(sample_level_df, figsize=(8, 5))

    console.print("Calculating level z-score mean series")
    df_level_zscore = calc_level_zscore_mean_series(sample_level_df)

    console.print("Plotting z-score mean time series")
    plot_zscore_mean_time_series(df_level_zscore, figsize=(8, 5))

    console.print("Plotting z-score and rain correlation")
    plot_zscore_rain_correlation(df_level_zscore, sample_rain_df, figsize=(8, 5))

    console.print("Plotting z-score flood violin plot")
    plot_zscore_flood_violin(df_level_zscore, sample_flood_points, figsize=(8, 5))

    console.print("Plotting network params scatter")
    plot_network_scatter(G_params, figsize=(8, 5))

    console.print("Plotting EBC vs flood count scatter")
    plot_ebc_flood_scatter(G_gdf, figsize=(8, 5))

    # Maps ---

    console.rule("Plotting maps")

    console.print("Plotting study area map")
    plot_study_area_map(tti_sample, od_zones_sample, pcd_sample, figsize=(13, 8))

    console.print("Plotting network and flood points map")
    plot_network_flood_map(G, sample_flood_points, tti_sample, figsize=(13, 8))

    console.print("Plotting network EBC map")
    plot_network_ebc_map(G_gdf, tti_sample, figsize=(13, 8))

    console.print("Plotting network EBC and flood highlight map")
    plot_network_ebc_flood_highlight_map(G_gdf, tti_sample, figsize=(13, 8))


if __name__ == "__main__":
    main()
