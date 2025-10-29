import geopandas as gpd
import pandas as pd


def select_tti_basin(tti_gpd: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    exclude_ids = ["AC 1.3.2", "3.6.1", "1.3.1", "3.6.9", "1.3.2"]
    return tti_gpd[~tti_gpd["id"].isin(exclude_ids)]


def filter_points(point_data: gpd.GeoDataFrame, basin_data: gpd.GeoDataFrame):
    if point_data.crs != basin_data.crs:
        point_data = point_data.to_crs(basin_data.crs)
    sample_points = gpd.sjoin(point_data, basin_data, predicate="intersects")
    return sample_points[point_data.columns].to_crs("EPSG:4326")


def filter_od_zones(od_zones, basin_data):
    if od_zones.crs != basin_data.crs:
        basin_data = basin_data.to_crs(od_zones.crs)
    dissolved_basin = basin_data.dissolve()
    sample_od_zones = gpd.sjoin(
        od_zones,
        dissolved_basin,
        predicate="intersects",
    )
    return sample_od_zones[od_zones.columns]


def filter_rain_data(rain_data, pcd_sample):
    return rain_data[rain_data["codestacao"].isin(pcd_sample["codestacao"])]
