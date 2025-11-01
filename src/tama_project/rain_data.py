import pandas as pd
import os
import geopandas as gpd
from rich.console import Console

console = Console()


def load_rain_data(path: str, parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        console.print(f"'{parquet_path}' already exists, loading parquet")
        rain_df = pd.read_parquet(parquet_path)
    else:
        console.print(f"Loading rain csv files from '{path}'")
        csv_paths = [path + csv for csv in os.listdir(path)]

        df_list = []

        for path in csv_paths:
            df = pd.read_csv(path)
            df_list.append(df)

        rain_df = pd.concat(df_list, ignore_index=True)
        console.print(f"Saving rain data to {parquet_path}")
        rain_df.to_parquet(parquet_path)
    return rain_df


def create_pcd_data(rain_data: pd.DataFrame) -> gpd.GeoDataFrame:
    cols = ["codestacao", "longitude", "latitude", "nome"]
    df_pcd = rain_data[cols].drop_duplicates(ignore_index=True)
    return gpd.GeoDataFrame(
        df_pcd,
        geometry=gpd.points_from_xy(df_pcd.longitude, df_pcd.latitude),
        crs="EPSG:4326",
    )


def calc_daily_rain(rain_data: pd.DataFrame, parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        console.print(f"'{parquet_path}' already exists, loading parquet")
        daily_rain_df = pd.read_parquet(parquet_path)
    else:
        console.print("Calculating daily rain data")
        df_rain = rain_data[rain_data["id_sensor"] == 10]
        df_rain = df_rain.copy()
        df_rain[["data", "hora"]] = df_rain.datahora.str.split(" ", expand=True)

        daily_rain_df = (
            df_rain.groupby(["codestacao", "data"])
            .agg(daily_value=("valor", "sum"))
            .reset_index()
        )

        console.print(f"Saving daily rain data to {parquet_path}")
        daily_rain_df.to_parquet(parquet_path)

    # df_daily_rain.data = pd.to_datetime(df_daily_rain.data)
    return daily_rain_df
