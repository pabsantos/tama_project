import pandas as pd
import os
import geopandas as gpd


def load_rain_data(path: str) -> pd.DataFrame:
    csv_paths = [path + csv for csv in os.listdir(path)]

    df_list = []

    for path in csv_paths:
        df = pd.read_csv(path)
        df_list.append(df)

    return pd.concat(df_list, ignore_index=True)


def create_pcd_data(rain_data: pd.DataFrame) -> gpd.GeoDataFrame:
    cols = ["codestacao", "longitude", "latitude", "nome"]
    df_pcd = rain_data[cols].drop_duplicates(ignore_index=True)
    return gpd.GeoDataFrame(
        df_pcd,
        geometry=gpd.points_from_xy(df_pcd.longitude, df_pcd.latitude),
        crs="EPSG:4326",
    )


def calc_daily_rain(rain_data: pd.DataFrame) -> pd.DataFrame:
    df_rain = rain_data[rain_data["id_sensor"] == 10]
    df_rain = df_rain.copy()
    df_rain[["data", "hora"]] = df_rain.datahora.str.split(" ", expand=True)

    df_daily_rain = (
        df_rain.groupby(["codestacao", "data"])
        .agg(daily_value=("valor", "sum"))
        .reset_index()
    )

    # df_daily_rain.data = pd.to_datetime(df_daily_rain.data)
    return df_daily_rain
