import pandas as pd
import os
from rich.console import Console

console = Console()


def load_rain_data(input_path: str, parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        console.print(f"'{parquet_path}' already exists, loading parquet")
        rain_df = pd.read_parquet(parquet_path)
    else:
        console.print(f"Loading rain data from '{input_path}'")
        rain_df = pd.read_csv(input_path)
        console.print(f"Saving rain data to {parquet_path}")
        rain_df.to_parquet(parquet_path)
    return rain_df


def calc_daily_rain(rain_data: pd.DataFrame, parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        console.print(f"'{parquet_path}' already exists, loading parquet")
        daily_rain_df = pd.read_parquet(parquet_path)
    else:
        df_rain = rain_data.copy()
        df_rain["timestamp"] = pd.to_datetime(df_rain["timestamp"])
        df_rain["data"] = df_rain["timestamp"].dt.date.astype(str)

        daily_rain_df = (
            df_rain.groupby(["station", "data"])
            .agg(daily_value=("value", "sum"))
            .reset_index()
        )

        console.print(f"Saving daily rain data to {parquet_path}")
        daily_rain_df.to_parquet(parquet_path)

    # df_daily_rain.data = pd.to_datetime(df_daily_rain.data)
    return daily_rain_df
