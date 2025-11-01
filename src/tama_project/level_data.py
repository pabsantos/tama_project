import pandas as pd
from rich.console import Console
import os

console = Console()


def load_level_data(input_path: str, parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        console.print(f"'{parquet_path}' already exists, loading now")
        level_df = pd.read_parquet(parquet_path)
    else:
        console.print(f"Loading from {input_path}")
        level_df = pd.read_csv(input_path)
        console.print(f"Saving to {parquet_path}")
        level_df.to_parquet(parquet_path)
    return level_df


def calc_daily_level(level_data: pd.DataFrame, parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        console.print(f"'{parquet_path}' already exists, loading parquet")
        daily_level_df = pd.read_parquet(parquet_path)
    else:
        console.print("Calculating daily level data")
        df_level = level_data.copy()
        df_level["timestamp"] = pd.to_datetime(df_level["timestamp"])
        df_level["data"] = df_level["timestamp"].dt.date.astype(str)

        daily_level_df = (
            df_level.groupby(["station", "data"])
            .agg(daily_value=("value", "mean"))
            .reset_index()
        )

        console.print(f"Saving daily level data to {parquet_path}")
        daily_level_df.to_parquet(parquet_path)

    return daily_level_df
