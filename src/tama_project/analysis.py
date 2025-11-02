import pandas as pd
import matplotlib.pyplot as plt


def fill_missing_dates(sample_rain_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures all dates have a value for each station.
    If there are missing dates, inserts the date and sets the value to 0.

    Args:
        sample_rain_df: DataFrame with columns 'codestacao', 'data' and
            'daily_value'

    Returns:
        DataFrame with all dates filled for each station
    """
    df = sample_rain_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    result_list = []

    for codestacao in df["codestacao"].unique():
        station_df = df[df["codestacao"] == codestacao].copy()
        min_date = station_df["data"].min()
        max_date = station_df["data"].max()
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")
        full_dates_df = pd.DataFrame(
            {
                "codestacao": codestacao,
                "data": date_range,
            }
        )

        merged_df = full_dates_df.merge(
            station_df[["data", "daily_value"]], on="data", how="left"
        )
        merged_df["daily_value"] = merged_df["daily_value"].fillna(0)
        result_list.append(merged_df)

    result_df = pd.concat(result_list, ignore_index=True)
    result_df = result_df.sort_values(["codestacao", "data"]).reset_index(drop=True)
    return result_df


def plot_rain_time_series(
    sample_rain_df: pd.DataFrame, figsize: tuple = (15, 10)
) -> None:
    """
    Plots time series of daily rain values for each station in a single
    plot.

    Args:
        sample_rain_df: DataFrame with columns 'codestacao', 'data' and
            'daily_value'
        figsize: Figure size tuple (width, height)
    """
    df = sample_rain_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    stations = df["codestacao"].unique()

    if len(stations) == 0:
        return

    fig, ax = plt.subplots(figsize=figsize)

    for codestacao in stations:
        station_df = df[df["codestacao"] == codestacao].copy()
        station_df = station_df.sort_values("data")
        ax.plot(
            station_df["data"],
            station_df["daily_value"],
            label=f"Station {codestacao}",
            linewidth=0.8,
        )

    ax.set_xlabel("Date")
    ax.set_ylabel("Daily Rain (mm)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    ax.tick_params(axis="x")

    plt.tight_layout()
    plt.savefig("plot/rain_time_series.png")
    plt.show()
