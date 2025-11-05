import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Any


def fill_missing_dates(sample_rain_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures all dates have a value for each station.
    If there are missing dates, inserts the date and sets the value to 0.

    Args:
        sample_rain_df: DataFrame with columns 'station', 'data' and
            'daily_value'

    Returns:
        DataFrame with all dates filled for each station
    """
    df = sample_rain_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    result_list = []

    for station in df["station"].unique():
        station_df = df[df["station"] == station].copy()
        min_date = station_df["data"].min()
        max_date = station_df["data"].max()
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")
        full_dates_df = pd.DataFrame(
            {
                "station": station,
                "data": date_range,
            }
        )

        merged_df = full_dates_df.merge(
            station_df[["data", "daily_value"]], on="data", how="left"
        )
        merged_df["daily_value"] = merged_df["daily_value"].fillna(0)
        result_list.append(merged_df)

    result_df = pd.concat(result_list, ignore_index=True)
    result_df = result_df.sort_values(["station", "data"]).reset_index(drop=True)
    return result_df


def plot_rain_time_series(
    sample_rain_df: pd.DataFrame, figsize: tuple = (15, 10)
) -> None:
    """
    Plots time series of daily rain values for each station in separate
    subplots stacked vertically.

    Args:
        sample_rain_df: DataFrame with columns 'station', 'data' and
            'daily_value'
        figsize: Figure size tuple (width, height)
    """
    df = sample_rain_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    stations = df["station"].unique()

    if len(stations) == 0:
        return

    n_stations = len(stations)
    fig, axes = plt.subplots(n_stations, 1, figsize=figsize, sharex=True, sharey=True)

    if n_stations == 1:
        axes = [axes]

    for idx, station in enumerate(stations):
        station_df = df[df["station"] == station].copy()
        station_df = station_df.sort_values("data")
        axes[idx].plot(
            station_df["data"],
            station_df["daily_value"],
            linewidth=0.8,
        )
        axes[idx].set_ylabel(f"{station}\nDaily rainfall (mm)")
        axes[idx].grid(True, alpha=0.3)
        axes[idx].tick_params(axis="x")

    axes[-1].set_xlabel("Date")

    plt.tight_layout()
    plt.savefig("plot/rain_time_series.png", dpi=300)
    # plt.show()


def plot_spearman_correlogram(
    sample_rain_df: pd.DataFrame, figsize: tuple = (10, 8)
) -> None:
    """
    Creates a Spearman correlation correlogram comparing different stations.

    Args:
        sample_rain_df: DataFrame with columns 'station', 'data' and
            'daily_value'
        figsize: Figure size tuple (width, height)
    """
    df = sample_rain_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    pivot_df = df.pivot(index="data", columns="station", values="daily_value")

    corr_matrix = pivot_df.corr(method="spearman")

    fig, ax = plt.subplots(figsize=figsize)

    sns.heatmap(
        corr_matrix,
        annot=True,
        fmt=".2f",
        cmap="coolwarm",
        center=0,
        square=True,
        linewidths=0.5,
        cbar_kws={"label": "Spearman Correlation"},
        ax=ax,
    )

    plt.tight_layout()
    plt.savefig("plot/spearman_correlogram.png", dpi=300)


def plot_mean_rain_time_series(
    sample_rain_df: pd.DataFrame, figsize: tuple = (13, 8)
) -> None:
    """
    Plots time series of mean daily rain values averaged across all stations.

    Args:
        sample_rain_df: DataFrame with columns 'station', 'data' and
            'daily_value'
        figsize: Figure size tuple (width, height)
    """
    df = sample_rain_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    mean_rain = df.groupby("data")["daily_value"].mean().reset_index()
    mean_rain = mean_rain.sort_values("data")

    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(mean_rain["data"], mean_rain["daily_value"], linewidth=0.8)
    ax.set_xlabel("Date")
    ax.set_ylabel("Mean daily rainfall (mm)")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("plot/mean_rain_time_series.png", dpi=300)
    # plt.show()


def plot_level_time_series(
    sample_level_df: pd.DataFrame, figsize: tuple = (15, 10)
) -> None:
    """
    Plots time series of daily water level values for each station in separate
    subplots stacked vertically.

    Args:
        sample_level_df: DataFrame with columns 'station', 'data' and
            'daily_value'
        figsize: Figure size tuple (width, height)
    """
    df = sample_level_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    stations = df["station"].unique()

    if len(stations) == 0:
        return

    n_stations = len(stations)
    fig, axes = plt.subplots(n_stations, 1, figsize=figsize, sharex=True, sharey=True)

    if n_stations == 1:
        axes = [axes]

    for idx, station in enumerate(stations):
        station_df = df[df["station"] == station].copy()
        station_df = station_df.sort_values("data")
        axes[idx].plot(
            station_df["data"],
            station_df["daily_value"],
            linewidth=0.8,
        )
        axes[idx].set_ylabel(f"{station}")
        axes[idx].grid(True, alpha=0.3)
        axes[idx].tick_params(axis="x")

    axes[-1].set_xlabel("Date")

    plt.tight_layout()
    plt.savefig("plot/level_time_series.png", dpi=300)
    # plt.show()


def plot_spearman_correlogram_level(
    sample_level_df: pd.DataFrame, figsize: tuple = (10, 8)
) -> None:
    """
    Creates a Spearman correlation correlogram comparing different stations
    for water level data.

    Args:
        sample_level_df: DataFrame with columns 'station', 'data' and
            'daily_value'
        figsize: Figure size tuple (width, height)
    """
    df = sample_level_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    pivot_df = df.pivot(index="data", columns="station", values="daily_value")

    corr_matrix = pivot_df.corr(method="spearman")

    fig, ax = plt.subplots(figsize=figsize)

    sns.heatmap(
        corr_matrix,
        annot=True,
        fmt=".2f",
        cmap="coolwarm",
        center=0,
        square=True,
        linewidths=0.5,
        cbar_kws={"label": "Spearman Correlation"},
        ax=ax,
    )

    plt.tight_layout()
    plt.savefig("plot/spearman_correlogram_level.png", dpi=300)


def plot_mean_level_time_series(
    sample_level_df: pd.DataFrame, figsize: tuple = (13, 8)
) -> None:
    """
    Plots time series of mean daily water level values averaged across all stations.

    Args:
        sample_level_df: DataFrame with columns 'station', 'data' and
            'daily_value'
        figsize: Figure size tuple (width, height)
    """
    df = sample_level_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    mean_level = df.groupby("data")["daily_value"].mean().reset_index()
    mean_level = mean_level.sort_values("data")

    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(mean_level["data"], mean_level["daily_value"], linewidth=0.8)
    ax.set_xlabel("Date")
    ax.set_ylabel("Mean daily water level")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("plot/mean_level_time_series.png", dpi=300)
    # plt.show()


def plot_network_scatter(G_params: pd.DataFrame, figsize: tuple = (10, 8)) -> None:
    """
    Plots a scatter plot of network parameters with k (degree) on x-axis
    and c (clustering coefficient) on y-axis.

    Args:
        G_params: DataFrame with columns 'node', 'k' (degree), and 'c' (clustering)
        figsize: Figure size tuple (width, height)
    """
    fig, ax = plt.subplots(figsize=figsize)

    ax.scatter(G_params["k"], G_params["c"], alpha=0.5, s=20)
    ax.set_xlabel(r"$k_i$")
    ax.set_ylabel(r"$c_i$")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.3, which="major")
    ax.grid(True, alpha=0.1, which="minor")

    plt.tight_layout()
    plt.savefig("plot/network_scatter.png", dpi=300)
    # plt.show()


def calc_level_zscore_mean_series(
    sample_level_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculates z-scores for river level data separately for each station,
    and exports a time series with the mean of these z-scores across all
    stations for each date.

    Args:
        sample_level_df: DataFrame with columns 'station', 'data' and
            'daily_value'
        output_path: Path to save the output parquet file

    Returns:
        DataFrame with columns 'data' and 'mean_zscore' containing the time
        series of mean z-scores across all stations
    """
    df = sample_level_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    station_stats = (
        df.groupby("station")["daily_value"]
        .agg(["mean", "std"])
        .reset_index()
        .rename(columns={"mean": "station_mean", "std": "station_std"})
    )

    station_stats["station_std"] = station_stats["station_std"].replace(0, 1).fillna(1)

    df = df.merge(station_stats, on="station", how="left")

    df["zscore"] = (df["daily_value"] - df["station_mean"]) / df["station_std"]

    mean_zscore_series = (
        df.groupby("data")["zscore"]
        .mean()
        .reset_index()
        .rename(columns={"zscore": "mean_zscore"})
    )
    mean_zscore_series = mean_zscore_series.sort_values("data").reset_index(drop=True)

    return mean_zscore_series


def plot_zscore_mean_time_series(
    zscore_df: pd.DataFrame, figsize: tuple = (13, 8)
) -> None:
    """
    Plots time series of mean z-score values averaged across all stations.

    Args:
        zscore_df: DataFrame with columns 'data' and 'mean_zscore'
        figsize: Figure size tuple (width, height)
    """
    df = zscore_df.copy()
    df["data"] = pd.to_datetime(df["data"])
    df = df.sort_values("data")

    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(df["data"], df["mean_zscore"], linewidth=0.8)
    ax.set_xlabel("Date")
    ax.set_ylabel(r"Mean $z$-score")
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color="r", linestyle="--", linewidth=0.8, alpha=0.5)

    plt.tight_layout()
    plt.savefig("plot/zscore_mean_time_series.png", dpi=300)
    # plt.show()


def plot_zscore_rain_correlation(
    zscore_df: pd.DataFrame,
    sample_rain_df: pd.DataFrame,
    figsize: tuple = (10, 8),
) -> None:
    """
    Plots correlation between mean z-score of water level and mean daily rain.

    Args:
        zscore_df: DataFrame with columns 'data' and 'mean_zscore'
        sample_rain_df: DataFrame with columns 'station', 'data' and 'daily_value'
        figsize: Figure size tuple (width, height)
    """
    zscore = zscore_df.copy()
    zscore["data"] = pd.to_datetime(zscore["data"])

    rain = sample_rain_df.copy()
    rain["data"] = pd.to_datetime(rain["data"])
    mean_rain = rain.groupby("data")["daily_value"].mean().reset_index()
    mean_rain = mean_rain.sort_values("data")

    merged = zscore.merge(mean_rain, on="data", how="inner")
    merged = merged.dropna()

    corr = merged["mean_zscore"].corr(merged["daily_value"], method="spearman")

    fig, ax = plt.subplots(figsize=figsize)

    ax.scatter(merged["daily_value"], merged["mean_zscore"], alpha=0.5, s=20)
    ax.set_xlabel("Mean daily rainfall (mm)")
    ax.set_ylabel(r"Mean $z$-Score")
    ax.grid(True, alpha=0.3)
    ax.set_title(f"Spearman Correlation: {corr:.3f}")

    plt.tight_layout()
    plt.savefig("plot/zscore_rain_correlation.png", dpi=300)
    # plt.show()


def _plot_flood_violin_generic(
    daily_data_df: pd.DataFrame,
    value_column: str,
    sample_flood_points: Any,
    ylabel: str,
    output_path: str,
    figsize: tuple = (10, 8),
) -> None:
    """
    Generic function to plot a violin plot comparing the distribution of a variable
    between days with flooding and days without flooding.

    Args:
        daily_data_df: DataFrame with columns 'data' (date) and a value column
        value_column: Name of the column containing the values to plot
        sample_flood_points: GeoDataFrame or DataFrame with column 'DATA' containing
            flood dates in format "dd/mm/yyyy"
        ylabel: Label for the y-axis
        output_path: Path to save the plot
        figsize: Figure size tuple (width, height)
    """
    df = daily_data_df.copy()
    df["data"] = pd.to_datetime(df["data"]).dt.date

    # Extract unique flood dates
    flood_dates = sample_flood_points["DATA"].dropna().unique()
    flood_dates_series = pd.Series(flood_dates)
    flood_dates_dt = pd.to_datetime(flood_dates_series, format="%d/%m/%Y").dt.date
    flood_dates_set = set(flood_dates_dt)

    # Classify each day
    df["classificacao"] = df["data"].apply(
        lambda x: "Days with flooding"
        if x in flood_dates_set
        else "Days without flooding"
    )

    # Prepare data for violin plot
    plot_df = df[[value_column, "classificacao"]].copy()

    # Create violin plot
    fig, ax = plt.subplots(figsize=figsize)

    sns.violinplot(
        data=plot_df,
        x="classificacao",
        y=value_column,
        ax=ax,
    )

    ax.set_xlabel("")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    # plt.show()


def plot_rain_flood_violin(
    sample_rain_df: pd.DataFrame,
    sample_flood_points: Any,
    figsize: tuple = (10, 8),
) -> None:
    """
    Plots a violin plot comparing the distribution of mean daily rainfall
    between days with flooding and days without flooding.

    Args:
        sample_rain_df: DataFrame with columns 'station', 'data' and 'daily_value'
        sample_flood_points: GeoDataFrame or DataFrame with column 'DATA' containing
            flood dates in format "dd/mm/yyyy"
        figsize: Figure size tuple (width, height)
    """
    # Calculate mean daily rain
    rain = sample_rain_df.copy()
    rain["data"] = pd.to_datetime(rain["data"])
    mean_rain = rain.groupby("data")["daily_value"].mean().reset_index()

    _plot_flood_violin_generic(
        daily_data_df=mean_rain,
        value_column="daily_value",
        sample_flood_points=sample_flood_points,
        ylabel="Mean daily rainfall (mm)",
        output_path="plot/rain_flood_violin.png",
        figsize=figsize,
    )


def plot_zscore_flood_violin(
    zscore_df: pd.DataFrame,
    sample_flood_points: Any,
    figsize: tuple = (10, 8),
) -> None:
    """
    Plots a violin plot comparing the distribution of mean z-score of river levels
    between days with flooding and days without flooding.

    Args:
        zscore_df: DataFrame with columns 'data' and 'mean_zscore'
        sample_flood_points: GeoDataFrame or DataFrame with column 'DATA' containing
            flood dates in format "dd/mm/yyyy"
        figsize: Figure size tuple (width, height)
    """
    df = zscore_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    _plot_flood_violin_generic(
        daily_data_df=df,
        value_column="mean_zscore",
        sample_flood_points=sample_flood_points,
        ylabel=r"Mean $z$-score",
        output_path="plot/zscore_flood_violin.png",
        figsize=figsize,
    )
