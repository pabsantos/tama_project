import geopandas as gpd
import matplotlib.pyplot as plt


def plot_study_area_map(
    tti_sample: gpd.GeoDataFrame,
    od_zones_sample: gpd.GeoDataFrame,
    pcd_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with the study area showing:
    - Unified TTI basin area (tti_sample dissolved)
    - Unified OD zones area (od_zones_sample dissolved)
    - PCD points (pcd_sample)

    Args:
        tti_sample: GeoDataFrame with TTI basin polygons
        od_zones_sample: GeoDataFrame with OD zone polygons
        pcd_sample: GeoDataFrame with PCD points
        figsize: Figure size tuple (width, height)
    """
    fig, ax = plt.subplots(figsize=figsize)

    # if tti_sample.crs != od_zones_sample.crs:
    #    tti_sample.to_crs(od_zones_sample.crs)

    if pcd_sample.crs != tti_sample.crs:
        pcd_sample = pcd_sample.to_crs(tti_sample.crs)

    tti_unified = tti_sample.dissolve()
    # od_zones_unified = gpd.GeoDataFrame(geometry=tti_sample.make_valid()).dissolve()

    # od_zones_unified.plot(
    #    ax=ax, color="lightgreen", edgecolor="green", alpha=0.5, label="OD Zones"
    # )
    pcd_sample.plot(
        ax=ax,
        color="red",
        markersize=30,
        label="PCD",
        marker="o",
    )

    tti_unified.plot(
        ax=ax, color="lightblue", edgecolor="blue", alpha=0.5, label="TTI Basin"
    )

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    # ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("plot/study_area_map.png", dpi=300)
    # plt.show()
