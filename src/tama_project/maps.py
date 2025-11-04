import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import contextily as ctx


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
    - CartoDB Positron basemap

    Args:
        tti_sample: GeoDataFrame with TTI basin polygons
        od_zones_sample: GeoDataFrame with OD zone polygons
        pcd_sample: GeoDataFrame with PCD points
        figsize: Figure size tuple (width, height)
    """
    fig, ax = plt.subplots(figsize=figsize)

    base_crs = tti_sample.crs

    if tti_sample.crs is None:
        raise ValueError("tti_sample must have a CRS defined")

    if od_zones_sample.crs != base_crs:
        od_zones_sample = od_zones_sample.to_crs(base_crs)

    if pcd_sample.crs != base_crs:
        pcd_sample = pcd_sample.to_crs(base_crs)

    tti_unified = tti_sample.dissolve()
    od_zones_sample_valid = od_zones_sample.copy()
    od_zones_sample_valid["geometry"] = od_zones_sample_valid["geometry"].make_valid()
    od_zones_unified = od_zones_sample_valid.dissolve()

    tti_unified_mercator = tti_unified.to_crs(epsg=3857)
    od_zones_unified_mercator = od_zones_unified.to_crs(epsg=3857)
    pcd_sample_mercator = pcd_sample.to_crs(epsg=3857)

    bounds_mercator = tti_unified_mercator.total_bounds
    margin_x = (bounds_mercator[2] - bounds_mercator[0]) * 0.1
    margin_y = (bounds_mercator[3] - bounds_mercator[1]) * 0.1

    ax.set_xlim(bounds_mercator[0] - margin_x, bounds_mercator[2] + margin_x)
    ax.set_ylim(bounds_mercator[1] - margin_y, bounds_mercator[3] + margin_y)

    try:
        ctx.add_basemap(
            ax,
            crs=tti_unified_mercator.crs,
            source=ctx.providers.CartoDB.Positron,
            attribution_size=6,
        )
    except Exception:
        pass

    od_zones_unified_mercator.plot(
        ax=ax,
        color="lightgreen",
        edgecolor="green",
        alpha=0.5,
        zorder=1,
    )

    tti_unified_mercator.plot(
        ax=ax,
        facecolor="lightblue",
        edgecolor="blue",
        alpha=0.5,
        zorder=2,
    )

    pcd_sample_mercator.plot(
        ax=ax,
        color="red",
        markersize=30,
        marker="o",
        edgecolor="darkred",
        linewidth=0.5,
        zorder=3,
    )

    od_zones_patch = mpatches.Patch(
        facecolor="lightgreen",
        edgecolor="green",
        alpha=0.5,
        label="OD zones",
    )
    tti_patch = mpatches.Patch(
        facecolor="lightblue",
        edgecolor="blue",
        alpha=0.5,
        label="TTI microbasin",
    )
    pcd_patch = mpatches.Circle(
        (0, 0),
        1,
        facecolor="red",
        edgecolor="darkred",
        linewidth=0.5,
        label="Stations",
    )

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal")
    ax.legend(
        handles=[od_zones_patch, tti_patch, pcd_patch],
        loc="upper right",
        framealpha=0.9,
    )

    plt.tight_layout()
    plt.savefig("plot/study_area_map.png", dpi=300, bbox_inches="tight")
    # plt.show()
