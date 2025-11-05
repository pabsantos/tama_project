import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.cm as cm
import contextily as ctx
import osmnx as ox
import networkx as nx


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


def plot_network_flood_map(
    G: nx.MultiDiGraph,
    sample_flood_points: gpd.GeoDataFrame,
    tti_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with the street network graph and flood points.

    Args:
        G: NetworkX MultiDiGraph of the street network
        sample_flood_points: GeoDataFrame with flood points
        tti_sample: GeoDataFrame with TTI basin polygons (for bounds)
        figsize: Figure size tuple (width, height)
    """
    fig, ax = plt.subplots(figsize=figsize)

    base_crs = tti_sample.crs

    if tti_sample.crs is None:
        raise ValueError("tti_sample must have a CRS defined")

    if sample_flood_points.crs != base_crs:
        sample_flood_points = sample_flood_points.to_crs(base_crs)

    tti_unified = tti_sample.dissolve()
    tti_unified_mercator = tti_unified.to_crs(epsg=3857)
    flood_points_mercator = sample_flood_points.to_crs(epsg=3857)

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

    G_edges = ox.graph_to_gdfs(G, nodes=False, edges=True)
    G_edges_mercator = G_edges.to_crs(epsg=3857)

    G_edges_mercator.plot(ax=ax, color="gray", linewidth=0.5, alpha=0.7, zorder=1)

    flood_points_mercator.plot(
        ax=ax,
        color="blue",
        markersize=35,
        marker="o",
        edgecolor="darkblue",
        linewidth=0.5,
        alpha=0.6,
        zorder=3,
    )

    network_patch = mpatches.Patch(
        facecolor="gray", edgecolor="none", alpha=0.7, label="Street network"
    )
    flood_patch = mpatches.Circle(
        (0, 0),
        1,
        facecolor="blue",
        edgecolor="darkblue",
        linewidth=0.5,
        alpha=0.6,
        label="Flood points",
    )
    handles = [network_patch, flood_patch]

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal")
    ax.legend(handles=handles, loc="upper right", framealpha=0.9)

    plt.tight_layout()
    plt.savefig("plot/network_flood_map.png", dpi=300, bbox_inches="tight")
    # plt.show()


def plot_network_ebc_map(
    G_gdf: gpd.GeoDataFrame,
    tti_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with the street network graph colored by Edge Betweenness
    Centrality (EBC) values.

    Args:
        G_gdf: GeoDataFrame with graph edges and 'ebc' column
        tti_sample: GeoDataFrame with TTI basin polygons (for bounds)
        figsize: Figure size tuple (width, height)
    """
    fig, ax = plt.subplots(figsize=figsize)

    base_crs = tti_sample.crs

    if tti_sample.crs is None:
        raise ValueError("tti_sample must have a CRS defined")

    if G_gdf.crs != base_crs:
        G_gdf = G_gdf.to_crs(base_crs)

    tti_unified = tti_sample.dissolve()
    tti_unified_mercator = tti_unified.to_crs(epsg=3857)
    G_gdf_mercator = G_gdf.to_crs(epsg=3857)

    bounds_mercator = tti_unified_mercator.total_bounds
    margin_x = (bounds_mercator[2] - bounds_mercator[0]) * 0.1
    margin_y = (bounds_mercator[3] - bounds_mercator[1]) * 0.1

    ax.set_xlim(bounds_mercator[0] - margin_x, bounds_mercator[2] + margin_x)
    ax.set_ylim(bounds_mercator[1] - margin_y, bounds_mercator[3] + margin_y)

    try:
        ctx.add_basemap(
            ax,
            crs=tti_unified_mercator.crs,
            source=ctx.providers.CartoDB.DarkMatter,
            attribution_size=6,
        )
    except Exception:
        pass

    if "ebc" not in G_gdf_mercator.columns:
        raise ValueError("G_gdf must contain an 'ebc' column")

    ebc_values = G_gdf_mercator["ebc"]
    vmin = ebc_values.min()
    vmax = ebc_values.max()

    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap("plasma")

    G_gdf_mercator.plot(
        ax=ax,
        column="ebc",
        cmap=cmap,
        linewidth=1.5,
        alpha=0.8,
        zorder=2,
        legend=False,
    )

    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, label="Edge Betweenness Centrality")
    cbar.ax.tick_params(labelsize=9)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal")

    plt.tight_layout()
    plt.savefig("plot/network_ebc_map.png", dpi=300, bbox_inches="tight")
    # plt.show()
