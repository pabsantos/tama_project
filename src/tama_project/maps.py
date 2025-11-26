import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.cm as cm
from matplotlib.lines import Line2D
import contextily as ctx
import osmnx as ox
import networkx as nx
from typing import Optional, Tuple


def _setup_map_base(
    tti_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> Tuple[plt.Figure, plt.Axes, gpd.GeoDataFrame]:
    """
    Sets up the base map with CRS validation and bounds calculation.

    Returns:
        Tuple of (fig, ax, tti_unified_mercator)
    """
    fig, ax = plt.subplots(figsize=figsize)

    base_crs = tti_sample.crs
    if base_crs is None:
        raise ValueError("tti_sample must have a CRS defined")

    tti_unified = tti_sample.dissolve()
    tti_unified_mercator = tti_unified.to_crs(epsg=3857)

    bounds_mercator = tti_unified_mercator.total_bounds
    margin_x = (bounds_mercator[2] - bounds_mercator[0]) * 0.1
    margin_y = (bounds_mercator[3] - bounds_mercator[1]) * 0.1

    ax.set_xlim(bounds_mercator[0] - margin_x, bounds_mercator[2] + margin_x)
    ax.set_ylim(bounds_mercator[1] - margin_y, bounds_mercator[3] + margin_y)

    return fig, ax, tti_unified_mercator


def _ensure_crs_match(
    gdf: gpd.GeoDataFrame, target_crs: gpd.GeoDataFrame.crs
) -> gpd.GeoDataFrame:
    """Ensures GeoDataFrame has the same CRS as target."""
    if gdf.crs != target_crs:
        return gdf.to_crs(target_crs)
    return gdf


def _add_basemap_to_ax(
    ax: plt.Axes,
    crs: gpd.GeoDataFrame.crs,
    source,
    attribution_size: int = 6,
) -> None:
    """Adds basemap to axes."""
    try:
        ctx.add_basemap(ax, crs=crs, source=source, attribution_size=attribution_size)
    except Exception:
        pass


def _finish_map(
    ax: plt.Axes,
    output_path: str,
    handles: Optional[list] = None,
    legend_loc: str = "upper right",
    dpi: int = 300,
) -> None:
    """Finalizes map with labels, legend, and saves."""
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal")

    if handles:
        ax.legend(handles=handles, loc=legend_loc, framealpha=0.9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=dpi, bbox_inches="tight")
    # plt.show()


def plot_study_area_map(
    tti_sample: gpd.GeoDataFrame,
    od_zones_sample: gpd.GeoDataFrame,
    # pcd_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with the study area showing:
    - Unified TTI basin area (tti_sample dissolved)
    - Unified OD zones area (od_zones_sample dissolved)
    - CartoDB Positron basemap

    Args:
        tti_sample: GeoDataFrame with TTI basin polygons
        od_zones_sample: GeoDataFrame with OD zone polygons
        pcd_sample: GeoDataFrame with PCD points
        figsize: Figure size tuple (width, height)
    """
    fig, ax, tti_unified_mercator = _setup_map_base(tti_sample, figsize)

    base_crs = tti_sample.crs
    od_zones_sample = _ensure_crs_match(od_zones_sample, base_crs)
    # pcd_sample = _ensure_crs_match(pcd_sample, base_crs)

    od_zones_sample_valid = od_zones_sample.copy()
    od_zones_sample_valid["geometry"] = od_zones_sample_valid["geometry"].make_valid()
    od_zones_unified = od_zones_sample_valid.dissolve()

    od_zones_unified_mercator = od_zones_unified.to_crs(epsg=3857)
    # pcd_sample_mercator = pcd_sample.to_crs(epsg=3857)

    _add_basemap_to_ax(ax, tti_unified_mercator.crs, ctx.providers.CartoDB.Positron)

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

    # pcd_sample_mercator.plot(
    #     ax=ax,
    #     color="red",
    #     markersize=30,
    #     marker="o",
    #     edgecolor="darkred",
    #     linewidth=0.5,
    #     zorder=3,
    # )

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
    # pcd_patch = mpatches.Circle(
    #     (0, 0),
    #     1,
    #     facecolor="red",
    #     edgecolor="darkred",
    #     linewidth=0.5,
    #     label="Stations",
    # )

    _finish_map(
        ax,
        "plot/study_area_map.png",
        handles=[od_zones_patch, tti_patch],
    )


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
    fig, ax, tti_unified_mercator = _setup_map_base(tti_sample, figsize)

    base_crs = tti_sample.crs
    sample_flood_points = _ensure_crs_match(sample_flood_points, base_crs)

    flood_points_mercator = sample_flood_points.to_crs(epsg=3857)

    _add_basemap_to_ax(ax, tti_unified_mercator.crs, ctx.providers.CartoDB.Positron)

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

    _finish_map(ax, "plot/network_flood_map.png", handles=handles)


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
    fig, ax, tti_unified_mercator = _setup_map_base(tti_sample, figsize)

    base_crs = tti_sample.crs
    G_gdf = _ensure_crs_match(G_gdf, base_crs)
    G_gdf_mercator = G_gdf.to_crs(epsg=3857)

    _add_basemap_to_ax(ax, tti_unified_mercator.crs, ctx.providers.CartoDB.DarkMatter)

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
        linewidth=0.9,
        alpha=0.8,
        zorder=2,
        legend=False,
    )

    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, label="Edge Betweenness Centrality")
    cbar.ax.tick_params(labelsize=9)

    _finish_map(ax, "plot/network_ebc_map.png")


def plot_network_ebc_flood_highlight_map(
    G_gdf: gpd.GeoDataFrame,
    tti_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with the street network graph highlighting links with both
    EBC and flood_count above their respective medians.

    Args:
        G_gdf: GeoDataFrame with graph edges, 'ebc' and 'flood_count' columns
        tti_sample: GeoDataFrame with TTI basin polygons (for bounds)
        figsize: Figure size tuple (width, height)
    """
    fig, ax, tti_unified_mercator = _setup_map_base(tti_sample, figsize)

    base_crs = tti_sample.crs
    G_gdf = _ensure_crs_match(G_gdf, base_crs)
    G_gdf_mercator = G_gdf.to_crs(epsg=3857)

    if "ebc" not in G_gdf_mercator.columns:
        raise ValueError("G_gdf must contain an 'ebc' column")
    if "flood_count" not in G_gdf_mercator.columns:
        raise ValueError("G_gdf must contain a 'flood_count' column")

    _add_basemap_to_ax(ax, tti_unified_mercator.crs, ctx.providers.CartoDB.DarkMatter)

    ebc_median = G_gdf_mercator["ebc"].median()
    flood_count_median = G_gdf_mercator["flood_count"].median()

    highlighted_mask = (G_gdf_mercator["ebc"] > ebc_median) & (
        G_gdf_mercator["flood_count"] > flood_count_median
    )

    G_gdf_normal = G_gdf_mercator[~highlighted_mask]
    G_gdf_highlighted = G_gdf_mercator[highlighted_mask]

    G_gdf_normal.plot(
        ax=ax,
        color="gray",
        linewidth=0.5,
        alpha=0.5,
        zorder=1,
        label="Other links",
    )

    G_gdf_highlighted.plot(
        ax=ax,
        color="aqua",
        linewidth=2.0,
        alpha=0.9,
        zorder=2,
        label="High EBC & High Flood Count",
    )

    normal_patch = mpatches.Patch(
        facecolor="gray",
        edgecolor="none",
        alpha=0.5,
        label="Other links",
    )
    highlighted_patch = mpatches.Patch(
        facecolor="aqua",
        edgecolor="none",
        alpha=0.9,
        label="High EBC & High Flood Count",
    )

    _finish_map(
        ax,
        "plot/network_ebc_flood_highlight.png",
        handles=[normal_patch, highlighted_patch],
    )


def plot_population_map(
    census_tracts: gpd.GeoDataFrame,
    tti_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with census tracts colored by population values.

    Args:
        census_tracts: GeoDataFrame with census tracts and 'v0001' column (population)
        tti_sample: GeoDataFrame with TTI basin polygons (for bounds)
        figsize: Figure size tuple (width, height)
    """
    fig, ax, tti_unified_mercator = _setup_map_base(tti_sample, figsize)

    base_crs = tti_sample.crs
    census_tracts = _ensure_crs_match(census_tracts, base_crs)
    census_tracts_mercator = census_tracts.to_crs(epsg=3857)

    if "v0001" not in census_tracts_mercator.columns:
        raise ValueError("census_tracts must contain a 'v0001' column")

    _add_basemap_to_ax(ax, tti_unified_mercator.crs, ctx.providers.CartoDB.Positron)

    population_values = census_tracts_mercator["v0001"]
    vmin = population_values.min()
    vmax = population_values.max()

    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap("plasma_r")

    census_tracts_mercator.plot(
        ax=ax,
        column="v0001",
        cmap=cmap,
        edgecolor="white",
        linewidth=0.1,
        alpha=0.7,
        zorder=2,
        legend=False,
    )

    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, label="Population")
    cbar.ax.tick_params(labelsize=9)

    _finish_map(ax, "plot/population_map.png")


def plot_nodes_population_map(
    nodes_with_population: gpd.GeoDataFrame,
    tti_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with graph nodes colored by population values.

    Args:
        nodes_with_population: GeoDataFrame with graph nodes and 'population' column
        tti_sample: GeoDataFrame with TTI basin polygons (for bounds)
        figsize: Figure size tuple (width, height)
    """
    fig, ax, tti_unified_mercator = _setup_map_base(tti_sample, figsize)

    base_crs = tti_sample.crs
    nodes_with_population = _ensure_crs_match(nodes_with_population, base_crs)
    nodes_mercator = nodes_with_population.to_crs(epsg=3857)

    if "population" not in nodes_mercator.columns:
        raise ValueError("nodes_with_population must contain a 'population' column")

    _add_basemap_to_ax(ax, tti_unified_mercator.crs, ctx.providers.CartoDB.Positron)

    population_values = nodes_mercator["population"]
    vmin = population_values.min()
    vmax = population_values.max()

    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap("plasma_r")

    nodes_mercator.plot(
        ax=ax,
        column="population",
        cmap=cmap,
        markersize=3,
        alpha=0.8,
        zorder=2,
        legend=False,
    )

    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, label="Population")
    cbar.ax.tick_params(labelsize=9)

    _finish_map(ax, "plot/nodes_population_map.png")


def plot_edges_population_map(
    G_gdf: gpd.GeoDataFrame,
    tti_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with the street network edges colored by population values.

    Args:
        G_gdf: GeoDataFrame with graph edges and 'population' column
        tti_sample: GeoDataFrame with TTI basin polygons (for bounds)
        figsize: Figure size tuple (width, height)
    """
    fig, ax, tti_unified_mercator = _setup_map_base(tti_sample, figsize)

    base_crs = tti_sample.crs
    G_gdf = _ensure_crs_match(G_gdf, base_crs)
    G_gdf_mercator = G_gdf.to_crs(epsg=3857)

    _add_basemap_to_ax(ax, tti_unified_mercator.crs, ctx.providers.CartoDB.Positron)

    if "population" not in G_gdf_mercator.columns:
        raise ValueError("G_gdf must contain a 'population' column")

    population_values = G_gdf_mercator["population"]
    vmin = population_values.min()
    vmax = population_values.max()

    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap("plasma_r")

    G_gdf_mercator.plot(
        ax=ax,
        column="population",
        cmap=cmap,
        linewidth=0.9,
        alpha=0.8,
        zorder=2,
        legend=False,
    )

    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, label="Population")
    cbar.ax.tick_params(labelsize=9)

    _finish_map(ax, "plot/edges_population_map.png")


def plot_ebc_critical_ranges_map(
    G_gdf: gpd.GeoDataFrame,
    tti_sample: gpd.GeoDataFrame,
    figsize: tuple = (12, 10),
) -> None:
    """
    Plots a map with street network edges colored by normalized flood count
    calculated per EBC bin (0.01 width). The normalized count is the sum of
    flood_count divided by the number of edges in each EBC bin.

    Args:
        G_gdf: GeoDataFrame with graph edges, 'ebc' and 'flood_count' columns
        tti_sample: GeoDataFrame with TTI basin polygons (for bounds)
        figsize: Figure size tuple (width, height)
    """
    fig, ax, tti_unified_mercator = _setup_map_base(tti_sample, figsize)

    base_crs = tti_sample.crs
    G_gdf = _ensure_crs_match(G_gdf, base_crs)
    G_gdf_mercator = G_gdf.to_crs(epsg=3857)

    if "ebc" not in G_gdf_mercator.columns:
        raise ValueError("G_gdf must contain an 'ebc' column")
    if "flood_count" not in G_gdf_mercator.columns:
        raise ValueError("G_gdf must contain a 'flood_count' column")

    _add_basemap_to_ax(ax, tti_unified_mercator.crs, ctx.providers.CartoDB.DarkMatter)

    df = G_gdf_mercator[["ebc", "flood_count"]].copy()

    ebc_min = df["ebc"].min()
    ebc_max = df["ebc"].max()

    bin_width = 0.01
    bin_edges = pd.interval_range(
        start=ebc_min,
        end=ebc_max + bin_width,
        freq=bin_width,
        closed="left",
    )
    bins = pd.cut(df["ebc"], bins=bin_edges, include_lowest=True)

    df["ebc_bin"] = bins

    normalized = (
        df.groupby("ebc_bin", observed=True)
        .agg(
            {
                "flood_count": lambda x: x.sum() / len(x) if len(x) > 0 else 0,
            }
        )
        .reset_index()
    )

    bin_to_normalized = dict(zip(normalized["ebc_bin"], normalized["flood_count"]))

    G_gdf_mercator["normalized_flood_count"] = (
        df["ebc_bin"].map(bin_to_normalized).fillna(0)
    )

    ebc_values = G_gdf_mercator["ebc"]
    ebc_min_val = ebc_values.min()
    ebc_max_val = ebc_values.max()

    linewidth_min = 0.3
    linewidth_max = 3.0
    if ebc_max_val > ebc_min_val:
        G_gdf_mercator["linewidth"] = (ebc_values - ebc_min_val) / (
            ebc_max_val - ebc_min_val
        ) * (linewidth_max - linewidth_min) + linewidth_min
    else:
        G_gdf_mercator["linewidth"] = linewidth_min

    normalized_values = G_gdf_mercator["normalized_flood_count"]
    vmin = normalized_values.min()
    vmax = normalized_values.max()

    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap("plasma")

    for idx, row in G_gdf_mercator.iterrows():
        geom = row.geometry
        if geom.geom_type == "LineString":
            coords = list(geom.coords)
            if len(coords) >= 2:
                x_coords = [coord[0] for coord in coords]
                y_coords = [coord[1] for coord in coords]
                color = cmap(norm(row["normalized_flood_count"]))
                ax.plot(
                    x_coords,
                    y_coords,
                    color=color,
                    linewidth=row["linewidth"],
                    alpha=0.8,
                    zorder=2,
                )
        elif geom.geom_type == "MultiLineString":
            for line in geom.geoms:
                coords = list(line.coords)
                if len(coords) >= 2:
                    x_coords = [coord[0] for coord in coords]
                    y_coords = [coord[1] for coord in coords]
                    color = cmap(norm(row["normalized_flood_count"]))
                    ax.plot(
                        x_coords,
                        y_coords,
                        color=color,
                        linewidth=row["linewidth"],
                        alpha=0.8,
                        zorder=2,
                    )

    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, label="Normalized Flood Count")
    cbar.ax.tick_params(labelsize=9)

    legend_ebc_values = [
        ebc_min_val,
        ebc_min_val + (ebc_max_val - ebc_min_val) * 0.33,
        ebc_min_val + (ebc_max_val - ebc_min_val) * 0.67,
        ebc_max_val,
    ]
    legend_linewidths = [
        (ebc_val - ebc_min_val)
        / (ebc_max_val - ebc_min_val)
        * (linewidth_max - linewidth_min)
        + linewidth_min
        if ebc_max_val > ebc_min_val
        else linewidth_min
        for ebc_val in legend_ebc_values
    ]

    legend_elements = [
        Line2D(
            [0],
            [0],
            color="gray",
            linewidth=lw,
            label=f"EBC = {ebc_val:.3f}",
        )
        for ebc_val, lw in zip(legend_ebc_values, legend_linewidths)
    ]

    ax.legend(
        handles=legend_elements,
        loc="lower right",
        title="Edge Betweenness Centrality",
        framealpha=0.9,
        fontsize=9,
    )

    _finish_map(ax, "plot/ebc_critical_ranges_map.png")
