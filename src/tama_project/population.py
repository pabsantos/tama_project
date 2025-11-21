import geopandas as gpd
import os
from rich.console import Console

console = Console()


def load_census_tracts_intersected(
    od_zones_sample: gpd.GeoDataFrame,
    population_url: str = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/setores/gpkg/UF/SP/SP_setores_CD2022.gpkg",
    output_path: str = "data/census_tracts_intersected.gpkg",
) -> gpd.GeoDataFrame:
    """
    Load census tracts intersected with OD zones sample. If the output file already exists,
    loads it. Otherwise, downloads census tracts from URL, performs spatial intersection
    with OD zones sample, and saves the result.

    Args:
        od_zones_sample: GeoDataFrame with sample OD zones already loaded
        population_url: URL of the gpkg file with census tracts
        output_path: Path where the intersected census tracts will be saved/loaded

    Returns:
        GeoDataFrame with intersected census tracts containing only geometry and v0001 variable
    """
    if os.path.exists(output_path):
        console.print(
            f"Census tracts file already exists, loading from '{output_path}'"
        )
        result = gpd.read_file(output_path)
        return result

    console.print("Downloading census tracts from IBGE FTP server...")
    census_tracts = gpd.read_file(population_url)
    console.print(f"-> {census_tracts.shape[0]} census tracts loaded")

    # Ensure CRS compatibility
    if census_tracts.crs != od_zones_sample.crs:
        od_zones_sample = od_zones_sample.to_crs(census_tracts.crs)

    # Perform spatial intersection keeping only overlapping census tracts
    console.print("Performing spatial intersection with OD zones sample")
    intersected = gpd.sjoin(
        census_tracts,
        od_zones_sample,
        predicate="intersects",
    )

    # Remove duplicate columns from sjoin, keeping only original census tracts columns
    census_tracts_columns = census_tracts.columns.tolist()
    intersected = intersected[census_tracts_columns].copy()

    # Select only geometry and v0001 variable
    if "v0001" in intersected.columns:
        result = intersected[["geometry", "v0001"]].copy()
    else:
        # If v0001 doesn't exist, return only geometry and inform
        result = intersected[["geometry"]].copy()
        console.print("Warning: Variable 'v0001' was not found in the GeoDataFrame")

    console.print(f"Saving intersected census tracts to '{output_path}'")
    result.to_file(output_path, driver="GPKG")
    console.print(f"-> {result.shape[0]} census tracts saved")

    return result
