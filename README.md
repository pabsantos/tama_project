# TAMA Project

Vulnerability analysis of urban road networks to floods - A case study in São Paulo, Brazil.

This project analyzes the vulnerability of urban road networks based on topology and flood occurrence, exploring the relationship between topological properties, flood events, and population distribution.

## Requirements

- **Python**: >= 3.12
- **Typst**: Required for compiling reports and slides (see installation instructions below)

## Installation

### 1. Install Dependencies

First, ensure you have Python 3.12 or higher installed. Then install the project dependencies.

If you're using `uv` (recommended):

```bash
uv sync
```

Alternatively, if you're using `pip`:

```bash
pip install -e .
```

### 2. Install Typst

**Important**: You must install Typst before running the project, as it is required to compile the report and slides.

Typst can be installed using one of the following methods:

#### Option A: Using Homebrew (macOS/Linux)

```bash
brew install typst
```

#### Option B: Using Cargo (Rust)

```bash
cargo install --git https://github.com/typst/typst --locked typst-cli
```

#### Option C: Download Binary

Visit [https://typst.app/](https://typst.app/) and download the appropriate binary for your system.

#### Verify Installation

After installing, verify that Typst is available:

```bash
typst --version
```

## Project Dependencies

The project uses the following Python packages (automatically installed with the package):

- `contextily` - Basemap tiles for geospatial visualizations
- `folium` - Interactive maps
- `geopandas` - Geospatial data operations
- `jupyter` - Jupyter notebook support
- `mapclassify` - Map classification schemes
- `matplotlib` - Plotting and visualization
- `networkx` - Network analysis
- `nx-parallel` - Parallel network computations
- `osmnx` - OpenStreetMap network extraction
- `pandas` - Data manipulation
- `pyarrow` - Apache Arrow support
- `python-dotenv` - Environment variable management
- `requests` - HTTP library
- `rich` - Rich text and beautiful formatting
- `seaborn` - Statistical data visualization

## Execution

### Running the Main Analysis

After installing the package and Typst, you can run the complete analysis pipeline using:

```bash
tama-project
```

Alternatively, you can run it directly with Python:

```bash
python -m tama_project.main
```

### What the Script Does

The main script performs the following operations:

1. **Loads geospatial data**: Flood points, OD zones, and TTI basin shapes
2. **Filters study sample**: Selects relevant microbasins and zones
3. **Processes street network**: Loads and analyzes the road network graph
4. **Calculates network metrics**: Computes degree distribution, edge betweenness centrality (EBC), and other topological properties
5. **Processes population data**: Loads census tracts and assigns population to network nodes and edges
6. **Generates visualizations**: Creates plots and maps saved to the `plot/` directory
7. **Compiles documents**: Generates PDF reports and slides from Typst source files

### Output Files

After execution, the following outputs are generated:

- **Plots**: Saved in the `plot/` directory (PNG format)
- **Report**: `report.pdf` (compiled from `report.typ`)
- **Slides**: `slides.pdf` (compiled from `slides.typ`)
- **Intermediate data**: Various GeoPackage files in the `data/` directory

## Project Structure

```
tama_project/
├── data/                    # Geospatial data files
│   ├── flood_cge/          # Flood point shapefiles
│   ├── od_zones/           # Origin-destination zones
│   ├── tti_shape/          # TTI basin shapes
│   └── network.graphml     # Road network graph
├── plot/                   # Generated visualizations
├── src/tama_project/       # Source code
│   ├── main.py            # Main entry point
│   ├── analysis.py        # Analysis functions
│   ├── maps.py            # Map generation
│   ├── network.py         # Network analysis
│   ├── population.py      # Population data processing
│   └── render.py          # Typst compilation
├── report.typ             # Typst report source
├── slides.typ             # Typst slides source
└── pyproject.toml         # Project configuration
```

## Notes

- The analysis requires geospatial data files in the `data/` directory. Ensure all required shapefiles and network files are present before running.
- Network computations use parallel processing (10 jobs by default) for improved performance.
- The script uses `rich` for console output, providing colored and formatted progress information.

