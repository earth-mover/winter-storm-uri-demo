# Winter Storm URI Demo

This project analyzes Texas energy systems during the February 2021 winter storm using ERA5 meteorological data and energy infrastructure data. It demonstrates how extreme weather events impact renewable energy production and heating demand across Texas.

## Features

- **Heating Demand Analysis**: Calculate heating degree days for major Texas cities during the winter storm
- **Renewable Energy Production**: Model wind and solar power generation at actual facility locations
- **Climatology Comparison**: Compare 2021 winter storm conditions to 30-year climatology (1990-2021)
- **Interactive Visualization**: Explore data through marimo notebooks with maps and time series plots

## Quick Start

Since the notebooks depend on local modules (`energy.py` and `plot.py`), you'll need to clone the repository:

```bash
git clone <repository-url>
cd winter-storm-uri-demo
```

Launch the interactive notebooks using uvx:

```bash
# Heating demand analysis
uvx --from . marimo run demand.py

# Renewable energy production analysis  
uvx --from . marimo run production.py
```

Or install locally and run:

```bash
uv sync
marimo run demand.py
marimo run production.py
```

## Data Sources

- **ERA5 Surface Data**: Accessed via Arraylake from "earthmover-public/era5-surface-aws"
- **EIA Generator Data**: Texas power plant inventory from `data/january_generator2021.xlsx`

## Key Technologies

- **arraylake**: Earthmover's data platform for cataloging and governing access to scientific data
- **icechunk**: Cloud-native transactional backend for Zarr
- **xarray**: Multi-dimensional array processing

## License

Apache 2.0
